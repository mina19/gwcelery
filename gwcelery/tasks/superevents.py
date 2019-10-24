"""Module containing the functionality for creation and management of
superevents.

*   There is serial processing of triggers from low latency pipelines.
*   Dedicated **superevent** queue for this purpose.
*   Primary logic to respond to low latency triggers contained in
    :meth:`process` function.
"""
from celery.utils.log import get_task_logger
from ligo.segments import segment, segmentlist

from ..import app
from . import gracedb, lvalert

log = get_task_logger(__name__)

required_labels_by_group = {
    'cbc': {'PASTRO_READY', 'EMBRIGHT_READY', 'SKYMAP_READY'},
    'burst': {'SKYMAP_READY'}
}
"""These labels should be present on an event to consider it to
be complete.
"""


@lvalert.handler('cbc_gstlal',
                 'cbc_spiir',
                 'cbc_pycbc',
                 'cbc_mbtaonline',
                 'burst_olib',
                 'burst_cwb',
                 shared=False)
def handle(payload):
    """Respond to lvalert nodes from low-latency search
    pipelines and delegate to :meth:`process` for
    superevent management.
    """
    alert_type = payload['alert_type']
    if alert_type not in ['new', 'label_added']:
        return

    gid = payload['object']['graceid']

    try:
        far = payload['object']['far']
    except KeyError:
        log.info(
            'Skipping %s because LVAlert message does not provide FAR', gid)
        return
    else:
        if far > app.conf['superevent_far_threshold']:
            log.info("Skipping processing of %s because of high FAR", gid)
            return
    group = payload['object']['group'].lower()
    required_labels = required_labels_by_group[group]
    if alert_type == 'new' or (alert_type == 'label_added' and
                               payload['data']['name'] in required_labels):
        process.delay(payload)
    elif alert_type == 'label_added' and payload['data']['name'] == 'EM_COINC':
        log.info('Label %s added to %s' % (payload['data']['name'], gid))
        # raven preliminary
        raise NotImplementedError


@gracedb.task(queue='superevent', shared=False)
@gracedb.catch_retryable_http_errors
def process(payload):
    """
    Respond to `payload` and serially processes them
    to create new superevents, add events to existing ones

    Parameters
    ----------
    payload : dict
        LVAlert payload
    """
    event_info = payload['object']
    gid = event_info['graceid']

    if event_info.get('search') == 'MDC':
        category = 'mdc'
    elif event_info['group'] == 'Test':
        category = 'test'
    else:
        category = 'production'

    if event_info.get('superevent'):
        # superevent already exists; label_added LVAlert
        s = gracedb.get_superevent(event_info['superevent'])
        superevent = _SuperEvent(s['t_start'],
                                 s['t_end'],
                                 s['t_0'],
                                 s['superevent_id'],
                                 s['preferred_event'], s)
        # no change in superevent times for label_added LVAlert
        _update_superevent(superevent,
                           event_info,
                           t_0=None,
                           t_start=None,
                           t_end=None)
        # check for publishability
        if should_publish(event_info):
            gracedb.create_label.delay('ADVREQ', event_info['superevent'])
            gracedb.create_label('EM_Selected', event_info['superevent'])
        return

    superevents = gracedb.get_superevents('category: {} {} .. {}'.format(
        category,
        event_info['gpstime'] - app.conf['superevent_query_d_t_start'],
        event_info['gpstime'] + app.conf['superevent_query_d_t_end']))

    for superevent in superevents:
        if gid in superevent['gw_events']:
            sid = superevent['superevent_id']
            break  # Found matching superevent
    else:
        sid = None  # No matching superevent

    t_0, t_start, t_end = get_ts(event_info)

    if sid is None:
        event_segment = _Event(event_info['gpstime'],
                               t_start, t_end,
                               event_info['graceid'],
                               event_info['group'],
                               event_info['pipeline'],
                               event_info.get('search'),
                               event_dict=event_info)

        superevent = _partially_intersects(superevents, event_segment)

        if not superevent:
            log.info('New event %s with no superevent in GraceDB, '
                     'creating new superevent', gid)
            gracedb.create_superevent(event_info['graceid'],
                                      t_0, t_start, t_end, category)
            if should_publish(event_info):
                new_superevent_id = gracedb.get_event(
                    event_info['graceid'])['superevent']
                gracedb.create_label.delay('ADVREQ', new_superevent_id)
                gracedb.create_label('EM_Selected', new_superevent_id)
            return

        log.info('Event %s in window of %s. Adding event to superevent',
                 gid, superevent.superevent_id)
        gracedb.add_event_to_superevent(superevent.superevent_id,
                                        event_segment.gid)
        # extend the time window of the superevent
        new_superevent = superevent | event_segment
        if new_superevent != superevent:
            log.info("%s not completely contained in %s, "
                     "extending superevent window",
                     event_segment.gid, superevent.superevent_id)
            new_t_start, new_t_end = new_superevent[0], new_superevent[1]

        else:
            log.info("%s is completely contained in %s",
                     event_segment.gid, superevent.superevent_id)
            new_t_start = new_t_end = None
        _update_superevent(superevent,
                           event_info,
                           t_0=t_0,
                           t_start=new_t_start,
                           t_end=new_t_end)
        if should_publish(event_info):
            gracedb.create_label.delay('ADVREQ', superevent.superevent_id)
            gracedb.create_label('EM_Selected', superevent.superevent_id)
    else:
        log.critical('Superevent %s exists for alert_type new for %s',
                     sid, gid)


def get_ts(event):
    """Get time extent of an event, depending on pipeline-specific parameters.

    *   For CWB, use the event's ``duration`` field.
    *   For oLIB, use the ratio of the event's ``quality_mean`` and
        ``frequency_mean`` fields.
    *   For all other pipelines, use the
        :obj:`~gwcelery.conf.superevent_d_t_start` and
        :obj:`~gwcelery.conf.superevent_d_t_start` configuration values.

    Parameters
    ----------
    event : dict
        Event dictionary (e.g., the return value from
        :meth:`gwcelery.tasks.gracedb.get_event`).

    Returns
    -------
    t_0: float
        Segment center time in GPS seconds.
    t_start : float
        Segment start time in GPS seconds.

    t_end : float
        Segment end time in GPS seconds.
    """
    pipeline = event['pipeline'].lower()
    if pipeline == 'cwb':
        attribs = event['extra_attributes']['MultiBurst']
        d_t_start = d_t_end = attribs['duration']
    elif pipeline == 'olib':
        attribs = event['extra_attributes']['LalInferenceBurst']
        d_t_start = d_t_end = (attribs['quality_mean'] /
                               attribs['frequency_mean'])
    else:
        d_t_start = app.conf['superevent_d_t_start'].get(
            pipeline, app.conf['superevent_default_d_t_start'])
        d_t_end = app.conf['superevent_d_t_end'].get(
            pipeline, app.conf['superevent_default_d_t_end'])
    return (event['gpstime'], event['gpstime'] - d_t_start,
            event['gpstime'] + d_t_end)


def get_snr(event):
    """Get the SNR from the LVAlert packet.

    Different groups and pipelines store the SNR in different fields.

    Parameters
    ----------
    event : dict
        Event dictionary (e.g., the return value from
        :meth:`gwcelery.tasks.gracedb.get_event`).

    Returns
    -------
    snr : float
        The SNR.
    """
    group = event['group'].lower()
    pipeline = event['pipeline'].lower()
    if group == 'cbc':
        attribs = event['extra_attributes']['CoincInspiral']
        return attribs['snr']
    elif pipeline == 'cwb':
        attribs = event['extra_attributes']['MultiBurst']
        return attribs['snr']
    elif pipeline == 'olib':
        attribs = event['extra_attributes']['LalInferenceBurst']
        return attribs['omicron_snr_network']
    else:
        raise NotImplementedError('SNR attribute not found')


def get_instruments(event):
    """Get the instruments that contributed data to an event.

    Parameters
    ----------
    event : dict
        Event dictionary (e.g., the return value from
        :meth:`gwcelery.tasks.gracedb.get_event`).

    Returns
    -------
    set
        The set of instruments that contributed to the event.

    """
    attribs = event['extra_attributes']['SingleInspiral']
    ifos = {single['ifo'] for single in attribs}
    return ifos


def get_instruments_in_ranking_statistic(event):
    """Get the instruments that contribute to the false alarm rate.

    Parameters
    ----------
    event : dict
        Event dictionary (e.g., the return value from
        :meth:`gwcelery.tasks.gracedb.get_event`).

    Returns
    -------
    set
        The set of instruments that contributed to the ranking statistic for
        the event.

    Notes
    -----
    The number of instruments that contributed *data* to an event is given by
    the ``instruments`` key of the GraceDB event JSON structure. However, some
    pipelines (e.g. gstlal) have a distinction between which instruments
    contributed *data* and which were considered in the *ranking* of the
    candidate. For such pipelines, we infer which pipelines contributed to the
    ranking by counting only the SingleInspiral records for which the chi
    squared field is non-empty.
    """
    try:
        attribs = event['extra_attributes']['SingleInspiral']
        ifos = {single['ifo'] for single in attribs
                if single.get('chisq') is not None}
    except KeyError:
        ifos = set(event['instruments'].split(','))
    return ifos


@app.task(shared=False)
def select_preferred_event(events):
    """Select the preferred event out of a list of events,
    typically contents of a superevent, based on :meth:`keyfunc`.

    Parameters
    ----------
    events : list
        list of event dictionaries
    """
    return min(events, key=keyfunc)


def is_complete(event):
    """
    Determine if a G event is complete in the sense of the event
    has its data products complete i.e. has PASTRO_READY, SKYMAP_READY,
    EMBRIGHT_READY for CBC events and the SKYMAP_READY label for the
    Burst events. Test events are not processed by low-latency infrastructure
    and are always labeled complete.

    Parameters
    ----------
    event : dict
        Event dictionary (e.g., the return value from
        :meth:`gwcelery.tasks.gracedb.get_event`).
    """
    group = event['group'].lower()
    label_set = set(event['labels'])
    required_labels = required_labels_by_group[group]
    return required_labels.issubset(label_set)


def should_publish(event):
    """Determine whether an event should be published as a public alert.

    All of the following conditions must be true for a public alert:

    *   The event's ``offline`` flag is not set.
    *   The event should be complete based on :meth:`is_complete`.
    *   The event's false alarm rate, weighted by the group-specific trials
        factor as specified by the
        :obj:`~gwcelery.conf.preliminary_alert_trials_factor` configuration
        setting, is less than or equal to
        :obj:`~gwcelery.conf.preliminary_alert_far_threshold`.

    Parameters
    ----------
    event : dict
        Event dictionary (e.g., the return value from
        :meth:`gwcelery.tasks.gracedb.get_event`).

    Returns
    -------
    should_publish : bool
        :obj:`True` if the event meets the criteria for a public alert or
        :obj:`False` if it does not.
    """
    return all(_should_publish(event))


def _should_publish(event):
    """Wrapper around :meth:`should_publish`. Returns the boolean returns
    of the publishability criteria as a tuple for later use.
    """
    group = event['group'].lower()
    trials_factor = app.conf['preliminary_alert_trials_factor'][group]
    far_threshold = app.conf['preliminary_alert_far_threshold'][group]
    far = trials_factor * event['far']
    return not event['offline'], far <= far_threshold, is_complete(event)


def keyfunc(event):
    """Key function for selection of the preferred event.

    Return a value suitable for identifying the preferred event. Given events
    ``a`` and ``b``, ``a`` is preferred over ``b`` if
    ``keyfunc(a) < keyfunc(b)``, else ``b`` is preferred.

    Parameters
    ----------
    event : dict
        Event dictionary (e.g., the return value from
        :meth:`gwcelery.tasks.gracedb.get_event`).

    Returns
    -------
    key : tuple
        The comparison key.

    Notes
    -----
    Tuples are compared lexicographically in Python: they are compared
    element-wise until an unequal pair of elements is found.
    """
    group = event['group'].lower()
    try:
        group_rank = ['cbc', 'burst'].index(group)
    except ValueError:
        group_rank = float('inf')

    if group == 'cbc':
        ifo_rank = -len(get_instruments(event))
        tie_breaker = -get_snr(event)
    else:
        ifo_rank = 0
        tie_breaker = event['far']
    # publishability criteria followed by group rank, ifo rank and tie breaker
    res_keyfunc = list(not ii for ii in _should_publish(event))
    res_keyfunc.extend((group_rank, ifo_rank, tie_breaker))
    return tuple(res_keyfunc)


def _update_superevent(superevent, new_event_dict,
                       t_0, t_start, t_end):
    """
    Update preferred event and/or change time window. Events with multiple
    detectors take precedence over single-detector events, then CBC events take
    precedence over burst events, and any remaining tie is broken by SNR/FAR
    values for CBC/Burst. Single detector are not promoted to preferred event
    status, if existing preferred event is multi-detector

    Parameters
    ----------
    superevent : object
        instance of :class:`_SuperEvent`
    new_event_dict : dict
        event info of the new trigger as a dictionary
    t_0 : float
        center time of `superevent_id`, None for no change
    t_start : float
        start time of `superevent_id`, None for no change
    t_end : float
        end time of `superevent_id`, None for no change
    """
    superevent_id = superevent.superevent_id
    preferred_event = superevent.preferred_event
    preferred_event_dict = gracedb.get_event(preferred_event)

    kwargs = {}
    if t_start is not None:
        kwargs['t_start'] = t_start
    if t_end is not None:
        kwargs['t_end'] = t_end
    if keyfunc(new_event_dict) < keyfunc(preferred_event_dict):
        kwargs['t_0'] = t_0
        if 'EM_Selected' not in superevent.event_dict['labels']:
            # update preferred event when EM_Selected is not applied
            kwargs['preferred_event'] = new_event_dict['graceid']

    if kwargs:
        gracedb.update_superevent(superevent_id, **kwargs)


def _superevent_segment_list(superevents):
    """Ingests a list of superevent dictionaries, and returns a segmentlist
    with start and end times as the duration of each segment.

    Parameters
    ----------
    superevents : list
        List of superevent dictionaries (e.g., the return value from
        :meth:`gwcelery.tasks.gracedb.get_superevents`).

    Returns
    -------
    superevent_list : segmentlist
        superevents as a segmentlist object
    """
    return segmentlist([_SuperEvent(s.get('t_start'),
                        s.get('t_end'),
                        s.get('t_0'),
                        s.get('superevent_id'),
                        s.get('preferred_event'),
                        s)
                       for s in superevents])


def _partially_intersects(superevents, event_segment):
    """Similar to :meth:`segmentlist.find` except it also returns the segment
    of `superevents` which partially intersects argument. If there are more
    than one intersections, first occurence is returned.

    Parameters
    ----------
    superevents : list
        list pulled down using the gracedb client
        :method:`superevents`
    event_segment : segment
        segment object whose index is wanted

    Returns
    -------
    match_segment   : segment
        segment in `self` which intersects. `None` if not found
    """
    # create a segmentlist using start and end times
    superevents = _superevent_segment_list(superevents)
    for s in superevents:
        if s.intersects(event_segment):
            return s
    return None


class _Event(segment):
    """An event implemented as an extension of :class:`segment`."""
    def __new__(cls, t0, t_start, t_end, *args, **kwargs):
        return super().__new__(cls, t_start, t_end)

    def __init__(self, t0, t_start, t_end, gid, group=None, pipeline=None,
                 search=None, event_dict={}):
        self.t0 = t0
        self.gid = gid
        self.group = group
        self.pipeline = pipeline
        self.search = search
        self.event_dict = event_dict


class _SuperEvent(segment):
    """An superevent implemented as an extension of :class:`segment`."""
    def __new__(cls, t_start, t_end, *args, **kwargs):
        return super().__new__(cls, t_start, t_end)

    def __init__(self, t_start, t_end, t_0, sid,
                 preferred_event=None, event_dict={}):
        self.t_start = t_start
        self.t_end = t_end
        self.t_0 = t_0
        self.superevent_id = sid
        self.preferred_event = preferred_event
        self.event_dict = event_dict
