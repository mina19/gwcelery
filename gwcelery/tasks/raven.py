"""Search for GRB-GW coincidences with ligo-raven."""
import ligo.raven.search
from celery import group
from celery.utils.log import get_task_logger

from ..import app
from .core import identity
from . import external_skymaps
from . import gracedb

log = get_task_logger(__name__)


@app.task(shared=False)
def calculate_coincidence_far(superevent, exttrig, tl, th,
                              use_superevent_skymap=None):
    """Compute coincidence FAR for external trigger and superevent
    coincidence by calling ligo.raven.search.calc_signif_gracedb,
    using sky map info if available.

    Parameters
    ----------
    superevent: dict
        superevent dictionary
    exttrig: dict
        external event dictionary
    tl: float
        start of coincident time window
    th: float
        end of coincident time window
    use_superevent_skymap: bool
        If True/False, use/don't use skymap info from superevent.
        Else if None, check SKYMAP_READY label in external event.

    """
    superevent_id = superevent['superevent_id']
    exttrig_id = exttrig['graceid']
    far_grb = exttrig['far']

    #  Don't compute coinc FAR for SNEWS coincidences
    if exttrig['pipeline'] == 'SNEWS':
        return {}

    #  Define max far thresholds for targeted subthreshold search
    if exttrig['search'] == 'SubGRBTargeted':
        far_thresholds = app.conf['raven_targeted_far_thresholds']
        far_gw_thresh = far_thresholds['GW'][exttrig['pipeline']]
        far_grb_thresh = far_thresholds['GRB'][exttrig['pipeline']]
    else:
        far_gw_thresh = None
        far_grb_thresh = None

    if ({'EXT_SKYMAP_READY', 'SKYMAP_READY'}.issubset(exttrig['labels']) or
            {'EXT_SKYMAP_READY', 'EM_READY'}.issubset(exttrig['labels'])):
        #  if both sky maps available, calculate spatial coinc far
        use_preferred_event_skymap = (
            not use_superevent_skymap
            if use_superevent_skymap is not None else
            'SKYMAP_READY' not in exttrig['labels'])
        se_skymap = external_skymaps.get_skymap_filename(
            (superevent['preferred_event'] if use_preferred_event_skymap
             else superevent_id), is_gw=True)
        ext_skymap = external_skymaps.get_skymap_filename(
            exttrig_id, is_gw=False)

        return ligo.raven.search.calc_signif_gracedb(
                   superevent_id, exttrig_id, tl, th,
                   se_dict=superevent, ext_dict=exttrig,
                   grb_search=exttrig['search'],
                   se_fitsfile=se_skymap, ext_fitsfile=ext_skymap,
                   se_moc=True, ext_moc=False,
                   use_radec=True if exttrig['pipeline'] == 'Swift' else False,
                   incl_sky=True, gracedb=gracedb.client,
                   far_grb=far_grb,
                   far_gw_thresh=far_gw_thresh,
                   far_grb_thresh=far_grb_thresh,
                   use_preferred_event_skymap=use_preferred_event_skymap)
    else:
        return ligo.raven.search.calc_signif_gracedb(
                   superevent_id, exttrig_id, tl, th,
                   se_dict=superevent, ext_dict=exttrig,
                   grb_search=exttrig['search'],
                   incl_sky=False, gracedb=gracedb.client,
                   far_grb=far_grb,
                   far_gw_thresh=far_gw_thresh,
                   far_grb_thresh=far_grb_thresh)


@app.task(shared=False)
def coincidence_search(gracedb_id, alert_object, group=None, pipelines=[],
                       searches=[], se_searches=[]):
    """Perform ligo-raven search for coincidences. Determines time window to
    use. If events found, launches raven pipeline.

    Parameters
    ----------
    gracedb_id: str
        ID of the trigger used by GraceDB
    alert_object: dict
        Alert dictionary
    group: str
        Burst or CBC
    pipelines: list
        list of external trigger pipeline names
    searches: list
        list of external trigger searches
    se_searches: list
        list of superevent searches

    """
    tl, th = _time_window(gracedb_id, group, pipelines, searches)

    (
        search.si(gracedb_id, alert_object, tl, th, group, pipelines,
                  searches, se_searches)
        |
        raven_pipeline.s(gracedb_id, alert_object, tl, th, group)
    ).delay()


def _time_window(gracedb_id, group, pipelines, searches):
    """Determine the time window to use given the parameters of the search.

    Parameters
    ----------
    gracedb_id: str
        ID of the trigger used by GraceDB
    group: str
        Burst or CBC
    pipelines: list
        list of external trigger pipeline names
    searches: list
        list of external trigger searches
    se_searches: list
        list of superevent searches

    """
    tl_cbc, th_cbc = app.conf['raven_coincidence_windows']['GRB_CBC']
    tl_subfermi, th_subfermi = \
        app.conf['raven_coincidence_windows']['GRB_CBC_SubFermi']
    tl_subswift, th_subswift = \
        app.conf['raven_coincidence_windows']['GRB_CBC_SubSwift']
    tl_burst, th_burst = app.conf['raven_coincidence_windows']['GRB_Burst']
    tl_snews, th_snews = app.conf['raven_coincidence_windows']['SNEWS']

    if 'SNEWS' in pipelines:
        tl, th = tl_snews, th_snews
    # Use Targeted search window if CBC or Burst
    elif not {'SubGRB', 'SubGRBTargeted'}.isdisjoint(searches):
        if 'Fermi' in pipelines:
            tl, th = tl_subfermi, th_subfermi
        elif 'Swift' in pipelines:
            tl, th = tl_subswift, th_subswift
        else:
            raise ValueError('Specify Fermi or Swift as pipeline when ' +
                             'launching subthreshold search')
    elif group == 'CBC':
        tl, th = tl_cbc, th_cbc
    elif group == 'Burst':
        tl, th = tl_burst, th_burst
    else:
        raise ValueError('Invalid RAVEN search request for {0}'.format(
            gracedb_id))
    if 'S' in gracedb_id:
        # If triggering on a superevent, need to reverse the time window
        tl, th = -th, -tl

    return tl, th


@app.task(shared=False)
def search(gracedb_id, alert_object, tl=-5, th=5, group=None,
           pipelines=[], searches=[], se_searches=[]):
    """Perform ligo-raven search for coincidences.

    Parameters
    ----------
    gracedb_id: str
        ID of the trigger used by GraceDB
    alert_object: dict
        Alert dictionary
    tl: int
        number of seconds to search before
    th: int
        number of seconds to search after
    group: str
        Burst or CBC
    pipelines: list
        list of external trigger pipelines for performing coincidence search
        against
    searches: list
        list of external trigger searches
    se_searches: list
        list of superevent searches

    Returns
    -------
        list with the dictionaries of related gracedb events

    """
    return ligo.raven.search.search(gracedb_id, tl, th,
                                    event_dict=alert_object,
                                    gracedb=gracedb.client,
                                    group=group, pipelines=pipelines,
                                    searches=searches,
                                    se_searches=se_searches)


@app.task(shared=False)
def raven_pipeline(raven_search_results, gracedb_id, alert_object, tl, th,
                   gw_group, use_superevent_skymap=None):
    """Executes much of the full raven pipeline, including adding
    the external trigger to the superevent, calculating the
    coincidence false alarm rate, and applying 'EM_COINC' to the
    appropriate events. Also a preimlinary alert will be triggered
    if the coincidence passes threshold.

    Parameters
    ----------
    raven_search_results: list
        list of dictionaries of each related gracedb trigger
    gracedb_id: str
        ID of either a superevent or external trigger
    alert_object: dict
        Alert dictionary, either a superevent or an external event
    gw_group: str
        Burst or CBC
    use_superevent_skymap: bool
        If True/False, use/don't use skymap info from superevent.
        Else if None, check SKYMAP_READY label in external event.

    """
    if not raven_search_results:
        return
    if 'S' not in gracedb_id:
        raven_search_results = preferred_superevent(raven_search_results)
    for result in raven_search_results:
        if 'S' in gracedb_id:
            superevent_id = gracedb_id
            exttrig_id = result['graceid']
            superevent = alert_object
            ext_event = result
        else:
            superevent_id = result['superevent_id']
            exttrig_id = gracedb_id
            superevent = result
            ext_event = alert_object
        # Don't continue if it is a different superevent than previous one.
        if ext_event['superevent'] is not None \
                and ext_event['superevent'] != superevent['superevent_id']:
            return

        canvas = (
            gracedb.add_event_to_superevent.si(superevent_id, exttrig_id)
            |
            calculate_coincidence_far.si(
                superevent, ext_event, tl, th,
                use_superevent_skymap=use_superevent_skymap
            )
            |
            group(gracedb.create_label.si('EM_COINC', superevent_id),
                  gracedb.create_label.si('EM_COINC', exttrig_id),
                  trigger_raven_alert.s(superevent, gracedb_id,
                                        ext_event, gw_group))
        )
        canvas.delay()


@app.task(shared=False)
def preferred_superevent(raven_search_results):
    """Chooses the superevent with the lowest far for an external
    event to be added to. This is to prevent errors from trying to
    add one external event to multiple superevents.

    Parameters
    ----------
    raven_search_results: list
        list of dictionaries of each related gracedb trigger

    """
    minfar, idx = min((result['far'], idx) for (idx, result) in
                      enumerate(raven_search_results))
    return [raven_search_results[idx]]


@app.task(shared=False)
def update_coinc_far(coinc_far_dict, superevent, ext_event):
    """Update joint info in superevent based on the current preferred
    coincidence. This prefers a spacetime joint FAR over a time-only joint
    FAR. A SNEWS coincidence is preferred over either.

      Parameters
    ----------
    coinc_far_dict : dict
        Dictionary containing coincidence false alarm rate results from
        RAVEN
    superevent : dict
        superevent dictionary
    ext_event: dict
        external event dictionary

    """
    #  Get graceids
    superevent_id = superevent['superevent_id']
    ext_id = ext_event['graceid']

    #  Get the latest info to prevent race condition
    superevent_latest = gracedb.get_superevent(superevent_id)

    #  Joint FAR isn't computed for SNEWS coincidence
    #  Choose SNEWS coincidence over any other type of coincidence
    if ext_event['pipeline'] == 'SNEWS':
        gracedb.update_superevent(superevent_id, em_type=ext_id,
                                  time_coinc_far=None,
                                  space_coinc_far=None)
        return coinc_far_dict

    #  Load needed variables
    infty = float('inf')
    new_time_far = coinc_far_dict['temporal_coinc_far']
    new_space_far = coinc_far_dict['spatiotemporal_coinc_far']
    #  Map None to infinity to make logic easier
    new_space_far_f = new_space_far if new_space_far else infty
    old_time_far = superevent_latest['time_coinc_far']
    old_time_far_f = old_time_far if old_time_far else infty
    old_space_far = superevent_latest['space_coinc_far']
    old_space_far_f = old_space_far if old_space_far else infty
    is_far_improved = (new_space_far_f < old_space_far_f or
                       (new_time_far < old_time_far_f and
                        old_space_far_f == infty))

    if superevent_latest['em_type']:
        #  If previous preferred external event, load to compare
        emtype_event = gracedb.get_event(superevent_latest['em_type'])
        #  Don't overwrite SNEWS with GRB event
        snews_to_grb = \
            (emtype_event['pipeline'] == 'SNEWS' and
             ext_event['pipeline'] != 'SNEWS')
        #  Determine which events are likely real or not
        is_old_grb_real, is_new_grb_real = \
            ('NOT_GRB' not in emtype_event['labels'],
             'NOT_GRB' not in ext_event['labels'])
        is_old_raven_alert, is_new_raven_alert = \
            ('RAVEN_ALERT' in emtype_event['labels'],
             'RAVEN_ALERT' in ext_event['labels'])
        #  Use new event only if it is better old event information
        is_event_improved = ((is_new_grb_real and not is_old_grb_real) or
                             (is_new_raven_alert and not is_old_raven_alert))
        #  if both real or both not, use FAR to differentiate
        if is_old_grb_real == is_new_grb_real \
                and is_old_raven_alert == is_new_raven_alert:
            is_event_improved = is_far_improved
    else:
        snews_to_grb = False
        is_event_improved = is_far_improved

    if is_event_improved and not snews_to_grb:
        gracedb.update_superevent(superevent_id, em_type=ext_id,
                                  time_coinc_far=new_time_far,
                                  space_coinc_far=new_space_far)
    return coinc_far_dict


@app.task(shared=False)
def trigger_raven_alert(coinc_far_dict, superevent, gracedb_id,
                        ext_event, gw_group):
    """Determine whether an event should be published as a preliminary alert.
    If yes, then triggers an alert by applying `RAVEN_ALERT` to the preferred
    event.

    All of the following conditions must be true for a preliminary alert:

    *   The external event must be a threshold GRB or SNEWS event.
    *   If triggered on a SNEWS event, the GW false alarm rate must pass
        :obj:`~gwcelery.conf.snews_gw_far_threshold`.
    *   The event's RAVEN coincidence false alarm rate, weighted by the
        group-specific trials factor as specified by the
        :obj:`~gwcelery.conf.preliminary_alert_trials_factor` configuration
        setting, is less than or equal to
        :obj:`~gwcelery.conf.preliminary_alert_far_threshold`.
    *   If the external event is from Swift, both sky maps must be present.

    Parameters
    ----------
    coinc_far_dict : dict
        Dictionary containing coincidence false alarm rate results from
        RAVEN
    superevent : dict
        superevent dictionary
    gracedb_id: str
        ID of the trigger that launched RAVEN
    ext_event: dict
        external event dictionary
    gw_group: str
        Burst or CBC

    """
    preferred_gwevent_id = superevent['preferred_event']
    superevent_id = superevent['superevent_id']
    ext_id = ext_event['graceid']
    gw_group = gw_group.lower()
    pipeline = ext_event['pipeline']
    trials_factor = app.conf['significant_alert_trials_factor'][gw_group]
    missing_skymap = True
    comments = []
    messages = []

    #  Since the significance of SNEWS triggers is so high, we will publish
    #  any trigger coincident with a decently significant GW candidate
    if 'SNEWS' == pipeline:
        gw_far = superevent['far']
        far_type = 'gw'
        far_threshold = app.conf['snews_gw_far_threshold']
        pass_far_threshold = gw_far * trials_factor < far_threshold
        is_ext_subthreshold = False
        missing_skymap = False
        #  Set coinc FAR to gw FAR only for the sake of a message below
        time_coinc_far = space_coinc_far = coinc_far = None
        coinc_far_f = gw_far

    #  The GBM team requested we not send automatic alerts from subthreshold
    #  GRBs. This checks that at least one threshold GRB present as well as
    #  the coinc far
    else:
        # check whether the GRB is threshold or sub-thresholds
        is_ext_subthreshold = 'SubGRB' == ext_event['search']

        # Use spatial FAR if available, otherwise use temporal
        time_coinc_far = coinc_far_dict['temporal_coinc_far']
        space_coinc_far = coinc_far_dict['spatiotemporal_coinc_far']
        if space_coinc_far is not None:
            coinc_far = space_coinc_far
            missing_skymap = False
        else:
            coinc_far = time_coinc_far

        far_type = 'joint'
        far_threshold = app.conf['significant_alert_far_threshold'][gw_group]
        coinc_far_f = coinc_far * trials_factor * (trials_factor - 1.)
        pass_far_threshold = coinc_far_f <= far_threshold

    #  Get most recent labels to prevent race conditions
    ext_labels = gracedb.get_labels(ext_id)
    no_previous_alert = {'RAVEN_ALERT'}.isdisjoint(ext_labels)
    likely_real_ext_event = {'NOT_GRB'}.isdisjoint(ext_labels)
    is_test_event = (superevent['preferred_event_data']['group'] == 'Test' or
                     ext_event['group'] == 'Test')

    #  If publishable, trigger an alert by applying `RAVEN_ALERT` label to
    #  preferred event
    if pass_far_threshold and not is_ext_subthreshold and \
            likely_real_ext_event and not missing_skymap and \
            not is_test_event and no_previous_alert:
        comments.append(('RAVEN: publishing criteria met for {0}-{1}. '
                         'Triggering RAVEN alert'.format(
                             preferred_gwevent_id, ext_id)))
        # Add label to local dictionary and to event on GraceDB server
        # NOTE: We may prefer to apply the superevent label first and the grab
        # labels to refresh in the future
        superevent['labels'] += 'RAVEN_ALERT'
        # Add RAVEN_ALERT to preferred event last to avoid race conditions
        # where superevent is expected to have it once alert is issued
        alert_canvas = (
            gracedb.create_label.si('RAVEN_ALERT', superevent_id)
            |
            gracedb.create_label.si('HIGH_PROFILE', superevent_id)
            |
            gracedb.create_label.si('RAVEN_ALERT', ext_id)
            |
            gracedb.create_label.si('RAVEN_ALERT', preferred_gwevent_id)
        )
    else:
        alert_canvas = identity.si()
    if not pass_far_threshold:
        comments.append(('RAVEN: publishing criteria not met for {0}-{1},'
                         ' {2} FAR (w/ trials) too large '
                         '({3:.4g} > {4:.4g})'.format(
                             preferred_gwevent_id, ext_id, far_type,
                             coinc_far_f, far_threshold)))
    if is_ext_subthreshold:
        comments.append(('RAVEN: publishing criteria not met for {0}-{1},'
                         ' {1} is subthreshold'.format(preferred_gwevent_id,
                                                       ext_id)))
    if not likely_real_ext_event:
        comments.append(('RAVEN: publishing criteria not met for {0}-{1},'
                         ' {1} is likely non-astrophysical.'.format(
                             preferred_gwevent_id, ext_id)))
    if is_test_event:
        comments.append('RAVEN: {0}-{1} is non-astrophysical, '
                        'at least one event is a Test event'.format(
                            preferred_gwevent_id, ext_id))
    if missing_skymap:
        comments.append('RAVEN: Will only publish GRB coincidence '
                        'if spatial-temporal FAR is present. '
                        'Waiting for both sky maps to be available '
                        'first.')
    for comment in comments:
        messages.append(gracedb.upload.si(None, None, superevent_id, comment,
                                          tags=['ext_coinc']))

    # Update coincidence FAR with latest info, including the application of
    # RAVEN_ALERT, then issue alert
    (
        update_coinc_far.si(coinc_far_dict, superevent, ext_event)
        |
        group(
            alert_canvas,
            external_skymaps.plot_overlap_integral.s(superevent, ext_event),
            *messages
        )
    ).delay()


@app.task(shared=False)
def sog_paper_pipeline(ext_event, superevent):
    """Determine whether an a speed of gravity measurment manuscript should be
    triggered for a given coincidence.
    This is denoted by applying the SOG_READY label to a superevent.

    All of the following conditions must be true for a SoG paper:

    *   The coincidence is significant and FARs more significant than in
        :obj:`~sog_paper_far_threshold`.
    *   The external event is a high-significance GRB and from a MOU partner.
    *   The GW event is a CBC candidate.

    Parameters
    ----------
    superevent : dict
        superevent dictionary
    ext_event: dict
        external event dictionary

    """
    gw_far = superevent['far']
    coinc_far = superevent['space_coinc_far']
    gw_far_threshold = app.conf['sog_paper_far_threshold']['gw']
    joint_far_threshold = app.conf['sog_paper_far_threshold']['joint']

    #  Check publishing conditions
    pass_gw_far_threshold = gw_far <= gw_far_threshold
    pass_joint_far_threshold = coinc_far <= joint_far_threshold
    is_grb = ext_event['search'] in ['GRB', 'MDC']
    is_mou_partner = ext_event['pipeline'] in ['Fermi', 'Swift']
    is_cbc = superevent['preferred_event_data']['group'] == 'CBC'

    if is_grb and is_cbc and is_mou_partner and \
            pass_gw_far_threshold and pass_joint_far_threshold:
        #  Trigger SOG_READY label alert to alert SOG analysts
        gracedb.create_label.si('SOG_READY',
                                superevent['superevent_id']).delay()
