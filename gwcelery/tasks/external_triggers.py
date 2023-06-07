from lxml import etree
from urllib.parse import urlparse
from celery import group
from celery.utils.log import get_logger

from ..import app
from . import detchar
from . import gcn
from . import gracedb
from . import external_skymaps
from . import igwn_alert
from . import raven

log = get_logger(__name__)


REQUIRED_LABELS_BY_TASK = {
    # EM_READY implies preferred event sky map is available
    'compare': {'EM_READY', 'EXT_SKYMAP_READY', 'EM_COINC'},
    'SoG': {'SKYMAP_READY', 'RAVEN_ALERT', 'ADVOK'}
}
"""These labels should be present on an external event to consider it to
be ready for sky map comparison or for post-alert analsis, such as a
measurment of the speed of gravity (SoG).
"""

FERMI_GRB_CLASS_VALUE = 4
"""This is the index that denote GRBs within Fermi's Flight Position
classification."""

FERMI_GRB_CLASS_THRESH = 50
"""This values denotes the threshold of the most likely Fermi source
classification, above which we will consider a Fermi Flight Position
notice."""


@gcn.handler(gcn.NoticeType.SNEWS,
             queue='exttrig',
             shared=False)
def handle_snews_gcn(payload):
    """Handles the payload from SNEWS alerts.

    Prepares the alert to be sent to graceDB as 'E' events.
    """
    root = etree.fromstring(payload)

    #  Get TrigID and Test Event Boolean
    trig_id = root.find("./What/Param[@name='TrigID']").attrib['value']
    ext_group = 'Test' if root.attrib['role'] == 'test' else 'External'

    event_observatory = 'SNEWS'
    if 'mdc-test_event' in root.attrib['ivorn'].lower():
        search = 'MDC'
    else:
        search = 'Supernova'
    query = 'group: External pipeline: {} grbevent.trigger_id = "{}"'.format(
        event_observatory, trig_id)
    events = gracedb.get_events(query=query)

    if events and ext_group == 'External':
        # Only update event if real
        assert len(events) == 1, 'Found more than one matching GraceDB entry'
        event, = events
        graceid = event['graceid']
        canvas = gracedb.replace_event.s(graceid, payload)

    else:
        canvas = gracedb.create_event.s(filecontents=payload,
                                        search=search,
                                        group=ext_group,
                                        pipeline=event_observatory)
        canvas |= _launch_external_detchar.s()

    canvas.delay()


@gcn.handler(gcn.NoticeType.FERMI_GBM_ALERT,
             gcn.NoticeType.FERMI_GBM_FLT_POS,
             gcn.NoticeType.FERMI_GBM_GND_POS,
             gcn.NoticeType.FERMI_GBM_FIN_POS,
             gcn.NoticeType.SWIFT_BAT_GRB_POS_ACK,
             gcn.NoticeType.FERMI_GBM_SUBTHRESH,
             gcn.NoticeType.INTEGRAL_WAKEUP,
             gcn.NoticeType.INTEGRAL_REFINED,
             gcn.NoticeType.INTEGRAL_OFFLINE,
             gcn.NoticeType.AGILE_MCAL_ALERT,
             queue='exttrig',
             shared=False)
def handle_grb_gcn(payload):
    """Handles the payload from Fermi, Swift, INTEGRAL, and AGILE MCAL
    GCN notices.

    Filters out candidates likely to be noise. Creates external events
    from the notice if new notice, otherwise updates existing event. Then
    creates and/or grabs external sky map to be uploaded to the external event.

    More info for these notices can be found at:
    Fermi-GBM: https://gcn.gsfc.nasa.gov/fermi_grbs.html
    Fermi-GBM sub: https://gcn.gsfc.nasa.gov/fermi_gbm_subthresh_archive.html
    Swift: https://gcn.gsfc.nasa.gov/swift.html
    INTEGRAL: https://gcn.gsfc.nasa.gov/integral.html
    AGILE-MCAL: https://gcn.gsfc.nasa.gov/agile_mcal.html
    """
    root = etree.fromstring(payload)
    u = urlparse(root.attrib['ivorn'])
    stream_path = u.path

    stream_obsv_dict = {'/SWIFT': 'Swift',
                        '/Fermi': 'Fermi',
                        '/INTEGRAL': 'INTEGRAL',
                        '/AGILE': 'AGILE'}
    event_observatory = stream_obsv_dict[stream_path]

    #  Get TrigID
    if event_observatory == 'INTEGRAL' and \
            not any([x in u.fragment for x in ['O3-replay', 'MDC-test']]):
        #  FIXME: revert all this if INTEGRAL fixes their GCN notices
        #  If INTEGRAL, get trigger ID from ivorn rather than the TrigID field
        #  unless O3 replay or MDC event
        trig_id = u.fragment.split('_')[-1].split('-')[0]
        #  Modify the TrigID field so GraceDB has the correct value
        root.find("./What/Param[@name='TrigID']").attrib['value'] = \
            str(trig_id).encode()
        #  Apply changes to payload delivered to GraceDB
        payload = etree.tostring(root, xml_declaration=True, encoding="UTF-8")
    else:
        try:
            trig_id = \
                root.find("./What/Param[@name='TrigID']").attrib['value']
        except AttributeError:
            trig_id = \
                root.find("./What/Param[@name='Trans_Num']").attrib['value']
    ext_group = 'Test' if root.attrib['role'] == 'test' else 'External'

    notice_type = \
        int(root.find("./What/Param[@name='Packet_Type']").attrib['value'])

    reliability = root.find("./What/Param[@name='Reliability']")
    if reliability is not None and int(reliability.attrib['value']) <= 4:
        return

    #  Check if Fermi trigger is likely noise by checking classification
    #  Most_Likely_Index of 4 is an astrophysical GRB
    #  If not at least 50% chance of GRB we will not consider it for RAVEN
    likely_source = root.find("./What/Param[@name='Most_Likely_Index']")
    likely_prob = root.find("./What/Param[@name='Most_Likely_Prob']")
    not_likely_grb = likely_source is not None and \
        (likely_source.attrib['value'] != FERMI_GRB_CLASS_VALUE
         or likely_prob.attrib['value'] < FERMI_GRB_CLASS_THRESH)

    #  Check if initial Fermi alert. These are generally unreliable and should
    #  never trigger a RAVEN alert, but will give us earlier warning of a
    #  possible coincidence. Later notices could change this.
    initial_gbm_alert = notice_type == gcn.NoticeType.FERMI_GBM_ALERT

    #  Check if Swift has lost lock. If so then veto
    lost_lock = \
        root.find("./What/Group[@name='Solution_Status']" +
                  "/Param[@name='StarTrack_Lost_Lock']")
    swift_veto = lost_lock is not None and lost_lock.attrib['value'] == 'true'

    #  Only send alerts if likely a GRB, is not a low-confidence early Fermi
    #  alert, and if not a Swift veto
    if not_likely_grb or initial_gbm_alert or swift_veto:
        labels = ['NOT_GRB']
    else:
        labels = None

    ivorn = root.attrib['ivorn']
    if 'subthresh' in ivorn.lower():
        search = 'SubGRB'
    elif 'mdc-test_event' in ivorn.lower():
        search = 'MDC'
    else:
        search = 'GRB'

    query = 'group: External pipeline: {} grbevent.trigger_id = "{}"'.format(
        event_observatory, trig_id)
    events = gracedb.get_events(query=query)

    group_canvas = ()
    if events and ext_group == 'External':
        # Only update event if real
        assert len(events) == 1, 'Found more than one matching GraceDB entry'
        event, = events
        graceid = event['graceid']
        if labels:
            canvas = gracedb.create_label.si(labels[0], graceid)
        else:
            canvas = gracedb.remove_label.si('NOT_GRB', graceid)

        # Prevent SubGRBs from appending GRBs
        if search == 'GRB':
            # Replace event and pass already existing event dictionary
            canvas |= gracedb.replace_event.si(graceid, payload)
            canvas |= gracedb.get_event.si(graceid)
        else:
            return

    else:
        canvas = gracedb.create_event.s(filecontents=payload,
                                        search=search,
                                        group=ext_group,
                                        pipeline=event_observatory,
                                        labels=labels)
        group_canvas += _launch_external_detchar.s(),

    if search in {'GRB', 'MDC'}:
        notice_date = root.find("./Who/Date").text
        group_canvas += external_skymaps.create_upload_external_skymap.s(
                      notice_type, notice_date),
    if event_observatory == 'Fermi':
        if search == 'SubGRB':
            skymap_link = \
                root.find("./What/Param[@name='HealPix_URL']").attrib['value']
            group_canvas += \
                external_skymaps.get_upload_external_skymap.s(skymap_link),
        elif search == 'GRB':
            skymap_link = None
            group_canvas += \
                external_skymaps.get_upload_external_skymap.s(skymap_link),

    (
        canvas
        |
        group(group_canvas)
    ).delay()


@igwn_alert.handler('superevent',
                    'mdc_superevent',
                    'external_fermi',
                    'external_swift',
                    'external_integral',
                    'external_agile',
                    shared=False)
def handle_grb_igwn_alert(alert):
    """Parse an IGWN alert message related to superevents/GRB external triggers
    and dispatch it to other tasks.

    Notes
    -----
    This IGWN alert message handler is triggered by creating a new superevent
    or GRB external trigger event, or a label associated with completeness of
    skymaps:

    * Any new event triggers a coincidence search with
      :meth:`gwcelery.tasks.raven.coincidence_search`.
    * When both a GW and GRB sky map are available during a coincidence,
      indicated by the labels ``EM_READY`` and ``EXT_SKYMAP_READY``
      respectively on the external event, this triggers the spacetime coinc
      FAR to be calculated and a combined GW-GRB sky map is created using
      :meth:`gwcelery.tasks.external_skymaps.create_combined_skymap`.

    """
    # Determine GraceDB ID
    graceid = alert['uid']

    # launch searches
    if alert['alert_type'] == 'new':
        if alert['object'].get('group') == 'External':
            # Create and upload Swift sky map for the joint targeted
            # sub-threshold search as agreed on in the MOU
            if alert['object']['search'] == 'SubGRBTargeted' and \
                    alert['object']['pipeline'] == 'Swift':
                external_skymaps.create_upload_external_skymap(
                    alert['object'], None, alert['object']['created'])

            # launch search with MDC events and exit
            if alert['object']['search'] == 'MDC':
                raven.coincidence_search(graceid, alert['object'],
                                         group='CBC', se_searches=['MDC'])
                raven.coincidence_search(graceid, alert['object'],
                                         group='Burst', se_searches=['MDC'])
                return

            # launch standard Burst-GRB search
            raven.coincidence_search(graceid, alert['object'], group='Burst',
                                     se_searches=['Allsky'])

            if alert['object']['search'] in ['SubGRB', 'SubGRBTargeted']:
                # if sub-threshold GRB, launch search with that pipeline
                raven.coincidence_search(
                    graceid, alert['object'], group='CBC',
                    searches=['SubGRB', 'SubGRBTargeted'],
                    pipelines=[alert['object']['pipeline']])
            else:
                # if threshold GRB, launch standard CBC-GRB search
                raven.coincidence_search(graceid, alert['object'],
                                         group='CBC', searches=['GRB'])
        elif 'S' in graceid:
            # launch standard GRB search based on group
            gw_group = alert['object']['preferred_event_data']['group']

            # launch search with MDC events and exit
            if alert['object']['preferred_event_data']['search'] == 'MDC':
                raven.coincidence_search(graceid, alert['object'],
                                         group=gw_group, searches=['MDC'])
                return

            if gw_group == 'CBC':
                # launch subthreshold searches if CBC
                # for Fermi and Swift separately to use different time windows
                for pipeline in ['Fermi', 'Swift']:
                    raven.coincidence_search(
                        graceid, alert['object'], group='CBC',
                        searches=['SubGRB', 'SubGRBTargeted'],
                        pipelines=[pipeline])
                se_searches = []
            else:
                se_searches = ['Allsky']
            # launch standard GRB search
            raven.coincidence_search(graceid, alert['object'],
                                     group=gw_group, searches=['GRB'],
                                     se_searches=se_searches)
    # re-run raven pipeline and create combined sky map (if not a Swift event)
    # when sky maps are available
    elif alert['alert_type'] == 'label_added' and \
            alert['object'].get('group') == 'External':
        if _skymaps_are_ready(alert['object'], alert['data']['name'],
                              'compare'):
            # if both sky maps present and a coincidence, re-run RAVEN
            # pipeline and create combined sky maps
            ext_event = alert['object']
            superevent_id, ext_id = _get_superevent_ext_ids(
                graceid, ext_event)
            superevent = gracedb.get_superevent(superevent_id)
            _relaunch_raven_pipeline_with_skymaps(
                superevent, ext_event, graceid)
        elif 'EM_COINC' in alert['object']['labels']:
            # if not complete, check if GW sky map; apply label to external
            # event if GW sky map
            se_labels = gracedb.get_labels(alert['object']['superevent'])
            if 'SKYMAP_READY' in se_labels:
                gracedb.create_label.si('SKYMAP_READY', graceid).delay()
            if 'EM_READY' in se_labels:
                gracedb.create_label.si('EM_READY', graceid).delay()
    # apply labels from superevent to external event to update state
    # and trigger functionality requiring sky maps, etc.
    elif alert['alert_type'] == 'label_added' and 'S' in graceid:
        if 'SKYMAP_READY' in alert['object']['labels']:
            # if sky map in superevent, apply label to all external events
            # at the time
            group(
                gracedb.create_label.si('SKYMAP_READY', ext_id)
                for ext_id in alert['object']['em_events']
            ).delay()
        if 'EM_READY' in alert['object']['labels']:
            # if sky map not in superevent but in preferred event, apply label
            # to all external events at the time
            group(
                gracedb.create_label.si('EM_READY', ext_id)
                for ext_id in alert['object']['em_events']
            ).delay()
        if _skymaps_are_ready(alert['object'], alert['data']['name'], 'SoG') \
                and alert['object']['space_coinc_far'] is not None:
            # if a superevent is vetted by ADVOK and a spatial joint FAR is
            # available, check if SoG publishing conditions are met
            (
                gracedb.get_event.si(alert['object']['em_type'])
                |
                raven.sog_paper_pipeline.s(alert['object'])
            ).delay()
    # if new GW or external sky map after first being available, try to remake
    # combine sky map and rerun raven pipeline
    elif alert['alert_type'] == 'log' and \
            'EM_COINC' in alert['object']['labels'] and \
            'fit' in alert['data']['filename'] and \
            'flat' not in alert['data']['comment'].lower() and \
            (alert['data']['filename'] not in
             {external_skymaps.COMBINED_SKYMAP_FILENAME_MULTIORDER,
              external_skymaps.COMBINED_SKYMAP_FILENAME_FLAT}):
        superevent_id, external_id = _get_superevent_ext_ids(
                                         graceid, alert['object'])
        if 'S' in graceid:
            superevent = alert['object']
        else:
            superevent = gracedb.get_superevent(alert['object']['superevent'])
            external_event = alert['object']
        # check if combined sky map already made, with the exception of Swift
        # which will fail
        if 'S' in graceid:
            # Rerun for all eligible external events
            for ext_id in superevent['em_events']:
                external_event = gracedb.get_event(ext_id)
                if REQUIRED_LABELS_BY_TASK['compare'].issubset(
                        set(external_event['labels'])):
                    _relaunch_raven_pipeline_with_skymaps(
                        superevent, external_event, graceid,
                        use_superevent_skymap=True)
        else:
            if REQUIRED_LABELS_BY_TASK['compare'].issubset(
                    set(external_event['labels'])):
                _relaunch_raven_pipeline_with_skymaps(
                    superevent, external_event, graceid)
    # Rerun the coincidence FAR calculation if possible with combined sky map
    # if the preferred event changes
    # We don't want to run this logic if PE results are present
    elif alert['alert_type'] == 'log' and \
            'PE_READY' not in alert['object']['labels'] and \
            'EM_COINC' in alert['object']['labels']:
        new_log_comment = alert['data'].get('comment', '')
        if 'S' in graceid and \
            new_log_comment.startswith('Updated superevent parameters: '
                                       'preferred_event: '):
            superevent = alert['object']
            # Rerun for all eligible external events
            for ext_id in superevent['em_events']:
                external_event = gracedb.get_event(ext_id)
                if REQUIRED_LABELS_BY_TASK['compare'].issubset(
                        set(external_event['labels'])):
                    _relaunch_raven_pipeline_with_skymaps(
                        superevent, external_event, graceid,
                        use_superevent_skymap=False)
    elif alert['alert_type'] == 'label_removed' and \
            alert['object'].get('group') == 'External':
        if alert['data']['name'] == 'NOT_GRB' and \
                'EM_COINC' in alert['object']['labels']:
            # if NOT_GRB is removed, re-check publishing conditions
            superevent_id = alert['object']['superevent']
            superevent = gracedb.get_superevent(superevent_id)
            gw_group = superevent['preferred_event_data']['group']
            coinc_far_dict = {
                'temporal_coinc_far': superevent['time_coinc_far'],
                'spatiotemporal_coinc_far': superevent['space_coinc_far']
            }
            raven.trigger_raven_alert(coinc_far_dict, superevent, graceid,
                                      alert['object'], gw_group)


@igwn_alert.handler('superevent',
                    'mdc_superevent',
                    'external_snews',
                    shared=False)
def handle_snews_igwn_alert(alert):
    """Parse an IGWN alert message related to superevents/SN external triggers
    and dispatch it to other tasks.

    Notes
    -----
    This igwn_alert message handler is triggered by creating a new superevent
    or SN external trigger event:

    * Any new event triggers a coincidence search with
      :meth:`gwcelery.tasks.raven.coincidence_search`.

    """
    # Determine GraceDB ID
    graceid = alert['uid']

    if alert['alert_type'] == 'new':
        if alert['object'].get('superevent_id'):
            group = alert['object']['preferred_event_data']['group']
            search = alert['object']['preferred_event_data']['search']
            if search == 'MDC':
                raven.coincidence_search(graceid, alert['object'],
                                         group='Burst', searches=['MDC'],
                                         pipelines=['SNEWS'])
            # Run on Test and Burst superevents
            elif group in {'Burst', 'Test'}:
                raven.coincidence_search(graceid, alert['object'],
                                         group='Burst', searches=['Supernova'],
                                         pipelines=['SNEWS'])
        else:
            # Run on SNEWS event, either real or test
            search = alert['object']['search']
            if search == 'MDC':
                raven.coincidence_search(graceid, alert['object'],
                                         group='Burst',
                                         se_searches=['MDC'],
                                         pipelines=['SNEWS'])
            elif search == 'Supernova':
                raven.coincidence_search(graceid, alert['object'],
                                         group='Burst', searches=['Supernova'],
                                         pipelines=['SNEWS'])


def _skymaps_are_ready(event, label, task):
    label_set = set(event['labels'])
    required_labels = REQUIRED_LABELS_BY_TASK[task]
    return required_labels.issubset(label_set) and label in required_labels


def _get_superevent_ext_ids(graceid, event):
    if 'S' in graceid:
        se_id = event['superevent_id']
        ext_id = event['em_type']
    else:
        se_id = event['superevent']
        ext_id = event['graceid']
    return se_id, ext_id


@app.task(shared=False)
def _launch_external_detchar(event):
    start = event['gpstime']
    if event['pipeline'] == 'SNEWS':
        start, end = event['gpstime'], event['gpstime']
    else:
        integration_time = \
            event['extra_attributes']['GRB']['trigger_duration'] or 4.0
        end = start + integration_time
    detchar.check_vectors.si(event, event['graceid'], start, end).delay()

    return event


def _relaunch_raven_pipeline_with_skymaps(superevent, ext_event, graceid,
                                          use_superevent_skymap=None):
    """Relaunch the RAVEN sky map comparison workflow, include recalculating
    the joint FAR with updated sky map info and create a new combined sky map.

    Parameters
    ----------
    superevent: dict
        superevent dictionary
    exttrig: dict
        external event dictionary
    graceid: str
        GraceDB ID of event
    use_superevent_skymap: bool
        If True/False, use/don't use skymap info from superevent.
        Else if None, check SKYMAP_READY label in external event.

    """
    gw_group = superevent['preferred_event_data']['group']
    tl, th = raven._time_window(graceid, gw_group,
                                [ext_event['pipeline']],
                                [ext_event['search']])
    # FIXME: both overlap integral and combined sky map could be
    # done by the same function since they are so similar
    use_superevent = (use_superevent_skymap
                      if use_superevent_skymap is not None else
                      'SKYMAP_READY' in ext_event['labels'])
    canvas = raven.raven_pipeline.si(
                 [ext_event] if 'S' in graceid else [superevent],
                 graceid,
                 superevent if 'S' in graceid else ext_event,
                 tl, th, gw_group, use_superevent_skymap=use_superevent)
    # Swift localizations are incredibly well localized and require
    # a different method from Fermi/Integral/AGILE
    # FIXME: Add Swift localization information in the future
    if ext_event['pipeline'] != 'Swift':
        # Create new updated combined sky map
        canvas |= external_skymaps.create_combined_skymap.si(
                      superevent['superevent_id'], ext_event['graceid'],
                      preferred_event=(
                          None if use_superevent
                          else superevent['preferred_event']))
    canvas.delay()
