from pathlib import Path
from urllib.parse import urlparse

from astropy.time import Time
from celery import group
from celery.utils.log import get_logger
from lxml import etree

from .. import app
from . import (alerts, detchar, external_skymaps, gcn, gracedb, igwn_alert,
               raven)

log = get_logger(__name__)


REQUIRED_LABELS_BY_TASK = {
    # EM_READY implies preferred event sky map is available
    'compare': {'EM_READY', 'EXT_SKYMAP_READY', 'EM_COINC'},
    'SoG': {'SKYMAP_READY', 'RAVEN_ALERT', 'ADVOK'}
}
"""These labels should be present on an external event to consider it to
be ready for sky map comparison or for post-alert analysis, such as a
measurment of the speed of gravity (SoG).
"""

FERMI_GRB_CLASS_VALUE = 4
"""This is the index that denote GRBs within Fermi's Flight Position
classification."""

FERMI_GRB_CLASS_THRESH = 50
"""This values denotes the threshold of the most likely Fermi source
classification, above which we will consider a Fermi Flight Position
notice."""


@alerts.handler('snews',
                queue='exttrig',
                shared=False)
def handle_snews_gcn(payload):
    """Handles the GCN notice payload from SNEWS alerts.

    Prepares the alert to be sent to graceDB as external events, updating the
    info if it already exists.

    Parameters
    ----------
    payload : str
        XML GCN notice alert packet in string format

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

    (
        gracedb.get_events.si(query=query)
        |
        _create_replace_external_event_and_skymap.s(
            payload, search, event_observatory, ext_group=ext_group
        )
    ).delay()


@alerts.handler('fermi_gbm_alert',
                'fermi_gbm_flt_pos',
                'fermi_gbm_gnd_pos',
                'fermi_gbm_fin_pos',
                'fermi_gbm_subthresh',
                'swift_bat_grb_pos_ack',
                'integral_wakeup',
                'integral_refined',
                'integral_offline',
                queue='exttrig',
                shared=False)
def handle_grb_gcn(payload):
    """Handles the payload from Fermi, Swift, and INTEGRAL GCN notices.

    Filters out candidates likely to be noise. Creates external events
    from the notice if new notice, otherwise updates existing event. Then
    creates and/or grabs external sky map to be uploaded to the external event.

    More info for these notices can be found at:
    Fermi-GBM: https://gcn.gsfc.nasa.gov/fermi_grbs.html
    Fermi-GBM sub: https://gcn.gsfc.nasa.gov/fermi_gbm_subthresh_archive.html
    Swift: https://gcn.gsfc.nasa.gov/swift.html
    INTEGRAL: https://gcn.gsfc.nasa.gov/integral.html

    Parameters
    ----------
    payload : str
        XML GCN notice alert packet in string format

    """
    root = etree.fromstring(payload)
    u = urlparse(root.attrib['ivorn'])
    stream_path = u.path

    stream_obsv_dict = {'/SWIFT': 'Swift',
                        '/Fermi': 'Fermi',
                        '/INTEGRAL': 'INTEGRAL'}
    event_observatory = stream_obsv_dict[stream_path]

    ext_group = 'Test' if root.attrib['role'] == 'test' else 'External'

    #  Block Test INTEGRAL events on the production server to prevent
    #  unneeded queries of old GW data during detchar check
    if event_observatory == 'INTEGRAL' and ext_group == 'Test' and \
            app.conf['gracedb_host'] == 'gracedb.ligo.org':
        return
    #  Get TrigID
    elif event_observatory == 'INTEGRAL' and \
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
        label = 'NOT_GRB'
    else:
        label = None

    ivorn = root.attrib['ivorn']
    if 'subthresh' in ivorn.lower():
        search = 'SubGRB'
    elif 'mdc-test_event' in ivorn.lower():
        search = 'MDC'
    else:
        search = 'GRB'

    if search == 'SubGRB' and event_observatory == 'Fermi':
        skymap_link = \
            root.find("./What/Param[@name='HealPix_URL']").attrib['value']
    else:
        skymap_link = None

    query = 'group: External pipeline: {} grbevent.trigger_id = "{}"'.format(
        event_observatory, trig_id)

    (
        gracedb.get_events.si(query=query)
        |
        _create_replace_external_event_and_skymap.s(
            payload, search, event_observatory,
            ext_group=ext_group, label=label,
            notice_date=root.find("./Who/Date").text,
            notice_type=notice_type,
            skymap_link=skymap_link,
            use_radec=search in {'GRB', 'MDC'}
        )
    ).delay()


@igwn_alert.handler('superevent',
                    'mdc_superevent',
                    'external_fermi',
                    'external_swift',
                    'external_integral',
                    shared=False)
def handle_grb_igwn_alert(alert):
    """Parse an IGWN alert message related to superevents/GRB external triggers
    and dispatch it to other tasks.

    Notes
    -----
    This IGWN alert message handler is triggered by creating a new superevent
    or GRB external trigger event, a label associated with completeness of
    skymaps or change in state, or if a sky map file is uploaded:

    *   New event/superevent triggers a coincidence search with
        :meth:`gwcelery.tasks.raven.coincidence_search`.
    *   If other type of IGWN alert, pass to _handle_skymaps to decide whether
        to re-run RAVEN pipeline based on labels or whether to add labels that
        could start this process.

    Parameters
    ----------
    alert : dict
        IGWN alert packet

    """
    # Determine GraceDB ID
    graceid = alert['uid']

    # launch searches
    if alert['alert_type'] == 'new':
        if alert['object'].get('group') == 'External':
            # launch search with MDC events and exit
            if alert['object']['search'] == 'MDC':
                raven.coincidence_search(graceid, alert['object'],
                                         group='CBC', se_searches=['MDC'])
                raven.coincidence_search(graceid, alert['object'],
                                         group='Burst', se_searches=['MDC'])
                return

            elif alert['object']['search'] == 'SubGRB':
                # Launch search with standard CBC
                raven.coincidence_search(
                    graceid, alert['object'],
                    searches=['SubGRB'],
                    group='CBC',
                    pipelines=[alert['object']['pipeline']])
                # Launch search with CWB BBH
                raven.coincidence_search(
                    graceid, alert['object'],
                    searches=['SubGRB'],
                    group='Burst',
                    se_searches=['BBH'],
                    pipelines=[alert['object']['pipeline']])
            elif alert['object']['search'] == 'SubGRBTargeted':
                # if sub-threshold GRB, launch search with that pipeline
                raven.coincidence_search(
                    graceid, alert['object'],
                    searches=['SubGRBTargeted'],
                    se_searches=['AllSky', 'BBH'],
                    pipelines=[alert['object']['pipeline']])
            elif alert['object']['search'] == 'GRB':
                # launch standard Burst-GRB search
                raven.coincidence_search(graceid, alert['object'],
                                         group='Burst', se_searches=['AllSky'])

                # launch standard CBC-GRB search and similar BBH search
                raven.coincidence_search(graceid, alert['object'],
                                         group='CBC', searches=['GRB'])
                raven.coincidence_search(graceid, alert['object'],
                                         group='Burst', searches=['GRB'],
                                         se_searches=['BBH'])
        elif 'S' in graceid:
            # launch standard GRB search based on group
            gw_group = alert['object']['preferred_event_data']['group']
            search = alert['object']['preferred_event_data']['search']
            CBC_like = gw_group == 'CBC' or search == 'BBH'

            # launch search with MDC events and exit
            if search == 'MDC':
                raven.coincidence_search(graceid, alert['object'],
                                         group=gw_group, searches=['MDC'])
                return
            # Don't run search for IMBH Burst search
            elif gw_group == 'Burst' and \
                    search.lower() not in {'allsky', 'bbh'}:
                return

            # Keep empty if CBC (field not needed), otherwise use AllSky or BBH
            se_searches = ([] if gw_group == 'CBC' else
                           [alert['object']['preferred_event_data']['search']])
            searches = (['SubGRB', 'SubGRBTargeted'] if CBC_like else
                        ['SubGRBTargeted'])
            # launch standard GRB search
            raven.coincidence_search(graceid, alert['object'],
                                     group=gw_group, searches=['GRB'],
                                     se_searches=se_searches)
            # launch subthreshold search for Fermi and Swift separately to use
            # different time windows, for both CBC and Burst
            for pipeline in ['Fermi', 'Swift']:
                raven.coincidence_search(
                    graceid, alert['object'], group=gw_group,
                    searches=searches, pipelines=[pipeline])
    else:
        _handle_skymaps(alert)


def _handle_skymaps(alert):
    """Parse an IGWN alert message related to superevents/GRB external triggers
    and dispatch tasks related to re-running RAVEN pipeline due to new sky
    maps.

    Notes
    -----
    This IGWN alert message handler is triggered by a label associated with
    completeness of skymaps or change in state, or if a sky map file is
    uploaded:

    *   When both a GW and GRB sky map are available during a coincidence,
        indicated by the labels ``EM_READY`` and ``EXT_SKYMAP_READY``
        respectively on the external event, this triggers the spacetime coinc
        FAR to be calculated and a combined GW-GRB sky map is created using
        :meth:`gwcelery.tasks.external_skymaps.create_combined_skymap`.
    *   If new label indicates sky maps are available in the superevent,
        apply to all associated external events.
    *   Re-run sky map comparison if complete, and also if either the GW or GRB
        sky map has been updated or if the preferred event changed.
    *   Re-check RAVEN publishing conditions if the GRB was previously
        considered non-astrophysical but now should be considered.

    Parameters
    ----------
    alert : dict
        IGWN alert packet

    """
    # Determine GraceDB ID
    graceid = alert['uid']

    # Define state variables
    is_superevent = 'S' in graceid
    is_external_event = alert['object'].get('group') == 'External'
    is_coincidence = 'EM_COINC' in alert['object']['labels']
    pe_ready = 'PE_READY' in alert['object']['labels']

    # re-run raven pipeline and create combined sky map (if not a Swift event)
    # when sky maps are available
    if alert['alert_type'] == 'label_added' and is_external_event:
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
        elif is_coincidence:
            # if not complete, check if GW sky map; apply label to external
            # event if GW sky map
            se_labels = gracedb.get_labels(alert['object']['superevent'])
            if 'SKYMAP_READY' in se_labels:
                gracedb.create_label.si('SKYMAP_READY', graceid).delay()
            if 'EM_READY' in se_labels:
                gracedb.create_label.si('EM_READY', graceid).delay()
    # apply labels from superevent to external event to update state
    # and trigger functionality requiring sky maps, etc.
    elif alert['alert_type'] == 'label_added' and is_superevent:
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
            is_coincidence and \
            'fit' in alert['data']['filename'] and \
            'flat' not in alert['data']['comment'].lower() and \
            (alert['data']['filename'] !=
             external_skymaps.COMBINED_SKYMAP_FILENAME_MULTIORDER):
        superevent_id, external_id = _get_superevent_ext_ids(
                                         graceid, alert['object'])
        # check if combined sky map already made, with the exception of Swift
        # which will fail
        if is_superevent:
            superevent = alert['object']
            # Rerun for all eligible external events
            for ext_id in superevent['em_events']:
                external_event = gracedb.get_event(ext_id)
                if REQUIRED_LABELS_BY_TASK['compare'].issubset(
                        set(external_event['labels'])):
                    _relaunch_raven_pipeline_with_skymaps(
                        superevent, external_event, graceid,
                        use_superevent_skymap=True)
        else:
            superevent = gracedb.get_superevent(alert['object']['superevent'])
            external_event = alert['object']
            if REQUIRED_LABELS_BY_TASK['compare'].issubset(
                    set(external_event['labels'])):
                _relaunch_raven_pipeline_with_skymaps(
                    superevent, external_event, graceid)
    # Rerun the coincidence FAR calculation if possible with combined sky map
    # if the preferred event changes
    # We don't want to run this logic if PE results are present
    elif alert['alert_type'] == 'log' and not pe_ready and is_coincidence:
        new_log_comment = alert['data'].get('comment', '')
        if is_superevent and \
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
    elif alert['alert_type'] == 'label_removed' and is_external_event:
        if alert['data']['name'] == 'NOT_GRB' and is_coincidence:
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
    """Parse an IGWN alert message related to superevents/Supernovae external
    triggers and dispatch it to other tasks.

    Notes
    -----
    This igwn_alert message handler is triggered whenever a new superevent
    or Supernovae external event is created:

    *   New event triggers a coincidence search with
        :meth:`gwcelery.tasks.raven.coincidence_search`.

    Parameters
    ----------
    alert : dict
        IGWN alert packet

    """
    # Determine GraceDB ID
    graceid = alert['uid']

    if alert['alert_type'] == 'new':
        if alert['object'].get('superevent_id'):
            group = alert['object']['preferred_event_data']['group']
            search = alert['object']['preferred_event_data']['search']
            searches = ['MDC'] if search == 'MDC' else ['Supernova']
            se_searches = ['MDC'] if search == 'MDC' else ['AllSky']
            # Run only on Test and Burst superevents
            if group in {'Burst', 'Test'} and search in {'MDC', 'AllSky'}:
                raven.coincidence_search(graceid, alert['object'],
                                         group='Burst', searches=searches,
                                         se_searches=se_searches,
                                         pipelines=['SNEWS'])
        else:
            # Run on SNEWS event, either real or test
            search = alert['object']['search']
            if search == 'MDC':
                raven.coincidence_search(graceid, alert['object'],
                                         group='Burst', searches=['MDC'],
                                         se_searches=['MDC'],
                                         pipelines=['SNEWS'])
            elif search == 'Supernova':
                raven.coincidence_search(graceid, alert['object'],
                                         group='Burst', searches=['Supernova'],
                                         se_searches=['AllSky'],
                                         pipelines=['SNEWS'])


@alerts.handler('fermi_targeted',
                'swift_targeted',
                queue='exttrig',
                shared=False)
def handle_targeted_kafka_alert(alert):
    """Parse an alert sent via Kafka from a MOU partner in our joint
    subthreshold targeted search.

    Parameters
    ----------
    alert : dict
        Kafka alert packet

    """
    # Convert alert to VOEvent format
    # FIXME: This is required until native ingesting of kafka events in GraceDB
    payload, pipeline, time, trig_id = \
        _kafka_to_voevent(alert, 'SubGRBTargeted')

    # Veto events that don't pass GRB FAR threshold
    far_grb = alert['far']
    veto_event = \
        app.conf['raven_targeted_far_thresholds']['GRB'][pipeline] < far_grb
    label = ('NOT_GRB' if alert['alert_type'] == "retraction" or veto_event
             else None)

    # Look whether a previous event with the same ID exists
    query = 'group: External pipeline: {} grbevent.trigger_id = "{}"'.format(
        pipeline, trig_id)

    (
        gracedb.get_events.si(query=query)
        |
        _create_replace_external_event_and_skymap.s(
            payload, 'SubGRBTargeted', pipeline,
            label=label, notice_date=time,
            skymap=alert.get('healpix_file'),
            use_radec=('ra' in alert and 'dec' in alert)
        )
    ).delay()


def _skymaps_are_ready(event, label, task):
    """Determine whether labels are complete to launch a certain task.

    Parameters
    ----------
    event : dict
        Either Superevent or external event dictionary
    graceid : str
        GraceDB ID
    task : str
        Determines which label schmema to check for completeness

    Returns
    -------
    labels_pass : bool
        True if all the require labels are present and the given label is part
        of that set
    """
    label_set = set(event['labels'])
    required_labels = REQUIRED_LABELS_BY_TASK[task]
    return required_labels.issubset(label_set) and label in required_labels


def _get_superevent_ext_ids(graceid, event):
    """Grab superevent and external event IDs from a given event.

    Parameters
    ----------
    graceid : str
        GraceDB ID
    event : dict
        Either Superevent or external event dictionary

    Returns
    -------
    se_id, ext_id : tuple
        Tuple of superevent and external event GraceDB IDs

    """
    if 'S' in graceid:
        se_id = event['superevent_id']
        ext_id = event['em_type']
    else:
        se_id = event['superevent']
        ext_id = event['graceid']
    return se_id, ext_id


@app.task(shared=False)
def _launch_external_detchar(event):
    """Launch detchar tasks for an external event.

    Parameters
    ----------
    event : dict
        External event dictionary

    Returns
    -------
    event : dict
        External event dictionary

    """
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
    the joint FAR with updated sky map info and creating a new combined sky
    map.

    Parameters
    ----------
    superevent : dict
        Superevent dictionary
    exttrig : dict
        External event dictionary
    graceid : str
        GraceDB ID of event
    use_superevent_skymap : bool
        If True/False, use/don't use skymap info from superevent.
        Else if None, check SKYMAP_READY label in external event.

    """
    gw_group = superevent['preferred_event_data']['group']
    tl, th = raven._time_window(
        graceid, gw_group, [ext_event['pipeline']], [ext_event['search']],
        [superevent['preferred_event_data']['search']])
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
    # Create new updated combined sky map
    canvas |= external_skymaps.create_combined_skymap.si(
                  superevent['superevent_id'], ext_event['graceid'],
                  preferred_event=(
                      None if use_superevent
                      else superevent['preferred_event']))
    canvas.delay()


@app.task(shared=False,
          queue='exttrig')
def _create_replace_external_event_and_skymap(
        events, payload, search, pipeline,
        label=None, ext_group='External', notice_date=None, notice_type=None,
        skymap=None, skymap_link=None, use_radec=False):
    """Either create a new external event or replace an old one if applicable
    Then either uploads a given sky map, try to download one given a link, or
    create one given coordinates.

    Parameters
    ----------
    events : list
        List of external events sharing the same trigger ID
    payload : str
        VOEvent of event being considered
    search : str
        Search of external event
    pipeline : str
        Pipeline of external evevent
    label : list
        Label to be uploaded along with external event. If None, removes
        'NOT_GRB' label from event
    ext_group : str
        Group of external event, 'External' or 'Test'
    notice_date : str
        External event trigger time in ISO format
    notice_type : int
        GCN notice type integer
    skymap : str
        Base64 encoded sky map
    skymap_link : str
        Link to external sky map to be downloaded
    use_radec : bool
        If True, try to create sky map using given coordinates

    """
    skymap_detchar_canvas = ()
    # If previous event, try to append
    if events and ext_group == 'External':
        assert len(events) == 1, 'Found more than one matching GraceDB entry'
        event, = events
        graceid = event['graceid']
        if label:
            create_replace_canvas = gracedb.create_label.si(label, graceid)
        else:
            create_replace_canvas = gracedb.remove_label.si('NOT_GRB', graceid)

        # Prevent SubGRBs from appending GRBs, also append if same search
        if search == 'GRB' or search == event['search']:
            # Replace event and pass already existing event dictionary
            create_replace_canvas |= gracedb.replace_event.si(graceid, payload)
            create_replace_canvas |= gracedb.get_event.si(graceid)
        else:
            # If not appending just exit
            return

    # If new event, create new entry in GraceDB and launch detchar
    else:
        create_replace_canvas = gracedb.create_event.si(
            filecontents=payload,
            search=search,
            group=ext_group,
            pipeline=pipeline,
            labels=[label] if label else None)
        skymap_detchar_canvas += _launch_external_detchar.s(),

    # Use sky map if provided
    if skymap:
        skymap_detchar_canvas += \
            external_skymaps.read_upload_skymap_from_base64.s(skymap),
    # Otherwise upload one based on given info
    else:
        # Grab sky map from provided link
        if skymap_link:
            skymap_detchar_canvas += \
                external_skymaps.get_upload_external_skymap.s(skymap_link),
        # Otherwise if FINAL Fermi notice try to grab sky map
        elif notice_type == gcn.NoticeType.FERMI_GBM_FIN_POS:
            # Wait 10 min and then look for official Fermi sky map
            skymap_detchar_canvas += \
                external_skymaps.get_upload_external_skymap.s(None).set(
                    countdown=600),
        # Otherwise create sky map from given coordinates
        if use_radec:
            skymap_detchar_canvas += \
                external_skymaps.create_upload_external_skymap.s(
                    notice_type, notice_date),

    (
        create_replace_canvas
        |
        group(skymap_detchar_canvas)
    ).delay()


def _kafka_to_voevent(alert, search):
    """Parse an alert in JSON format sent via Kafka from public GCN Notices or
    from a MOU partner in our joint subthreshold targeted search, converting
    to an equivalent XML string VOEvent.

    Parameters
    ----------
    alert : dict
        Kafka alert packet
    search : list
        External trigger search

    Returns
    -------
    payload : str
        XML GCN notice alert packet in string format

    """
    # Define basic values
    pipeline = alert.get('mission')
    if pipeline is None:
        if 'instrument' not in alert:
            raise ValueError("Alert does not contain pipeline information.")
        elif alert['instrument'].lower() == 'wxt':
            # FIXME: Replace with official GraceDB name once added
            pipeline = 'EinsteinProbe'

    # Get times, adding missing
    start_time = alert['trigger_time']
    alert_time = alert.get('alert_datetime', start_time)
    # If missing, add character onto end
    if not start_time.endswith('Z'):
        start_time += 'Z'
    if not alert_time.endswith('Z'):
        alert_time += 'Z'

    # Try to get FAR is there
    far = alert.get('far')

    # Go through list of possible values for durations, 0. if no keys match
    duration = 0.
    duration_params = ['rate_duration', 'image_duration']
    for param in duration_params:
        if param in alert.keys():
            duration = alert[param]
            break

    # Form ID out of given list
    id = '_'.join(str(x) for x in alert['id'])
    if search == 'SubGRBTargeted':
        # Use central time since starting time is not well measured
        central_time = \
            Time(start_time, format='isot', scale='utc').to_value('gps') + \
            .5 * duration
        trigger_time = \
            str(Time(central_time, format='gps', scale='utc').isot) + 'Z'
    else:
        trigger_time = start_time

    # sky localization may not be available
    ra = alert.get('ra')
    dec = alert.get('dec')

    # Go through list of possible values for error, None if no keys match
    error = None
    error_params = ['dec_uncertainty', 'ra_uncertainty', 'ra_dec_error']
    for param in error_params:
        if param in alert.keys():
            error = alert[param]
            break

    # If argument is list, get value
    if isinstance(error, list):
        error = error[0]

    # if any missing sky map info, set to zeros so will be ignored later
    if ra is None or dec is None or error is None:
        ra, dec, error = 0., 0., 0.

    # Load template
    fname = \
        str(Path(__file__).parent /
            f'../tests/data/{pipeline.lower()}_{search.lower()}_template.xml')
    root = etree.parse(fname)

    # Update template values
    # Change ivorn to indicate this is a subthreshold targeted event
    root.xpath('.')[0].attrib['ivorn'] = \
        'ivo://lvk.internal/{0}#{1}-{2}'.format(
            pipeline.lower(),
            ('targeted_subthreshold' if search == 'SubGRBTargeted'
             else search.lower()),
            trigger_time).encode()

    # Change times to chosen time
    root.find("./Who/Date").text = str(alert_time).encode()
    root.find(("./WhereWhen/ObsDataLocation/"
               "ObservationLocation/AstroCoords/Time/TimeInstant/"
               "ISOTime")).text = str(trigger_time).encode()

    # Update ID and duration
    # SVOM has separate template, use different paths
    if pipeline == 'SVOM':
        root.find(("./What/Group[@name='Svom_Identifiers']"
                   "/Param[@name='Burst_Id']")).attrib['value'] = \
            id.encode()

        root.find(("./What/Group[@name='Detection_Info']"
                  "/Param[@name='Timescale']")).attrib['value'] = \
            str(duration).encode()
    # Every other type uses similar format
    else:
        root.find("./What/Param[@name='TrigID']").attrib['value'] = \
            id.encode()

        root.find("./What/Param[@name='Integ_Time']").attrib['value'] = \
            str(duration).encode()

    if far is not None:
        root.find("./What/Param[@name='FAR']").attrib['value'] = \
            str(far).encode()

    # Sky position
    root.find(("./WhereWhen/ObsDataLocation/"
               "ObservationLocation/AstroCoords/Position2D/Value2/"
               "C1")).text = str(ra).encode()
    root.find(("./WhereWhen/ObsDataLocation/"
               "ObservationLocation/AstroCoords/Position2D/Value2/"
               "C2")).text = str(dec).encode()
    root.find(("./WhereWhen/ObsDataLocation/"
               "ObservationLocation/AstroCoords/Position2D/"
               "Error2Radius")).text = str(error).encode()

    return (etree.tostring(root, xml_declaration=True, encoding="UTF-8",
                           pretty_print=True),
            pipeline, trigger_time.replace('Z', ''), id)
