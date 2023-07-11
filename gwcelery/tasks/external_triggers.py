from lxml import etree
from urllib.parse import urlparse
from celery import group
from celery.utils.log import get_logger
from pathlib import Path

from ..import app
from . import alerts
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

    (
        gracedb.get_events.si(query=query)
        |
        _create_replace_external_event_and_skymap.s(
            payload, search, event_observatory, ext_group=ext_group
        )
    ).delay()


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
            # launch search with MDC events and exit
            if alert['object']['search'] == 'MDC':
                raven.coincidence_search(graceid, alert['object'],
                                         group='CBC', se_searches=['MDC'])
                raven.coincidence_search(graceid, alert['object'],
                                         group='Burst', se_searches=['MDC'])
                return

            if alert['object']['search'] in ['SubGRB', 'SubGRBTargeted']:
                # if sub-threshold GRB, launch search with that pipeline
                raven.coincidence_search(
                    graceid, alert['object'],
                    searches=['SubGRB', 'SubGRBTargeted'],
                    se_searches=['AllSky'],
                    pipelines=[alert['object']['pipeline']])
            else:
                # launch standard Burst-GRB search
                raven.coincidence_search(graceid, alert['object'],
                                         group='Burst', se_searches=['AllSky'])

                # launch standard CBC-GRB search
                raven.coincidence_search(graceid, alert['object'],
                                         group='CBC', searches=['GRB'])
        elif 'S' in graceid:
            # launch standard GRB search based on group
            gw_group = alert['object']['preferred_event_data']['group']
            search = alert['object']['preferred_event_data']['search']

            # launch search with MDC events and exit
            if alert['object']['preferred_event_data']['search'] == 'MDC':
                raven.coincidence_search(graceid, alert['object'],
                                         group=gw_group, searches=['MDC'])
                return
            # Don't run search for BBH or IMBH Burst search
            elif gw_group == 'Burst' and search.lower() != 'allsky':
                return

            se_searches = [] if gw_group == 'CBC' else ['AllSky']
            searches = (['SubGRB', 'SubGRBTargeted'] if gw_group == 'CBC' else
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


@alerts.handler('fermi',
                'swift')
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
    payload, pipeline, time, trig_id = _kafka_to_voevent(alert)

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


@app.task(shared=False)
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
        Time and date of external event
    notice_type : str
        GCN Notice type of external event
    skymap : str
        Base64 encoded sky map
    skymap_link : str
        Link to external sky map to be downloaded
    use_radec : bool
        If true, try to create sky map using given coordinates

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
    else:
        # Otherwise grab sky map from provided link
        if skymap_link:
            skymap_detchar_canvas += \
                external_skymaps.get_upload_external_skymap.s(skymap_link),
        # Otherwise if threshold Fermi try to grab sky map
        elif pipeline == 'Fermi' and search == 'GRB':
            skymap_detchar_canvas += \
                external_skymaps.get_upload_external_skymap.s(None),
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


def _kafka_to_voevent(alert):
    # Define basic values
    pipeline = alert['mission']
    trigger_time = alert['trigger_time']
    alert_time = alert['alert_datetime']
    far = alert['far']
    integration_time = alert['rate_duration']
    id = '_'.join(str(x) for x in alert['id'])

    # sky localization may not be available
    ra = alert.get('ra')
    dec = alert.get('dec')
    # Try to get dec first then ra, None if both misssing
    error = alert.get('dec_uncertainty')
    if error is None:
        error = alert.get('ra_uncertainty')
    # Argument should be list if not None
    if isinstance(error, list):
        error = error[0]
    # if any missing sky map info, set to zeros so will be ignored later
    if ra is None or dec is None or error is None:
        ra, dec, error = 0., 0., 0.

    # Load template
    fname = str(Path(__file__).parent /
                '../tests/data/{}_subgrbtargeted_template.xml'.format(
                    pipeline.lower()))
    root = etree.parse(fname)

    # Update template values
    # Change ivorn to indicate this is a subthreshold targeted event
    root.xpath('.')[0].attrib['ivorn'] = \
        'ivo://lvk.internal/{0}#targeted_subthreshold-{1}'.format(
            pipeline.lower(), trigger_time).encode()

    # Update ID
    root.find("./What/Param[@name='TrigID']").attrib['value'] = \
        id.encode()

    # Change times to chosen time
    root.find("./Who/Date").text = str(alert_time).encode()
    root.find(("./WhereWhen/ObsDataLocation/"
               "ObservationLocation/AstroCoords/Time/TimeInstant/"
               "ISOTime")).text = str(trigger_time).encode()

    root.find("./What/Param[@name='FAR']").attrib['value'] = \
        str(far).encode()

    root.find("./What/Param[@name='Integ_Time']").attrib['value'] = \
        str(integration_time).encode()

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
