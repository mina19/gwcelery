"""Create mock external events to be in coincidence
   with MDC superevents."""

import random
import re
from pathlib import Path

import numpy as np
from astropy.time import Time
from lxml import etree

from .. import app
from ..tests import data
from ..util import read_json
from . import external_triggers, igwn_alert, raven


def create_upload_external_event(gpstime, pipeline, ext_search):
    """Create a random external event VOEvent or Kafka alert packet.

    Parameters
    ----------
    gpstime : float
        Event's GPS time
    pipeline : str
        External trigger pipeline name
    ext_search : str
        Search field for external event

    Returns
    -------
    event : str or dict
        Alert packet in format as if it was sent from GCN or Kafka, in a
        XML GCN notice alert packet in string format for the GRB or SubGRB
        search or a dictionary Kafka packet if for SubGRBTargeted search.
    """
    new_date = str(Time(gpstime, format='gps', scale='utc').isot) + 'Z'
    new_TrigID = str(int(gpstime))
    ra = random.uniform(0, 360)
    thetas = np.arange(-np.pi / 2, np.pi / 2, .01)
    dec = random.choices(np.rad2deg(thetas),
                         weights=np.cos(thetas) / sum(np.cos(thetas)))[0]
    error = .05 if pipeline == 'Swift' else random.uniform(1, 30)

    if pipeline in {'Fermi', 'Swift', 'INTEGRAL'}:
        is_grb = True
    else:
        is_grb = False

    # If SubGRBTargeted modify alert packet from Kafka
    if ext_search == 'SubGRBTargeted':
        if pipeline == 'Fermi':
            # Template based on:
            # https://github.com/joshuarwood/gcn-schema/tree/main/gcn/notices/
            # fermi/gbm
            alert = read_json(data, 'kafka_alert_fermi.json')
            # Remove sky map file to create our own sky map
            alert.pop('healpix_file')
            alert['dec_uncertainty'] = [error]
        elif pipeline == 'Swift':
            # Template based on:
            # https://github.com/nasa-gcn/gcn-schema/tree/main/gcn/notices/
            # swift/bat/guano
            alert = read_json(data, 'kafka_alert_swift.json')
            alert['ra_uncertainty'] = [error]
        else:
            raise ValueError(
                'Only use Fermi or Swift for SubGRBTargeted search')

        alert['trigger_time'] = new_date
        alert['alert_datetime'] = new_date
        alert['id'] = [new_TrigID]
        alert['ra'] = ra
        alert['dec'] = dec
        # Generate FAR from max threshold and trials factors
        alert['far'] = \
            (app.conf['raven_targeted_far_thresholds']['GRB'][pipeline] *
             random.uniform(0, 1))

        external_triggers.handle_targeted_kafka_alert(alert)

        # Return VOEvent for testing until GraceDB natively ingests kafka
        # alerts
        return external_triggers._kafka_to_voevent(alert, ext_search)[0]

    # Otherwise modify respective VOEvent template
    else:
        fname = str(Path(__file__).parent /
                    '../tests/data/{0}{1}_{2}gcn.xml'.format(
                        pipeline.lower(),
                        '_subthresh' if ext_search == 'SubGRB' else '',
                        'grb_' if is_grb else ''))

        root = etree.parse(fname)
        # Change ivorn to indicate if this is an MDC event or O3 replay event
        root.xpath('.')[0].attrib['ivorn'] = \
            'ivo://lvk.internal/{0}#{1}{2}_event_{3}'.format(
                pipeline if pipeline != 'Swift' else 'SWIFT',
                '_subthresh' if ext_search == 'SubGRB' else '',
                'MDC-test' if ext_search == 'MDC' else 'O3-replay',
                new_date).encode()

        # Change times to chosen time
        root.find("./Who/Date").text = str(new_date).encode()
        root.find(("./WhereWhen/ObsDataLocation/"
                   "ObservationLocation/AstroCoords/Time/TimeInstant/"
                   "ISOTime")).text = str(new_date).encode()
        if ext_search == 'SubGRB':
            root.find("./What/Param[@name='Trans_Num']").attrib['value'] = \
                str(new_TrigID).encode()
        else:
            root.find("./What/Param[@name='TrigID']").attrib['value'] = \
                str(new_TrigID).encode()

        if is_grb:
            # Give random sky position
            root.find(("./WhereWhen/ObsDataLocation/"
                       "ObservationLocation/AstroCoords/Position2D/Value2/"
                       "C1")).text = str(ra).encode()
            root.find(("./WhereWhen/ObsDataLocation/"
                       "ObservationLocation/AstroCoords/Position2D/Value2/"
                       "C2")).text = str(dec).encode()
            if pipeline != 'Swift':
                root.find(
                    ("./WhereWhen/ObsDataLocation/"
                     "ObservationLocation/AstroCoords/Position2D/"
                     "Error2Radius")).text = str(error).encode()

        event = etree.tostring(root, xml_declaration=True, encoding="UTF-8",
                               pretty_print=True)

        # Upload as from GCN
        if is_grb:
            external_triggers.handle_grb_gcn(event)
        else:
            external_triggers.handle_snews_gcn(event)

        return event


def _offset_time(gpstime, group, pipeline, ext_search, se_search):
    """Offsets the given GPS time by applying a random number within a time
    window, determine by the search being done.

    Parameters
    ----------
    gpstime : float
        Event's GPS time
    group : str
        Burst or CBC
    pipeline : str
        Pipeline field for external event
    ext_search : str
        Search field for external event
    se_search : list
        Search field for superevent

    Returns
    -------
    gsptime_adjusted : float
        Event's original gps time plus a random number within the determined
        search window

    """
    tl, th = raven._time_window('S1', group, [pipeline],
                                [ext_search], [se_search])
    return gpstime + random.uniform(tl, th)


def _is_joint_mdc(graceid, se_search):
    """Determine whether to upload an external events using user-defined
    frequency of MDC or AllSky superevents.

    Looks at the ending letters of a superevent (e.g. 'ac' from 'MS190124ac'),
    converts to a number, and checks if divisible by a number given in the
    configuration file.

    For example, if the configuration number
    :obj:`~gwcelery.conf.joint_mdc_freq` is 10,
    this means joint events with superevents ending with 'j', 't', 'ad', etc.

    Parameters
    ----------
    graceid : str
        GraceDB ID of superevent
    se_search : str
        Search field for preferred event in superevent

    Returns
    -------
    is_joint_mdc : bool
        Returns True if the GraceDB ID matches pre-determined frequency rates

    """
    end_string = re.split(r'\d+', graceid)[-1].lower()
    val = 0
    for i in range(len(end_string)):
        val += (ord(end_string[i]) - 96) * 26 ** (len(end_string) - i - 1)
    return val % int(app.conf['joint_mdc_freq']) == 0 if se_search == 'MDC' \
        else val % int(app.conf['joint_O3_replay_freq']) == 0


@igwn_alert.handler('mdc_superevent',
                    'superevent',
                    shared=False)
def upload_external_event(alert, ext_search=None):
    """Upload a random GRB event(s) for a certain fraction of MDC
    or O3-replay superevents.

    Notes
    -----
    Every n superevents, upload a GRB candidate within the
    standard CBC-GRB or Burst-GRB search window, where the frequency n is
    determined by the configuration variable
    :obj:`~gwcelery.conf.joint_mdc_freq` or
    :obj:`~gwcelery.conf.joint_O3_replay_freq`.

    For O3 replay testing with RAVEN pipeline, only runs on gracedb-playground.

    Parameters
    ----------
    alert : dict
        IGWN alert packet
    ext_search : str
        Search field for external event

    Returns
    -------
    events, pipelines : tuple
        Returns tuple of the list of external events created and the list of
        pipelines chosen for each event

    """
    if alert['alert_type'] != 'new':
        return
    se_search = alert['object']['preferred_event_data']['search']
    group = alert['object']['preferred_event_data']['group']
    is_gracedb_playground = app.conf['gracedb_host'] \
        == 'gracedb-playground.ligo.org'
    joint_mdc_alert = se_search == 'MDC' and _is_joint_mdc(alert['uid'], 'MDC')
    joint_o3replay_alert = se_search in {'AllSky', 'BBH'} and \
        _is_joint_mdc(alert['uid'], 'AllSky') and is_gracedb_playground and \
        group in {'CBC', 'Burst'}
    if not (joint_mdc_alert or joint_o3replay_alert):
        return

    # Potentially upload 1, 2, or 3 GRB events
    num = 1 + np.random.choice(np.arange(3), p=[.6, .3, .1])

    if joint_mdc_alert:
        # If joint MDC alert, make external event MDC
        if ext_search is None:
            ext_search = 'MDC'
        elif ext_search == 'MDC':
            pass
        else:
            raise ValueError('External search must be "MDC" if MDC superevent')
    # If O3 replay, choose search from acceptable list
    elif ext_search is None:
        # Determine search for external event and then pipelines
        ext_search = \
            (np.random.choice(['GRB', 'SubGRB', 'SubGRBTargeted'],
                              p=[.6, .1, .3])
             if joint_o3replay_alert else 'GRB')
    # Choose pipeline(s) based on search
    if ext_search in {'GRB', 'MDC'}:
        pipelines = np.random.choice(['Fermi', 'Swift', 'INTEGRAL'],
                                     p=[.6, .3, .1,],
                                     size=num, replace=False)
    elif ext_search == 'SubGRB':
        pipelines = np.full(num, 'Fermi')
    elif ext_search == 'SubGRBTargeted':
        # Only two current pipelines in targeted search so map 3 => 2
        num = min(num, 2)
        pipelines = np.random.choice(['Fermi', 'Swift'],
                                     p=[.5, .5], size=num, replace=False)
    events = []
    for pipeline in pipelines:
        gpstime = float(alert['object']['t_0'])
        new_time = _offset_time(gpstime, group, pipeline,
                                ext_search, se_search)

        # Choose external grb pipeline to simulate
        ext_event = create_upload_external_event(
                        new_time, pipeline, ext_search)

        events.append(ext_event)

    return events, pipelines


@app.task(ignore_result=True,
          shared=False)
def upload_snews_event():
    """Create and upload a SNEWS-like MDC external event.

    Returns
    -------
    ext_event : str
        XML GCN notice alert packet in string format
    """
    current_time = Time(Time.now(), format='gps').value
    ext_event = create_upload_external_event(current_time, 'SNEWS', 'MDC')
    return ext_event
