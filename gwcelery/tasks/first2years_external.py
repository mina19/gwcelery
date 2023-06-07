"""Create mock external events to be in coincidence
   with MDC superevents."""

from astropy.time import Time
from lxml import etree
import numpy as np
from pathlib import Path
import random
import re

from ..import app
from . import external_triggers
from . import igwn_alert


def create_external_event(gpstime, pipeline, se_search):
    """ Create a random external event VOEvent.

    Parameters
    ----------
    gpstime : float
        Event's gps time
    pipeline : str
        External trigger pipeline name
    se_search : str
        Search field for preferred event, 'MDC' or 'AllSky'
    """
    new_date = str(Time(gpstime, format='gps', scale='utc').isot) + 'Z'
    new_TrigID = str(int(gpstime))

    if pipeline in {'Fermi', 'Swift', 'INTEGRAL', 'AGILE'}:
        is_grb = True
    else:
        is_grb = False

    fname = str(Path(__file__).parent /
                '../tests/data/{0}_{1}gcn.xml'.format(
                    pipeline.lower(),
                    'grb_' if is_grb else ''))

    root = etree.parse(fname)
    # Change ivorn to indicate if this is an MDC event or O3 replay event
    root.xpath('.')[0].attrib['ivorn'] = \
        'ivo://lvk.internal/{0}#{1}_event{2}'.format(
            pipeline if pipeline != 'Swift' else 'SWIFT',
            'MDC-test' if se_search == 'MDC' else 'O3-replay',
            new_date).encode()

    # Change times to chosen time
    root.find("./Who/Date").text = str(new_date).encode()
    root.find(("./WhereWhen/ObsDataLocation/"
               "ObservationLocation/AstroCoords/Time/TimeInstant/"
               "ISOTime")).text = str(new_date).encode()
    root.find("./What/Param[@name='TrigID']").attrib['value'] = \
        str(new_TrigID).encode()

    if is_grb:
        # Give random sky position
        root.find(("./WhereWhen/ObsDataLocation/"
                   "ObservationLocation/AstroCoords/Position2D/Value2/"
                   "C1")).text = str(random.uniform(0, 360)).encode()
        thetas = np.arange(-np.pi / 2, np.pi / 2, .01)
        root.find(("./WhereWhen/ObsDataLocation/"
                   "ObservationLocation/AstroCoords/Position2D/Value2/"
                   "C2")).text = \
            str(random.choices(
                np.rad2deg(thetas),
                weights=np.cos(thetas) / sum(np.cos(thetas)))[0]).encode()
        if pipeline != 'Swift':
            root.find(
                ("./WhereWhen/ObsDataLocation/"
                 "ObservationLocation/AstroCoords/Position2D/"
                 "Error2Radius")).text = str(random.uniform(1, 30)).encode()

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8",
                          pretty_print=True)


def _offset_time(gpstime, group):
    """ This function checks coincident time windows for superevents if they
    are of Burst or CBC group.

       Parameters
       ----------
       gpstime : float
           Event's gps time
       group : str
           Burst or CBC
    """
    if group == 'Burst':
        th, tl = app.conf['raven_coincidence_windows']['GRB_Burst']
    elif group == 'CBC':
        th, tl = app.conf['raven_coincidence_windows']['GRB_CBC']
    else:
        raise AssertionError(
            'Invalid group {}. Use only CBC or Burst.'.format(group))
    return gpstime + random.uniform(-tl, -th)


def _is_joint_mdc(graceid, se_search):
    """Upload external events to the user-defined frequency of MDC or AllSky
    superevents.

    Looks at the ending letters of a superevent (e.g. 'ac' from 'MS190124ac'),
    converts to a number, and checks if divisible by a number given in the
    configuration file.

    For example, if the configuration number
    :obj:`~gwcelery.conf.joint_mdc_freq` is 10,
    this means joint events with superevents ending with 'j', 't', 'ad', etc.
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
def upload_external_event(alert):
    """Upload a random GRB event for a certain percentage of MDC
    or O3-replay superevents.

    Notes
    -----
    Every n superevents, upload a GRB candidate within the
    standard CBC-GRB or Burst-GRB search window, where the frequency n is
    determined by the configuration variable
    :obj:`~gwcelery.conf.joint_mdc_freq` or
    :obj:`~gwcelery.conf.joint_O3_replay_freq`.

    For O3 replay testing with RAVEN pipeline, only run on gracedb-playground.
    """
    if alert['alert_type'] != 'new':
        return
    se_search = alert['object']['preferred_event_data']['search']
    group = alert['object']['preferred_event_data']['group']
    is_gracedb_playground = app.conf['gracedb_host'] \
        == 'gracedb-playground.ligo.org'
    joint_mdc_alert = se_search == 'MDC' and _is_joint_mdc(alert['uid'], 'MDC')
    joint_allsky_alert = se_search == 'AllSky' and \
        _is_joint_mdc(alert['uid'], 'AllSky') and is_gracedb_playground
    if not (joint_mdc_alert or joint_allsky_alert):
        return

    # Potentially upload 1, 2, or 3 GRB events
    num = 1 + np.random.choice(np.arange(3), p=[.6, .3, .1])
    events = []
    pipelines = []
    for i in range(num):
        gpstime = float(alert['object']['t_0'])
        new_time = _offset_time(gpstime, group)

        # Choose external grb pipeline to simulate
        pipeline = np.random.choice(['Fermi', 'Swift', 'INTEGRAL', 'AGILE'],
                                    p=[.5, .3, .1, .1])
        ext_event = create_external_event(new_time, pipeline, se_search)

        # Upload as from GCN
        external_triggers.handle_grb_gcn(ext_event)

        events.append(ext_event), pipelines.append(pipeline)

    return events, pipelines


@app.task(ignore_result=True,
          shared=False)
def upload_snews_event():
    """Create and upload a SNEWS-like MDC external event."""
    current_time = Time(Time.now(), format='gps').value
    ext_event = create_external_event(current_time, 'SNEWS', 'MDC')
    external_triggers.handle_snews_gcn(ext_event)
    return ext_event
