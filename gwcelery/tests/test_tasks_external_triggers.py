from importlib import resources
from unittest.mock import Mock, call, patch

import pytest
from lxml import etree

from .. import app
from ..tasks import detchar, external_skymaps, external_triggers
from ..util import read_binary, read_json
from . import data


@pytest.mark.parametrize('pipeline, path',
                         [['Fermi', 'fermi_grb_gcn.xml'],
                          ['Fermi_final', 'fermi_final_gcn.xml'],
                          ['INTEGRAL', 'integral_grb_gcn.xml'],
                          ['INTEGRAL_MDC', 'integral_mdc_gcn.xml']])
@patch('gwcelery.tasks.external_skymaps.create_upload_external_skymap.run')
@patch('gwcelery.tasks.external_skymaps.get_upload_external_skymap.run')
@patch('gwcelery.tasks.detchar.dqr_json', return_value='dqrjson')
@patch('gwcelery.tasks.gracedb.upload.run')
@patch('gwcelery.tasks.gracedb.create_event.run', return_value={
    'graceid': 'E1', 'gpstime': 1, 'instruments': '', 'pipeline': 'Fermi',
    'search': 'GRB',
    'extra_attributes': {'GRB': {'trigger_duration': 1, 'trigger_id': 123,
                                 'ra': 0., 'dec': 0., 'error_radius': 10.}},
    'links': {'self': 'https://gracedb.ligo.org/events/E356793/'}})
def test_handle_create_grb_event(mock_create_event,
                                 mock_upload, mock_json,
                                 mock_get_upload_external_skymap,
                                 mock_create_upload_external_skymap,
                                 pipeline, path):
    if pipeline == 'INTEGRAL':
        with resources.as_file(resources.files(data).joinpath(path)) as fname:
            root = etree.parse(fname)
        root.find("./What/Param[@name='TrigID']").attrib['value'] = \
            '123456'.encode()
        text = etree.tostring(root, xml_declaration=True, encoding="UTF-8")
    else:
        text = read_binary(data, path)
    external_triggers.handle_grb_gcn(payload=text)
    mock_create_event.assert_called_once_with(
        filecontents=text,
        search='GRB',
        pipeline=pipeline.split('_')[0],
        group='External',
        labels=None)
    calls = [
        call(
            '"dqrjson"', 'gwcelerydetcharcheckvectors-E1.json', 'E1',
            'DQR-compatible json generated from check_vectors results'),
        call(
            None, None, 'E1',
            ('Detector state for active instruments is unknown.\n{}'
             'Check looked within -2/+2 seconds of superevent. ').format(
                 detchar.generate_table(
                     'Data quality bits', [], [],
                     ['H1:NO_OMC_DCPD_ADC_OVERFLOW',
                      'H1:NO_DMT-ETMY_ESD_DAC_OVERFLOW',
                      'L1:NO_OMC_DCPD_ADC_OVERFLOW',
                      'L1:NO_DMT-ETMY_ESD_DAC_OVERFLOW',
                      'H1:HOFT_OK', 'H1:OBSERVATION_INTENT',
                      'L1:HOFT_OK', 'L1:OBSERVATION_INTENT',
                      'V1:HOFT_OK', 'V1:OBSERVATION_INTENT',
                      'V1:GOOD_DATA_QUALITY_CAT1'])),
            ['data_quality'])
    ]
    mock_upload.assert_has_calls(calls, any_order=True)
    gcn_type_dict = {'Fermi': 111, 'Fermi_final': 115,
                     'INTEGRAL': 53, 'INTEGRAL_MDC': 53}
    time_dict = {'Fermi': '2018-05-24T18:35:45',
                 'Fermi_final': '2018-05-24T18:35:45',
                 'INTEGRAL': '2017-02-03T19:00:05',
                 'INTEGRAL_MDC': '2023-04-04T06:31:24'}
    mock_create_upload_external_skymap.assert_called_once_with(
        {'graceid': 'E1',
         'gpstime': 1,
         'instruments': '',
         'pipeline': 'Fermi',
         'search': 'GRB',
         'extra_attributes': {
             'GRB': {
                 'trigger_duration': 1,
                 'trigger_id': 123,
                 'ra': 0.0,
                 'dec': 0.0,
                 'error_radius': 10.0
                    }
              },
         'links': {
             'self': 'https://gracedb.ligo.org/events/E356793/'
                  }
         },
        gcn_type_dict[pipeline], time_dict[pipeline])
    # If Fermi FINAL notice, check we try to grab sky map from HEASARC
    if 'final' in path:
        mock_get_upload_external_skymap.assert_called_once_with(
            {'graceid': 'E1',
             'gpstime': 1,
             'instruments': '',
             'pipeline': 'Fermi',
             'search': 'GRB',
             'extra_attributes': {
                 'GRB': {
                     'trigger_duration': 1,
                     'trigger_id': 123,
                     'ra': 0.0,
                     'dec': 0.0,
                     'error_radius': 10.0
                        }
             },
             'links': {
                 'self': 'https://gracedb.ligo.org/events/E356793/'
                      }
             },
            None
        )
    else:
        mock_get_upload_external_skymap.assert_not_called()


@patch('gwcelery.tasks.gracedb.get_events.run', return_value=[])
@patch('gwcelery.tasks.gracedb.create_event.run', return_value={
    'graceid': 'E1', 'gpstime': 1, 'instruments': '', 'pipeline': 'Fermi',
    'search': 'SubGRB',
    'extra_attributes': {'GRB': {'trigger_duration': None, 'trigger_id': 123,
                                 'ra': 0., 'dec': 0., 'error_radius': 10.}},
    'links': {'self': 'https://gracedb.ligo.org/events/E356793/'}})
@patch('gwcelery.tasks.detchar.check_vectors.run')
@patch('gwcelery.tasks.external_skymaps.get_upload_external_skymap.run')
def test_handle_create_subthreshold_grb_event(mock_get_upload_ext_skymap,
                                              mock_check_vectors,
                                              mock_create_event,
                                              mock_get_events):
    text = read_binary(data, 'fermi_subthresh_grb_lowconfidence.xml')
    external_triggers.handle_grb_gcn(payload=text)
    mock_create_event.assert_not_called()
    text = read_binary(data, 'fermi_subthresh_grb_gcn.xml')
    external_triggers.handle_grb_gcn(payload=text)
    mock_get_events.assert_called_once_with(query=(
                                            'group: External pipeline: '
                                            'Fermi grbevent.trigger_id '
                                            '= "578679123"'))
    # Note that this is the exact ID in the .xml file
    mock_create_event.assert_called_once_with(filecontents=text,
                                              search='SubGRB',
                                              pipeline='Fermi',
                                              group='External',
                                              labels=None)
    mock_check_vectors.assert_called_once()
    mock_get_upload_ext_skymap.assert_called_with(
        {'graceid': 'E1', 'gpstime': 1, 'instruments': '',
         'pipeline': 'Fermi', 'search': 'SubGRB',
         'extra_attributes': {
             'GRB': {'trigger_duration': None, 'trigger_id': 123,
                     'ra': 0., 'dec': 0., 'error_radius': 10.}},
         'links': {'self': 'https://gracedb.ligo.org/events/E356793/'}},
        ('https://gcn.gsfc.nasa.gov/notices_gbm_sub/' +
         'gbm_subthresh_578679393.215999_healpix.fits'))


@pytest.mark.parametrize('filename',
                         ['fermi_noise_gcn.xml',
                          'fermi_noise_gcn_2.xml'])
@patch('gwcelery.tasks.external_skymaps.get_upload_external_skymap.run')
@patch('gwcelery.tasks.gracedb.get_events.run', return_value=[])
@patch('gwcelery.tasks.gracedb.create_event.run', return_value={
    'graceid': 'E1', 'gpstime': 1, 'instruments': '', 'pipeline': 'Fermi',
    'search': 'GRB',
    'extra_attributes': {'GRB': {'trigger_duration': 1, 'trigger_id': 123,
                                 'ra': 0., 'dec': 0., 'error_radius': 10.}},
    'links': {'self': 'https://gracedb.ligo.org/events/E356793/'}})
@patch('gwcelery.tasks.detchar.check_vectors.run')
def test_handle_noise_fermi_event(mock_check_vectors,
                                  mock_create_event,
                                  mock_get_events,
                                  mock_get_upload_external_skymap,
                                  filename):
    text = read_binary(data, filename)
    external_triggers.handle_grb_gcn(payload=text)
    mock_get_events.assert_called_once_with(query=(
                                            'group: External pipeline: '
                                            'Fermi grbevent.trigger_id '
                                            '= "598032876"'))
    # Note that this is the exact ID in the .xml file
    mock_create_event.assert_called_once_with(filecontents=text,
                                              search='GRB',
                                              pipeline='Fermi',
                                              group='External',
                                              labels=['NOT_GRB'])
    mock_check_vectors.assert_called_once()
    mock_get_upload_external_skymap.assert_not_called()


@patch('gwcelery.tasks.external_skymaps.create_external_skymap')
@patch('gwcelery.tasks.external_skymaps.get_upload_external_skymap.run')
@patch('gwcelery.tasks.gracedb.get_events.run', return_value=[])
@patch('gwcelery.tasks.gracedb.create_event.run', return_value={
    'graceid': 'E1', 'gpstime': 1, 'instruments': '', 'pipeline': 'Fermi',
    'search': 'GRB',
    'extra_attributes': {'GRB': {'trigger_duration': 1, 'trigger_id': 123,
                                 'ra': 0., 'dec': 0., 'error_radius': 0.}},
    'links': {'self': 'https://gracedb.ligo.org/events/E356793/'}})
@patch('gwcelery.tasks.detchar.check_vectors.run')
def test_handle_initial_fermi_event(mock_check_vectors,
                                    mock_create_event,
                                    mock_get_events,
                                    mock_get_upload_external_skymap,
                                    mock_create_external_skymap):
    text = read_binary(data, 'fermi_initial_grb_gcn.xml')
    external_triggers.handle_grb_gcn(payload=text)
    mock_get_events.assert_called_once_with(query=(
                                            'group: External pipeline: '
                                            'Fermi grbevent.trigger_id '
                                            '= "548841234"'))
    # Note that this is the exact ID in the .xml file
    mock_create_event.assert_called_once_with(filecontents=text,
                                              search='GRB',
                                              pipeline='Fermi',
                                              group='External',
                                              labels=['NOT_GRB'])
    mock_check_vectors.assert_called_once()
    mock_get_upload_external_skymap.assert_not_called()
    mock_create_external_skymap.assert_not_called()


@pytest.mark.parametrize('filename',
                         ['fermi_grb_gcn.xml',
                          'fermi_noise_gcn.xml',
                          'fermi_subthresh_grb_gcn.xml'])
@patch('gwcelery.tasks.external_skymaps.create_upload_external_skymap.run')
@patch('gwcelery.tasks.external_skymaps.get_upload_external_skymap.run')
@patch('gwcelery.tasks.external_skymaps.get_skymap_filename',
       return_value=(external_skymaps.FERMI_OFFICIAL_SKYMAP_FILENAME +
                     'fits.gz'))
@patch('gwcelery.tasks.gracedb.create_label.run')
@patch('gwcelery.tasks.gracedb.remove_label.run')
@patch('gwcelery.tasks.gracedb.replace_event.run')
@patch('gwcelery.tasks.gracedb.get_event.run', return_value=[{
    'graceid': 'E1', 'gpstime': 1, 'instruments': '', 'pipeline': 'Fermi',
    'search': 'GRB', 'labels': ['EXT_SKYMAP_READY'],
    'extra_attributes': {'GRB': {'trigger_duration': 1, 'trigger_id': 123,
                                 'ra': 0., 'dec': 0., 'error_radius': 10.}},
    'links': {'self': 'https://gracedb.ligo.org/events/E356793/'}}])
@patch('gwcelery.tasks.gracedb.get_events.run', return_value=[{
    'graceid': 'E1', 'gpstime': 1, 'instruments': '', 'pipeline': 'Fermi',
    'search': 'GRB', 'labels': ['EXT_SKYMAP_READY'],
    'extra_attributes': {'GRB': {'trigger_duration': 1, 'trigger_id': 123,
                                 'ra': 0., 'dec': 0., 'error_radius': 15.}},
    'links': {'self': 'https://gracedb.ligo.org/events/E356793/'}}])
def test_handle_replace_grb_event(mock_get_events,
                                  mock_get_event,
                                  mock_replace_event, mock_remove_label,
                                  mock_create_label,
                                  mock_get_skymap_filename,
                                  mock_get_upload_external_skymap,
                                  mock_create_upload_external_skymap,
                                  filename):
    text = read_binary(data, filename)
    external_triggers.handle_grb_gcn(payload=text)
    if 'subthresh' in filename:
        mock_replace_event.assert_not_called()
        mock_remove_label.assert_not_called()
        mock_create_label.assert_not_called()
    elif 'grb' in filename:
        mock_replace_event.assert_called_once_with('E1', text)
        mock_remove_label.assert_called_once_with('NOT_GRB', 'E1')
    elif 'noise' in filename:
        mock_replace_event.assert_called_once_with('E1', text)
        mock_create_label.assert_called_once_with('NOT_GRB', 'E1')


@pytest.mark.parametrize('host',
                         ['gracedb.ligo.org',
                          'gracedb-playground.ligo.org',
                          'gracedb-test.ligo.org'])
def test_handle_integral_test(monkeypatch, host):
    mock_create_upload_external_skymap = Mock()
    mock_get_upload_external_skymap = Mock()
    mock_create_label = Mock()
    mock_create_event = Mock(return_value={
        'graceid': 'E1', 'gpstime': 1, 'instruments': '',
        'pipeline': 'INTEGRAL', 'search': 'GRB',
        'extra_attributes': {'GRB': {'trigger_duration': 1, 'trigger_id': 123,
                                     'ra': 0., 'dec': 0., 'error_radius': 5.}},
        'links': {'self': 'https://gracedb.ligo.org/events/E356793/'}})
    mock_remove_label = Mock()
    mock_get_events = Mock(return_value=[])

    monkeypatch.setattr(app.conf, 'gracedb_host', host)
    monkeypatch.setattr(
        'gwcelery.tasks.external_skymaps.create_upload_external_skymap.run',
        mock_create_upload_external_skymap)
    monkeypatch.setattr(
        'gwcelery.tasks.external_skymaps.get_upload_external_skymap.run',
        mock_get_upload_external_skymap)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.create_event.run',
        mock_create_event)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.create_label.run',
        mock_create_label)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.remove_label.run',
        mock_remove_label)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.get_events.run',
        mock_get_events)

    text = read_binary(data, 'integral_test_gcn.xml')
    external_triggers.handle_grb_gcn(payload=text)
    # Block Test INTEGRAL on production server but not for test servers
    if 'gracedb.ligo.org' == host:
        mock_create_event.assert_not_called()
    else:
        mock_create_event.assert_called_once_with(
            filecontents=text, search='GRB', group='Test', pipeline='INTEGRAL',
            labels=None)


@patch('gwcelery.tasks.gracedb.get_group', return_value='CBC')
@patch('gwcelery.tasks.gracedb.create_label.run')
@patch('gwcelery.tasks.gracedb.get_labels',
       return_value=['SKYMAP_READY', 'EM_READY'])
def test_handle_create_skymap_label_from_ext_event(mock_get_labels,
                                                   mock_create_label,
                                                   mock_get_group):
    alert = {"uid": "E1212",
             "alert_type": "label_added",
             "data": {"name": "EM_COINC"},
             "object": {
                 "group": "External",
                 "labels": ["EM_COINC", "EXT_SKYMAP_READY"],
                 "superevent": "S1234"
                       }
             }
    external_triggers.handle_grb_igwn_alert(alert)
    calls = [call('SKYMAP_READY', 'E1212'), call('EM_READY', 'E1212')]
    mock_create_label.assert_has_calls(calls)


@patch('gwcelery.tasks.gracedb.get_group', return_value='CBC')
@patch('gwcelery.tasks.gracedb.create_label.run')
def test_handle_create_skymap_label_from_superevent(mock_create_label,
                                                    mock_get_group):
    alert = {"uid": "S1234",
             "alert_type": "label_added",
             "data": {"name": "SKYMAP_READY"},
             "object": {
                 "group": "CBC",
                 "labels": ["SKYMAP_READY", "EM_READY"],
                 "superevent_id": "S1234",
                 "em_events": ['E1212']
                       }
             }
    external_triggers.handle_grb_igwn_alert(alert)
    calls = [call('SKYMAP_READY', 'E1212'), call('EM_READY', 'E1212')]
    mock_create_label.assert_has_calls(calls)


@pytest.mark.parametrize('pipeline',
                         ['Fermi', 'Swift'])
@patch('gwcelery.tasks.raven.raven_pipeline.run')
@patch('gwcelery.tasks.external_skymaps.create_combined_skymap.run')
@patch('gwcelery.tasks.gracedb.get_superevent.run',
       return_value={
           'superevent_id': 'S1234',
           'preferred_event': 'G1234',
           'preferred_event_data':
               {'group': 'CBC',
                'search': 'AllSky'}
                    })
def test_handle_skymaps_ready(mock_get_superevent,
                              mock_create_combined_skymap,
                              mock_raven_pipeline,
                              pipeline):
    """This test makes sure that once sky maps are available for a coincidence
    that the RAVEN pipeline is rerun to calculate the joint FAR with sky map
    information and check publishing conditions, as well as creating a
    combined sky map.
    """
    alert = {"uid": "E1212",
             "alert_type": "label_added",
             "data": {"name": "EM_READY"},
             "object": {
                 "graceid": "E1212",
                 "group": "External",
                 "labels": ["EM_COINC", "EXT_SKYMAP_READY", "EM_READY"],
                 "superevent": "S1234",
                 "pipeline": pipeline,
                 "search": "GRB"
                       }
             }
    external_triggers.handle_grb_igwn_alert(alert)
    mock_raven_pipeline.assert_called_once_with([{'superevent_id': 'S1234',
                                                  'preferred_event': 'G1234',
                                                  'preferred_event_data':
                                                      {'group': 'CBC',
                                                       'search': 'AllSky'}}],
                                                'E1212', alert['object'],
                                                -5, 1, 'CBC',
                                                use_superevent_skymap=False)
    mock_create_combined_skymap.assert_called_once_with(
        'S1234', 'E1212', preferred_event='G1234')


@patch('gwcelery.tasks.raven.trigger_raven_alert')
@patch('gwcelery.tasks.gracedb.get_superevent',
       return_value={'superevent_id': 'S1234',
                     'preferred_event': 'G1234',
                     'preferred_event_data': {
                         'group': 'CBC'},
                     'time_coinc_far': 1e-9,
                     'space_coinc_far': 1e-10})
def test_handle_label_removed(mock_get_superevent,
                              mock_trigger_raven_alert):
    alert = {"uid": "E1212",
             "alert_type": "label_removed",
             "data": {"name": "NOT_GRB"},
             "object": {
                 "graceid": "E1212",
                 "group": "External",
                 "labels": ["EM_COINC", "EXT_SKYMAP_READY", "SKYMAP_READY"],
                 "superevent": "S1234",
                 "pipeline": "Fermi",
                 "search": "GRB"
                       }
             }
    superevent = {'superevent_id': 'S1234',
                  'preferred_event': 'G1234',
                  'preferred_event_data': {
                      'group': 'CBC'},
                  'time_coinc_far': 1e-9,
                  'space_coinc_far': 1e-10}
    coinc_far_dict = {
                'temporal_coinc_far': 1e-9,
                'spatiotemporal_coinc_far': 1e-10
            }
    external_triggers.handle_grb_igwn_alert(alert)
    mock_trigger_raven_alert.assert_called_once_with(
        coinc_far_dict, superevent, alert['uid'],
        alert['object'], 'CBC'
    )


def _mock_get_event(graceid):
    if graceid == 'E1':
        pipeline = 'Fermi'
        extra_labels = []
    elif graceid == 'E2':
        pipeline = 'Swift'
        extra_labels = []
    elif graceid == 'E3':
        pipeline = 'Fermi'
        extra_labels = ['SKYMAP_READY']
    return {'graceid': graceid,
            'pipeline': pipeline,
            'search': 'GRB',
            'superevent': 'S1',
            'labels': (['EM_COINC', 'EXT_SKYMAP_READY',
                        'EM_READY'] + extra_labels)
            }


def _mock_get_superevent(graceid):
    return {'superevent_id': 'S1',
            'preferred_event': 'G1',
            'preferred_event_data':
                {'group': 'CBC',
                 'search': 'AllSky'},
            'em_events': ['E1', 'E2', 'E3'],
            'em_type': 'E1',
            'far': 1e-5,
            'labels': ['COMBINEDSKYMAP_READY', 'RAVEN_ALERT',
                       'EM_COINC', 'GCN_PRELIM_SENT']}


@pytest.mark.parametrize('graceid',
                         ['S1', 'E1', 'E2', 'E3'])
@patch('gwcelery.tasks.raven.raven_pipeline.run')
@patch('gwcelery.tasks.external_skymaps.create_combined_skymap.run')
@patch('gwcelery.tasks.gracedb.get_event', _mock_get_event)
@patch('gwcelery.tasks.gracedb.get_superevent.run', _mock_get_superevent)
def test_handle_rerun_combined_skymap(mock_create_combined_skymap,
                                      mock_raven_pipeline,
                                      graceid):
    """This tests re-creating a combined sky map and rerunning the RAVEN
    pipeline whenever a new GW or external skymap is added after sending the
    first alert (and likely after creating the first combined skymap). For
    superevents (S1), this tests this is run for all related em_events. Note
    the combined sky map is not required for Swift (E2) since these are
    produced in this case."""
    event = (_mock_get_superevent(graceid) if 'S' in graceid else
             _mock_get_event(graceid))
    alert = {
        "data": {"filename": ("skymap.multiorder.fits" if 'S' in graceid
                              else 'fermi_skymap.fits.gz'),
                 "file_version": 1,
                 "comment": ''},
        "uid": graceid,
        "alert_type": "log",
        "object": event
    }
    external_triggers.handle_grb_igwn_alert(alert)
    if graceid == 'S1':
        mock_create_combined_skymap.assert_has_calls(
            [call('S1', 'E1', preferred_event=None),
             call('S1', 'E2', preferred_event=None),
             call('S1', 'E3', preferred_event=None)]
        )
    elif 'E' in graceid:
        mock_create_combined_skymap.assert_called_once_with(
            'S1', graceid,
            preferred_event=('G1' if graceid in ['E1', 'E2'] else None))
    else:
        mock_create_combined_skymap.assert_not_called()
    if 'S' in graceid:
        calls = [
            call(
                [_mock_get_event('E1')],
                graceid, event,
                -1, 5, 'CBC',
                use_superevent_skymap=True),
            call(
                [_mock_get_event('E2')],
                graceid, event,
                -1, 5, 'CBC',
                use_superevent_skymap=True),
            call(
                [_mock_get_event('E3')],
                graceid, event,
                -1, 5, 'CBC',
                use_superevent_skymap=True)
        ]
    else:
        calls = [
            call(
                [_mock_get_superevent('S1')],
                graceid, event,
                -5, 1, 'CBC',
                use_superevent_skymap=(graceid == 'E3'))
        ]
    mock_raven_pipeline.assert_has_calls(calls)


@pytest.mark.parametrize(
    "alert_type, comment, expected_skymap_calls, expected_result",
    [
        ("log",
         "Updated superevent parameters: preferred_event: example", 2, True),
        ("log",
         "Some other comment", 0, False),
        ("other_type",
         "Updated superevent parameters: preferred_event: example", 0, False),
        ("log",
         "Updated superevent parameters: different_param: example", 0, False)
    ]
)
@patch('gwcelery.tasks.raven.raven_pipeline.run')
@patch('gwcelery.tasks.external_skymaps.create_combined_skymap.run')
@patch('gwcelery.tasks.gracedb.get_event', _mock_get_event)
def test_preferred_event_coinc_far_calculation(
        mock_create_combined_skymap,
        mock_raven_pipeline,
        alert_type,
        comment,
        expected_skymap_calls,
        expected_result):
    alert = {
        "alert_type": alert_type,
        "uid": 'S1',
        "object": {"labels": ["EM_COINC"], "superevent_id": 'S1',
                   "em_events": ["E1", "E2"],
                   "preferred_event": 'G1',
                   "em_type": 'E1',
                   "preferred_event_data": {"group": "CBC",
                                            "search": "AllSky"}},
        "data": {"comment": comment, "filename": ""}
    }

    external_triggers.handle_grb_igwn_alert(alert)

    assert mock_create_combined_skymap.call_count == expected_skymap_calls
    assert mock_raven_pipeline.call_count == expected_skymap_calls

    if expected_result:
        mock_create_combined_skymap.assert_has_calls(
            [call(
                'S1', 'E1', preferred_event='G1'),
             call(
                'S1', 'E2', preferred_event='G1')])
        mock_raven_pipeline.assert_has_calls(
            [call(
                [_mock_get_event('E1')],
                'S1', alert['object'],
                -1, 5, 'CBC',
                use_superevent_skymap=False),
             call(
                 [_mock_get_event('E2')],
                 'S1', alert['object'],
                 -1, 5, 'CBC',
                 use_superevent_skymap=False)])


@pytest.mark.parametrize('labels',
                         [["EM_COINC", "SKYMAP_READY", "RAVEN_ALERT", "ADVOK"],
                          ["EM_COINC", "SKYMAP_READY", "ADVOK"]])
@patch('gwcelery.tasks.gracedb.client')
@patch('gwcelery.tasks.gracedb.get_event.run', return_value={
    'graceid': 'E1234', 'pipeline': 'Fermi'})
@patch('gwcelery.tasks.gracedb.create_label.run')
@patch('gwcelery.tasks.raven.sog_paper_pipeline.run')
def test_handle_sog_manuscript_pipeline(mock_sog_paper_pipeline,
                                        mock_create_label,
                                        mock_get_superevent,
                                        mock_gracedb, labels):
    alert = {"uid": "S1212",
             "alert_type": "label_added",
             "data": {"name": "ADVOK"},
             "object": {
                 "graceid": "S1212",
                 "labels": labels,
                 "superevent_id": "S1212",
                 "space_coinc_far": 1e-9,
                 "em_type": "E1234",
                 "em_events": ["E1234"]}
             }

    # Create IGWN alert
    external_triggers.handle_grb_igwn_alert(alert)

    if 'RAVEN_ALERT' in labels:
        mock_sog_paper_pipeline.assert_called_once_with(
            {'graceid': 'E1234', 'pipeline': 'Fermi'}, alert['object'])
    else:
        mock_sog_paper_pipeline.assert_not_called()


@patch('gwcelery.tasks.detchar.dqr_json', return_value='dqrjson')
@patch('gwcelery.tasks.gracedb.upload.run')
@patch('gwcelery.tasks.gracedb.create_event.run', return_value={
    'graceid': 'E1', 'gpstime': 1, 'instruments': '', 'pipeline': 'SNEWS',
    'search': 'Supernova'})
def test_handle_create_snews_event(mock_create_event,
                                   mock_upload, mock_json):
    text = read_binary(data, 'snews_gcn.xml')
    external_triggers.handle_snews_gcn(payload=text)
    mock_create_event.assert_called_once_with(filecontents=text,
                                              search='Supernova',
                                              pipeline='SNEWS',
                                              group='External',
                                              labels=None)
    calls = [
        call(
            '"dqrjson"', 'gwcelerydetcharcheckvectors-E1.json', 'E1',
            'DQR-compatible json generated from check_vectors results'),
        call(
            None, None, 'E1',
            ('Detector state for active instruments is unknown.\n{}'
             'Check looked within -10/+10 seconds of superevent. ').format(
                 detchar.generate_table(
                     'Data quality bits', [], [],
                     ['H1:NO_OMC_DCPD_ADC_OVERFLOW',
                      'H1:NO_DMT-ETMY_ESD_DAC_OVERFLOW',
                      'L1:NO_OMC_DCPD_ADC_OVERFLOW',
                      'L1:NO_DMT-ETMY_ESD_DAC_OVERFLOW',
                      'H1:HOFT_OK', 'H1:OBSERVATION_INTENT',
                      'L1:HOFT_OK', 'L1:OBSERVATION_INTENT',
                      'V1:HOFT_OK', 'V1:OBSERVATION_INTENT',
                      'V1:GOOD_DATA_QUALITY_CAT1'])),
            ['data_quality'])
    ]
    mock_upload.assert_has_calls(calls, any_order=True)


@patch('gwcelery.tasks.gracedb.replace_event.run')
@patch('gwcelery.tasks.gracedb.remove_label.run')
@patch('gwcelery.tasks.gracedb.get_events.run',
       return_value=[{'graceid': 'E1', 'search': 'Supernova', 'gpstime': 100,
                      'pipeline': 'SNEWS', 'labels': []}])
def test_handle_replace_snews_event(mock_get_events, mock_remove_label,
                                    mock_replace_event):
    text = read_binary(data, 'snews_gcn.xml')
    external_triggers.handle_snews_gcn(payload=text)
    mock_replace_event.assert_called_once_with('E1', text)


@patch('gwcelery.tasks.raven.coincidence_search')
def test_handle_grb_exttrig_creation(mock_raven_coincidence_search):
    """Test dispatch of an IGWN alert message for an exttrig creation."""
    # Test IGWN alert payload.
    alert = read_json(data, 'igwn_alert_exttrig_creation.json')

    # Run function under test
    external_triggers.handle_grb_igwn_alert(alert)

    # Check that the correct tasks were dispatched.
    mock_raven_coincidence_search.assert_has_calls([
        call('E1234', alert['object'], group='Burst', se_searches=['AllSky']),
        call('E1234', alert['object'], group='CBC', searches=['GRB'])])


@patch('gwcelery.tasks.raven.coincidence_search')
def test_handle_subgrb_exttrig_creation(mock_raven_coincidence_search):
    """Test dispatch of an IGWN alert message for an exttrig creation."""
    # Test IGWN alert payload.
    alert = read_json(data, 'igwn_alert_subgrb_creation.json')

    # Run function under test
    external_triggers.handle_grb_igwn_alert(alert)

    # Check that the correct tasks were dispatched.
    mock_raven_coincidence_search.assert_has_calls([
        call('E1234', alert['object'], searches=['SubGRB'],
             group='CBC', pipelines=['Fermi']),
        call('E1234', alert['object'], searches=['SubGRB'],
             group='Burst', se_searches=['BBH'], pipelines=['Fermi'])])


@patch('gwcelery.tasks.external_skymaps.create_upload_external_skymap')
@patch('gwcelery.tasks.raven.coincidence_search')
def test_handle_subgrb_targeted_creation(mock_raven_coincidence_search,
                                         mock_create_upload_external_skymap):
    """Test dispatch of an IGWN alert message for an exttrig creation."""
    # Test IGWN alert payload.
    alert = read_json(data, 'igwn_alert_exttrig_subgrb_targeted_creation.json')

    # Run function under test
    external_triggers.handle_grb_igwn_alert(alert)

    # Check that the correct tasks were dispatched.
    mock_raven_coincidence_search.assert_has_calls([
        call('E1234', alert['object'], se_searches=['AllSky', 'BBH'],
             searches=['SubGRBTargeted'],
             pipelines=['Swift'])])


@pytest.mark.parametrize('path',
                         ['igwn_alert_snews_test_creation.json',
                          'igwn_alert_snews_creation.json',
                          'igwn_alert_superevent_creation.json',
                          'igwn_alert_superevent_burst_bbh_creation.json'])
@patch('gwcelery.tasks.raven.coincidence_search')
def test_handle_sntrig_creation(mock_raven_coincidence_search, path):
    """Test dispatch of an IGWN alert message for SNEWS alerts.
    This now includes both real and test SNEWS events to ensure both are
    ingested correctly, as well as a superevent.
    """
    # Test IGWN alert payload.
    alert = read_json(data, path)

    if 'superevent' in path:
        alert['object']['preferred_event_data']['group'] = 'Burst'
        search = alert['object']['preferred_event_data']['search']

    # Run function under test
    external_triggers.handle_snews_igwn_alert(alert)

    if 'test' in path:
        graceid = 'E1236'
    else:
        graceid = 'E1235'

    if 'superevent' in path and search == 'BBH':
        mock_raven_coincidence_search.assert_not_called()
    elif 'superevent' in path:
        mock_raven_coincidence_search.assert_called_once_with(
            'S180616h', alert['object'], group='Burst', searches=['Supernova'],
            se_searches=['AllSky'], pipelines=['SNEWS'])
    elif 'snews' in path:
        mock_raven_coincidence_search.assert_called_once_with(
            graceid, alert['object'], group='Burst', searches=['Supernova'],
            se_searches=['AllSky'], pipelines=['SNEWS'])


@patch('gwcelery.tasks.gracedb.get_superevent',
       return_value={'preferred_event': 'M4634'})
@patch('gwcelery.tasks.gracedb.get_group', return_value='CBC')
@patch('gwcelery.tasks.raven.coincidence_search')
def test_handle_superevent_cbc_creation(mock_raven_coincidence_search,
                                        mock_get_group,
                                        mock_get_superevent):
    """Test dispatch of an IGWN alert message for a CBC superevent creation."""
    # Test IGWN alert payload.
    alert = read_json(data, 'igwn_alert_superevent_creation.json')

    # Run function under test
    external_triggers.handle_grb_igwn_alert(alert)

    # Check that the correct tasks were dispatched.
    mock_raven_coincidence_search.assert_has_calls([
        call('S180616h', alert['object'], group='CBC',
             searches=['GRB'], se_searches=[]),
        call('S180616h', alert['object'], group='CBC',
             pipelines=['Fermi'], searches=['SubGRB', 'SubGRBTargeted']),
        call('S180616h', alert['object'], group='CBC',
             pipelines=['Swift'], searches=['SubGRB', 'SubGRBTargeted'])])


@pytest.mark.parametrize('search', ['AllSky', 'BBH', 'IMBH'])
@patch('gwcelery.tasks.gracedb.get_superevent',
       return_value={'preferred_event': 'M4634'})
@patch('gwcelery.tasks.raven.coincidence_search')
def test_handle_superevent_burst_creation(mock_raven_coincidence_search,
                                          mock_get_superevent, search):
    """
    Test dispatch of an IGWN alert message for a burst superevent
    creation.
    """
    # Test IGWN alert payload.
    alert = read_json(data, 'igwn_alert_superevent_creation.json')
    alert['object']['preferred_event_data']['group'] = 'Burst'
    alert['object']['preferred_event_data']['search'] = search

    # Run function under test
    external_triggers.handle_grb_igwn_alert(alert)

    # Check that the correct tasks were dispatched.
    if search == 'AllSky':
        mock_raven_coincidence_search.assert_has_calls([
            call('S180616h', alert['object'], group='Burst',
                 searches=['GRB'], se_searches=['AllSky']),
            call('S180616h', alert['object'], group='Burst',
                 pipelines=['Fermi'], searches=['SubGRBTargeted']),
            call('S180616h', alert['object'], group='Burst',
                 pipelines=['Swift'], searches=['SubGRBTargeted'])])
    elif search == 'BBH':
        mock_raven_coincidence_search.assert_has_calls([
            call('S180616h', alert['object'], group='Burst',
                 searches=['GRB'], se_searches=['BBH']),
            call('S180616h', alert['object'], group='Burst',
                 pipelines=['Fermi'], searches=['SubGRB', 'SubGRBTargeted']),
            call('S180616h', alert['object'], group='Burst',
                 pipelines=['Swift'], searches=['SubGRB', 'SubGRBTargeted'])])
    else:
        mock_raven_coincidence_search.assert_not_called()


def _mock_get_events(query):
    if "12345" in query:
        return [{"graceid": "E12345", "search": "SubGRBTargeted",
                 "labels": []}]
    else:
        return {}


@pytest.mark.parametrize('alert_type',
                         ['initial', 'update', 'retraction'])
@pytest.mark.parametrize('kafka_path',
                         ['kafka_alert_fermi.json',
                          'kafka_alert_fermi_ignore.json',
                          'kafka_alert_swift.json',
                          'kafka_alert_swift_noloc.json',
                          'kafka_alert_swift_wskymap.json'])
@patch('gwcelery.tasks.gracedb.create_event.run', return_value={
    'graceid': 'E12345', 'gpstime': 100, 'pipeline': 'Fermi', 'labels': [],
    'extra_attributes': {'GRB': {'trigger_duration': 1, 'trigger_id': 123,
                                 'ra': 10., 'dec': 20., 'error_radius': 10.}}})
@patch('gwcelery.tasks.external_skymaps.create_upload_external_skymap.run')
@patch('gwcelery.tasks.external_skymaps.get_upload_external_skymap.run')
@patch('gwcelery.tasks.external_skymaps.read_upload_skymap_from_base64.run')
@patch('gwcelery.tasks.gracedb.replace_event.run')
@patch('gwcelery.tasks.gracedb.get_event.run',
       return_value={'graceid': 'E12345'})
@patch('gwcelery.tasks.gracedb.create_label.run')
@patch('gwcelery.tasks.gracedb.get_events.run', side_effect=_mock_get_events)
@patch('gwcelery.tasks.detchar.check_vectors.run')
@patch('gwcelery.tasks.gracedb.upload.run')
@patch('gwcelery.tasks.skymaps.plot_allsky.run')
def test_handle_targeted_kafka_alert(mock_plot_allsky,
                                     mock_gracedb_upload,
                                     mock_check_vectors,
                                     mock_get_events,
                                     mock_create_label,
                                     mock_get_event,
                                     mock_replace_event,
                                     mock_read_upload_skymap_from_base64,
                                     mock_get_upload_external_skymap,
                                     mock_create_upload_external_skymap,
                                     mock_create_event,
                                     alert_type, kafka_path):

    alert = read_json(data, kafka_path)
    alert["alert_type"] = alert_type
    voevent_path = '{}_subgrbtargeted_template.xml'.format(
        'fermi' if 'fermi' in kafka_path else 'swift')
    no_loc = 'noloc' in kafka_path
    replace_event = alert_type in ['update', 'retraction']
    pipeline = 'Fermi' if 'fermi' in kafka_path else 'Swift'
    labels = ['NOT_GRB'] if 'ignore' in kafka_path else None
    # Update VOEvent with sky localization info or ID if needed
    if no_loc or replace_event or labels:
        with resources.as_file(
                resources.files(data).joinpath(voevent_path)) as fname:
            root = etree.parse(fname)
        if no_loc:
            # Set template to also be zeros if nothing in kafka alert
            root.find(("./WhereWhen/ObsDataLocation/"
                       "ObservationLocation/AstroCoords/Position2D/Value2/"
                       "C1")).text = str(0.).encode()
            root.find(("./WhereWhen/ObsDataLocation/"
                       "ObservationLocation/AstroCoords/Position2D/Value2/"
                       "C2")).text = str(0.).encode()
            root.find(("./WhereWhen/ObsDataLocation/"
                       "ObservationLocation/AstroCoords/Position2D/"
                       "Error2Radius")).text = str(0.).encode()
        if replace_event:
            # Update ID so we can find a previous event and replace it
            new_id = "12345"
            root.find("./What/Param[@name='TrigID']").attrib['value'] = \
                new_id.encode()
            alert["id"] = [new_id]
        if labels:
            # Overwrite value in GCN template to be that in the kafka packet
            new_far = str(alert['far'])
            root.find("./What/Param[@name='FAR']").attrib['value'] = \
                new_far.encode()
        text = etree.tostring(root, xml_declaration=True, encoding="UTF-8")
        # Add back removed newline
        text += b'\n'
    # If no changes needed, read the file directly
    else:
        text = read_binary(data, voevent_path)

    # Send alert through handler
    external_triggers.handle_targeted_kafka_alert(alert)

    # Check either event was created or updated based on alert type
    if replace_event:
        mock_replace_event.assert_called_once_with("E12345", text)
    else:
        mock_create_event.assert_called_once_with(filecontents=text,
                                                  search='SubGRBTargeted',
                                                  pipeline=pipeline,
                                                  group='External',
                                                  labels=labels)

    # Ensure correct localization method used
    if pipeline == 'Fermi' or 'wskymap' in kafka_path:
        mock_read_upload_skymap_from_base64.assert_called()
    elif 'noloc' in kafka_path:
        mock_read_upload_skymap_from_base64.assert_not_called()
        mock_create_upload_external_skymap.assert_not_called()
    else:
        mock_create_upload_external_skymap.assert_called_once()

    if alert_type == "retraction" or (labels and alert_type == "update"):
        mock_create_label.assert_called_once_with("NOT_GRB", "E12345")
    else:
        mock_create_label.assert_not_called()


@pytest.mark.parametrize('path',
                         ['igwn_alert_superevent_creation.json',
                          'igwn_alert_exttrig_creation.json'])
@patch('gwcelery.tasks.raven.coincidence_search')
def test_handle_mdc_creation(mock_raven_coincidence_search,
                             path):
    """Test dispatch of an IGWN alert message for a CBC superevent creation."""
    # Test IGWN alert payload.
    alert = read_json(data, path)
    if 'superevent' in path:
        alert['object']['preferred_event_data']['search'] = 'MDC'
    elif 'exttrig' in path:
        alert['object']['search'] = 'MDC'

    # Run function under test
    external_triggers.handle_grb_igwn_alert(alert)

    # Check that the correct tasks were dispatched.
    if 'superevent' in path:
        calls = [call('S180616h', alert['object'], group='CBC',
                      searches=['MDC'])]
    elif 'exttrig' in path:
        calls = [call('E1234', alert['object'], group='CBC',
                      se_searches=['MDC']),
                 call('E1234', alert['object'], group='Burst',
                      se_searches=['MDC'])]
    mock_raven_coincidence_search.assert_has_calls(calls, any_order=True)


@pytest.mark.parametrize('path',
                         ['igwn_alert_superevent_creation.json',
                          'igwn_alert_snews_creation.json'])
@patch('gwcelery.tasks.raven.coincidence_search')
def test_handle_mdc_sn_creation(mock_raven_coincidence_search,
                                path):
    """Test dispatch of an MDC with the supernovae igwn alert listener,
    both a superevent and external event."""
    # Test IGWN alert payload.
    alert = read_json(data, path)
    if 'superevent' in path:
        alert['object']['preferred_event_data']['search'] = 'MDC'
        alert['object']['preferred_event_data']['group'] = 'Burst'
        graceid = 'S180616h'
    elif 'snews' in path:
        alert['object']['search'] = 'MDC'
        graceid = 'E1235'

    # Run function under test
    external_triggers.handle_snews_igwn_alert(alert)

    # Check that the correct tasks were dispatched.
    if 'superevent' in path:
        mock_raven_coincidence_search.assert_called_once_with(
            graceid, alert['object'], group='Burst', searches=['MDC'],
            se_searches=['MDC'], pipelines=['SNEWS'])
    elif 'snews' in path:
        mock_raven_coincidence_search.assert_called_once_with(
            graceid, alert['object'], group='Burst', searches=['MDC'],
            se_searches=['MDC'], pipelines=['SNEWS'])


@pytest.mark.parametrize(
    'kafka_path,voevent_path,search',
    [['kafka_alert_fermi.json',
      'fermi_subgrbtargeted_template.xml',
      'SubGRBTargeted'],
     ['kafka_alert_swift.json',
      'swift_subgrbtargeted_template.xml',
      'SubGRBTargeted'],
     ['kafka_alert_svom_wakeup.json',
      'svom_grb_template.xml',
      'GRB'],
     ['kafka_alert_einteinprobe.json',
      'einsteinprobe_grb_template.xml',
      'GRB']])
def test_kafka_to_voevent(kafka_path, voevent_path, search):

    voevent_text = read_binary(data, voevent_path)
    alert = read_json(data, kafka_path)

    voevent_result, pipeline, time, id = \
        external_triggers._kafka_to_voevent(alert, search)

    assert voevent_result == voevent_text


def test_kafka_to_voevent_empty():
    """Test that giving a file with no pipeline/instrument info gives a
    ValueError."""
    with pytest.raises(ValueError):
        external_triggers._kafka_to_voevent({}, 'GRB')
