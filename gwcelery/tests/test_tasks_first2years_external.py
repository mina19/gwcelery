from unittest.mock import Mock, call, patch

import pytest

from .. import app
from ..tasks import first2years_external
from ..util import read_json
from . import data


@pytest.mark.parametrize('ext_search', ['GRB', 'SubGRB', 'SubGRBTargeted'])
@pytest.mark.parametrize(
    'host,se_search,group,superevent_id,expected_result',
    [['gracedb-playground.ligo.org', 'MDC', 'CBC', 'MS180616j', True],
     ['gracedb-playground.ligo.org', 'AllSky', 'CBC', 'MS180616j', True],
     ['gracedb-playground.ligo.org', 'AllSky', 'Burst', 'MS180616j', True],
     ['gracedb-playground.ligo.org', 'BBH', 'CBC', 'MS180616j', True],
     ['gracedb-playground.ligo.org', 'IMBH', 'CBC', 'MS180616j', False],
     ['gracedb-playground.ligo.org', 'AllSky', 'Test', 'TS180616j', False],
     ['gracedb.ligo.org', 'MDC', 'CBC', 'MS180616j', True],
     ['gracedb.ligo.org', 'AllSky', 'CBC', 'MS180616j', False],
     ['gracedb.ligo.org', 'AllSky', 'Burst', 'MS180616j', False],
     ['gracedb.ligo.org', 'MDC', 'CBC', 'MS180616k', False]])
def test_handle_create_grb_event(monkeypatch,
                                 ext_search,
                                 host,
                                 se_search,
                                 group,
                                 superevent_id,
                                 expected_result):
    # Test IGWN alert payload.
    alert = read_json(data, 'igwn_alert_superevent_creation.json')
    alert['uid'] = superevent_id
    alert['object']['superevent_id'] = alert['uid']
    alert['object']['preferred_event_data']['search'] = se_search
    alert['object']['preferred_event_data']['group'] = group

    ext_search = 'MDC' if se_search == 'MDC' else ext_search

    mock_create_upload_external_skymap = Mock()
    mock_get_upload_external_skymap = Mock()
    mock_check_vectors = Mock()
    mock_create_event = Mock(
        return_value={'graceid': 'E1',
                      'gpstime': 1,
                      'instruments': '',
                      'pipeline': 'Fermi',
                      'search': ext_search,
                      'extra_attributes':
                      {'GRB': {'trigger_duration': 1,
                               'trigger_id': 123,
                               'ra': 0., 'dec': 0.,
                               'error_radius': 10.}},
                      'links': {
                          'self':
                              'https://gracedb.ligo.org/events/E356793/'}})
    mock_get_events = Mock(return_value=[])

    monkeypatch.setattr(
        'gwcelery.tasks.external_skymaps.create_upload_external_skymap.run',
        mock_create_upload_external_skymap)
    monkeypatch.setattr(
        'gwcelery.tasks.external_skymaps.get_upload_external_skymap.run',
        mock_get_upload_external_skymap)
    monkeypatch.setattr('gwcelery.tasks.detchar.check_vectors.run',
                        mock_check_vectors)
    monkeypatch.setattr('gwcelery.tasks.gracedb.create_event.run',
                        mock_create_event)
    monkeypatch.setattr('gwcelery.tasks.gracedb.get_events.run',
                        mock_get_events)
    monkeypatch.setattr(app.conf, 'gracedb_host', host)
    res = first2years_external.upload_external_event(
                alert, ext_search=ext_search)
    if not expected_result:
        assert res is None
        events, pipelines = [], []
    else:
        events, pipelines = res

    calls = []
    for i in range(len(events)):
        calls.append(call(filecontents=events[i],
                          search=ext_search,
                          pipeline=pipelines[i],
                          group='External',
                          labels=None))
    if expected_result:
        mock_create_event.assert_has_calls(calls)
        if ext_search == 'SubGRB':
            mock_get_upload_external_skymap.assert_called()
        else:
            mock_create_upload_external_skymap.assert_called()
            mock_get_upload_external_skymap.assert_not_called()
    else:
        mock_create_event.assert_not_called()
        mock_create_upload_external_skymap.assert_not_called()


def test_handle_create_grb_event_error():
    with pytest.raises(ValueError):
        first2years_external.create_upload_external_event(
            100., 'INTEGRAL', 'SubGRBTargeted')


@patch('gwcelery.tasks.first2years_external.create_upload_external_event')
def test_upload_external_event_ignore(mock_create_upload_external_event):
    # Test IGWN alert payload.
    alert = read_json(data, 'igwn_alert_fits.json')
    first2years_external.upload_external_event(alert)
    mock_create_upload_external_event.assert_not_called()


@patch('gwcelery.tasks.first2years_external.create_upload_external_event')
def test_upload_external_event_choose_mdc(mock_create_upload_external_event):
    # Test IGWN alert payload.
    alert = read_json(data, 'igwn_alert_superevent_creation.json')
    alert['object']['preferred_event_data']['search'] = 'MDC'
    first2years_external.upload_external_event(alert)
    mock_create_upload_external_event.assert_called()


def test_upload_external_event_choose_o3_replay(monkeypatch):
    # Test IGWN alert payload.
    alert = read_json(data, 'igwn_alert_superevent_creation.json')
    alert['uid'] = 'MS180616j'
    alert['object']['superevent_id'] = alert['uid']
    alert['object']['preferred_event_data']['group'] = 'CBC'
    alert['object']['preferred_event_data']['search'] = 'AllSky'

    mock_create_upload_external_event = Mock()
    monkeypatch.setattr(
        'gwcelery.tasks.first2years_external.create_upload_external_event',
        mock_create_upload_external_event)
    monkeypatch.setattr(app.conf, 'gracedb_host',
                        'gracedb-playground.ligo.org')

    first2years_external.upload_external_event(alert)
    mock_create_upload_external_event.assert_called()


@patch('gwcelery.tasks.first2years_external.create_upload_external_event')
def test_upload_external_event_wrong_search(mock_create_upload_external_event):
    # Test IGWN alert payload.
    alert = read_json(data, 'igwn_alert_superevent_creation.json')
    alert['object']['preferred_event_data']['search'] = 'MDC'
    with pytest.raises(ValueError):
        first2years_external.upload_external_event(alert, ext_search='AllSky')
    mock_create_upload_external_event.assert_not_called()


@patch('gwcelery.tasks.detchar.check_vectors.run')
@patch('gwcelery.tasks.gracedb.create_event.run', return_value={
    'graceid': 'M1', 'gpstime': 1, 'instruments': '', 'pipeline': 'SNEWS',
    'search': 'MDC',
    'links': {'self': 'https://gracedb.ligo.org/events/E356793/'}})
@patch('gwcelery.tasks.gracedb.get_events.run', return_value=[])
def test_upload_snews_event(mock_get_events,
                            mock_create_event,
                            mock_check_vectors):
    event = first2years_external.upload_snews_event()
    mock_create_event.assert_called_once_with(
        filecontents=event,
        search='MDC',
        pipeline='SNEWS',
        group='External',
        labels=None)
    mock_check_vectors.assert_called_once()
