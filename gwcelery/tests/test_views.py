import datetime
from unittest.mock import MagicMock, Mock, patch, call

from flask import get_flashed_messages, url_for
from requests.exceptions import HTTPError
from requests.models import Response
import pytest
from werkzeug.http import HTTP_STATUS_CODES

from .. import flask
from .. import views as _  # noqa: F401


@pytest.fixture
def app():
    return flask.app


def test_send_preliminary_gcn_post_no_data(client, monkeypatch):
    """Test send_update_gcn endpoint with no form data."""
    mock_update_superevent = Mock()
    mock_get_event = Mock()
    mock_preliminary_alert = Mock()
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.update_superevent.run',
        mock_update_superevent)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.get_event.run',
        mock_get_event)
    monkeypatch.setattr(
        'gwcelery.tasks.orchestrator.earlywarning_preliminary_alert.run',
        mock_preliminary_alert)

    response = client.post(url_for('send_preliminary_gcn'))
    assert HTTP_STATUS_CODES[response.status_code] == 'Found'
    assert get_flashed_messages() == [
        'No alert sent. Please fill in all fields.']
    mock_update_superevent.assert_not_called()
    mock_get_event.assert_not_called()
    mock_preliminary_alert.assert_not_called()


def test_send_preliminary_gcn_post(client, monkeypatch):
    """Test send_update_gcn endpoint with complete form data."""
    mock_update_superevent = Mock()
    mock_event = MagicMock()
    event_dict_spoofer = {
        'pipeline': 'gstlal',
        'gpstime': 12345.
    }
    mock_event.__getitem__.side_effect = event_dict_spoofer.__getitem__
    mock_superevent = Mock()
    mock_get_event = Mock(return_value=mock_event)
    mock_get_superevent = Mock(return_value=mock_superevent)
    mock_preliminary_alert = Mock()
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.update_superevent.run',
        mock_update_superevent)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.get_event.run',
        mock_get_event)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.get_superevent.run',
        mock_get_superevent)
    monkeypatch.setattr(
        'gwcelery.tasks.orchestrator.earlywarning_preliminary_alert.run',
        mock_preliminary_alert)

    response = client.post(url_for('send_preliminary_gcn'), data={
        'superevent_id': 'MS190208a',
        'event_id': 'M12345'})
    assert HTTP_STATUS_CODES[response.status_code] == 'Found'
    assert get_flashed_messages() == [
        'Queued preliminary alert for MS190208a.']
    mock_update_superevent.assert_called_once_with(
        'MS190208a', preferred_event='M12345', t_0=12345.)
    mock_get_event.assert_has_calls([call('M12345'), call('M12345')])
    mock_get_superevent.assert_called_once_with('MS190208a')

    alert = {
        'uid': 'MS190208a',
        'object': mock_superevent
    }
    mock_preliminary_alert.assert_called_once_with(mock_event, alert,
                                                   alert_type='preliminary',
                                                   initiate_voevent=True)


def test_change_preferred_event_post(client, monkeypatch):
    """Test changing preferred event with complete form data."""
    mock_update_superevent = Mock()
    mock_add_pipeline_pref_event = Mock()
    mock_event = MagicMock()
    mock_superevent = MagicMock()
    event_dict_spoofer = {
        'pipeline': 'gstlal',
        'gpstime': 12345.
    }
    superevent_dict_spoofer = {
        'pipeline_preferred_events': {'gstlal': {'graceid': 'M12345'}}
    }
    mock_event.__getitem__.side_effect = event_dict_spoofer.__getitem__
    mock_superevent.__getitem__.side_effect = \
        superevent_dict_spoofer.__getitem__
    mock_get_event = Mock(return_value=mock_event)
    mock_get_superevent = Mock(return_value=mock_superevent)
    mock_preliminary_alert = Mock()
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.update_superevent.run',
        mock_update_superevent)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.get_event.run',
        mock_get_event)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.get_superevent.run',
        mock_get_superevent)
    monkeypatch.setattr(
        'gwcelery.views._construct_igwn_alert_and_send_prelim_alert.run',
        mock_preliminary_alert)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.add_pipeline_preferred_event.run',
        mock_add_pipeline_pref_event)

    response = client.post(url_for('change_preferred_event'), data={
        'superevent_id': 'MS190208a',
        'event_id': 'M12345'})
    assert HTTP_STATUS_CODES[response.status_code] == 'Found'
    assert get_flashed_messages() == [
        'Changed preferred event for MS190208a.']
    mock_update_superevent.assert_called_once_with(
        'MS190208a', preferred_event='M12345', t_0=12345.)
    mock_get_event.assert_called_once_with('M12345')
    mock_get_superevent.assert_called_once_with('MS190208a')
    mock_preliminary_alert.assert_called_once_with([mock_superevent,
                                                   mock_event], 'MS190208a',
                                                   initiate_voevent=False)
    # This should only be called if the new preferred event is not already a
    # pipeline preferred event
    mock_add_pipeline_pref_event.assert_not_called()


def test_change_preferred_event_pipeline_preferred_event_post(client,
                                                              monkeypatch):
    """Test changing preferred event to event that isn't a pipeline preferred
    event with complete form data."""
    mock_update_superevent = Mock()
    mock_add_pipeline_pref_event = Mock()
    mock_event = MagicMock()
    mock_superevent = MagicMock()
    event_dict_spoofer = {'pipeline': 'gstlal', 'gpstime': 12345.}
    superevent_dict_spoofer = {
        'pipeline_preferred_events': {'gstlal': {'graceid': 'M12344'}}
    }
    mock_event.__getitem__.side_effect = event_dict_spoofer.__getitem__
    mock_superevent.__getitem__.side_effect = \
        superevent_dict_spoofer.__getitem__
    mock_get_event = Mock(return_value=mock_event)
    mock_get_superevent = Mock(return_value=mock_superevent)
    mock_preliminary_alert = Mock()
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.update_superevent.run',
        mock_update_superevent)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.get_event.run',
        mock_get_event)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.get_superevent.run',
        mock_get_superevent)
    monkeypatch.setattr(
        'gwcelery.views._construct_igwn_alert_and_send_prelim_alert.run',
        mock_preliminary_alert)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.add_pipeline_preferred_event.run',
        mock_add_pipeline_pref_event)

    response = client.post(url_for('change_preferred_event'), data={
        'superevent_id': 'MS190208a',
        'event_id': 'M12345'})
    assert HTTP_STATUS_CODES[response.status_code] == 'Found'
    assert get_flashed_messages() == [
        'Changed preferred event for MS190208a.']
    mock_update_superevent.assert_called_once_with(
        'MS190208a', preferred_event='M12345', t_0=12345.)
    mock_get_event.assert_called_once_with('M12345')
    mock_get_superevent.assert_called_once_with('MS190208a')
    mock_preliminary_alert.assert_called_once_with([mock_superevent,
                                                   mock_event], 'MS190208a',
                                                   initiate_voevent=False)
    # This should only be called if the new preferred event is not already a
    # pipeline preferred event
    mock_add_pipeline_pref_event.assert_called_once_with('MS190208a', 'M12345')


def test_successful_change_pipeline_preferred_event_post(client, monkeypatch):
    """Test changing pipeline preferred event with complete form data."""
    mock_add_pipeline_pref_event = Mock()
    mock_event = MagicMock()
    mock_superevent = MagicMock()
    event_dict_spoofer = {'pipeline': 'gstlal'}
    superevent_dict_spoofer = {
        'preferred_event_data': {'pipeline': 'mbta'},
        'pipeline_preferred_events': {'gstlal': {'graceid': 'M12344'}}
    }
    mock_event.__getitem__.side_effect = event_dict_spoofer.__getitem__
    mock_superevent.__getitem__.side_effect = \
        superevent_dict_spoofer.__getitem__
    mock_get_event = Mock(return_value=mock_event)
    mock_get_superevent = Mock(return_value=mock_superevent)
    mock_preliminary_alert = Mock()
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.get_event.run',
        mock_get_event)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.get_superevent.run',
        mock_get_superevent)
    monkeypatch.setattr(
        'gwcelery.views._construct_igwn_alert_and_send_prelim_alert.run',
        mock_preliminary_alert)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.add_pipeline_preferred_event.run',
        mock_add_pipeline_pref_event)

    response = client.post(url_for('change_pipeline_preferred_event'), data={
        'superevent_id': 'MS190208a',
        'pipeline': 'gstlal',
        'event_id': 'M12345'})
    assert HTTP_STATUS_CODES[response.status_code] == 'Found'
    assert get_flashed_messages() == [
        'Changed gstlal preferred event for MS190208a.']
    mock_get_event.assert_called_once_with('M12345')
    mock_get_superevent.assert_called_once_with('MS190208a')
    mock_preliminary_alert.assert_called_once_with([mock_superevent,
                                                   mock_event], 'MS190208a',
                                                   initiate_voevent=False)
    mock_add_pipeline_pref_event.assert_called_once_with('MS190208a', 'M12345')


def test_rejected_change_pipeline_preferred_event_post(client, monkeypatch):
    """Test attempting to change a pipeline preferred event that is also the
    superevent's preferred event with complete form data."""
    mock_add_pipeline_pref_event = Mock()
    mock_event = MagicMock()
    mock_superevent = MagicMock()
    event_dict_spoofer = {'pipeline': 'gstlal'}
    superevent_dict_spoofer = {
        'preferred_event_data': {'pipeline': 'gstlal'},
        'pipeline_preferred_events': {'gstlal': {'graceid': 'M12344'}}
    }
    mock_event.__getitem__.side_effect = event_dict_spoofer.__getitem__
    mock_superevent.__getitem__.side_effect = \
        superevent_dict_spoofer.__getitem__
    mock_get_event = Mock(return_value=mock_event)
    mock_get_superevent = Mock(return_value=mock_superevent)
    mock_preliminary_alert = Mock()
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.get_event.run',
        mock_get_event)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.get_superevent.run',
        mock_get_superevent)
    monkeypatch.setattr(
        'gwcelery.views._construct_igwn_alert_and_send_prelim_alert.run',
        mock_preliminary_alert)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.add_pipeline_preferred_event.run',
        mock_add_pipeline_pref_event)

    response = client.post(url_for('change_pipeline_preferred_event'), data={
        'superevent_id': 'MS190208a',
        'pipeline': 'gstlal',
        'event_id': 'M12345'})
    assert HTTP_STATUS_CODES[response.status_code] == 'Found'
    assert get_flashed_messages() == [
        'No change performed. User specified pipeline, gstlal, is the same '
        'pipeline that produced MS190208a\'s preferred event.'
    ]
    mock_get_event.assert_called_once_with('M12345')
    mock_get_superevent.assert_called_once_with('MS190208a')
    mock_preliminary_alert.assert_not_called()
    mock_add_pipeline_pref_event.assert_not_called()


def test_send_update_gcn_get(client):
    """Test send_update_gcn endpoint with disallowed HTTP method."""
    # GET requests not allowed
    response = client.get(url_for('send_update_gcn'))
    assert HTTP_STATUS_CODES[response.status_code] == 'Method Not Allowed'


def test_send_update_gcn_post_no_data(client):
    """Test send_update_gcn endpoint with no form data."""
    response = client.post(url_for('send_update_gcn'))
    assert HTTP_STATUS_CODES[response.status_code] == 'Found'
    assert get_flashed_messages() == [
        'No alert sent. Please fill in all fields.']


def test_send_update_gcn_post(client, monkeypatch):
    """Test send_update_gcn endpoint with complete form data."""
    mock_update_alert = Mock()
    monkeypatch.setattr(
        'gwcelery.tasks.orchestrator.update_alert.run', mock_update_alert)

    response = client.post(url_for('send_update_gcn'), data={
        'superevent_id': 'MS190208a',
        'skymap_filename': 'bayestar.fits.gz',
        'em_bright_filename': 'em_bright.json',
        'p_astro_filename': 'p_astro.json'})

    assert HTTP_STATUS_CODES[response.status_code] == 'Found'
    assert get_flashed_messages() == [
        'Queued update alert for MS190208a.']
    mock_update_alert.assert_called_once_with(
        ['bayestar.fits.gz', 'em_bright.json', 'p_astro.json'], 'MS190208a')


def test_send_update_gcn_circular_post_no_data(client):
    """Test send_update_gcn_circular endpoint with no form data."""
    response = client.post(url_for('create_update_gcn_circular'))
    assert HTTP_STATUS_CODES[response.status_code] == 'Found'
    assert get_flashed_messages() == [
        'No circular created. Please fill in superevent ID and at ' +
        'least one update type.']


@pytest.mark.parametrize(
     'sky_loc,em_bright,p_astro,raven,answer',
     [["True", None, None, None, ['sky_localization']],
      [None, "True", "True", None, ['em_bright', 'p_astro']],
      [None, None, None, "True", ['raven']],
      ["True", "True", "True", "True",
       ['sky_localization', 'em_bright', 'p_astro', 'raven']]])
@patch('gwcelery.tasks.circulars.create_update_circular', return_value='')
def test_send_update_gcn_circular_post(mock_create_circular,
                                       sky_loc, em_bright, p_astro, raven,
                                       answer,
                                       client):
    """Test send_update_gcn_circular endpoint with complete form data."""
    response = client.post(url_for('create_update_gcn_circular'), data={
        'superevent_id': 'MS190208a',
        'sky_localization': sky_loc,
        'em_bright': em_bright,
        'p_astro': p_astro,
        'raven': raven})

    assert HTTP_STATUS_CODES[response.status_code] == 'OK'
    mock_create_circular.assert_called_once_with(
        'MS190208a', update_types=answer)


@patch('gwcelery.tasks.circulars.create_medium_latency_circular',
       return_value='')
def test_send_medium_latency_gcn_circular_post(mock_create_circular,
                                               client):
    """Test send_medium_latency_gcn_circular endpoint with complete form
    data."""
    response = client.post(url_for('create_medium_latency_gcn_circular'),
                           data={'ext_event_id': 'E12345'})

    assert HTTP_STATUS_CODES[response.status_code] == 'OK'
    mock_create_circular.assert_called_once_with('E12345')


def test_send_medium_latency_gcn_circular_post_no_data(client):
    """Test send_medium_latency_gcn_circular endpoint with no form
    data."""
    response = client.post(url_for('create_medium_latency_gcn_circular'))
    assert HTTP_STATUS_CODES[response.status_code] == 'Found'
    assert get_flashed_messages() == [
        'No circular created. Please fill in external event ID']


def test_typeahead_superevent_id(client, monkeypatch):
    """Test typeahead filtering for superevent_id."""
    mock_superevents = Mock(return_value=(
        {
            'superevent_id': (
                datetime.date(2019, 2, 1) + datetime.timedelta(i)
            ).strftime('MS%y%m%da')
        } for i in range(31)))
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.client.superevents.search', mock_superevents)

    response = client.get(
        url_for('typeahead_superevent_id', superevent_id='MS1902'))

    assert HTTP_STATUS_CODES[response.status_code] == 'OK'
    assert response.json == [
        'MS190201a', 'MS190202a', 'MS190203a', 'MS190204a', 'MS190205a',
        'MS190206a', 'MS190207a', 'MS190208a']


def test_typeahead_superevent_id_invalid_date(client, monkeypatch):
    """Test typeahead filtering for superevent_id when the search term contains
    an invalid date fragment.
    """
    mock_superevents = Mock()
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.client.superevents.search', mock_superevents)

    response = client.get(
        url_for('typeahead_superevent_id', superevent_id='MS190235'))

    mock_superevents.assert_not_called()
    assert HTTP_STATUS_CODES[response.status_code] == 'OK'
    assert response.json == []


def test_typeahead_skymap_filename_gracedb_error_404(client, monkeypatch):
    """Test that the typeahead endpoints return an empty list if GraceDB
    returns a 404 error.
    """
    response = Response()
    response.status_code = 404
    monkeypatch.setattr('gwcelery.tasks.gracedb.get_log',
                        Mock(side_effect=HTTPError(response=response)))

    response = client.get(
        url_for('typeahead_skymap_filename', superevent_id='MS190208a'))

    assert HTTP_STATUS_CODES[response.status_code] == 'OK'
    assert response.json == []


def test_typeahead_skymap_filename_gracedb_error_non_404(client, monkeypatch):
    """Test that the typeahead raises an internal error if GraceDB
    returns an error other than 404.
    """
    response = Response()
    response.status_code = 403
    monkeypatch.setattr('gwcelery.tasks.gracedb.get_log',
                        Mock(side_effect=HTTPError(response=response)))

    response = client.get(
        url_for('typeahead_skymap_filename', superevent_id='MS190208a'))

    assert HTTP_STATUS_CODES[response.status_code] == 'Internal Server Error'


@pytest.mark.parametrize('endpoint,tag', [
    ('typeahead_em_bright_filename', 'em_bright'),
    ('typeahead_p_astro_filename', 'p_astro')
])
def test_typeahead_em_bright_and_p_astro(
        endpoint, tag, client, monkeypatch):
    """Test typeahead filtering for em_bright and p_astro files."""
    mock_logs = Mock(return_value=[
        {'file_version': 0,
         'filename': 'foobar.txt',
         'tag_names': [tag]},
        {'file_version': 0,
         'filename': 'bar.json',
         'tag_names': [tag]},
        {'file_version': 0,
         'filename': 'foobar.json',
         'tag_names': [tag]},
        {'file_version': 0,
         'filename': 'foobat.json',
         'tag_names': [tag]},
        {'file_version': 0,
         'filename': 'foobaz.json',
         'tag_names': ['wrong_tag']}])
    monkeypatch.setattr('gwcelery.tasks.gracedb.get_log', mock_logs)

    response = client.get(
        url_for(endpoint, superevent_id='MS190208a', filename='foo'))

    assert HTTP_STATUS_CODES[response.status_code] == 'OK'
    mock_logs.assert_called_once_with('MS190208a')
    assert response.json == ['foobar.json,0', 'foobat.json,0']


def test_download_upload_external_skymap(client, monkeypatch):
    """Test download event from URL and upload to external event."""
    mock_get_upload_external_skymap = Mock()
    monkeypatch.setattr(
        'gwcelery.tasks.external_skymaps.get_upload_external_skymap',
        mock_get_upload_external_skymap)
    response = client.post(url_for('download_upload_external_skymap'), data={
        'ext_id': 'E12345', 'skymap_url': 'https://url.fits.gz'
    })
    assert HTTP_STATUS_CODES[response.status_code] == 'Found'
    mock_get_upload_external_skymap.assert_called_once_with(
            {'graceid': 'E12345', 'search': 'FromURL'},
            skymap_link='https://url.fits.gz')


@pytest.mark.parametrize('search,time_far,space_far', [
    ('GRB', 1e-6, 1e-8),
    ('SNEWS', None, None)
])
def test_apply_raven_labels(search, time_far, space_far,
                            client, monkeypatch):
    """Test assigning RAVEN alert labels and update the preferred
    external event."""
    mock_create_label = Mock()
    mock_gracedb_download = Mock(return_value={
        'temporal_coinc_far': time_far,
        'spatiotemporal_coinc_far': space_far
    })
    mock_update_superevent = Mock()
    mock_get_event = Mock(return_value={
        'graceid': 'E12345',
        'search': search
    })
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.create_label.run',
        mock_create_label)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.download',
        mock_gracedb_download)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.update_superevent',
        mock_update_superevent)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.get_event.run',
        mock_get_event)
    response = client.post(url_for('apply_raven_labels'), data={
        'superevent_id': 'MS190208a',
        'ext_id': 'E12345',
        'event_id': 'G54321'})
    assert HTTP_STATUS_CODES[response.status_code] == 'Found'
    mock_create_label.assert_has_calls(
        [call('RAVEN_ALERT', 'MS190208a'),
         call('RAVEN_ALERT', 'E12345'),
         call('RAVEN_ALERT', 'G54321')]
    )
    mock_update_superevent.assert_called_once_with(
        'MS190208a', em_type='E12345',
        time_coinc_far=time_far, space_coinc_far=space_far
    )


def test_apply_raven_labels_no_data(client, monkeypatch):
    """Test assigning a RAVEN alert."""
    response = client.post(url_for('apply_raven_labels'))
    assert HTTP_STATUS_CODES[response.status_code] == 'Found'
    assert get_flashed_messages() == [
        'No alert sent. Please fill in all fields.']


def test_send_mock_event(client, monkeypatch):
    """Test creating a mock event."""
    mock_first2years_upload = Mock()
    monkeypatch.setattr(
        'gwcelery.tasks.first2years.upload_event.run',
        mock_first2years_upload)
    response = client.post(url_for('send_mock_event'))
    assert HTTP_STATUS_CODES[response.status_code] == 'Found'
    mock_first2years_upload.assert_called_once()


def test_send_mock_joint_event(client, monkeypatch):
    """Test creating a joint mock event."""
    mock_first2years_upload = Mock(return_value=100.)
    mock_handle_grb_gcn = Mock()
    monkeypatch.setattr(
        'gwcelery.tasks.first2years.upload_event.run',
        mock_first2years_upload)
    monkeypatch.setattr(
        'gwcelery.tasks.external_triggers.handle_grb_gcn',
        mock_handle_grb_gcn)
    response = client.post(url_for('send_mock_joint_event'))
    assert HTTP_STATUS_CODES[response.status_code] == 'Found'
    mock_first2years_upload.assert_called_once()
    mock_handle_grb_gcn.assert_called_once()
