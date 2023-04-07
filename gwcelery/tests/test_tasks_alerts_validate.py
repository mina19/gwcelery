import fastavro
from importlib import resources
import json
from unittest.mock import Mock, patch

import pytest

from .. import app
from ..tasks import alerts
from . import data
from ..kafka.bootsteps import AvroBlobWrapper, schema


@patch('gwcelery.tasks.alerts._upload_notice.run')
@patch('gwcelery.tasks.gracedb.download._orig_run',
       return_value=resources.read_binary(
           data,
           'MS220722v_bayestar.multiorder.fits'
       ))
def test_validate_alert(mock_download, mock_upload, monkeypatch):
    """Validate public alerts against the schema from the userguide.
    """
    def _validate_alert(public_alert_avro_blob):
        assert len(public_alert_avro_blob.content) == 1
        fastavro.validation.validate(public_alert_avro_blob.content[0],
                                     schema(), strict=True)

    # Replace the stream object with a mock object with _validiate_alert as its
    # write attribute
    mock_stream = Mock()
    mock_stream_write = Mock(side_effect=_validate_alert)
    mock_stream.write = mock_stream_write
    mock_stream.serialization_model = AvroBlobWrapper
    monkeypatch.setitem(app.conf, 'kafka_streams', {'scimma': mock_stream})

    # Load superevent dictionary, and embright/pastro json tuple
    superevent = json.loads(resources.read_binary(data, 'MS220722v.xml'))
    classification = (
        '{"HasNS": 1.0, "HasRemnant": 1.0}',
        '{"BNS": 0.9999976592278448, "NSBH": 0.0, "BBH": 0.0,'
        '"Terrestrial": 2.3407721552252815e-06}'
    )
    skymap_fname = 'MS220722v_bayestar.multiorder.fits'

    # Test preliminary, initial, and update alerts. All three are generated
    # using the same code path, so we only need to test 1
    alerts.download_skymap_and_send_alert(classification, superevent,
                                          'initial', skymap_fname)

    mock_download.assert_called_once()
    mock_upload.assert_called_once()
    mock_stream_write.assert_called_once()

    # Reset mocks
    mock_stream_write.reset_mock()
    mock_download.reset_mock()
    mock_upload.reset_mock()

    # Test retraction alerts.
    alerts.send(None, superevent, 'retraction', None)
    mock_download.assert_not_called()
    mock_upload.assert_called_once()
    mock_stream_write.assert_called_once()

    # Reset mocks
    mock_stream_write.reset_mock()
    mock_download.reset_mock()
    mock_upload.reset_mock()

    # Test that missing fields trigger validation failure
    alerts.download_skymap_and_send_alert(classification, superevent,
                                          'initial', skymap_fname)

    with pytest.raises(fastavro._validate_common.ValidationError):
        # Change the superevent_id field of the superevent to None because the
        # schema does not allow None, so we know this should make the
        # validation fail
        superevent['superevent_id'] = None
        alerts.download_skymap_and_send_alert(classification, superevent,
                                              'initial', skymap_fname)
