from unittest.mock import Mock, patch

import fastavro
import pytest

from .. import app
from ..kafka.bootsteps import AvroBlobWrapper, schema
from ..tasks import alerts
from ..util import read_binary, read_json
from . import data


def _validate_alert(public_alert_avro_blob):
    assert len(public_alert_avro_blob.content) == 1
    fastavro.validation.validate(public_alert_avro_blob.content[0],
                                 schema(), strict=True)


@pytest.mark.parametrize(
    'superevent,classification,skymap', [
        ['MS220722v.json', [
            '{"HasNS": 1.0, "HasRemnant": 1.0}',
            '{"BNS": 0.9999976592278448, "NSBH": 0.0, "BBH": 0.0,'
            '"Terrestrial": 2.3407721552252815e-06}'
        ], 'MS220722v_bayestar.multiorder.fits'],
        ['S230413b.json', [None, None], 'cwb.multiorder.fits'],
        ['S230413g.json', [None, None], 'mly.multiorder.fits'],
        ['S230413h.json', [None, None], 'olib.multiorder.fits']
    ])
@patch('gwcelery.tasks.alerts._upload_notice.run')
@patch('gwcelery.tasks.gracedb.download._orig_run',
       return_value=read_binary(
           data,
           'MS220722v_bayestar.multiorder.fits'
       ))  # Using only bayestar sky map because content doesnt matter
def test_validate_alert(mock_download, mock_upload, monkeypatch,
                        superevent, classification, skymap):
    """Validate CBC public alerts against the schema.
    """

    # Replace the stream object with a mock object with _validiate_alert as its
    # write attribute
    mock_stream = Mock()
    mock_stream_write = Mock(side_effect=_validate_alert)
    mock_stream.write = mock_stream_write
    mock_stream.serialization_model = AvroBlobWrapper
    monkeypatch.setitem(app.conf, 'kafka_streams', {'scimma': mock_stream})

    # Load superevent dictionary
    superevent_dict = read_json(data, superevent)

    # Test preliminary, initial, and update alerts. All three are generated
    # using the same code path, so we only need to test 1
    alerts.download_skymap_and_send_alert(classification, superevent_dict,
                                          'initial', skymap)

    mock_download.assert_called_once()
    mock_upload.assert_called_once()
    mock_stream_write.assert_called_once()


@patch('gwcelery.tasks.alerts._upload_notice.run')
@patch('gwcelery.tasks.gracedb.download._orig_run',
       return_value=read_binary(
           data,
           'MS220722v_bayestar.multiorder.fits'
       ))
def test_validate_retraction_alert(mock_download, mock_upload, monkeypatch):
    """Validate retraction public alerts against the schema.
    """

    # Replace the stream object with a mock object with _validiate_alert as its
    # write attribute
    mock_stream = Mock()
    mock_stream_write = Mock(side_effect=_validate_alert)
    mock_stream.write = mock_stream_write
    mock_stream.serialization_model = AvroBlobWrapper
    monkeypatch.setitem(app.conf, 'kafka_streams', {'scimma': mock_stream})

    # Load superevent dictionary, and embright/pastro json tuple
    superevent = read_json(data, 'MS220722v.json')

    # Test retraction alerts.
    alerts.send(None, superevent, 'retraction', None)
    mock_download.assert_not_called()
    mock_upload.assert_called_once()
    mock_stream_write.assert_called_once()


@patch('gwcelery.tasks.alerts._upload_notice.run')
@patch('gwcelery.tasks.gracedb.download._orig_run',
       return_value=read_binary(
           data,
           'MS220722v_bayestar.multiorder.fits'
       ))
def test_validation_fail(mock_download, mock_upload, monkeypatch):
    """Confirm that an alert that doesn't follow the schema fails validation.
    """

    # Replace the stream object with a mock object with _validiate_alert as its
    # write attribute
    mock_stream = Mock()
    mock_stream_write = Mock(side_effect=_validate_alert)
    mock_stream.write = mock_stream_write
    mock_stream.serialization_model = AvroBlobWrapper
    monkeypatch.setitem(app.conf, 'kafka_streams', {'scimma': mock_stream})

    # Load superevent dictionary, and embright/pastro json tuple
    superevent = read_json(data, 'MS220722v.json')
    classification = (
        '{"HasNS": 1.0, "HasRemnant": 1.0}',
        '{"BNS": 0.9999976592278448, "NSBH": 0.0, "BBH": 0.0,'
        '"Terrestrial": 2.3407721552252815e-06}'
    )
    skymap_fname = 'MS220722v_bayestar.multiorder.fits'

    with pytest.raises(fastavro._validate_common.ValidationError):
        # Change the superevent_id field of the superevent to None because the
        # schema does not allow None, so we know this should make the
        # validation fail
        superevent['superevent_id'] = None
        alerts.download_skymap_and_send_alert(classification, superevent,
                                              'initial', skymap_fname)
