from unittest.mock import Mock, patch

import pytest

from .. import app
from ..tasks import alerts
from ..util import read_binary, read_json
from . import data


@patch('gwcelery.tasks.alerts._upload_notice.run')
@patch('gwcelery.tasks.gracedb.download._orig_run',
       return_value=read_binary(
           data,
           'MS220722v_bayestar.multiorder.fits'
       ))  # Using only bayestar sky map because content doesnt matter
def test_not_implemented_burst_pipeline(mock_download, mock_upload,
                                        monkeypatch):
    """Confirm that using an event from a  Burst pipeline that isn't
    cwb/mly/olib to generate an alert will raise a NotImplementedError.
    """

    # Replace the stream object with a mock object with _validiate_alert as its
    # write attribute
    mock_stream = Mock()
    mock_stream_write = Mock()
    mock_stream.write = mock_stream_write
    monkeypatch.setitem(app.conf, 'kafka_streams', {'scimma': mock_stream})

    # Load superevent dictionary from cwb, change pipeline name
    superevent_dict = read_json(data, 'S230413b.json')
    superevent_dict['preferred_event_data']['pipeline'] = 'RealFakeDoors'
    skymap = 'MS220722v_bayestar.multiorder.fits'

    with pytest.raises(NotImplementedError):
        # Change the superevent_id field of the superevent to None because the
        # schema does not allow None, so we know this should make the
        # validation fail
        alerts.download_skymap_and_send_alert([None, None], superevent_dict,
                                              'initial', skymap)

    mock_upload.assert_not_called()
    mock_stream_write.assert_not_called()
