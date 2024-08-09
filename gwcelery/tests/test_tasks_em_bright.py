import json
from unittest.mock import patch

import h5py
import numpy as np
import pytest

from ..tasks import em_bright
from ..util.tempfile import NamedTemporaryFile


@patch('gwcelery.tasks.gracedb.download.run', return_value='mock_em_bright')
@patch('gwcelery.tasks.gracedb.upload.run')
@patch('gwcelery.tasks.em_bright.plot.run')
def test_handle_em_bright_json(mock_plot, mock_upload, mock_download):
    alert = {
        "data": {
            "tag_names": ["em_bright"],
            "filename": "em_bright.json"
        },
        "uid": "TS123456",
        "alert_type": "log"
    }
    em_bright.handle(alert)
    mock_download.assert_called_once_with(
        'em_bright.json',
        'TS123456'
    )
    mock_plot.assert_called_once_with('mock_em_bright')
    mock_upload.assert_called_once()


@pytest.mark.parametrize(
    'pipeline,search,embright',
    [['gstlal', 'allsky',
     {'HasNS': 1.0, 'HasRemnant': 1.0, 'HasMassGap': 0.0}],
     ['gstlal', 'ssm',
     {'HasNS': 0.92, 'HasSSM': 0.84, 'HasMassGap': 0.04}],
     ['mbta', 'ssm',
     {'HasNS': 0.92, 'HasSSM': 0.92, 'HasMassGap': 0.04}]]
)
def test_classifier(pipeline, search, embright, socket_enabled):
    res = json.loads(
        em_bright.source_properties(
            1.355607, 1.279483, 0.0, 0.0, 15.6178,
            pipeline=pipeline, search=search
        )
    )
    assert all(
        v == pytest.approx(res[k], abs=1e-3) for k, v in embright.items())


@pytest.mark.parametrize(
        'posterior_samples,embright',
        [[[(1.2, 1.0, 0.0, 0.0, 100.0, 0.0, 0.0),
           (2.0, 0.5, 0.99, 0.99, 150.0, 0.0, 0.0)],
          {'HasNS': 1.0, 'HasRemnant': 1.0, 'HasMassGap': 0.5, 'HasSSM': 0.0}],
         [[(0.7, 1.0, 0.0, 0.0, 100.0, 0.0, 0.0)],
         {'HasNS': 0.0, 'HasRemnant': 0.0, 'HasMassGap': 0.0, 'HasSSM': 1.0}],
         [[(20., 12.0, 0.0, 0.0, 200.0, 0.0, 0.0),
           (22.0, 11.5, 0.80, 0.00, 250.0, 0.0, 0.0),
           (21.0, 10.0, 0.0, 0.0, 250, 0.0, 0.0)],
          {'HasNS': 0.0, 'HasRemnant': 0.0, 'HasMassGap': 0.0,
           'HasSSM': 0.0}]])
def test_posterior_samples(posterior_samples, embright, socket_enabled):
    with NamedTemporaryFile() as f:
        filename = f.name
        with h5py.File(f, 'r+') as tmp_h5:
            data = np.array(
                    posterior_samples,
                    dtype=[('chirp_mass', '<f8'), ('mass_ratio', '<f8'),
                           ('a_1', '<f8'), ('a_2', '<f8'),
                           ('luminosity_distance', '<f8'), ('tilt_1', '<f8'),
                           ('tilt_2', '<f8')])
            tmp_h5.create_dataset(
                'posterior_samples',
                data=data)
        content = open(filename, 'rb').read()
    r = json.loads(em_bright.em_bright_posterior_samples(content))
    assert r == embright
