import gzip
import io
from unittest.mock import patch
import json

from astropy.table import Table
import numpy as np
import scipy.stats as stats
import pytest

from ..tasks import gwskynet


def get_toy_3d_fits_filecontents():
    np.random.seed(1000)
    bytesio = io.BytesIO()
    mean = np.random.uniform(low=-1.0, high=1.0)
    sigma = np.random.uniform(low=0.0, high=0.5)
    size = 786432
    prob_cor = np.linspace(mean - 3 * sigma, mean + 3 * sigma, size)
    prob = stats.norm.pdf(prob_cor, mean, sigma)
    table = Table(
        [prob] * 4,
        names=['PROB', 'DISTMU', 'DISTSIGMA', 'DISTNORM'])
    table.meta['LOGBSN'] = 8.0
    table.meta['LOGBCI'] = 3.5
    table.meta['ORDERING'] = 'NESTED'
    table.meta['creator'] = 'BAYESTAR'
    table.meta['distmean'] = np.random.uniform(low=0.0, high=1000)
    table.meta['diststd'] = np.random.uniform(low=0.0, high=250)
    table.meta['instruments'] = ['H1', 'V1']
    with gzip.GzipFile(fileobj=bytesio, mode='wb') as f:
        table.write(f, format='fits')
    return bytesio.getvalue()


def test_gwskynet_annotation():
    outputs = json.loads(gwskynet.gwskynet_annotation(
        get_toy_3d_fits_filecontents()))
    expected_output = {"class_score": 0,
                       "FAP": 1,
                       "FNP": 0}
    for k, v in expected_output.items():
        assert outputs[k] == pytest.approx(v, abs=1e-3)


@patch('gwcelery.tasks.gracedb.download.run',
       return_value=get_toy_3d_fits_filecontents())
@patch('gwcelery.tasks.gracedb.upload.run')
def test_handle_cbc_superevent(mock_upload, mock_download):
    alert = {
        "data": {
            "file_version": 0,
            "name": "GCN_PRELIM_SENT"
        },
        "uid": "TS12345",
        "alert_type": "label_added",
        "object": {
            "preferred_event_data": {
                "group": "CBC",
                "extra_attributes": {
                    'SingleInspiral': [
                        {"ifo": "H1",
                         "snr": 5.6},
                        {"ifo": "L1",
                         "snr": 6.2},
                        {"ifo": "V1",
                         "snr": 3.5}]
                },
                "far": 1e-9,
                "search": 'AllSky',
                "labels": [],
                "offline": False
            }
        }
    }

    gwskynet.handle_cbc_superevent(alert)
    mock_download.assert_called_once_with('bayestar.multiorder.fits',
                                          'TS12345')
    mock_upload.assert_called_once()


@patch('gwcelery.tasks.gracedb.download.run',
       return_value=get_toy_3d_fits_filecontents())
@patch('gwcelery.tasks.gracedb.upload.run')
def test_handle_extreme_high_FAR_cbc_superevent(mock_upload, mock_download):
    alert = {
        "data": {
            "file_version": 0,
            "name": "SKYMAP_READY"
        },
        "uid": "TS12345",
        "alert_type": "label_added",
        "object": {
            "preferred_event_data": {
                "group": "CBC",
                "extra_attributes": {
                    'SingleInspiral': [
                        {"ifo": "H1",
                         "snr": 6.6},
                        {"ifo": "L1",
                         "snr": 6.2},
                        {"ifo": "V1",
                         "snr": 3.5}]
                },
                "far": 1e-04,
                "search": 'AllSky',
                "labels": [],
                "offline": False
            }
        }
    }

    gwskynet.handle_cbc_superevent(alert)
    mock_download.assert_not_called()
    mock_upload.assert_not_called()


@patch('gwcelery.tasks.gracedb.download.run',
       return_value=get_toy_3d_fits_filecontents())
@patch('gwcelery.tasks.gracedb.upload.run')
def test_handle_high_FAR_cbc_superevent(mock_upload, mock_download):
    alert = {
        "data": {
            "file_version": 0,
            "name": "SKYMAP_READY"
        },
        "uid": "TS12345",
        "alert_type": "label_added",
        "object": {
            "preferred_event_data": {
                "group": "CBC",
                "extra_attributes": {
                    'SingleInspiral': [
                        {"ifo": "H1",
                         "snr": 4.6},
                        {"ifo": "L1",
                         "snr": 6.2},
                        {"ifo": "V1",
                         "snr": 4.5}]
                },
                "far": 3e-05,
                "search": 'AllSky',
                "labels": [],
                "offline": False
            }
        }
    }

    gwskynet.handle_cbc_superevent(alert)
    mock_download.assert_called_once_with('bayestar.multiorder.fits',
                                          'TS12345')
    mock_upload.assert_called_once()


@patch('gwcelery.tasks.gracedb.download.run',
       return_value=get_toy_3d_fits_filecontents())
@patch('gwcelery.tasks.gracedb.upload.run')
def test_handle_prefer_update_cbc_superevent(mock_upload, mock_download):
    alert = {
        "data": {
            "file_version": 0,
            "comment": ("Updated superevent parameters: "
                        "preferred_event: TG12344 -> TG12345")
        },
        "uid": "TS12345",
        "alert_type": "log",
        "object": {
            "labels": ["LOW_SIGNIF_PRELIM_SENT"],
            "preferred_event_data": {
                "group": "CBC",
                "extra_attributes": {
                    'SingleInspiral': [
                        {"ifo": "H1",
                         "snr": 4.6},
                        {"ifo": "L1",
                         "snr": 6.2},
                        {"ifo": "V1",
                         "snr": 4.5}]
                },
                "far": 4e-06,
                "search": 'AllSky',
                "labels": [],
                "offline": False
            }
        }
    }

    gwskynet.handle_cbc_superevent(alert)
    mock_download.assert_called_once_with('bayestar.multiorder.fits',
                                          'TS12345')
    mock_upload.assert_called_once()


@patch('gwcelery.tasks.gracedb.download.run',
       return_value=get_toy_3d_fits_filecontents())
@patch('gwcelery.tasks.gracedb.upload.run')
def test_handle_man_prefer_update_cbc_superevent(mock_upload, mock_download):
    alert = {
        "data": {
            "file_version": 0,
            "comment": ("User Albert.Einstein queued a preferred event"
                        " change to TS12345.")
        },
        "uid": "TS12345",
        "alert_type": "log",
        "object": {
            "labels": ["LOW_SIGNIF_PRELIM_SENT"],
            "preferred_event_data": {
                "group": "CBC",
                "extra_attributes": {
                    'SingleInspiral': [
                        {"ifo": "H1",
                         "snr": 4.6},
                        {"ifo": "L1",
                         "snr": 6.2},
                        {"ifo": "V1",
                         "snr": 4.5}]
                },
                "far": 4e-06,
                "search": 'AllSky',
                "labels": [],
                "offline": False
            }
        }
    }

    gwskynet.handle_cbc_superevent(alert)
    mock_download.assert_called_once_with('bayestar.multiorder.fits',
                                          'TS12345')
    mock_upload.assert_called_once()


@patch('gwcelery.tasks.gracedb.download.run',
       return_value=get_toy_3d_fits_filecontents())
@patch('gwcelery.tasks.gracedb.upload.run')
def test_handle_not_prefer_update_cbc_superevent(mock_upload, mock_download):
    alert = {
        "data": {
            "file_version": 0,
            "name": "SKYMAP_READY",
            "labels": ["LOW_SIGNIF_PRELIM_SENT"]
        },
        "uid": "TS12345",
        "alert_type": "update",
        "object": {
            "preferred_event_data": {
                "group": "CBC",
                "extra_attributes": {
                    'SingleInspiral': [
                        {"ifo": "H1",
                         "snr": 4.6},
                        {"ifo": "L1",
                         "snr": 6.2},
                        {"ifo": "V1",
                         "snr": 4.5}]
                },
                "far": 4e-06,
                "search": 'AllSky',
                "labels": [],
                "offline": False
            }
        }
    }

    gwskynet.handle_cbc_superevent(alert)
    mock_download.assert_not_called()
    mock_upload.assert_not_called()


@patch('gwcelery.tasks.gracedb.download.run',
       return_value=get_toy_3d_fits_filecontents())
@patch('gwcelery.tasks.gracedb.upload.run')
def test_handle_earlywarning_cbc_event(mock_upload, mock_download):
    alert = {
        "data": {
            "file_version": 0,
            "name": "SKYMAP_READY"
        },
        "uid": "TS12345",
        "alert_type": "label_added",
        "object": {
            "preferred_event_data": {
                "group": "CBC",
                "extra_attributes": {
                    'SingleInspiral': [
                        {"ifo": "H1",
                         "snr": 6.6},
                        {"ifo": "L1",
                         "snr": 6.2},
                        {"ifo": "V1",
                         "snr": 3.5}]
                },
                "far": 1e-15,
                "search": 'EarlyWarning',
                "labels": [],
                "offline": False
            }
        }
    }

    gwskynet.handle_cbc_superevent(alert)
    mock_download.assert_not_called()
    mock_upload.assert_not_called()
