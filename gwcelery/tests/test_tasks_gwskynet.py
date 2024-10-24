import gzip
import io
import json
from unittest.mock import patch

import numpy as np
import pytest
import scipy.stats as stats
from astropy.table import Table

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
    table.meta['instruments'] = ['H1', 'L1', 'V1']
    with gzip.GzipFile(fileobj=bytesio, mode='wb') as f:
        table.write(f, format='fits')
    return bytesio.getvalue()


def get_toy_snrs():
    np.random.seed(1000)
    return np.random.uniform(low=0, high=10, size=3)


def get_toy_file_list():
    toy_file_list = {"bayestar.multiorder.fits":
                     'https://gracedb-playground.ligo.org/api/superevents/',
                     "bayestar.multiorder.fits,0":
                     'https://gracedb-playground.ligo.org/api/superevents/',
                     "bayestar.multiorder.fits,1":
                     'https://gracedb-playground.ligo.org/api/superevents/',
                     "bayestar.multiorder.fits,2":
                     'https://gracedb-playground.ligo.org/api/superevents/',
                     "H1_omegascan.png":
                     'https://gracedb-playground.ligo.org/api/superevents/',
                     "H1_omegascan.png,0":
                     'https://gracedb-playground.ligo.org/api/superevents/',
                     "em_bright.json":
                     'https://gracedb-playground.ligo.org/api/superevents/',
                     "em_bright.json,0":
                     'https://gracedb-playground.ligo.org/api/superevents/'}
    return toy_file_list


def get_toy_id_and_filename():
    toy_id_and_filename = {"graceid": 'TS12345',
                           "skymap_filename": 'bayestar.multiorder.fits,1'}
    return toy_id_and_filename


def test_gwskynet_annotation():
    toy_id_and_filename = get_toy_id_and_filename()
    outputs = json.loads(gwskynet.gwskynet_annotation(
        [get_toy_3d_fits_filecontents(),
         toy_id_and_filename["skymap_filename"]], get_toy_snrs(),
        toy_id_and_filename["graceid"]))
    expected_output = {"GWSkyNet_score": 0.141,
                       "GWSkyNet_FAP": 0.614,
                       "GWSkyNet_FNP": 0.007}
    for k, v in expected_output.items():
        assert outputs[k] == pytest.approx(v, abs=1e-3)


@patch('gwcelery.tasks.gracedb.get_superevent_file_list',
       return_value=get_toy_file_list())
@patch('gwcelery.tasks.gracedb.download.run',
       return_value=get_toy_3d_fits_filecontents())
@patch('gwcelery.tasks.gracedb.upload.run')
def test_handle_cbc_superevent(mock_upload, mock_download, mock_filelist):
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
    mock_filelist.assert_called_once_with('TS12345')
    mock_download.assert_called_once_with('bayestar.multiorder.fits,2',
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


@patch('gwcelery.tasks.gracedb.get_superevent_file_list',
       return_value=get_toy_file_list())
@patch('gwcelery.tasks.gracedb.download.run',
       return_value=get_toy_3d_fits_filecontents())
@patch('gwcelery.tasks.gracedb.upload.run')
def test_handle_high_FAR_cbc_superevent(mock_upload, mock_download,
                                        mock_filelist):
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
    mock_filelist.assert_called_once_with('TS12345')
    mock_download.assert_called_once_with('bayestar.multiorder.fits,2',
                                          'TS12345')
    mock_upload.assert_called_once()


@patch('gwcelery.tasks.gracedb.get_superevent_file_list',
       return_value=get_toy_file_list())
@patch('gwcelery.tasks.gracedb.download.run',
       return_value=get_toy_3d_fits_filecontents())
@patch('gwcelery.tasks.gracedb.upload.run')
def test_handle_prefer_update_cbc_superevent(mock_upload, mock_download,
                                             mock_filelist):
    alert = {
        "data": {
            "file_version": 0,
            "comment": ("Localization copied from TG12345")
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
    mock_filelist.assert_called_once_with('TS12345')
    mock_download.assert_called_once_with('bayestar.multiorder.fits,2',
                                          'TS12345')
    mock_upload.assert_called_once()


@patch('gwcelery.tasks.gracedb.get_superevent_file_list',
       return_value=get_toy_file_list())
@patch('gwcelery.tasks.gracedb.download.run',
       return_value=get_toy_3d_fits_filecontents())
@patch('gwcelery.tasks.gracedb.upload.run')
def test_handle_man_prefer_update_cbc_superevent(mock_upload, mock_download,
                                                 mock_filelist):
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
    mock_filelist.assert_called_once_with('TS12345')
    mock_download.assert_called_once_with('bayestar.multiorder.fits,2',
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
