from unittest.mock import call, patch

import pytest
from celery import exceptions
from requests.exceptions import HTTPError
from requests.models import Response

from .. import app
from ..tasks import gracedb, superevents
from ..util import read_json
from . import data

SUPEREVENTS_NEIGHBOURS = read_json(data, 'superevents.json')
"""Dictionary storing mock superevent neighbors"""


def assert_not_called_with(mock, *args, **kwargs):
    assert call(*args, **kwargs) not in mock.call_args_list


def test_select_preferred_event():
    """Provide the list of trigger information of a sample event."""
    events = read_json(data, 'sample_events.json')
    r = superevents.select_preferred_event(events)
    assert r['graceid'] == 'G3'


def test_select_pipeline_preferred_event():
    """Test selecting preferred event based on pipeline"""
    sample_event = {
        'instruments': 'H1,L1',
        'offline': False,
        'far': 6e-30,
        'search': 'AllSky',
        'labels': ['SKYMAP_READY', 'EMBRIGHT_READY', 'PASTRO_READY'],
        'extra_attributes': {
            'CoincInspiral': {
                'ifos': 'H1,L1',
                'snr': 30,
                'combined_far': 6e-30
            },
            'SingleInspiral': [{'ifo': 'H1'}, {'ifo': 'L1'}]
        }
    }
    event_snr_combination = {
        'G1': {'group': 'CBC', 'pipeline': 'gstlal', 'snr': 30},
        'G2': {'group': 'CBC', 'pipeline': 'gstlal', 'snr': 20},
        'G3': {'group': 'CBC', 'pipeline': 'pycbc', 'snr': 40},
        'G4': {'group': 'CBC', 'pipeline': 'pycbc', 'snr': 10},
        'G5': {'group': 'CBC', 'pipeline': 'spiir', 'snr': 15},
        'G6': {'group': 'Burst', 'pipeline': 'cwb', 'far': 1e-15},
        'G7': {'group': 'Burst', 'pipeline': 'cwb', 'far': 1e-30}
    }
    events = []
    for event_id, content in event_snr_combination.items():
        event = sample_event.copy()
        event['graceid'] = event_id
        event['pipeline'] = content['pipeline']
        event['group'] = content['group']
        snr = content.get('snr')
        far = content.get('far')
        if snr:
            event['extra_attributes']['CoincInspiral']['snr'] = snr
        if far:
            event['far'] = far
        events.append(event)
    res = superevents.select_pipeline_preferred_event(events)
    assert len(res) == 4
    # expected pipeline preferred events
    for pipeline, expected_event in {
            'gstlal': 'G1', 'pycbc': 'G3',
            'spiir': 'G5', 'cwb': 'G7'}.items():
        assert res[pipeline]['graceid'] == expected_event


@pytest.mark.parametrize(
    'superevent_labels,new_event_labels,preferred_event_labels,new_event_id',
    [[[],
      ['EMBRIGHT_READY', 'PASTRO_READY', 'SKYMAP_READY'],
      ['EMBRIGHT_READY', 'PASTRO_READY'], 'T1234'],
     [[],
      ['EMBRIGHT_READY', 'PASTRO_READY'],
      ['EMBRIGHT_READY', 'PASTRO_READY', 'SKYMAP_READY'], 'T1234'],
     [[],
      ['EMBRIGHT_READY', 'PASTRO_READY',
       'SKYMAP_READY'],
      ['EMBRIGHT_READY', 'PASTRO_READY',
       'SKYMAP_READY'], 'T1234'],
     [[],
      ['EMBRIGHT_READY', 'PASTRO_READY'],
      ['EMBRIGHT_READY', 'PASTRO_READY',
       'SKYMAP_READY'], 'T0212'],
     [['LOW_SIGNIF_LOCKED', 'ADVREQ', 'DQOK'],
      ['EMBRIGHT_READY', 'PASTRO_READY',
       'SKYMAP_READY'],
      ['EMBRIGHT_READY', 'PASTRO_READY',
       'SKYMAP_READY'], 'T1234']])
def test_update_preferred_event(superevent_labels, new_event_labels,
                                preferred_event_labels, new_event_id):
    """Test scenarios pertaining to superevent S0039, with a
    preferred event T0212. The new event dictionary corresponds
    to either T0212, when new labels are added, or a new event
    T1234. The new event T1234 passes FAR threshold and has higher
    SNR compared to the preferred event.
    """
    preferred_event_dictionary = read_json(data, 'T0212_S0039_preferred.json')
    preferred_event_dictionary['labels'] = preferred_event_labels
    if new_event_id == 'T1234':
        new_event_dictionary = dict(
            graceid="T1234",
            instruments="I1,J1,K1,L1,M1",
            group="CBC",
            pipeline="gstlal",
            search="AllSky",
            offline=False,
            superevent="maybe S0039 or empty",
            far=1e-30,
            labels=[],
            extra_attributes=dict(
                CoincInspiral=dict(snr=30.0),
                SingleInspiral=[
                    {'ifo': ifo} for ifo in "I1,J1,K1,L1,M1".split(',')
                ]
            )
        )
    elif new_event_id == 'T0212':
        new_event_dictionary = preferred_event_dictionary.copy()

    new_event_dictionary['labels'] = new_event_labels

    superevent_response = SUPEREVENTS_NEIGHBOURS['S0039'].copy()
    superevent_response['labels'] = superevent_labels
    superevent_response['preferred_event_data'] = preferred_event_dictionary
    mock_get_superevent = patch('gwcelery.tasks.gracedb.get_superevent',
                                return_value=superevent_response)

    with mock_get_superevent, \
            patch('gwcelery.tasks.gracedb.update_superevent') as p, \
            patch('gwcelery.tasks.gracedb.create_label.run') as create_label:
        superevents._update_superevent('S0039',
                                       new_event_dictionary,
                                       None,
                                       None,
                                       None)
        # presence of FROZEN_LABEL should not update superevent
        if superevents.FROZEN_LABEL in superevent_labels:
            p.assert_not_called()

        else:
            is_complete_preferred = superevents.is_complete(
                preferred_event_dictionary
            )
            is_complete_new = superevents.is_complete(
                new_event_dictionary
            )

            if new_event_id == 'T1234' and (
                    is_complete_preferred and not is_complete_new):
                p.assert_not_called()
                create_label.assert_not_called()
            elif new_event_id == 'T1234' and (
                    not is_complete_preferred and is_complete_new):
                p.assert_called_with('S0039', preferred_event='T1234',
                                     t_0=None)
                create_label.assert_called_once_with('EM_READY', 'S0039')

            elif new_event_id == 'T1234' and (
                    is_complete_preferred and is_complete_new):
                p.assert_called_with('S0039', preferred_event='T1234',
                                     t_0=None)
                create_label.assert_called_once_with('EM_READY', 'S0039')
            elif new_event_id == 'T1234' and (
                    not is_complete_preferred and not is_complete_new):
                p.assert_called_with('S0039', preferred_event='T1234',
                                     t_0=None)
                create_label.assert_called_once_with('EM_READY', 'S0039')
            # this is the case of label addition to existing preferred event
            elif new_event_id == 'T0212':
                p.assert_not_called()
                # label addition completes event
                if not is_complete_preferred and is_complete_new:
                    create_label.assert_called_once_with('EM_READY', 'S0039')
                else:
                    create_label.assert_not_called()


@pytest.mark.parametrize('labels',
                         [['PASTRO_READY', 'RAVEN_ALERT'],
                          ['SKYMAP_READY', 'EMBRIGHT_READY',
                           'PASTRO_READY', 'RAVEN_ALERT']])
@patch('gwcelery.tasks.gracedb.create_label.run')
def test_raven_alert(mock_create_label, labels):
    payload = {
        "uid": "G000003",
        "alert_type": "label_added",
        "object": {
            "graceid": "G000003",
            "pipeline": "gstlal",
            "group": "CBC",
            "search": "AllSky",
            "far": 1e-6,
            "instruments": "H1,L1",
            "superevent": "S100",
            "superevent_neighbours": {
                "S100": {
                    "far": 0.0000342353,
                    "gw_events": [
                        "G000003"
                    ],
                    "preferred_event": "G000003",
                    "preferred_event_data": {
                        "created": "",
                        "extra_attributes": {
                            'SingleInspiral': [
                                {
                                    'chisq': 1.07,
                                    'snr': 10.95,
                                    'ifo': 'L1'
                                },
                                {
                                    'chisq': 0.54,
                                    'snr': 12.35,
                                    'ifo': 'H1'
                                }
                            ],
                            'CoincInspiral': {
                                'snr': 10.17
                            },
                        },
                        "far": 1e-6,
                        "far_is_upper_limit": False,
                        "gpstime": 1000000,
                        "graceid": "G000003",
                        "group": "CBC",
                        "instruments": "H1,L1",
                        "labels": labels,
                        "likelihood": "",
                        "nevents": 0,
                        "offline": False,
                        "pipeline": "gstlal",
                        "reporting_latency": "",
                        "search": "AllSky",
                        "submitter": "albert.einstein@LIGO.ORG"
                    },
                    "superevent_id": "S100",
                    "labels": [],
                    "t_0": 1167264018.0,
                    "t_end": 1167264025.0,
                    "t_start": 1167264014.0
                }
            },
            "offline": False,
            "gpstime": 1000000,
            "labels": labels,
            'extra_attributes': {
                'SingleInspiral': [
                    {
                        'chisq': 1.07,
                        'snr': 10.95,
                        'ifo': 'L1'
                    },
                    {
                        'chisq': 0.54,
                        'snr': 12.35,
                        'ifo': 'H1'
                    }
                ],
                'CoincInspiral': {
                    'snr': 10.17
                },
            },
        },
        "data": {
            "name": "RAVEN_ALERT"
        }
    }
    superevent_response = payload['object']['superevent_neighbours']['S100']
    with patch('gwcelery.tasks.gracedb.get_superevent',
               return_value=superevent_response):
        superevents.handle(payload)
    calls = [call('ADVREQ', 'S100')]
    if {'SKYMAP_READY', 'EMBRIGHT_READY', 'PASTRO_READY'}.issubset(labels):
        calls.extend(
            [call('EM_READY', 'S100'), call('LOW_SIGNIF_LOCKED', 'S100'),
             call('SIGNIF_LOCKED', 'S100')]
        )
    mock_create_label.assert_has_calls(calls, any_order=True)


@pytest.mark.parametrize('allsky_far', [1e-6, 1e-9])
def test_allsky_preferred_over_earlywarning(allsky_far):
    """New AllSky event (both low-significance and otherwise) updates
    preferred event of an existing superevent with a low-significance
    EarlyWarning preferred event."""
    payload = {
        "uid": "G123456",
        "alert_type": "new",
        "object": {
            "graceid": "G123456",
            "pipeline": "gstlal",
            "group": "CBC",
            "search": "AllSky",
            "far": allsky_far,
            "instruments": "H1,L1",
            "superevent": None,
            "superevent_neighbours": {
                "S100": {  # assumed to be low-significance early-warning
                    "far": 1e-6,
                    "gw_events": [
                        "G567890"
                    ],
                    "preferred_event": "G567890",
                    "preferred_event_data": {
                        "extra_attributes": {
                            'SingleInspiral': [
                                {
                                    'chisq': 1.07,
                                    'snr': 10.95,
                                    'ifo': 'L1'
                                },
                                {
                                    'chisq': 0.54,
                                    'snr': 12.35,
                                    'ifo': 'H1'
                                }
                            ],
                            'CoincInspiral': {
                                'snr': 10.17
                            },
                        },
                        "far": 1.e-6,
                        "far_is_upper_limit": False,
                        "gpstime": 1000000,
                        "graceid": "G567890",
                        "group": "CBC",
                        "instruments": "H1,L1",
                        "labels": ['SKYMAP_READY', 'EMBRIGHT_READY'],
                        "offline": False,
                        "pipeline": "gstlal[]",
                        "reporting_latency": "",
                        "search": "EarlyWarning",
                        "submitter": "albert.einstein@LIGO.ORG"
                    },
                    "superevent_id": "S100",
                    "labels": [],
                    "t_0": 1000000,
                    "t_end": 1000001,
                    "t_start": 999999
                }
            },
            "offline": False,
            "gpstime": 1000000.1,
            "labels": [],
            'extra_attributes': {
                'SingleInspiral': [
                    {
                        'chisq': 1.07,
                        'snr': 8,
                        'ifo': 'L1'
                    },
                    {
                        'chisq': 0.54,
                        'snr': 8,
                        'ifo': 'H1'
                    }
                ],
                'CoincInspiral': {
                    'snr': 11.3
                },
            },
        },
    }
    superevent_response = payload['object']['superevent_neighbours']['S100']
    with patch('gwcelery.tasks.gracedb.get_superevents',
               return_value=[superevent_response]), \
            patch('gwcelery.tasks.gracedb.get_superevent',
                  return_value=superevent_response), \
            patch('gwcelery.tasks.gracedb.update_superevent') as p:
        superevents.handle(payload)
        p.assert_called_once_with(
            'S100', t_start=999999, t_end=1000001.1, t_0=1000000.1,
            preferred_event='G123456'
        )


@pytest.mark.parametrize('search', ['EarlyWarning', 'SSM'])
def test_do_not_send_low_signif_alerts(search):
    """Test that uploads from EarlyWarning and SSM do not trigger low
    significance notices.
    """
    payload = {
        "uid": "G123456",
        "alert_type": "label_added",
        "data": {"name": "SKYMAP_READY"},
        "object": {
            "graceid": "G123456",
            "pipeline": "gstlal",
            "group": "CBC",
            "search": search,
            "far": 2e-6,
            "instruments": "H1,L1",
            "superevent": "S100",
            "superevent_neighbours":
            {
                "S100": {
                    "far": 2e-6,
                    "gw_events": [
                        "G123456"
                    ],
                    "preferred_event": "G123456",
                    "preferred_event_data": {
                        'extra_attributes': {
                            'SingleInspiral': [
                                {
                                    'chisq': 1.07,
                                    'snr': 8,
                                    'ifo': 'L1'
                                },
                                {
                                    'chisq': 0.54,
                                    'snr': 8,
                                    'ifo': 'H1'
                                }
                            ],
                            'CoincInspiral': {
                                'snr': 11.3
                            },
                        },
                        "far": 3.e-6,
                        "far_is_upper_limit": False,
                        "gpstime": 1000000.1,
                        "graceid": "G123456",
                        "group": "CBC",
                        "instruments": "H1,L1",
                        "labels": ['SKYMAP_READY', 'PASTRO_READY',
                                   'EMBRIGHT_READY'],
                        "offline": False,
                        "pipeline": "gstlal",
                        "reporting_latency": "",
                        "search": search,
                        "submitter": "albert.einstein@LIGO.ORG"
                    },
                    "superevent_id": "S100",
                    "labels": [],
                    "t_0": 1000000,
                    "t_end": 1000001,
                    "t_start": 999999
                }
            },
            "offline": False,
            "gpstime": 1000000.1,
            "labels": ['SKYMAP_READY', 'PASTRO_READY', 'EMBRIGHT_READY'],
            'extra_attributes': {
                'SingleInspiral': [
                    {
                        'chisq': 1.07,
                        'snr': 8,
                        'ifo': 'L1'
                    },
                    {
                        'chisq': 0.54,
                        'snr': 8,
                        'ifo': 'H1'
                    }
                ],
                'CoincInspiral': {
                    'snr': 11.3
                },
            },
        },
    }
    superevent_response = payload['object']['superevent_neighbours']['S100']

    with patch('gwcelery.tasks.gracedb.get_superevents',
               return_value=[superevent_response]), \
            patch('gwcelery.tasks.gracedb.get_superevent',
                  return_value=superevent_response), \
            patch('gwcelery.tasks.gracedb.update_superevent') as p1, \
            patch('gwcelery.tasks.gracedb.create_label._orig_run') as p2:
        superevents.handle(payload)

        p1.assert_not_called()
        p2.assert_called_with(superevents.READY_LABEL, 'S100')
        assert_not_called_with(p2, superevents.FROZEN_LABEL, 'S100')


@pytest.mark.parametrize('labels,search',
                         [[['EMBRIGHT_READY', 'PASTRO_READY'], 'AllSky'],
                          [['EMBRIGHT_READY', 'PASTRO_READY'], 'SSM'],
                          [['EMBRIGHT_READY', 'SKYMAP_READY'], 'SSM'],
                          [['SKYMAP_READY', 'EMBRIGHT_READY', 'PASTRO_READY'],
                           'AllSky']])
def test_is_complete(labels, search):
    mock_event_dict = _mock_event('G000002')
    mock_event_dict['labels'] = labels
    mock_event_dict['search'] = search
    result = superevents.is_complete(mock_event_dict)
    if search.lower() == 'allsky':
        if len(labels) == 2:
            assert result is False
        elif len(labels) == 3:
            assert result is True
    elif search.lower() == 'ssm':
        assert result == (set(labels).intersection(
            {'EMBRIGHT_READY', 'SKYMAP_READY'}
        ) == {'EMBRIGHT_READY', 'SKYMAP_READY'})


@pytest.mark.parametrize(
    'labels,label,search,alert_type,group,ifos',
    [[['EMBRIGHT_READY', 'PASTRO_READY'], 'EMBRIGHT_READY', 'AllSky',
      'label_added', 'CBC', 'H1,L1'],
     [['SKYMAP_READY', 'EMBRIGHT_READY', 'PASTRO_READY'], 'SKYMAP_READY',
      'AllSky', 'label_added', 'CBC', 'H1,L1'],
     [['EMBRIGHT_READY'], 'EMBRIGHT_READY', 'EarlyWarning',
      'label_added', 'CBC', 'H1,L1'],  # Less-significant EW
     [['EMBRIGHT_READY'], 'EMBRIGHT_READY', 'EarlyWarning', 'new',
      'CBC', 'H1,L1,V1'],
     [[''], '', 'SSM', 'new', 'CBC', 'H1,L1'],
     [['PASTRO_READY'], '', 'SSM', 'label_added', 'CBC', 'H1,L1'],
     [[''], '', 'AllSky', 'new', 'CBC', 'H1,L1,K1'],
     [[''], '', 'AllSky', 'new', 'CBC', 'H1,L1,V1,K1'],
     [[''], '', 'AllSky', 'new', 'Burst', 'H1,L1,K1']]
)
def test_process_called(labels, label, search, alert_type, group, ifos):
    """Test whether the :meth:`superevents.process` is called
    new type IGWN alerts, and label additions that complete the event.
    """
    payload = {
        "alert_type": alert_type,
        "data": {"name": label},
        "group": group,
        "instruments": ifos,
        "object": {
            "graceid": "ABCD",
            "far": 1e-6,
            "group": group,
            "pipeline": "pipeline",
            "search": search,
            "labels": labels,
            "instruments": ifos
        }
    }
    payload['object']["extra_attributes"] = {
        "CoincInspiral": {'snr': 4.0},
        "SingleInspiral": [
            {"ifo": ifo} for ifo in
            ifos.split(',')]
    } if group.lower() == 'cbc' else {"burst_attributes": "burst_values"}

    with patch('gwcelery.tasks.superevents.process.run') as process:
        superevents.handle(payload)
        if "K1" in ifos:
            process.assert_not_called()
        elif search == superevents.VT_SEARCH_NAME:
            process.assert_not_called()
        elif superevents.is_complete(payload['object']):
            process.assert_called()
        elif alert_type == 'new':
            process.assert_called()
        else:
            process.assert_not_called()


_mock_event_data = {
    "graceid": "G000002",
    "gpstime": 100.0,
    "pipeline": "gstlal",
    "group": "CBC",
    "search": "AllSky",
    "far": 1.e-31,
    "instruments": "H1,L1",
    "labels": [],
    "extra_attributes": {
        "CoincInspiral": {"snr": 20},
        "SingleInspiral": [{"ifo": ifo} for ifo in ["H1", "L1"]]
    },
    "offline": False
}

_mock_superevent_data = {
    "superevent_id": "S123456",
    "preferred_event": "G000002",
    "preferred_event_data": _mock_event_data,
    "t_start": 99.0,
    "t_0": 100.0,
    "gw_events": [
        "G000002",
    ],
    "t_end": 101.0,
    "labels": []
}


def _mock_event(event):
    if event == "G000002":
        _mock_event_data['superevent_neighbours'] = {
            _mock_superevent_data['superevent_id']: _mock_superevent_data}
        return _mock_event_data


@patch('gwcelery.tasks.gracedb.get_superevent',
       lambda _: _mock_superevent_data)
@patch('gwcelery.tasks.gracedb.get_superevents',
       lambda _: [_mock_superevent_data])
def test_upload_same_event():
    """New event uploaded with the same coinc file as an
    existing event G000002. The second upload should be added to
    the superevent, but no attribute should change.
    """
    # payload same as _mock_event except graceid
    object_for_G000003 = _mock_event("G000002").copy()
    object_for_G000003['graceid'] = "G000003"
    payload = {
        "uid": "G000003",
        "alert_type": "new",
        "object": object_for_G000003,
        "data": object_for_G000003
    }
    with patch('gwcelery.tasks.gracedb.add_event_to_superevent') as p1, \
            patch('gwcelery.tasks.gracedb.update_superevent') as p2:
        superevents.handle(payload)
        p1.assert_called_once()
        p2.assert_not_called()


@pytest.mark.parametrize(
    'group,pipeline,search,offline,far,instruments,labels,expected_result,'
    'expected_result_less_significant',
    [['CBC', 'gstlal', 'AllSky', False, 1.e-10, 'H1',  # complete significant
        ['PASTRO_READY', 'SKYMAP_READY', 'EMBRIGHT_READY'], True, True],
     ['CBC', 'gstlal', 'AllSky', False, 1.e-6, 'H1',  # complete less-signif
        ['PASTRO_READY', 'SKYMAP_READY', 'EMBRIGHT_READY'], False, True],
     ['CBC', 'gstlal', 'AllSky', False, 2.5e-5, 'H1',  # complete noise
        ['PASTRO_READY', 'SKYMAP_READY', 'EMBRIGHT_READY'], False, False],
     ['CBC', 'gstlal', 'AllSky', False, 1.e-6, 'H1',  # incomplete less-signif
        ['PASTRO_READY', 'SKYMAP_READY'], False, False],
     ['CBC', 'gstlal', 'AllSky', False, 1.e-6, 'H1,L1',  # incomplete CBC
        ['EMBRIGHT_READY'], False, False],
     ['Burst', 'cwb', 'AllSky', False, 1e-15, 'H1,L1',  # complete Burst
        ['SKYMAP_READY'], True, True],
     ['Burst', 'cwb', 'AllSky', False, 1e-15, 'H1,L1',  # incomplete Burst
        [], False, False],
     ['Burst', 'cwb', 'BBH', False, 1e-15, 'H1,L1',  # complete cWB-BBH
        ['SKYMAP_READY', 'PASTRO_READY', 'EMBRIGHT_READY'], True, True],
     ['Burst', 'cwb', 'BBH', False, 1e-15, 'H1,L1',  # incomplete cWB-BBH
        ['SKYMAP_READY', 'PASTRO_READY'], False, False],
     ['Burst', 'cwb', 'BBH', False, 1e-15, 'H1,L1',  # incomplete cWB-BBH
        ['SKYMAP_READY', 'EMBRIGHT_READY'], False, False],
     ['Burst', 'cwb', 'BBH', False, 1e-15, 'H1,L1',  # incomplete cWB-BBH
        ['SKYMAP_READY'], False, False],
     ['Burst', 'cwb', 'BBH', False, 1e-15, 'H1,L1',  # incomplete cWB-BBH
        [], False, False],
     ['Burst', 'cwb', 'BBH', True, 1e-30, 'H1,L1,V1',  # incomplete cWB-BBH
        [], False, False],
     ['CBC', 'gstlal', 'AllSky', False, 1.e-6, 'H1,L1,V1',  # incomplete ext
        ['RAVEN_ALERT'], False, False],
     ['CBC', 'gstlal', 'AllSky', False, 1.e-6, 'H1,L1,V1',  # complete ext
        ['PASTRO_READY', 'SKYMAP_READY', 'EMBRIGHT_READY', 'RAVEN_ALERT'],
        True, True],
     ['CBC', 'gstlal', 'AllSky', False, 1.e-15, 'H1,L1,V1',  # complete 3 ifo significant  # noqa: E501
        ['PASTRO_READY', 'SKYMAP_READY', 'EMBRIGHT_READY', 'RAVEN_ALERT'],
        True, True],
     ['CBC', 'gstlal', 'EarlyWarning', False, 1.e-15, 'H1,L1,V1',  # EW search
        ['PASTRO_READY', 'SKYMAP_READY', 'EMBRIGHT_READY'],
        True, True],
     ['CBC', 'gstlal', 'EarlyWarning', False, 2.e-6, 'H1,L1,V1',  # EW search
        ['PASTRO_READY', 'SKYMAP_READY', 'EMBRIGHT_READY'],
        False, False],
     ['CBC', 'gstlal', 'SSM', False, 2.e-6, 'H1,L1,V1',  # SSM search
        ['PASTRO_READY', 'SKYMAP_READY', 'EMBRIGHT_READY'],
        False, False],
     ['CBC', 'gstlal', 'SSM', False, 1.e-10, 'H1,L1,V1',  # significant SSM
        ['SKYMAP_READY', 'EMBRIGHT_READY'], True, True],
     ['CBC', 'gstlal', 'SSM', False, 1.e-10, 'H1,L1,V1',  # significant SSM
        ['SKYMAP_READY'], False, False]])
def test_should_publish(group, pipeline, search, offline, far, instruments,
                        labels, expected_result,
                        expected_result_less_significant):
    """Test publishability criteria under different states of a superevent."""
    event = dict(graceid='G123456',
                 search=search,
                 group=group,
                 pipeline=pipeline,
                 labels=labels,
                 far=far,
                 offline=offline,
                 instruments=instruments)
    result_less_significant = (
        superevents.is_complete(event) and
        superevents.should_publish(event, significant=False))
    result = (superevents.is_complete(event) and
              superevents.should_publish(event))
    assert result_less_significant == expected_result_less_significant
    assert result == expected_result


def test_parse_trigger_raising():
    garbage_payload = dict(some_value=999, something_else=99)
    with pytest.raises(KeyError):
        superevents.handle(garbage_payload)


response_bad_gateway = Response()
response_bad_gateway.status_code = 502


@patch('gwcelery.tasks.gracedb.create_superevent',
       side_effect=HTTPError(response=response_bad_gateway))
def test_raising_http_error(failing_create_superevent):
    payload = {
        "uid": "G000003",
        "alert_type": "new",
        "description": "",
        "object": {
            "graceid": "G000003",
            "gpstime": 100.0,
            "pipeline": "gstlal",
            "group": "CBC",
            "search": "AllSky",
            "far": 1.e-31,
            "instruments": "H1,L1",
            "extra_attributes": {
                "CoincInspiral": {"snr": 20},
                "SingleInspiral": [
                    {'ifo': ifo} for ifo in ['H1', 'L1']
                ]
            },
            "offline": False,
            "labels": [],
            "superevent": None,
            "superevent_neighbours": {}
        },
        "data": {
            "graceid": "G000003",
            "gpstime": 100.0,
            "pipeline": "gstlal",
            "group": "CBC",
            "far": 1.e-31,
            "instruments": "H1,L1",
            "extra_attributes": {
                "CoincInspiral": {"snr": 20}
            },
            "offline": False,
            "labels": [],
            "superevent": None,
            "superevent_neighbours": {}
        }
    }
    with pytest.raises(
            exceptions.Retry), pytest.raises(gracedb.RetryableHTTPError):
        superevents.handle.delay(payload)


def test_parse_trigger_cbc_1():
    """New trigger G000000, less significant than already
    existing superevent. Superevent window much larger than event
    window of G000000.
    """
    # New trigger G000000 time falls in S0039 window
    # addEventToSuperevent should be called
    # updateSuperevent should not be updated
    event_dictionary = {'graceid': 'G000000',
                        'gpstime': 1163905224.4332082,
                        'group': 'CBC',
                        'pipeline': 'gstlal',
                        'search': 'AllSky',
                        'offline': False,
                        'far': 3e-09,
                        'instruments': 'H1,L1',
                        'labels': [],
                        'superevent': None,
                        'superevent_neighbours': SUPEREVENTS_NEIGHBOURS,
                        'extra_attributes': {
                            'CoincInspiral': {'snr': 10.0},
                            'SingleInspiral': [
                                {'ifo': ifo} for ifo in ['H1', 'L1']]}}
    payload = dict(object=event_dictionary,
                   data=event_dictionary,
                   alert_type='new',
                   uid='G000000')
    superevent_response = patch(
        'gwcelery.tasks.gracedb.get_superevent',
        return_value=SUPEREVENTS_NEIGHBOURS['S0039'])
    with superevent_response, \
            patch('gwcelery.tasks.gracedb.add_event_to_superevent') as p1, \
            patch('gwcelery.tasks.gracedb.update_superevent') as p2, \
            patch('gwcelery.tasks.gracedb.get_superevents',
                  return_value=SUPEREVENTS_NEIGHBOURS.values()) as p3:
        superevents.handle(payload)
        p1.assert_called_once()
        p2.assert_not_called()
        p3.assert_called_once()


def test_parse_trigger_cbc_2():
    """New trigger G000003, more significant than already
    existing superevent. Superevent window is much larger that
    event window of G000000
    """
    event_dictionary = {'graceid': 'G000003',
                        'gpstime': 1163905224.4332082,
                        'group': 'CBC',
                        'pipeline': 'gstlal',
                        'search': 'AllSky',
                        'offline': False,
                        'far': 3e-31,
                        'instruments': 'H1,L1',
                        'labels': [],
                        'superevent': None,
                        'superevent_neighbours': SUPEREVENTS_NEIGHBOURS,
                        'extra_attributes': {
                            'CoincInspiral': {'snr': 30.0},
                            'SingleInspiral': [
                                {"ifo": ifo} for ifo in ["H1", "L1"]]}}
    payload = dict(object=event_dictionary,
                   data=event_dictionary,
                   alert_type='new',
                   uid='G000003')
    superevent_response = patch(
        'gwcelery.tasks.gracedb.get_superevent',
        return_value=SUPEREVENTS_NEIGHBOURS['S0039'])

    # addEventToSuperevent should be called
    # preferred event should be updated, t_0 should change
    with superevent_response, \
            patch('gwcelery.tasks.gracedb.add_event_to_superevent') as p1, \
            patch('gwcelery.tasks.gracedb.update_superevent') as p2, \
            patch('gwcelery.tasks.gracedb.get_superevents',
                  return_value=SUPEREVENTS_NEIGHBOURS.values()) as p3, \
            patch('gwcelery.tasks.gracedb.create_label.run') as create_label:
        superevents.handle(payload)
        p1.assert_called_once()
        p2.assert_called_once_with('S0039', preferred_event='G000003',
                                   t_0=1163905224.4332082)
        p3.assert_called_once()
        expected_calls = [call('ADVREQ', 'S0039')]
        if superevents.is_complete(event_dictionary):
            expected_calls.extend(
                [call('EM_READY', 'S0039'), call('LOW_SIGNIF_LOCKED', 'S0039'),
                 call('SIGNIF_LOCKED', 'S0039')])
        create_label.assert_has_calls(expected_calls, any_order=True)


def test_parse_trigger_cbc_3():
    """New trigger G000001, not present among superevents
    New superevent created.
    """
    event_dictionary = {'graceid': 'G000001',
                        'gpstime': 1286741861.52678,
                        'group': 'CBC',
                        'pipeline': 'gstlal',
                        'search': 'AllSky',
                        'offline': False,
                        'far': 3e-31,
                        'instruments': 'H1,L1,V1',
                        'labels': [],
                        'superevent': None,
                        'superevent_neighbours': SUPEREVENTS_NEIGHBOURS,
                        'extra_attributes': {
                            'CoincInspiral': {'snr': 12.0},
                            'SingleInspiral': [
                                {"ifo": ifo} for ifo in
                                ["H1", "L1", "V1"]]}}
    payload = dict(object=event_dictionary,
                   data=event_dictionary,
                   alert_type='new',
                   uid='G000001')
    # G000001 absent in any superevent window, new superevent created
    # createSuperevent should be called exactly once
    with patch('gwcelery.tasks.gracedb.create_superevent') as p1, \
            patch('gwcelery.tasks.gracedb.get_superevents',
                  return_value=SUPEREVENTS_NEIGHBOURS.values()) as p2, \
            patch('gwcelery.tasks.gracedb.create_label'):
        superevents.handle(payload)
        p1.assert_called_once()
        p2.assert_called_once()


def test_parse_trigger_cbc_4():
    """New trigger G000002, doesn't pass far threshold"""
    event_dictionary = {'graceid': 'G000002',
                        'gpstime': 1286741861.52678,
                        'group': 'CBC',
                        'pipeline': 'gstlal',
                        'search': 'AllSky',
                        'offline': False,
                        'far': 5.5e-02,
                        'instruments': 'H1,L1,V1',
                        'labels': [],
                        'superevent': None,
                        'superevent_neighbours': SUPEREVENTS_NEIGHBOURS,
                        'extra_attributes': {
                            'CoincInspiral': {'snr': 4.0},
                            'SingleInspiral': [
                                {"ifo": ifo} for ifo in
                                ["H1", "L1", "V1"]]}}
    payload = dict(alert_type='new',
                   object=event_dictionary,
                   data=event_dictionary,
                   uid='G000002')
    superevents.handle(payload)
    # neither method is called due to low far
    with patch.object(superevents, 'process') as mock_process:
        superevents.handle(payload)
        mock_process.assert_not_called()


def test_parse_trigger_cbc_5():
    """New trigger G000002 is complete, with no intersecting superevent,
    passes less-significant far, but not significant far. New superevent
    should be created, and labeled LOW_SIGNIF_LOCKED."""
    event_dictionary = {'graceid': 'G000002',
                        'gpstime': 1286741861.52678,
                        'group': 'CBC',
                        'pipeline': 'gstlal',
                        'search': 'AllSky',
                        'offline': False,
                        'far': 5.5e-07,
                        'instruments': 'H1,L1,V1',
                        'labels': [
                            'PASTRO_READY',
                            'SKYMAP_READY',
                            'EMBRIGHT_READY'
                        ],
                        'superevent': None,
                        'superevent_neighbours': SUPEREVENTS_NEIGHBOURS,
                        'extra_attributes': {
                            'CoincInspiral': {'snr': 4.0},
                            'SingleInspiral': [
                                {"ifo": ifo} for ifo in
                                ["H1", "L1", "V1"]]}}
    payload = dict(alert_type='new',
                   object=event_dictionary,
                   data=event_dictionary,
                   uid='G000002')
    superevents.handle(payload)
    with patch('gwcelery.tasks.gracedb.create_superevent',
               return_value='S123456') as p1, \
            patch('gwcelery.tasks.gracedb.get_superevents',
                  return_value=SUPEREVENTS_NEIGHBOURS.values()) as p2, \
            patch('gwcelery.tasks.gracedb.create_label') as p3:
        superevents.handle(payload)
        p1.assert_called_once()
        p2.assert_called_once()
        p3.assert_called_once_with('LOW_SIGNIF_LOCKED', 'S123456')


def test_parse_trigger_burst_1():
    """New cwb trigger G000005 with gpstime lying partially in
    S0039 window, not more significant than existing preferred
    event, superevent window changed.
    """
    event_dictionary = {'graceid': 'G000005',
                        'gpstime': 1163905214.4,
                        'group': 'Burst',
                        'pipeline': 'cwb',
                        'search': 'AllSky',
                        'offline': False,
                        'far': 3.02e-09,
                        'instruments': 'H1,L1',
                        'labels': [],
                        'superevent': None,
                        'superevent_neighbours': SUPEREVENTS_NEIGHBOURS,
                        'extra_attributes': {
                            'MultiBurst': {
                                'duration': 0.23639,
                                'start_time': 1163905215,
                                'snr': 10.3440}}}
    payload = dict(object=event_dictionary,
                   data=event_dictionary,
                   alert_type='new',
                   uid='G000005')

    superevent_response = patch(
        'gwcelery.tasks.gracedb.get_superevent',
        return_value=SUPEREVENTS_NEIGHBOURS['S0039'])

    # preferred event should not be updated
    # superevent window should change
    with superevent_response, \
            patch('gwcelery.tasks.gracedb.add_event_to_superevent') as p1, \
            patch('gwcelery.tasks.gracedb.update_superevent') as p2, \
            patch('gwcelery.tasks.gracedb.get_superevents',
                  return_value=SUPEREVENTS_NEIGHBOURS.values()) as p3:
        superevents.handle(payload)
        p1.assert_called_once()
        p2.assert_called_once_with('S0039',
                                   t_end=pytest.approx(1163905239, abs=1),
                                   t_start=pytest.approx(1163905214, abs=1))
        p3.assert_called_once()


def test_parse_trigger_burst_2():
    """New oLIB trigger G000006 with gpstime lying partially in
    S0039 window, more significant than already existing preferred
    event. superevent window changed
    """
    # preferred event not be updated, in spite of lower far of new event
    # because existing preferred event is CBC
    event_dictionary = {'graceid': 'G000006',
                        'gpstime': 1163905239.5,
                        'group': 'Burst',
                        'pipeline': 'oLIB',
                        'search': 'AllSky',
                        'offline': False,
                        'far': 3.02e-16,
                        'instruments': 'H1,L1',
                        'labels': [],
                        'superevent': None,
                        'superevent_neighbours': SUPEREVENTS_NEIGHBOURS,
                        'extra_attributes': {
                            'LalInferenceBurst': {
                                'quality_mean': 20.6458,
                                'frequency_mean': 117.9644,
                                'omicron_snr_network': 8.77}}}
    payload = dict(object=event_dictionary,
                   data=event_dictionary,
                   alert_type='new',
                   uid='G000006')
    superevent_response = patch(
        'gwcelery.tasks.gracedb.get_superevent',
        return_value=SUPEREVENTS_NEIGHBOURS['S0039'])

    with superevent_response, \
            patch('gwcelery.tasks.gracedb.add_event_to_superevent') as p1, \
            patch('gwcelery.tasks.gracedb.update_superevent') as p2, \
            patch('gwcelery.tasks.gracedb.get_superevents',
                  return_value=SUPEREVENTS_NEIGHBOURS.values()) as p3:
        superevents.handle(payload)
        p1.assert_called_once()
        p2.assert_called_once_with('S0039',
                                   t_end=pytest.approx(1163905239, abs=1),
                                   t_start=pytest.approx(1163905214, abs=1))
        p3.assert_called_once()


def test_parse_trigger_burst_3():
    """New oLIB trigger G000007, not present among superevents
    New superevent created.
    Q_mean = frequency_mean = 100., hence d_t_start = d_t_end = 1s
    """
    event_dictionary = {'graceid': 'G000007',
                        'gpstime': 1163905249.5,
                        'group': 'Burst',
                        'pipeline': 'oLIB',
                        'search': 'AllSky',
                        'offline': False,
                        'far': 3.02e-16,
                        'instruments': 'H1,L1',
                        'labels': [],
                        'superevent': None,
                        'superevent_neighbours': SUPEREVENTS_NEIGHBOURS,
                        'extra_attributes': {
                            'LalInferenceBurst': {
                                'quality_mean': 100.0,
                                'frequency_mean': 100.0,
                                'omicron_snr_network': 8.0}}}
    payload = dict(object=event_dictionary,
                   data=event_dictionary,
                   alert_type='new',
                   uid='G000007')
    # G000007 absent in any superevent window, new superevent created
    with patch('gwcelery.tasks.gracedb.create_superevent') as p, \
            patch('gwcelery.tasks.gracedb.create_label'):
        superevents.handle(payload)
        p.assert_called_once_with('G000007',
                                  pytest.approx(1163905249.5),
                                  pytest.approx(1163905248.5),
                                  pytest.approx(1163905250.5))


def test_parse_trigger_burst_4():
    """New CWB trigger G000008, not present among superevents
    New superevent created.
    extra attribute duration = 0.02s
    """
    event_dictionary = {'graceid': 'G000008',
                        'gpstime': 1128658942.9878,
                        'group': 'Burst',
                        'pipeline': 'CWB',
                        'search': 'AllSky',
                        'offline': False,
                        'far': 1.23e-09,
                        'instruments': 'H1,L1',
                        'labels': [],
                        'superevent': None,
                        'superevent_neighbours': SUPEREVENTS_NEIGHBOURS,
                        'extra_attributes': {
                            'MultiBurst': {
                                'duration': 0.02,
                                'start_time': 1128658942,
                                'snr': 19.824}}}
    payload = dict(object=event_dictionary,
                   data=event_dictionary,
                   alert_type='new',
                   uid='G000008')
    # G000008 absent in any superevent window, new superevent created
    with patch('gwcelery.tasks.gracedb.create_superevent') as p, \
            patch('gwcelery.tasks.gracedb.create_label'):
        superevents.handle(payload)
        p.assert_called_once()


def test_parse_trigger_burst_5():
    """New less-significant oLIB trigger, which is complete, no intersecting
    superevent. New superevent created and labeled LOW_SIGNIF_LOCKED.
    """
    event_dictionary = {'graceid': 'G000007',
                        'gpstime': 1.0,
                        'group': 'Burst',
                        'pipeline': 'oLIB',
                        'search': 'AllSky',
                        'offline': False,
                        'far': 1e-6,
                        'instruments': 'H1,L1',
                        'labels': ['SKYMAP_READY'],
                        'superevent': None,
                        'superevent_neighbours': SUPEREVENTS_NEIGHBOURS,
                        'extra_attributes': {
                            'LalInferenceBurst': {
                                'quality_mean': 100.0,
                                'frequency_mean': 100.0,
                                'omicron_snr_network': 8.0}}}
    payload = dict(object=event_dictionary,
                   data=event_dictionary,
                   alert_type='new',
                   uid='G000007')
    with patch('gwcelery.tasks.gracedb.create_superevent',
               return_value='S123456') as p1, \
            patch('gwcelery.tasks.gracedb.create_label') as p2:
        superevents.handle(payload)
        p1.assert_called_once()
        p2.assert_called_once_with('LOW_SIGNIF_LOCKED', 'S123456')


def test_parse_trigger_burst_6():
    """New less-significant MLy trigger, which is complete, no intersecting
    superevent. New superevent created and labeled LOW_SIGNIF_LOCKED.
    """
    event_dictionary = {'graceid': 'G123456',
                        'gpstime': 1.0,
                        'group': 'Burst',
                        'pipeline': 'MLy',
                        'search': 'AllSky',
                        'offline': False,
                        'far': 1e-6,
                        'instruments': 'H1,L1',
                        'labels': ['SKYMAP_READY'],
                        'superevent': None,
                        'superevent_neighbours': SUPEREVENTS_NEIGHBOURS,
                        'extra_attributes': {
                            'MLyBurst': {
                                'duration': 5.0,
                                'SNR': 100.0}}}
    payload = dict(object=event_dictionary,
                   data=event_dictionary,
                   alert_type='new',
                   uid='G123456')
    with patch('gwcelery.tasks.gracedb.create_superevent',
               return_value='S123456') as p1, \
            patch('gwcelery.tasks.gracedb.create_label') as p2:
        superevents.handle(payload)
        p1.assert_called_once()
        p2.assert_called_once_with('LOW_SIGNIF_LOCKED', 'S123456')


def test_S190421ar_spiir_scenario():    # noqa: N802
    """Test to ensure that a low FAR event with accidental high
    SNR is not promoted to the preferred event status. For example, here,
    the new event G330298 has SNR 10.51, higher than the preferred event
    G330308 which has SNR 10.17. But the preferred event is not changed on
    the basis of low FAR.
    """
    event_dictionary = {'graceid': 'G330298',
                        'gpstime': 1239917954.40918,
                        'far': 5.57979637960671e-06,
                        'group': 'CBC',
                        'instruments': 'H1,L1',
                        'pipeline': 'spiir',
                        'search': 'AllSky',
                        'offline': False,
                        'labels': [],
                        'superevent': None,
                        'superevent_neighbours': SUPEREVENTS_NEIGHBOURS,
                        'extra_attributes': {
                            'CoincInspiral': {
                                'snr': 10.5107507705688},
                            'SingleInspiral': [
                                {"ifo": ifo} for ifo in ["H1", "L1"]]}}
    payload = dict(alert_type='new',
                   object=event_dictionary,
                   data=event_dictionary,
                   uid='G330298')
    superevent_response = patch(
        'gwcelery.tasks.gracedb.get_superevent',
        return_value=SUPEREVENTS_NEIGHBOURS['S190421ar'])

    with superevent_response, \
            patch('gwcelery.tasks.gracedb.add_event_to_superevent') as p1, \
            patch('gwcelery.tasks.gracedb.update_superevent') as p2, \
            patch('gwcelery.tasks.gracedb.get_superevents',
                  return_value=SUPEREVENTS_NEIGHBOURS.values()) as p3:
        superevents.handle(payload)
        p1.assert_called_once()
        p2.assert_not_called()
        p3.assert_called_once()


def test_inj_means_should_not_publish():
    event_dictionary = {'graceid': 'G1234',
                        'gpstime': 1239917954.40918,
                        'far': 5.57979637960671e-06,
                        'group': 'CBC',
                        'instruments': 'H1,L1',
                        'pipeline': 'spiir',
                        'search': 'AllSky',
                        'offline': False,
                        'labels': ['INJ'],
                        'extra_attributes': {
                            'CoincInspiral': {
                                'snr': 10.5107507705688},
                            'SingleInspiral': [
                                {"ifo": ifo} for ifo in ["H1", "L1"]]}}
    assert superevents.should_publish(event_dictionary) is False


@pytest.fixture
def burst_bbh_ranked_higher_than_burst_allsky(monkeypatch):
    monkeypatch.setitem(
        app.conf, 'superevent_candidate_preference',
        {'burst': {'bbh': 1, 'allsky': 0}})


def test_burst_bbh_vs_allsky(burst_bbh_ranked_higher_than_burst_allsky):
    """Test event ranking when different searches under the same group
    is ranked differently. In this case the burst BBH search is ranked higher
    than burst AllSky."""
    event_dictionary = {
        'graceid': 'G1',
        'gpstime': 1.0,
        'pipeline': 'CWB',
        'offline': False,
        'far': 1e-10,
        'instruments': 'H1,L1',
        'labels': [],
        'superevent': None,
        'superevent_neighbours': SUPEREVENTS_NEIGHBOURS,
        'extra_attributes': {
            'MultiBurst': {
                'duration': 0.02,
                'start_time': 0.9,
                'snr': 19.0
            }
        }
    }

    event_1 = dict(**event_dictionary, group='Burst', search='AllSky')
    event_2 = dict(**event_dictionary, group='Burst', search='BBH')
    assert superevents.keyfunc(event_1) < superevents.keyfunc(event_2)
