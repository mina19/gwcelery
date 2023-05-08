import json
import pytest
from importlib import resources
from unittest.mock import Mock
from ..tasks import rrt_utils
from gwcelery.tests import data


skymap_small = resources.read_binary(data, 'rrt_small_area.fits')
skymap_large = resources.read_binary(
    data, 'MS220722v_bayestar.multiorder.fits')

# checks
# 1. Burst far checks   ---> True
# 2. HasRemnant check   ---> True
# 3. p_BNS check        ---> True
# 4. p_NSBH check       ---> True
# 5. skymap check       ---> True
# 6. Simple BBH         ---> False
# 7. Burst but low far  ---> False
# 8. High p-terrestrial ---> False
# 9. Burst BBH low far ---> False
# 10. HIGH_PROFILE_applied-> False


def get_event(graceid):
    if graceid == 'G123':
        event = {
            'group': 'CBC',
            'pipeline': 'gstlal',
            'search': 'AllSky',
            'graceid': graceid,
            'far': 1e-9,
            'gpstime': 1234,
        }
    elif graceid == 'G456':
        event = {
            'group': 'Burst',
            'pipeline': 'cwb',
            'search': 'AllSky',
            'graceid': graceid,
            'far': 1e-10,
            'gpstime': 1234,
        }
    elif graceid == 'G789':
        event = {
            'group': 'Burst',
            'pipeline': 'cwb',
            'search': 'AllSky',
            'graceid': graceid,
            'far': 1e-8,
            'gpstime': 1234,
        }
    elif graceid == 'G246':
        event = {
            'group': 'Burst',
            'pipeline': 'cwb',
            'search': 'BBH',
            'graceid': graceid,
            'far': 1e-10,
            'gpstime': 1234,
        }
    return event


@pytest.mark.parametrize(
        'superevent,embright,pastro,skymap,result',
        [[{'labels': [], 'gw_events':['G123', 'G456']},
          {'HasNS': 0.0, 'HasRemnant': 0.0, 'HasMassGap': 0.0},
          {"BNS": 0.0, "NSBH": 0.0, "BBH": 0.7, "Terrestrial": 0.3},
          skymap_large, True],
         [{'labels': [], 'gw_events':['G123']},
          {'HasNS': 0.8, 'HasRemnant': 0.2, 'HasMassGap': 0.0},
          {"BNS": 0.0, "NSBH": 0.0, "BBH": 0.7, "Terrestrial": 0.3},
          skymap_large, True],
         [{'labels': [], 'gw_events':['G123']},
          {'HasNS': 0.9, 'HasRemnant': 0.0, 'HasMassGap': 0.0},
          {"BNS": 0.8, "NSBH": 0.2, "BBH": 0.0, "Terrestrial": 0.0},
          skymap_large, True],
         [{'labels': [], 'gw_events':['G123']},
          {'HasNS': 0.9, 'HasRemnant': 0.2, 'HasMassGap': 0.0},
          {"BNS": 0.0, "NSBH": 0.9, "BBH": 0.0, "Terrestrial": 0.1},
          skymap_large, True],
         [{'labels': [], 'gw_events':['G123']},
          {'HasNS': 0.0, 'HasRemnant': 0.0, 'HasMassGap': 0.0},
          {"BNS": 0.0, "NSBH": 0.0, "BBH": 0.9, "Terrestrial": 0.1},
          skymap_small, True],
         [{'labels': [], 'gw_events':['G123']},
          {'HasNS': 0.0, 'HasRemnant': 0.0, 'HasMassGap': 0.0},
          {"BNS": 0.0, "NSBH": 0.0, "BBH": 1.0, "Terrestrial": 0.0},
          skymap_large, False],
         [{'labels': [], 'gw_events':['G123', 'G789']},
          {'HasNS': 0.0, 'HasRemnant': 0.0, 'HasMassGap': 0.0},
          {"BNS": 0.0, "NSBH": 0.0, "BBH": 1.0, "Terrestrial": 0.0},
          skymap_large, False],
         [{'labels': [], 'gw_events':['G123']},
          {'HasNS': 1.0, 'HasRemnant': 1.0, 'HasMassGap': 0.0},
          {"BNS": 0.2, "NSBH": 0.2, "BBH": 0.0, "Terrestrial": 0.6},
          skymap_large, False],
         [{'labels': [], 'gw_events':['G123', 'G246']},
          {'HasNS': 0.0, 'HasRemnant': 0.0, 'HasMassGap': 0.0},
          {"BNS": 0.0, "NSBH": 0.0, "BBH": 0.7, "Terrestrial": 0.3},
          skymap_large, False],
         [{'labels': ['HIGH_PROFILE'], 'gw_events':['G123', 'G246']},
          {'HasNS': 1.0, 'HasRemnant': 1.0, 'HasMassGap': 0.0},
          {"BNS": 1.0, "NSBH": 0.0, "BBH": 0.0, "Terrestrial": 0.0},
          skymap_small, False]
         ])
def test_high_profile(monkeypatch, superevent, embright,
                      pastro, skymap, result):
    embright = json.dumps(embright)
    pastro = json.dumps(pastro)
    superevent.update({'superevent_id': 'S123',
                       'preferred_event_data': get_event('G123')})
    mock_create_label = Mock()
    monkeypatch.setattr('gwcelery.tasks.gracedb.create_label.run',
                        mock_create_label)
    monkeypatch.setattr('gwcelery.tasks.gracedb.get_event.run',
                        get_event)
    rrt_utils.check_high_profile(skymap, embright,
                                 pastro, superevent)
    if result is True:
        mock_create_label.assert_called_once_with(
            "HIGH_PROFILE", "S123"
        )
    elif result is False:
        mock_create_label.assert_not_called()
