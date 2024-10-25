from unittest.mock import Mock, call, patch

import pytest

from .. import app
from ..tasks import gracedb, raven
from .test_tasks_skymaps import toy_fits_filecontents  # noqa: F401


@pytest.mark.live_worker
@pytest.mark.parametrize(
    'group,gracedb_id,pipelines,se_search,ext_search,tl,th',
    [['CBC', 'S1', ['Fermi', 'Swift'], [], ['GRB'], -1, 5],
     ['Burst', 'S2', ['Fermi', 'Swift'], [], ['GRB'], -60, 600],
     ['Burst', 'S3', ['SNEWS'], ['Supernova'], [], -10, 10],
     ['Burst', 'T4', ['SNEWS'], 'Supernova', [], -10, 10],
     ['CBC', 'MS4', ['Fermi'], ['MDC'], [], -1, 5],
     ['CBC', 'E1', ['Fermi'], [], ['GRB'], -5, 1],
     ['CBC', 'M1', ['Fermi'], [], ['MDC'], -5, 1],
     ['CBC', 'E1', ['Fermi'], [], ['SubGRB'], -11, 1],
     ['Burst', 'E1', ['Swift'], [], ['SubGRBTargeted'], -20, 10],
     ['Burst', 'E1', ['Swift'], ['BBH'], [], -5, 1],
     [None, 'E1', ['Swift'], [], ['SubGRBTargeted'], -20, 10]])
@patch('gwcelery.tasks.gracedb.create_label')
@patch('gwcelery.tasks.raven.raven_pipeline.s')
@patch('gwcelery.tasks.raven.search.si', return_value=[{'superevent_id': 'S5',
                                                        'graceid': 'E2'}])
@patch('gwcelery.tasks.raven.calculate_coincidence_far')
def test_coincidence_search(mock_calculate_coincidence_far,
                            mock_search, mock_raven_pipeline,
                            mock_create_label,
                            group, gracedb_id, pipelines,
                            se_search, ext_search, tl, th):
    """Test that correct time windows are used for each RAVEN search."""
    alert_object = {'superevent_id': gracedb_id}
    if 'E' in gracedb_id:
        alert_object['group'] = 'External'
    raven.coincidence_search(gracedb_id, alert_object, group,
                             pipelines, ext_search, se_search)

    mock_search.assert_called_once_with(
        gracedb_id, alert_object, tl, th, group, pipelines,
        ext_search, se_search)
    mock_raven_pipeline.assert_called_once_with(
        gracedb_id, alert_object, tl, th, group)


@pytest.mark.parametrize(
    'group,search', [['CBC', 'SubGRBTargeted'], ['Test', 'GRB']])
def test_raven_window_errors(group, search):
    with pytest.raises(ValueError):
        raven._time_window('S1', group, ['INTEGRAL'], [search], [])


@pytest.mark.parametrize(
    'event_type,event_id', [['SE', 'S1234'], ['ExtTrig', 'E1234']])
@patch('ligo.raven.search.search')
def test_raven_search(mock_raven_search, event_type, event_id):
    """Test that correct input parameters are used for raven."""
    alert_object = {}
    if event_type == 'SE':
        alert_object['superevent_id'] = event_id

    # call raven search
    raven.search(event_id, alert_object)
    mock_raven_search.assert_called_once_with(
        event_id, -5, 5, event_dict=alert_object,
        gracedb=gracedb.client, group=None, pipelines=[], searches=[],
        se_searches=[])


@pytest.mark.parametrize('group', ['CBC', 'Burst'])
@patch('ligo.raven.search.calc_signif_gracedb')
def test_calculate_coincidence_far(
        mock_calc_signif, group):
    se = {'superevent_id': 'S1234'}
    ext = {'graceid': 'E4321',
           'pipeline': 'Fermi',
           'search': 'GRB',
           'labels': [],
           'far': None}
    if group == 'CBC':
        tl, th = -5, 1
    else:
        tl, th = -600, 60
    raven.calculate_coincidence_far(se, ext, tl, th)
    mock_calc_signif.assert_called_once_with(
        'S1234', 'E4321', tl, th,
        se_dict=se, ext_dict=ext,
        incl_sky=False, grb_search=ext['search'],
        em_rate=app.conf['raven_ext_rates'][ext['search']],
        gracedb=gracedb.client, far_grb=None,
        far_gw_thresh=None, far_grb_thresh=None)
    assert isinstance(app.conf['raven_ext_rates'][ext['search']], float)


@patch('ligo.raven.search.calc_signif_gracedb')
def test_calculate_coincidence_far_subgrb(mock_calc_signif):
    se = {'superevent_id': 'S1234'}
    ext = {'graceid': 'E4321',
           'pipeline': 'Fermi',
           'search': 'SubGRBTargeted',
           'labels': [],
           'far': 1e-5}
    tl, th = -1, 10
    raven.calculate_coincidence_far(se, ext, tl, th)
    mock_calc_signif.assert_called_once_with(
        'S1234', 'E4321', tl, th,
        se_dict=se, ext_dict=ext,
        incl_sky=False, grb_search=ext['search'],
        gracedb=gracedb.client, far_grb=ext['far'],
        em_rate=None,
        far_gw_thresh=(
            app.conf['raven_targeted_far_thresholds']['GW']['Fermi']),
        far_grb_thresh=(
            app.conf['raven_targeted_far_thresholds']['GRB']['Fermi']))
    assert isinstance(
        app.conf['raven_targeted_far_thresholds']['GW']['Fermi'], float)
    assert isinstance(
        app.conf['raven_targeted_far_thresholds']['GRB']['Fermi'], float)


@pytest.mark.parametrize('group', ['CBC', 'Burst'])  # noqa: F811
@patch('gwcelery.tasks.external_skymaps.get_skymap_filename',
       return_value='fermi_skymap.fits.gz')
@patch('ligo.raven.search.calc_signif_gracedb')
def test_calculate_spacetime_coincidence_far_fermi(
        mock_calc_signif, mock_get_skymap_filename, group):
    se = {'superevent_id': 'S1234'}
    ext = {'graceid': 'E4321',
           'pipeline': 'Fermi',
           'search': 'GRB',
           'labels': ['EXT_SKYMAP_READY', 'SKYMAP_READY'],
           'far': None}
    if group == 'CBC':
        tl, th = -5, 1
    else:
        tl, th = -600, 60
    raven.calculate_coincidence_far(se, ext, tl, th)
    mock_calc_signif.assert_called_once_with(
        'S1234', 'E4321', tl, th,
        incl_sky=True, grb_search=ext['search'],
        se_dict=se, ext_dict=ext,
        se_fitsfile='fermi_skymap.fits.gz',
        ext_fitsfile='fermi_skymap.fits.gz',
        se_moc=True, ext_moc=False,
        em_rate=app.conf['raven_ext_rates'][ext['search']],
        gracedb=gracedb.client, far_grb=None,
        far_gw_thresh=None, far_grb_thresh=None,
        use_preferred_event_skymap=False)
    assert isinstance(app.conf['raven_ext_rates'][ext['search']], float)


@pytest.mark.parametrize('group', ['CBC', 'Burst'])  # noqa: F811
@patch('gwcelery.tasks.external_skymaps.get_skymap_filename',
       return_value='swift_skymap.multiorder.fits')
@patch('ligo.raven.search.calc_signif_gracedb')
def test_calculate_spacetime_coincidence_far_swift(
        mock_calc_signif, mock_get_skymap_filename, group):
    se = {'superevent_id': 'S1234'}
    ext = {'graceid': 'E4321',
           'pipeline': 'Swift',
           'search': 'GRB',
           'labels': ['EXT_SKYMAP_READY', 'SKYMAP_READY'],
           'far': None}
    if group == 'CBC':
        tl, th = -5, 1
    else:
        tl, th = -600, 60
    raven.calculate_coincidence_far(se, ext, tl, th)
    mock_calc_signif.assert_called_once_with(
        'S1234', 'E4321', tl, th,
        incl_sky=True, grb_search=ext['search'],
        se_dict=se, ext_dict=ext,
        se_fitsfile='swift_skymap.multiorder.fits',
        ext_fitsfile='swift_skymap.multiorder.fits',
        em_rate=app.conf['raven_ext_rates'][ext['search']],
        se_moc=True, ext_moc=True,
        gracedb=gracedb.client, far_grb=None,
        far_gw_thresh=None, far_grb_thresh=None,
        use_preferred_event_skymap=False)
    assert isinstance(app.conf['raven_ext_rates'][ext['search']], float)


@pytest.mark.parametrize('group', ['CBC', 'Burst'])  # noqa: F811
@patch('gwcelery.tasks.external_skymaps.get_skymap_filename',
       return_value='fermi_skymap.fits.gz')
@patch('ligo.raven.search.calc_signif_gracedb')
def test_calculate_spacetime_coincidence_far_preferred(
        mock_calc_signif, mock_get_skymap_filename, group):
    se = {'superevent_id': 'S1234',
          'preferred_event': 'G1'}
    ext = {'graceid': 'E4321',
           'pipeline': 'Fermi',
           'search': 'GRB',
           'labels': ['EXT_SKYMAP_READY', 'EM_READY'],
           'far': None}
    if group == 'CBC':
        tl, th = -5, 1
    else:
        tl, th = -600, 60
    raven.calculate_coincidence_far(se, ext, tl, th)
    mock_calc_signif.assert_called_once_with(
        'S1234', 'E4321', tl, th,
        incl_sky=True, grb_search=ext['search'],
        se_dict=se, ext_dict=ext,
        se_fitsfile='fermi_skymap.fits.gz',
        ext_fitsfile='fermi_skymap.fits.gz',
        em_rate=app.conf['raven_ext_rates'][ext['search']],
        se_moc=True, ext_moc=False,
        gracedb=gracedb.client, far_grb=None,
        far_gw_thresh=None, far_grb_thresh=None,
        use_preferred_event_skymap=True)
    assert isinstance(app.conf['raven_ext_rates'][ext['search']], float)


@patch('ligo.raven.search.calc_signif_gracedb')
def test_calculate_coincidence_far_snews(
        mock_calc_signif):
    se = {'superevent_id': 'S1234'}
    ext = {'graceid': 'E4321',
           'pipeline': 'SNEWS',
           'search': 'Supernova',
           'labels': [],
           'far': None}
    tl, th = -600, 60
    raven.calculate_coincidence_far(se, ext, tl, th)
    # We should not calculate the joint FAR for SNEWS candidates
    mock_calc_signif.assert_not_called()


def mock_get_labels(superevent_id):
    if superevent_id == 'S14':
        return {'ADVREQ'}
    else:
        return {}


def mock_coinc_far(*args):
    return {'temporal_coinc_far': 1e-7,
            'spatiotemporal_coinc_far': None,
            'overlap_integral': None}


def mock_get_superevent(superevent_id):
    if superevent_id == 'S1':
        old_time_far = None
        old_space_far = None
    elif superevent_id == 'S2':
        old_time_far = 1e-5
        old_space_far = None
    elif superevent_id == 'S3':
        old_time_far = 1e-4
        old_space_far = None
    elif superevent_id == 'S4':
        old_time_far = 1e-5
        old_space_far = 1e-6
    elif superevent_id == 'S5':
        old_time_far = 1e-4
        old_space_far = 1e-6
    else:
        old_time_far = 1e-5
        old_space_far = None
    return {'time_coinc_far': old_time_far,
            'space_coinc_far': old_space_far,
            'superevent_id': superevent_id,
            'labels': [],
            'em_type': 'E101' if superevent_id != 'S101' else None}


@pytest.mark.parametrize(
    'raven_search_results,graceid,tl,th,group',
    [[[{'graceid': 'E1', 'search': 'GRB'}], 'S1', -5, 1, 'CBC'],
     [[{'superevent_id': 'S10', 'far': 1, 'preferred_event': 'G1'}],
        'E2', -1, 5, 'CBC'],
     [[{'graceid': 'E3', 'search': 'GRB'},
       {'graceid': 'E4', 'search': 'GRB'}], 'S2', -600, 60, 'Burst'],
     [[{'superevent_id': 'S11', 'far': 1, 'preferred_event': 'G2'},
       {'superevent_id': 'S12', 'far': .001, 'preferred_event': 'G3'}],
        'E5', -1, 5, 'CBC'],
     [[], 'S13', -1, 5, 'CBC'],
     [[{'graceid': 'E4', 'search': 'GRB'}], 'S14', -1, 5, 'CBC'],
     [[{'graceid': 'E5', 'search': 'SubGRBTargeted'}], 'S14', -1, 5, 'CBC'],
     [[{'graceid': 'E7', 'search': 'SubGRBTargeted'}], 'S14', -1, 5, 'CBC'],
     [[{'superevent_id': 'S12', 'far': .001, 'preferred_event': 'G3'}],
        'T4', -10, 10, 'Burst'],
     [[{'superevent_id': 'MS13', 'far': 1, 'preferred_event': 'M3'}],
        'M15', -1, 5, 'CBC'],
     [[{'superevent_id': 'S13', 'far': 1, 'preferred_event': 'G4'}],
        'E6', -60, 600, 'Burst']])
@patch('gwcelery.tasks.raven.trigger_raven_alert.run')
@patch('gwcelery.tasks.gracedb.get_labels', mock_get_labels)
@patch('gwcelery.tasks.raven.calculate_coincidence_far.run',
       return_value=mock_coinc_far())
@patch('gwcelery.tasks.gracedb.create_label.run')
@patch('gwcelery.tasks.gracedb.get_superevent', mock_get_superevent)
def test_raven_pipeline(mock_create_label,
                        mock_calculate_coincidence_far,
                        mock_trigger_raven_alert,
                        raven_search_results, graceid, tl, th, group):
    """This function tests that the RAVEN pipeline runs correctly for scenarios
    where RAVEN finds nothing, a coincidence is found but does not pass
    threshold, when a coincidence is found but does pass threshold, and when
    multiple events are found.
    """
    alert_object = {'preferred_event': 'G1', 'pipeline': 'Fermi',
                    'search': 'Supernova' if graceid == 'T4' else 'GRB',
                    'labels': [],
                    'superevent': None if graceid != 'E6' else 'S14'}

    for result in raven_search_results:
        # Check if is an external event and is SubGRBTargeted, picking one
        # to be low-significance
        if result.get('graceid') and result['search'] == 'SubGRBTargeted' and \
                result['graceid'] == 'E5':
            result['labels'] = 'NOT_GRB'
        else:
            result['labels'] = []
    if 'S' not in graceid:
        alert_object['group'] = 'External'
        if 'T' in graceid:
            alert_object['group'] = 'Test'
        alert_object['graceid'] = graceid
        for result in raven_search_results:
            result['time_coinc_far'] = 1e-5
            result['space_coinc_far'] = None
            result['overlap_integral'] = None
    else:
        alert_object['superevent_id'] = graceid
        alert_object['time_coinc_far'] = 1e-5
        alert_object['space_coinc_far'] = None
        alert_object['overlap_integral'] = None
        for result in raven_search_results:
            result['superevent'] = None
    raven.raven_pipeline(raven_search_results, graceid, alert_object, tl, th,
                         group)

    coinc_calls = []
    label_calls = []
    if not raven_search_results:
        mock_calculate_coincidence_far.assert_not_called()
        mock_create_label.assert_not_called()
        return
    if 'S' in graceid:
        for result in raven_search_results:
            label_calls.append(call('EM_COINC', result['graceid']))
            coinc_calls.append(call(alert_object, result, tl, th,
                                    use_superevent_skymap=None))
            label_calls.append(call('EM_COINC', graceid))
    else:
        result = raven.preferred_superevent(raven_search_results)[0]
        label_calls.append(call('EM_COINC', result['superevent_id']))
        coinc_calls.append(call(result, alert_object, tl, th,
                                use_superevent_skymap=None))
        label_calls.append(call('EM_COINC', graceid))

    alert_calls = []
    if 'S' in graceid:
        for result in raven_search_results:
            alert_calls.append(call(mock_coinc_far(),
                                    alert_object, graceid, result, group))
    else:
        alert_calls.append(call(
            mock_coinc_far(),
            result, graceid, alert_object, group))
    if graceid != 'E6':
        mock_trigger_raven_alert.assert_has_calls(alert_calls, any_order=True)

        mock_calculate_coincidence_far.assert_has_calls(coinc_calls,
                                                        any_order=True)
        if result.get('graceid') and result['search'] == 'SubGRBTargeted' and \
                result['graceid'] == 'E5':
            mock_create_label.assert_not_called()
        else:
            mock_create_label.assert_has_calls(label_calls, any_order=True)
    else:
        mock_trigger_raven_alert.assert_not_called()
        mock_calculate_coincidence_far.assert_not_called()
        mock_create_label.assert_not_called()


@pytest.mark.parametrize(
    'raven_search_results, testnum',
    [[[{'superevent_id': 'S10', 'far': 1, 'preferred_event': 'G1'}], 1],
     [[{'superevent_id': 'S11', 'far': 1, 'preferred_event': 'G2'},
       {'superevent_id': 'S12', 'far': .001, 'preferred_event': 'G3'}], 2],
     [[{'superevent_id': 'S13', 'far': 1, 'preferred_event': 'G4'},
       {'superevent_id': 'S14', 'far': .0001, 'preferred_event': 'G5'},
       {'superevent_id': 'S15', 'far': .001, 'preferred_event': 'G6'}], 3]])
def test_preferred_superevent(raven_search_results, testnum):

    preferred_superevent = raven.preferred_superevent(raven_search_results)
    if testnum == 1:
        assert preferred_superevent == [{'superevent_id': 'S10', 'far': 1,
                                         'preferred_event': 'G1'}]
    if testnum == 2:
        assert preferred_superevent == [{'superevent_id': 'S12', 'far': .001,
                                         'preferred_event': 'G3'}]
    if testnum == 3:
        assert preferred_superevent == [{'superevent_id': 'S14', 'far': .0001,
                                         'preferred_event': 'G5'}]


@pytest.mark.parametrize(
    'new_time_far,new_space_far,superevent_id,search,result',
    [[1e-4, None, 'S1', 'GRB', True],
     [1e-4, 1e-3, 'S1', 'GRB', True],
     [1e-4, None, 'S2', 'GRB', False],
     [1e-4, 1e-3, 'S3', 'GRB', True],
     [1e-4, 1e-3, 'S4', 'GRB', False],
     [1e-8, None, 'S4', 'GRB', False],
     [1e-4, 1e-8, 'S5', 'GRB', True],
     [1e-10, 1e-15, 'S5', 'SubGRB', False],
     [1e-10, 1e-15, 'S5', 'SubGRBTargeted', False],
     [None, None, 'S1', 'Supernova', True]])
def test_update_superevent(monkeypatch,
                           new_time_far, new_space_far,
                           superevent_id,
                           search, result):
    """This function tests that new coincidences update the superevent when
    they have a superior joint FAR or are a SNEWS event.
    """

    def _mock_get_ext_event(graceid):
        return {'graceid': graceid,
                'pipeline': 'Fermi',
                'search': search if graceid == 'E100' else 'GRB',
                'labels': []}

    mock_update_superevent = Mock()
    monkeypatch.setattr('gwcelery.tasks.gracedb.update_superevent',
                        mock_update_superevent)
    monkeypatch.setattr('gwcelery.tasks.gracedb.get_superevent',
                        mock_get_superevent)
    monkeypatch.setattr('gwcelery.tasks.gracedb.get_event',
                        _mock_get_ext_event)

    superevent = {'superevent_id': superevent_id,
                  'labels': []}
    ext_event = _mock_get_ext_event('E100')
    if search == 'Supernova':
        coinc_far_dict = {}
    else:
        coinc_far_dict = {'temporal_coinc_far': new_time_far,
                          'spatiotemporal_coinc_far': new_space_far}
    raven.update_coinc_far(coinc_far_dict, superevent, ext_event)
    if result:
        mock_update_superevent.assert_called_with(
            superevent_id, em_type='E100',
            time_coinc_far=new_time_far,
            space_coinc_far=new_space_far)
    else:
        mock_update_superevent.assert_not_called()


def mock_get_event(graceid):
    if 'E' in graceid:
        if graceid == "E1":
            pipeline, search, labels = 'Fermi', 'GRB', []
        elif graceid == "E2":
            pipeline, search, labels = 'Fermi', 'GRB', ['NOT_GRB']
        elif graceid == "E3":
            pipeline, search, labels = 'SNEWS', 'Supernova', []
        elif graceid == "E4":
            pipeline, search, labels = 'Fermi', 'GRB', []
        elif graceid == "E5":
            pipeline, search, labels = 'Fermi', 'GRB', ['RAVEN_ALERT']
        elif graceid == "E6":
            pipeline, search, labels = \
                'Fermi', 'GRB', ['NOT_GRB', 'RAVEN_ALERT']
        return {'superevent_id': graceid.replace('E', 'S'),
                'graceid': graceid,
                'pipeline': pipeline,
                'search': search,
                'labels': labels}
    elif 'S' in graceid:
        return {'em_type': graceid.replace('S', 'E'),
                'time_coinc_far': 1e-20,
                'space_coinc_far': 1e-20,
                'superevent_id': graceid,
                'labels': ['COMBINEDSKYMAP_READY'] if graceid == 'S4' else []}


@pytest.mark.parametrize(
    'superevent_id,ext_event_id_old,ext_event_id_new,result',
    # NOT_GRB should not supersede real GRB
    [['S1', 'E1', 'E2', False],
     # Real GRB should supersede NOT_GRB
     ['S2', 'E2', 'E1', True],
     # SNEWS event should not be overwritten by GRB
     ['S3', 'E3', 'E1', False],
     # RAVEN_ALERT should supersede subthreshold joint candidate
     ['S4', 'E4', 'E5', True],
     # Subthreshold joint candidate should not overwrite RAVEN_ALERT
     ['S5', 'E5', 'E4', False],
     # New real event should supercede old NOT_GRB with an alert
     ['S6', 'E6', 'E1', True]]
)
@patch('gwcelery.tasks.gracedb.update_superevent')
@patch('gwcelery.tasks.gracedb.get_event', mock_get_event)
@patch('gwcelery.tasks.gracedb.get_superevent', mock_get_event)
def test_update_superevent_w_emtype(mock_update_superevent,
                                    superevent_id,
                                    ext_event_id_old, ext_event_id_new,
                                    result):
    """This function tests that new coincidences update the superevent
    if the new GRB is likely a real GRB and the old is likely not.
    Also tested is that SNEWS event aren't overwritten by GRBs and locked
    events are overwritten, denote by the COMBINEDSKYMAP_READY label in the
    superevent which is applied just before sending an alert.
    """
    superevent = mock_get_event(superevent_id)
    superevent['em_type'] = ext_event_id_old
    ext_event_new = mock_get_event(ext_event_id_new)
    ext_event_old = mock_get_event(ext_event_id_old)
    time_coinc_far = 1e-10 if ext_event_old['labels'] else 1e-20
    space_coinc_far = 1e-10 if ext_event_old['labels'] else 1e-30
    coinc_far_dict = {'temporal_coinc_far': time_coinc_far,
                      'spatiotemporal_coinc_far': space_coinc_far}
    raven.update_coinc_far(coinc_far_dict, superevent, ext_event_new)
    if result:
        mock_update_superevent.assert_called_with(
            superevent_id,
            em_type=ext_event_id_new if result else ext_event_id_old,
            time_coinc_far=time_coinc_far,
            space_coinc_far=space_coinc_far)
    else:
        mock_update_superevent.assert_not_called()


def _mock_get_event(graceid):
    if graceid == "S1234":
        return {"superevent_id": "S1234",
                "preferred_event": "G000001",
                "preferred_event_data": {"group": "Burst", "search": "AllSky"},
                "far": 1e-5}
    elif graceid == "S2345":
        return {"superevent_id": "S2345",
                "preferred_event": "G000002",
                "preferred_event_data": {"group": "Burst", "search": "AllSky"},
                "far": 1e-10}
    elif graceid == "S2468":
        return {"superevent_id": "S2468",
                "preferred_event": "G000002",
                "preferred_event_data": {"group": "CBC", "search": "AllSky"},
                "far": 1e-4}
    elif graceid == "S5678":
        return {"superevent_id": "S5678",
                "preferred_event": "G000003",
                "preferred_event_data": {"group": "CBC", "search": "AllSky"},
                "far": 1e-5}
    elif graceid == "S8642":
        return {"superevent_id": "S8642",
                "preferred_event": "G000003",
                "preferred_event_data": {"group": "Burst", "search": "AllSky"},
                "far": 1e-3}
    elif graceid == "S9876":
        return {"superevent_id": "S9876",
                "preferred_event": "G000003",
                "preferred_event_data": {"group": "Burst", "search": "AllSky"},
                "far": 1e-6}
    elif graceid == "S9988":
        return {"superevent_id": "S9988",
                "preferred_event": "G000003",
                "preferred_event_data": {"group": "Burst", "search": "BBH"},
                "far": 1e-7}
    elif graceid == "S9999":
        return {"superevent_id": "S9999",
                "preferred_event": "G000003",
                "preferred_event_data": {"group": "Burst", "search": "AllSky"},
                "far": -1e-6}
    elif graceid == "TS1111":
        return {"superevent_id": "TS1111",
                "preferred_event": "G000003",
                "preferred_event_data": {"group": "Test", "search": "AllSky"},
                "far": 1e-6}
    elif graceid == "MS1111":
        return {"superevent_id": "MS1111",
                "preferred_event": "M000004",
                "preferred_event_data": {"group": "CBC", "search": "MDC"},
                "far": 1e-9}
    elif graceid == 'E1':
        return {"graceid": "E1",
                "pipeline": 'Swift',
                "search": 'GRB',
                "labels": [],
                "group": 'External'}
    elif graceid == 'E2':
        return {"graceid": "E2",
                "pipeline": 'Fermi',
                "search": 'SubGRB',
                "labels": [],
                "group": 'External'}
    elif graceid == 'E3':
        return {"graceid": "E3",
                "pipeline": 'SNEWS',
                "search": 'Supernova',
                "labels": [],
                "group": 'External'}
    elif graceid == 'T4':
        return {"graceid": "T4",
                "pipeline": 'SNEWS',
                "search": 'Supernova',
                "labels": [],
                "group": 'Test'}
    elif graceid == 'E5':
        return {"graceid": "E5",
                "pipeline": 'Fermi',
                "search": 'GRB',
                "labels": ['NOT_GRB'],
                "group": 'External'}
    elif graceid == 'M6':
        return {"graceid": "M6",
                "pipeline": 'Fermi',
                "search": 'MDC',
                "labels": [],
                "group": 'External'}
    elif graceid == 'E7':
        return {"graceid": "E7",
                "pipeline": 'Fermi',
                "search": 'GRB',
                "labels": [],
                "group": 'External'}
    elif graceid == 'E8':
        return {"graceid": "E8",
                "pipeline": 'Fermi',
                "search": 'SubGRBTargeted',
                "labels": [],
                "group": 'External'}
    elif graceid == 'E9':
        return {"graceid": "E9",
                "pipeline": 'Fermi',
                "search": 'SubGRBTargeted',
                "labels": ['NOT_GRB'],
                "group": 'External'}
    else:
        raise AssertionError


def _mock_get_labels(graceid):
    return _mock_get_event(graceid)['labels']


def _mock_get_coinc_far(graceid):
    if graceid == "S1234":
        return {"temporal_coinc_far": 1e-12,
                "spatiotemporal_coinc_far": 1e-05}
    elif graceid == "S2468":
        return {"temporal_coinc_far": 1e-09,
                "spatiotemporal_coinc_far": None}
    elif graceid == "S5678":
        return {"temporal_coinc_far": 1e-15,
                "spatiotemporal_coinc_far": None}
    elif graceid == "S8642":
        return {"temporal_coinc_far": 1e-03,
                "spatiotemporal_coinc_far": None}
    elif graceid == "S9876":
        return {"temporal_coinc_far": 1e-07,
                "spatiotemporal_coinc_far": 1e-13}
    elif graceid == "S9988":
        return {"temporal_coinc_far": 1e-09,
                "spatiotemporal_coinc_far": 1e-14}
    elif graceid == "MS1111":
        return {"temporal_coinc_far": 1e-09,
                "spatiotemporal_coinc_far": 1e-10}
    elif graceid == "S9999":
        return {"temporal_coinc_far": -1e-09,
                "spatiotemporal_coinc_far": -1e-10}
    else:
        return {}


def _return_coinc_far_dict(coinc_far_dict, *args):
    return coinc_far_dict


@pytest.mark.parametrize(
    'graceid,result_id,group,expected_result',
    [['S1234', 'E1', 'Burst', False],
     ['S2468', 'E1', 'CBC', False],
     ['S2468', 'E5', 'CBC', False],
     ['S2468', 'E2', 'CBC', False],
     ['S2345', 'E3', 'Burst', True],
     ['MS1111', 'M6', 'CBC', True],
     ['S5678', 'E1', 'CBC', False],
     ['S5678', 'E7', 'CBC', False],
     ['E1', 'S9876', 'CBC', True],
     ['E1', 'S9988', 'Burst', True],
     ['E1', 'S2468', 'CBC', False],
     ['E2', 'S5678', 'CBC', False],
     ['E3', 'S8642', 'Burst', False],
     ['E3', 'S9876', 'Burst', True],
     ['T4', 'TS1111', 'CBC', False],
     ['E5', 'S5678', 'CBC', False],
     ['E8', 'S9876', None, True],
     ['S9876', 'E8', None, True],
     # Negative FARs
     ['E8', 'S9999', 'Burst', False],
     ['E9', 'S9999', 'Burst', False],
     ['E3', 'S9999', 'Burst', False]])
@patch('gwcelery.tasks.gracedb.get_labels', side_effect=_mock_get_labels)
@patch('gwcelery.tasks.gracedb.upload.run')
@patch('gwcelery.tasks.raven.update_coinc_far.run',
       side_effect=_return_coinc_far_dict)
@patch('gwcelery.tasks.external_skymaps.plot_overlap_integral.run')
@patch('gwcelery.tasks.gracedb.create_label.run')
def test_trigger_raven_alert(mock_create_label, mock_plot_overlap_integral,
                             mock_update_coinc_far, mock_upload,
                             mock_get_labels,
                             graceid, result_id, group, expected_result):
    if 'S' in graceid:
        superevent_id = graceid
        ext_id = result_id
    else:
        superevent_id = result_id
        ext_id = graceid
    superevent = _mock_get_event(superevent_id)
    superevent['labels'] = []
    coinc_far_json = _mock_get_coinc_far(superevent_id)
    ext_event = _mock_get_event(ext_id)
    ext_event['far'] = 1e-7
    # Test for additional log message
    if ext_id == 'E9':
        ext_event['far'] = 1e-2
    preferred_id = superevent['preferred_event']
    gw_group = group if group is not None else 'Burst'
    raven.trigger_raven_alert(coinc_far_json, superevent,
                              graceid, ext_event, gw_group)

    if expected_result:
        label_calls = [call('RAVEN_ALERT', superevent_id),
                       call('HIGH_PROFILE', superevent_id),
                       call('RAVEN_ALERT', ext_id),
                       call('RAVEN_ALERT', preferred_id)]
        mock_create_label.assert_has_calls(label_calls)
        superevent['labels'] += 'RAVEN_ALERT'
    else:
        mock_create_label.assert_not_called()

    mock_update_coinc_far.assert_called_with(
        coinc_far_json, superevent, ext_event)
    mock_plot_overlap_integral.assert_called_with(
        coinc_far_json, superevent, ext_event)
    mock_upload.assert_called()


@pytest.mark.parametrize(
    'se_id,ext_id,expected_result',
    [['S2468', 'E5', True],
     ['S2468', 'E1', True],
     ['S2468', 'E2', False],
     ['S2468', 'M6', True],
     ['S2345', 'E1', False],
     ['S2345', 'E2', False]])
@patch('gwcelery.tasks.gracedb.create_label.run')
def test_sog_paper_pipeline(mock_create_label,
                            se_id, ext_id, expected_result):

    superevent = _mock_get_event(se_id)
    ext_event = _mock_get_event(ext_id)
    if se_id == 'S2468':
        superevent['far'] = 1e-12
        superevent['space_coinc_far'] = 1e-13
    elif se_id == 'S2345':
        superevent['far'] = 1e-14
        superevent['space_coinc_far'] = 1e-8
    raven.sog_paper_pipeline(ext_event, superevent)

    if expected_result:
        mock_create_label.assert_called_once_with(
            'SOG_READY', superevent['superevent_id'])
    else:
        mock_create_label.assert_not_called()
