from importlib import resources
import json
from unittest.mock import call, Mock, patch

import pytest

from .. import app
from ..tasks import orchestrator
from ..tasks import superevents
from ..util import read_json
from .test_tasks_skymaps import toy_3d_fits_filecontents  # noqa: F401
from . import data


@pytest.mark.parametrize(  # noqa: F811
    'alert_type,alert_label,group,pipeline,offline,far,instruments,'
    'superevent_id,superevent_labels',
    [['label_added', 'LOW_SIGNIF_LOCKED', 'CBC', 'gstlal', False, 1.e-9,
        ['H1'], 'S1234', ['LOW_SIGNIF_LOCKED']],
     ['label_added', 'ADVREQ', 'CBC', 'gstlal', False, 1.e-9,
        ['H1'], 'S1234', ['ADVREQ']],
     ['label_added', 'SIGNIF_LOCKED', 'CBC', 'gstlal', False, 1.e-9,
         ['H1', 'L1'], 'S1234', ['LOW_SIGNIF_LOCKED']],
     ['label_added', 'SIGNIF_LOCKED', 'CBC', 'gstlal', False, 1.e-9,
         ['H1', 'L1', 'V1'], 'S1234', ['LOW_SIGNIF_LOCKED']],
     ['label_added', 'SIGNIF_LOCKED', 'CBC', 'gstlal', False, 1.e-9,
         ['H1', 'L1', 'V1'], 'S2468',
         ['LOW_SIGNIF_LOCKED', 'COMBINEDSKYMAP_READY',
          'RAVEN_ALERT', 'EM_COINC']],
     ['label_added', 'SIGNIF_LOCKED', 'Burst', 'CWB', False, 1.e-9,
         ['H1', 'L1', 'V1'], 'S1234', ['LOW_SIGNIF_LOCKED']],
     ['label_added', 'SIGNIF_LOCKED', 'Burst', 'oLIB', False, 1.e-9,
         ['H1', 'L1', 'V1'], 'S1234', ['LOW_SIGNIF_LOCKED']],
     ['label_added', 'GCN_PRELIM_SENT', 'CBC', 'gstlal', False, 1.e-9,
         ['H1', 'L1', 'V1'], 'S1234', ['SIGNIF_LOCKED']],
     ['label_added', 'LOW_SIGNIF_LOCKED', 'CBC', 'gstlal', False, 1.e-9,
         ['H1', 'L1', 'V1'], 'S1234', ['SIGNIF_LOCKED']],
     ['label_added', 'LOW_SIGNIF_LOCKED', 'CBC', 'gstlal', False, 1.e-9,
         ['H1', 'L1', 'V1'], 'S1234', []],
     ['label_added', 'LOW_SIGNIF_LOCKED', 'CBC', 'gstlal', False, 1.e-5,
         ['H1', 'L1', 'V1'], 'S1234', []],
     ['label_added', 'LOW_SIGNIF_LOCKED', 'CBC', 'gstlal', False, 1.e-10,
         ['H1', 'L1', 'V1'], 'S1234', ['EARLY_WARNING']],
     ['label_added', 'LOW_SIGNIF_PRELIM_SENT', 'CBC', 'gstlal', False, 1.e-9,
         ['H1', 'L1', 'V1'], 'S1234', ['EM_SelectedConfident']],
     ['label_added', 'LOW_SIGNIF_PRELIM_SENT', 'CBC', 'gstlal', False, 1.e-7,
         ['H1', 'L1', 'V1'], 'S1234', ['EM_Selected']],
     ['new', '', 'CBC', 'gstlal', False, 1.e-9, ['H1', 'L1'], 'S1234',
         ['LOW_SIGNIF_LOCKED']],
     ['label_added', 'EARLY_WARNING', 'CBC', 'gstlal', False, 1.e-10,
         ['H1', 'L1', 'V1'], 'S1234', []],
     ['label_added', 'EARLY_WARNING', 'CBC', 'gstlal', False, 1.e-10,
         ['H1', 'L1', 'V1'], 'S1234', ['SIGNIF_LOCKED']],
     ['label_added', 'SIGNIF_LOCKED', 'CBC', 'gstlal', False, 1.e-10,
         ['H1', 'L1', 'V1'], 'S1234', []]])
def test_handle_superevent(monkeypatch, toy_3d_fits_filecontents,  # noqa: F811
                           alert_type, alert_label, group, pipeline,
                           offline, far, instruments, superevent_id,
                           superevent_labels):
    """Test a superevent is dispatched to the correct annotation task based on
    its preferred event's search group.
    """
    def get_superevent(superevent_id):
        return {
            'preferred_event': 'G1234',
            'gw_events': ['G1234'],
            'preferred_event_data': get_event('G1234'),
            'category': "Production",
            'labels': superevent_labels,
            'superevent_id': superevent_id,
            'em_type': None if superevent_id == 'S1234' else 'E1234'
        }

    def get_event(graceid):
        assert graceid == 'G1234'
        event = {
            'group': group,
            'pipeline': pipeline,
            'search': (
                'EarlyWarning' if alert_label == 'EARLY_WARNING'
                else 'AllSky'
            ),
            'graceid': 'G1234',
            'offline': offline,
            'far': far,
            'gpstime': 1234.,
            'extra_attributes': {},
            'labels': ['PASTRO_READY', 'EMBRIGHT_READY', 'SKYMAP_READY']
        }
        if pipeline == 'gstlal':
            # Simulate subthreshold triggers for gstlal. Subthreshold triggers
            # do not contribute to the significance estimate. The way that we
            # can tell that a subthreshold trigger is present is that the chisq
            # entry in the SingleInspiral record is empty (``None``).
            event['extra_attributes']['SingleInspiral'] = [
                {'chisq': 1 if instrument in instruments else None}
                for instrument in ['H1', 'L1', 'V1']]
            event['instruments'] = 'H1,L1,V1'
        else:
            event['instruments'] = ','.join(instruments)
        return event

    def get_labels(graceid):
        return ['COMBINEDSKYMAP_READY']

    def download(filename, graceid):
        if '.fits' in filename:
            return toy_3d_fits_filecontents
        elif filename == 'em_bright.json' and group == 'CBC':
            return json.dumps({'HasNS': 0.0, 'HasRemnant': 0.0})
        elif filename == 'psd.xml.gz':
            with resources.path('psd.xml.gz') as p:
                return str(p)
        elif filename == 'S1234-1-Preliminary.xml':
            return b'fake VOEvent file contents'
        elif group == 'CBC' and filename == f'{pipeline}.p_astro.json':
            return json.dumps(
                dict(BNS=0.94, NSBH=0.03, BBH=0.02, Terrestrial=0.01))
        else:
            raise ValueError

    alert = {
        'alert_type': alert_type,
        'uid': superevent_id,
        'object': {
            'superevent_id': superevent_id,
            't_start': 1214714160,
            't_0': 1214714162,
            't_end': 1214714164,
            'preferred_event': 'G1234',
            'preferred_event_data': get_event('G1234'),
            'category': "Production",
            'labels': superevent_labels
        }
    }
    if alert['alert_type'] == 'new':
        # new alert -> event info in data
        alert['data'] = alert['object']
        alert['labels'] = []
    else:
        alert['data'] = {'name': alert_label}

    if superevent_id == 'S1234':
        raven_coinc = False
    else:
        raven_coinc = True

    create_initial_circular = Mock()
    create_emcoinc_circular = Mock()
    expose = Mock()
    rrt_channel_creation = Mock()
    check_high_profile = Mock()
    annotate_fits = Mock(return_value=None)
    # FIXME: remove logic of mocking return value
    # when live worker testing is enabled
    proceed_if_not_blocked_by = Mock(return_value=None) if \
        alert_label == 'LOW_SIGNIF_PRELIM_SENT' and \
        'EM_SelectedConfident' in superevent_labels else \
        Mock(return_value=(('skymap', 'skymap-filename'),
                           ('em-bright', 'em-bright-filename'),
                           ('p-astro', 'p-astro-filename')))
    gcn_send = Mock()
    alerts_send = Mock()
    setup_dag = Mock()
    start_pe = Mock()
    create_voevent = Mock(return_value='S1234-1-Preliminary.xml')
    create_label = Mock()
    create_tag = Mock()
    select_preferred_event_task = Mock(return_value=get_event('G1234'))
    update_superevent_task = Mock()
    select_pipeline_preferred_event_task = Mock()
    select_pipeline_preferred_event_task.return_value = {
        'pycbc': {'graceid': 'G1'},
        'gstlal': {'graceid': 'G2'},
        'mbta': {'graceid': 'G3'},
        'spiir': {'graceid': 'G4'},
        'cwb': {'graceid': 'G5'}
    }
    add_pipeline_preferred_event_task = Mock()
    omegascan = Mock()
    check_vectors = Mock(return_value=get_event('G1234'))

    monkeypatch.setattr('gwcelery.tasks.gcn.send.run', gcn_send)
    monkeypatch.setattr('gwcelery.tasks.alerts.send.run',
                        alerts_send)
    # FIXME: should test skymaps.annotate_fits instead
    monkeypatch.setattr(
        'gwcelery.tasks.orchestrator._annotate_fits_and_return_input.run',
        annotate_fits
    )
    monkeypatch.setattr(
        'gwcelery.tasks.orchestrator._proceed_if_not_blocked_by.run',
        proceed_if_not_blocked_by
    )
    monkeypatch.setattr('gwcelery.tasks.gracedb.create_tag._orig_run',
                        create_tag)
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.add_pipeline_preferred_event._orig_run',
        add_pipeline_preferred_event_task
    )
    monkeypatch.setattr('gwcelery.tasks.gracedb.download._orig_run', download)
    monkeypatch.setattr('gwcelery.tasks.gracedb.expose._orig_run', expose)
    monkeypatch.setattr('gwcelery.tasks.gracedb.get_event._orig_run',
                        get_event)
    monkeypatch.setattr('gwcelery.tasks.gracedb.get_labels', get_labels)
    # FIXME: should test gracedb.create_voevent instead
    monkeypatch.setattr('gwcelery.tasks.orchestrator._create_voevent.run',
                        create_voevent)
    monkeypatch.setattr('gwcelery.tasks.gracedb.get_superevent._orig_run',
                        get_superevent)
    monkeypatch.setattr('gwcelery.tasks.circulars.create_initial_circular.run',
                        create_initial_circular)
    monkeypatch.setattr('gwcelery.tasks.circulars.create_emcoinc_circular.run',
                        create_emcoinc_circular)
    monkeypatch.setattr('gwcelery.tasks.inference._setup_dag_for_bilby.run',
                        setup_dag)
    monkeypatch.setattr('gwcelery.tasks.inference.start_pe.run',
                        start_pe)
    monkeypatch.setattr('gwcelery.tasks.gracedb.create_label._orig_run',
                        create_label)
    monkeypatch.setattr(
        'gwcelery.tasks.superevents.select_preferred_event.run',
        select_preferred_event_task
    )
    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.update_superevent',
        update_superevent_task
    )
    monkeypatch.setattr(
        'gwcelery.tasks.superevents.select_pipeline_preferred_event.run',
        select_pipeline_preferred_event_task
    )
    monkeypatch.setattr('gwcelery.tasks.detchar.omegascan.run', omegascan)
    monkeypatch.setattr('gwcelery.tasks.detchar.check_vectors.run',
                        check_vectors)
    monkeypatch.setattr('gwcelery.tasks.orchestrator.channel_creation.'
                        'rrt_channel_creation', rrt_channel_creation)
    monkeypatch.setattr('gwcelery.tasks.rrt_utils.check_high_profile.run',
                        check_high_profile)
    monkeypatch.setattr(app.conf, 'create_mattermost_channel', True)
    # Run function under test
    orchestrator.handle_superevent(alert)

    if alert_label == 'GCN_PRELIM_SENT':
        dqr_request_label = list(
            filter(
                lambda x: x.args == ('DQR_REQUEST', superevent_id),
                create_label.call_args_list
            )
        )
        assert len(dqr_request_label) == 1  # DQR_REQUEST is applied once
        select_preferred_event_task.assert_called_once()
        update_superevent_task.assert_called_once()
        select_pipeline_preferred_event_task.assert_called_once()
        assert add_pipeline_preferred_event_task.call_count == len(
            select_pipeline_preferred_event_task.return_value
        )

    elif alert_label == 'SIGNIF_LOCKED':
        if superevents.FROZEN_LABEL not in superevent_labels:
            create_label.assert_has_calls(
                [call(superevents.FROZEN_LABEL, superevent_id)])
        annotate_fits.assert_called_once()
        _event_info = get_event('G1234')  # this gets the preferred event info
        assert superevents.should_publish(_event_info)
        expose.assert_called_once_with(superevent_id)
        create_tag.assert_has_calls(
            [call('S1234-1-Preliminary.xml', 'public', superevent_id),
             call('em-bright-filename', 'public', superevent_id),
             call('p-astro-filename', 'public', superevent_id),
             call('skymap-filename', 'public', superevent_id)],
            any_order=True
        )
        # FIXME: uncomment block below when patching
        # gracedb.create_voevent instead of orchestrator._create_voevent
        # if group == 'CBC':
        #     create_voevent.assert_called_once_with(
        #         'S1234', 'preliminary', BBH=0.02, BNS=0.94, NSBH=0.03,
        #         ProbHasNS=0.0, ProbHasRemnant=0.0, Terrestrial=0.01,
        #         internal=False, open_alert=True,
        #         skymap_filename='bayestar.fits.gz', skymap_type='bayestar')
        gcn_send.assert_called_once()
        alerts_send.assert_called_once()
        check_high_profile.assert_called_once()
        assert call(
            'GCN_PRELIM_SENT', superevent_id) in create_label.call_args_list

        if raven_coinc:
            create_emcoinc_circular.assert_called_once()
        else:
            create_initial_circular.assert_called_once()

    elif alert_label == 'EARLY_WARNING':
        if 'SIGNIF_LOCKED' in superevent_labels:
            expose.assert_not_called()
            alerts_send.assert_not_called()
            gcn_send.assert_not_called()
            create_tag.assert_not_called()
            create_initial_circular.assert_not_called()
            check_high_profile.assert_not_called()
        else:
            annotate_fits.assert_called_once()
            update_superevent_task.assert_called_once_with(
                'S1234', preferred_event='G1234', t_0=1234.
            )
            expose.assert_called_once_with('S1234')
            create_tag.assert_has_calls(
                [call('S1234-1-Preliminary.xml', 'public', 'S1234'),
                 call('em-bright-filename', 'public', 'S1234'),
                 call('p-astro-filename', 'public', 'S1234'),
                 call('skymap-filename', 'public', 'S1234')],
                any_order=True
            )
            assert call('GCN_PRELIM_SENT', superevent_id) \
                not in create_label.call_args_list

    elif alert_label == 'LOW_SIGNIF_LOCKED':
        if ('SIGNIF_LOCKED' in superevent_labels) or \
                ('EARLY_WARNING' in superevent_labels):
            expose.assert_not_called()
            alerts_send.assert_not_called()
            gcn_send.assert_not_called()
            create_tag.assert_not_called()
            create_initial_circular.assert_not_called()
            check_high_profile.assert_not_called()
        elif far > 3.8e-7:
            check_high_profile.assert_not_called()
        else:
            annotate_fits.assert_called_once()
            # no superevent clean up needed
            update_superevent_task.assert_not_called()
            # no circular creation for less-significant alerts
            create_initial_circular.assert_not_called()
            create_emcoinc_circular.assert_not_called()
            assert call('LOW_SIGNIF_PRELIM_SENT', superevent_id) \
                in create_label.call_args_list
    elif alert_label == 'ADVREQ':
        rrt_channel_creation.assert_called_once_with(
            superevent_id,
            app.conf['gracedb_host'])
    elif alert_label == 'LOW_SIGNIF_PRELIM_SENT':
        if 'EM_SelectedConfident' in superevent_labels:
            expose.assert_not_called()
            alerts_send.assert_not_called()
            gcn_send.assert_not_called()
            create_tag.assert_not_called()
            create_initial_circular.assert_not_called()
            check_high_profile.assert_not_called()
        else:
            # check alert type is less-significant
            _files, _superevent, _alert_type = alerts_send.call_args.args
            assert _alert_type == 'less-significant'

    if alert_type == 'new' and group == 'CBC':
        threshold = (
            app.conf['preliminary_alert_far_threshold']['cbc'] /
            app.conf['preliminary_alert_trials_factor']['cbc']
        )
        if far <= threshold:
            assert start_pe.call_count == 2
            call_args = [
                call_args.args for call_args in start_pe.call_args_list]
            assert all(
                [(get_event('G1234'), superevent_id, pipeline) in call_args
                 for pipeline in ('bilby', 'rapidpe')])
        else:
            start_pe.assert_not_called()

    if alert_type == 'new':
        omegascan.assert_called_once()
        check_vectors.assert_called_once()


@pytest.mark.parametrize(
        "orig_labels,overall_dq_active_state",
        [[["ADVREQ"], None],
         [["DQV", "ADVREQ"], False],
         [["DQV", "ADVREQ"], True]])
@patch('gwcelery.tasks.gracedb.update_superevent.run')
@patch('gwcelery.tasks.gracedb.upload.run')
def test_handle_superevent_event_added(mock_upload,
                                       mock_update_superevent,
                                       monkeypatch, orig_labels,
                                       overall_dq_active_state):
    # make a copy of orig_labels that we mutate in order to change the result
    # of get_labels calls. In actuality, the second call will make an API
    # request.
    labels = orig_labels.copy()

    alert = {
        'alert_type': 'event_added',
        'uid': 'TS123456a',
        'data': {'superevent_id': 'TS123456a',
                 'preferred_event': 'G123456',
                 't_start': 1.,
                 't_0': 2.,
                 't_end': 3.},
        'object': {'superevent_id': 'TS123456a',
                   'preferred_event': 'G123456',
                   't_start': 1.,
                   't_0': 2.,
                   't_end': 3.,
                   'preferred_event_data': {
                       'graceid': 'G123456',
                       'gpstime': 2.1,
                       'labels': labels
                   }}
    }

    # mocks the effect of detchar.check_vectors on the result of get_labels
    def _mock_check_vectors(event, *args):
        if overall_dq_active_state is True:
            labels.append('DQOK')
            try:
                labels.remove('DQV')
            except ValueError:  # not in list
                pass
        elif overall_dq_active_state is False:
            labels.append('DQV')
            try:
                labels.remove('DQOK')
            except ValueError:  # not in list
                pass
        return event
    check_vectors = Mock(side_effect=_mock_check_vectors)
    monkeypatch.setattr('gwcelery.tasks.detchar.check_vectors.run',
                        check_vectors)

    get_labels = Mock(return_value=labels)
    monkeypatch.setattr('gwcelery.tasks.gracedb.get_labels.run',
                        get_labels)

    orchestrator.handle_superevent(alert)

    if "DQV" in orig_labels:
        check_vectors.assert_called_once_with(
            {'graceid': 'G123456', 'gpstime': 2.1, 'labels': labels},
            'TS123456a', 1., 3.)
    else:
        check_vectors.assert_not_called()

    if overall_dq_active_state is True:
        mock_update_superevent.assert_called_once_with(
            'TS123456a', preferred_event='G123456', t_0=2.1)
        mock_upload.assert_called_once()
    else:
        mock_update_superevent.assert_not_called()
        mock_upload.assert_not_called()


def superevent_initial_alert_download(filename, graceid):
    if filename == 'S1234-Initial-1.xml':
        return 'contents of S1234-Initial-1.xml'
    elif filename == 'em_bright.json,0':
        return json.dumps({'HasNS': 0.0, 'HasRemnant': 0.0})
    elif filename == 'p_astro.json,0':
        return b'{"BNS": 0.94, "NSBH": 0.03, "BBH": 0.02, "Terrestrial": 0.01}'
    elif filename == 'foobar.multiorder.fits,0':
        return 'contents of foobar.multiorder.fits,0'
    elif 'combined-ext.multiorder.fits' in filename:
        return 'contents of combined-ext.multiorder.fits'
    elif 'combined-ext.png' in filename:
        return 'contents of combined-ext.png'
    else:
        raise ValueError


def _mock_get_labels(ext_id):
    if ext_id == 'E1':
        return []
    elif ext_id == 'E2':
        return ['EM_COINC', 'RAVEN_ALERT']
    elif ext_id == 'E3':
        return ['EM_COINC', 'RAVEN_ALERT', 'COMBINEDSKYMAP_READY']


def _mock_get_log(se_id):
    logs = [{'tag_names': ['sky_loc', 'public'],
             'filename': 'foobar.multiorder.fits',
             'file_version': 0},
            {'tag_names': ['em_bright'],
             'filename': 'em_bright.json',
             'file_version': 0},
            {'tag_names': ['p_astro', 'public'],
             'filename': 'p_astro.json',
             'file_version': 0}]
    if se_id == 'S2468':
        logs.append({'tag_names': ['sky_loc', 'ext_coinc'],
                     'filename': 'combined-ext.multiorder.fits',
                     'file_version': 0})
    return logs


@pytest.mark.parametrize(  # noqa: F811
    'labels,superevent_id,ext_id',
    [[[], 'S1234', 'E1'],
     [['EM_COINC', 'RAVEN_ALERT'], 'S1234', 'E2'],
     [['EM_COINC', 'RAVEN_ALERT', 'COMBINEDSKYMAP_READY'], 'S1234', 'E3'],
     [['EM_COINC', 'RAVEN_ALERT', 'COMBINEDSKYMAP_READY'], 'S2468', 'E3']])
@patch('gwcelery.tasks.gracedb.expose._orig_run', return_value=None)
@patch('gwcelery.tasks.gracedb.get_log',
       side_effect=_mock_get_log)
@patch('gwcelery.tasks.gracedb.create_tag._orig_run', return_value=None)
@patch('gwcelery.tasks.gracedb.create_voevent._orig_run',
       return_value='S1234-Initial-1.xml')
@patch('gwcelery.tasks.gracedb.get_labels',
       side_effect=_mock_get_labels)
@patch('gwcelery.tasks.gracedb.download._orig_run',
       superevent_initial_alert_download)
@patch('gwcelery.tasks.gcn.send.run')
@patch('gwcelery.tasks.alerts.send.run')
@patch('gwcelery.tasks.circulars.create_emcoinc_circular.run')
@patch('gwcelery.tasks.circulars.create_initial_circular.run')
def test_handle_superevent_initial_alert(mock_create_initial_circular,
                                         mock_create_emcoinc_circular,
                                         mock_alerts_send,
                                         mock_gcn_send,
                                         mock_get_labels,
                                         mock_create_voevent,
                                         mock_create_tag, mock_get_log,
                                         mock_expose, labels,
                                         superevent_id, ext_id):
    """Test that the ``ADVOK`` label triggers an initial alert.
    This test varies the labels in the superevent and external event in order
    to test the non-RAVEN alerts, RAVEN alerts without a combined sky map, and
    RAVEN alerts with a combined sky map respectively."""
    alert = {
        'alert_type': 'label_added',
        'uid': superevent_id,
        'data': {'name': 'ADVOK'},
        'object': {
            'labels': labels,
            'category': 'Production',
            'superevent_id': superevent_id,
            'em_type': ext_id if labels else '',
            'category': 'Production'}
    }
    combined_skymap_needed = ('COMBINEDSKYMAP_READY' in labels)
    if combined_skymap_needed:
        combined_skymap_filename = \
            ('combined-ext.multiorder.fits,')
        if superevent_id == 'S1234':
            combined_skymap_filename += '0'
        elif superevent_id == 'S2468':
            combined_skymap_filename += '1'
    else:
        combined_skymap_filename = None

    # Run function under test
    orchestrator.handle_superevent(alert)

    mock_create_voevent.assert_called_once_with(
        superevent_id, 'initial', BBH=0.02, BNS=0.94, NSBH=0.03, ProbHasNS=0.0,
        ProbHasRemnant=0.0, Terrestrial=0.01, internal=False, open_alert=True,
        skymap_filename='foobar.multiorder.fits,0', skymap_type='foobar',
        raven_coinc='RAVEN_ALERT' in labels,
        Significant=1,  # this field is true for initial alerts
        combined_skymap_filename=(combined_skymap_filename if
                                  combined_skymap_needed else None))
    mock_alerts_send.assert_called_once_with(
        (superevent_initial_alert_download('foobar.multiorder.fits,0',
                                           superevent_id),
         superevent_initial_alert_download('em_bright.json,0', superevent_id),
         superevent_initial_alert_download('p_astro.json,0', superevent_id)) +
        ((6 if combined_skymap_needed else 4) * (None,)),
        alert['object'], 'initial', raven_coinc='RAVEN_ALERT' in labels,
        combined_skymap_filename=combined_skymap_filename)
    mock_gcn_send.assert_called_once_with('contents of S1234-Initial-1.xml')
    if 'RAVEN_ALERT' in labels:
        mock_create_emcoinc_circular.assert_called_once_with(superevent_id)
    else:
        mock_create_initial_circular.assert_called_once_with(superevent_id)
    mock_create_tag.assert_has_calls(
        [call('foobar.multiorder.fits,0', 'public', superevent_id),
         call('em_bright.json,0', 'public', superevent_id),
         call('p_astro.json,0', 'public', superevent_id),
         call('S1234-Initial-1.xml', 'public', superevent_id)],
        any_order=True)
    mock_expose.assert_called_once_with(superevent_id)


def superevent_retraction_alert_download(filename, graceid):
    if filename == 'S1234-Retraction-2.xml':
        return 'contents of S1234-Retraction-2.xml'
    else:
        raise ValueError


@patch('gwcelery.tasks.gracedb.expose._orig_run', return_value=None)
@patch('gwcelery.tasks.gracedb.create_tag._orig_run')
@patch('gwcelery.tasks.gracedb.create_voevent._orig_run',
       return_value='S1234-Retraction-2.xml')
@patch('gwcelery.tasks.gracedb.download._orig_run',
       superevent_retraction_alert_download)
@patch('gwcelery.tasks.gcn.send.run')
@patch('gwcelery.tasks.alerts.send.run')
@patch('gwcelery.tasks.circulars.create_retraction_circular.run')
def test_handle_superevent_retraction_alert(mock_create_retraction_circular,
                                            mock_alerts_send,
                                            mock_gcn_send,
                                            mock_create_voevent,
                                            mock_create_tag, mock_expose):
    """Test that the ``ADVNO`` label triggers a retraction alert."""
    alert = {
        'alert_type': 'label_added',
        'uid': 'S1234',
        'data': {'name': 'ADVNO'},
        'object': {
            'labels': [],
            'category': 'Production',
            'superevent_id': 'S1234'
        }
    }

    # Run function under test
    orchestrator.handle_superevent(alert)

    mock_create_voevent.assert_called_once_with(
        'S1234', 'retraction', internal=False, open_alert=True)
    mock_gcn_send.assert_called_once_with('contents of S1234-Retraction-2.xml')
    mock_alerts_send.assert_called_once_with(None, alert['object'],
                                             'retraction')
    mock_create_retraction_circular.assert_called_once_with('S1234')
    mock_create_tag.assert_called_once_with(
        'S1234-Retraction-2.xml', 'public', 'S1234')
    mock_expose.assert_called_once_with('S1234')


def mock_download(filename, graceid, *args, **kwargs):
    assert graceid == 'M394156'
    filenames = {'coinc.xml': 'coinc.xml',
                 'ranking_data.xml.gz': 'ranking_data_G322589.xml.gz'}
    return resources.read_binary(data, filenames[filename])


@pytest.mark.parametrize(
    'alert_type,filename',
    [['new', ''], ['log', 'psd.xml.gz'],
     ['log', 'test.posterior_samples.hdf5']])
def test_handle_posterior_samples(monkeypatch, alert_type, filename):
    alert = {
        'alert_type': alert_type,
        'uid': 'S1234',
        'data': {'comment': 'samples', 'filename': filename}
    }

    download = Mock(return_value='posterior-sample-file-content')
    em_bright_pe = Mock()
    skymap_from_samples = Mock(return_value='skymap-content')
    fits_header = Mock()
    plot_allsky = Mock()
    annotate_fits_volume = Mock()
    upload = Mock()
    flatten = Mock()

    monkeypatch.setattr('gwcelery.tasks.em_bright.em_bright_posterior_'
                        'samples.run', em_bright_pe)
    monkeypatch.setattr('gwcelery.tasks.gracedb.download._orig_run', download)
    monkeypatch.setattr('gwcelery.tasks.skymaps.skymap_from_samples.run',
                        skymap_from_samples)
    monkeypatch.setattr('gwcelery.tasks.skymaps.fits_header.run', fits_header)
    monkeypatch.setattr('gwcelery.tasks.skymaps.plot_allsky.run', plot_allsky)
    monkeypatch.setattr('gwcelery.tasks.skymaps.annotate_fits_volume.run',
                        annotate_fits_volume)
    monkeypatch.setattr('gwcelery.tasks.gracedb.upload._orig_run', upload)
    monkeypatch.setattr('gwcelery.tasks.skymaps.flatten.run', flatten)

    # Run function under test
    orchestrator.handle_posterior_samples(alert)

    if alert['alert_type'] != 'log' or \
            not alert['data']['filename'].endswith('.posterior_samples.hdf5'):
        skymap_from_samples.assert_not_called()
        fits_header.assert_not_called()
        plot_allsky.assert_not_called()
        annotate_fits_volume.assert_not_called()
        flatten.assert_not_called()
    else:
        em_bright_pe.assert_called_once()
        skymap_from_samples.assert_called_once()
        fits_header.assert_called_once()
        plot_allsky.assert_called_once()
        annotate_fits_volume.assert_called_once()
        flatten.assert_called_once()


@pytest.mark.parametrize("alert_search,alert_pipeline",
                         [(search, pipeline)
                          for search in ['earlywarning', 'mdc', 'allsky']
                          for pipeline in ['gstlal', 'pycbc', 'mbta', 'spiir']
                          ])
@patch('gwcelery.tasks.gracedb.download._orig_run', mock_download)
@patch('gwcelery.tasks.p_astro.compute_p_astro.run')
@patch('gwcelery.tasks.bayestar.localize.run')
@patch('gwcelery.tasks.em_bright.source_properties.run')
def test_handle_cbc_event_new_event(mock_source_properties,
                                    mock_localize, mock_p_astro,
                                    alert_search, alert_pipeline):
    alert = read_json(data, 'lvalert_event_creation.json')
    pipelines_stock_p_astro = {('spiir', 'earlywarning'),
                               ('pycbc', 'earlywarning'), ('gstlal', 'mdc')}
    alert['object']['search'] = alert_search
    alert['object']['pipeline'] = alert_pipeline
    orchestrator.handle_cbc_event(alert)
    mock_source_properties.assert_called_once()
    if (alert_pipeline, alert_search) in pipelines_stock_p_astro:
        mock_p_astro.assert_called_once()
    else:
        mock_p_astro.assert_not_called()
    mock_localize.assert_called_once()


@patch(
    'gwcelery.tasks.gracedb.get_event._orig_run',
    return_value={'graceid': 'T250822', 'group': 'CBC', 'pipeline': 'gstlal',
                  'far': 1e-7, 'labels': [],
                  'extra_attributes':
                      {'CoincInspiral': {'snr': 10.},
                       'SingleInspiral': [{'mass1': 10., 'mass2': 5.}]}})
@patch('gwcelery.tasks.gracedb.download._orig_run', mock_download)
@patch('gwcelery.tasks.em_bright.source_properties.run')
@patch('gwcelery.tasks.bayestar.localize.run')
def test_handle_cbc_event_ignored(mock_localize,
                                  mock_classifier,
                                  mock_get_event):
    """Test that unrelated LVAlert messages do not trigger BAYESTAR."""
    alert = read_json(data, 'igwn_alert_detchar.json')
    orchestrator.handle_cbc_event(alert)
    mock_localize.assert_not_called()
    mock_classifier.assert_not_called()


@pytest.mark.live_worker
@patch('gwcelery.tasks.gcn.send')
@patch('gwcelery.tasks.alerts.send')
def test_alerts_skip_inj(mock_gcn_send, mock_alerts_send):
    orchestrator.earlywarning_preliminary_alert.delay(
        ('bayestar.fits.gz', 'em_bright.json', 'p_astro.json'),
        {'superevent_id': 'S1234', 'labels': ['INJ']},
        'preliminary'
    ).get()
    mock_gcn_send.assert_not_called()
    mock_alerts_send.assert_not_called()


@pytest.fixture
def only_mdc_alerts(monkeypatch):
    monkeypatch.setitem(app.conf, 'only_alert_for_mdc', True)


@pytest.mark.live_worker
@patch('gwcelery.tasks.skymaps.flatten')
@patch('gwcelery.tasks.gracedb.download')
@patch('gwcelery.tasks.gracedb.upload')
@patch('gwcelery.tasks.alerts.send')
def test_only_mdc_alerts_switch(mock_alert, mock_upload, mock_download,
                                mock_flatten, only_mdc_alerts):
    """Test to ensure that if the `only_alert_for_mdc` configuration variable
    is True, only events with search type "MDC" can result in alerts.
    """
    for search in ['AllSky', 'GRB', 'BBH']:
        event_dictionary = {'graceid': 'G2',
                            'gpstime': 1239917954.40918,
                            'far': 5.57979637960671e-06,
                            'group': 'CBC',
                            'search': search,
                            'instruments': 'H1,L1',
                            'pipeline': 'spiir',
                            'offline': False,
                            'labels': []}
        superevent_id = 'S1234'
        orchestrator.earlywarning_preliminary_alert.delay(
            event_dictionary,
            superevent_id,
            'preliminary'
        ).get()
        mock_alert.assert_not_called()


@pytest.mark.parametrize(
    'far,event,pe_pipeline',
    [[1, {'gpstime': 1187008882, 'group': 'CBC', 'search': 'AllSky'}, 'bilby'],
     [1e-30, {'gpstime': 1187008882, 'group': 'Burst', 'search': 'AllSky'},
         'bilby'],
     [1e-30, {'gpstime': 1187008882, 'group': 'CBC', 'search': 'MDC'},
         'bilby'],
     [1e-30, {'gpstime': 1187008882, 'group': 'CBC',
              'search': 'AllSky', 'pipeline': 'gstlal'}, 'bilby'],
     [1e-30, {'gpstime': 1187008882, 'group': 'CBC',
              'search': 'AllSky', 'pipeline': 'pycbc'}, 'bilby'],
     [1, {'gpstime': 1187008882, 'group': 'CBC', 'search': 'AllSky'},
         'rapidpe'],
     [1e-30, {'gpstime': 1187008882, 'group': 'Burst', 'search': 'AllSky'},
         'rapidpe'],
     [1e-30, {'gpstime': 1187008882, 'group': 'CBC', 'search': 'MDC'},
         'rapidpe'],
     [1e-30, {'gpstime': 1187008882, 'group': 'CBC',
              'search': 'AllSky', 'pipeline': 'gstlal'}, 'rapidpe'],
     [1e-30, {'gpstime': 1187008882, 'group': 'CBC',
              'search': 'AllSky', 'pipeline': 'pycbc'}, 'rapidpe']]
)
def test_parameter_estimation(monkeypatch, far, event, pe_pipeline):
    superevent_id = 'S1234'
    mock_upload = Mock()
    mock_start_pe = Mock()
    monkeypatch.setattr('gwcelery.tasks.gracedb.upload.run', mock_upload)
    monkeypatch.setattr('gwcelery.tasks.inference.start_pe.run', mock_start_pe)

    orchestrator.parameter_estimation.delay(
        far_event=(far, event), superevent_id=superevent_id,
        pe_pipeline=pe_pipeline)

    threshold = (app.conf['significant_alert_far_threshold']['cbc'] /
                 app.conf['significant_alert_trials_factor']['cbc'])
    if (
        far <= threshold and event['group'] == 'CBC' and
        event['search'] != 'MDC'
    ):
        mock_start_pe.assert_any_call(
            event, superevent_id, pe_pipeline)
        assert mock_start_pe.call_count == 1
    else:
        mock_upload.assert_called_once()


@pytest.mark.parametrize(
    'search_pipeline,pe_pipeline',
    [["MBTA", "bilby"], ["MBTA", "rapidpe"],
     ["spiir", "bilby"], ["spiir", "rapidpe"]],
)
def test_mbta_disabled_on_playground(
    monkeypatch, search_pipeline, pe_pipeline
):
    superevent_id = "S1234"
    mock_upload = Mock()
    mock_start_pe = Mock()
    monkeypatch.setattr('gwcelery.tasks.gracedb.upload.run', mock_upload)
    monkeypatch.setattr('gwcelery.tasks.inference.start_pe.run', mock_start_pe)
    monkeypatch.setitem(
        app.conf, "gracedb_host", "gracedb-playground.ligo.org"
    )
    far = 1e-30
    event = {'gpstime': 1187008882, 'group': 'CBC',
             'search': 'AllSky', 'pipeline': search_pipeline}

    orchestrator.parameter_estimation.delay(
        far_event=(far, event), superevent_id=superevent_id,
        pe_pipeline=pe_pipeline)

    if search_pipeline != 'MBTA':
        mock_start_pe.assert_any_call(
            event, superevent_id, pe_pipeline)
        assert mock_start_pe.call_count == 1
    else:
        mock_upload.assert_called_once()


@pytest.mark.parametrize(
    "superevent_labels,block_by_labels",
    [
        [["LOW_SIGNIF_LOCKED"], set()],
        [["LOW_SIGNIF_LOCKED"], {"ADVOK", "ADVNO"}],
        [["SIGNIF_LOCKED", "ADVOK"], {"ADVOK", "ADVNO"}]
    ]
)
def test_blocking_labels(superevent_labels, block_by_labels):
    with patch('gwcelery.tasks.gracedb.get_labels',
               return_value=superevent_labels):
        r = orchestrator._proceed_if_not_blocked_by(
            ('bayestar', 'em-bright', 'p-astro'),
            'S123456',
            block_by_labels
        )
    if "ADVOK" in superevent_labels:
        assert r is None  # blocked
    else:
        assert r == ('bayestar', 'em-bright', 'p-astro')  # passed
