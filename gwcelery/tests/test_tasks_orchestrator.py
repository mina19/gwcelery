import json
from importlib import resources
from unittest.mock import Mock, call, patch

import pytest

from .. import app
from ..kafka.bootsteps import AvroBlobWrapper
from ..tasks import orchestrator, superevents
from ..util import read_binary, read_json
from . import data
from .test_tasks_skymaps import toy_3d_fits_filecontents  # noqa: F401


@pytest.mark.parametrize(  # noqa: F811
    'alert_type,alert_label,group,search,pipeline,offline,far,instruments,'
    'superevent_id,superevent_labels',
    [['label_added', superevents.FROZEN_LABEL, 'CBC', 'AllSky', 'gstlal',
        False, 1.e-9, ['H1'], 'S1234', [superevents.FROZEN_LABEL]],
     ['label_added', 'ADVREQ', 'CBC', 'AllSky', 'gstlal', False, 1.e-9,
        ['H1'], 'S1234', ['ADVREQ']],
     ['label_added', superevents.SIGNIFICANT_LABEL, 'CBC', 'AllSky', 'gstlal',
         False, 1.e-9, ['H1', 'L1'], 'S1234', [superevents.FROZEN_LABEL]],
     ['label_added', superevents.SIGNIFICANT_LABEL, 'CBC', 'AllSky', 'gstlal',
         False, 1.e-9, ['H1', 'L1', 'V1'], 'S1234',
         [superevents.FROZEN_LABEL]],
     ['label_added', superevents.SIGNIFICANT_LABEL, 'CBC', 'AllSky', 'gstlal',
         False, 1.e-9, ['H1', 'L1', 'V1'], 'S2468', [
             superevents.FROZEN_LABEL, 'COMBINEDSKYMAP_READY', 'RAVEN_ALERT',
             'EM_COINC'
         ]],
     ['label_added', superevents.SIGNIFICANT_LABEL, 'Burst', 'AllSky', 'CWB',
         False, 1.e-9, ['H1', 'L1', 'V1'], 'S1234',
         [superevents.FROZEN_LABEL]],
     ['label_added', superevents.SIGNIFICANT_LABEL, 'Burst', 'AllSky', 'oLIB',
         False, 1.e-9, ['H1', 'L1', 'V1'], 'S1234',
         [superevents.FROZEN_LABEL]],
     ['label_added', 'GCN_PRELIM_SENT', 'CBC', 'AllSky', 'gstlal', False,
         1.e-9, ['H1', 'L1', 'V1'], 'S1234', [superevents.SIGNIFICANT_LABEL]],
     ['label_added', superevents.FROZEN_LABEL, 'CBC', 'AllSky', 'gstlal',
         False, 1.e-9, ['H1', 'L1', 'V1'], 'S1234',
         [superevents.SIGNIFICANT_LABEL]],
     ['label_added', superevents.FROZEN_LABEL, 'CBC', 'AllSky', 'gstlal',
         False, 1.e-9, ['H1', 'L1', 'V1'], 'S1234', []],
     ['label_added', superevents.FROZEN_LABEL, 'CBC', 'AllSky', 'gstlal',
         False, 1.e-5, ['H1', 'L1', 'V1'], 'S1234', []],
     ['label_added', superevents.FROZEN_LABEL, 'CBC', 'AllSky', 'gstlal',
         False, 1.e-10, ['H1', 'L1', 'V1'], 'S1234',
         [superevents.EARLY_WARNING_LABEL]],
     ['label_added', 'LOW_SIGNIF_PRELIM_SENT', 'CBC', 'AllSky', 'gstlal',
         False, 1.e-9, ['H1', 'L1', 'V1'], 'S1234',
         [superevents.SIGNIFICANT_LABEL]],
     ['label_added', 'LOW_SIGNIF_PRELIM_SENT', 'CBC', 'AllSky', 'gstlal',
         False, 1.e-7, ['H1', 'L1', 'V1'], 'S1234',
         [superevents.FROZEN_LABEL]],
     ['new', '', 'CBC', 'AllSky', 'gstlal', False, 1.e-9, ['H1', 'L1'],
         'S1234', [superevents.FROZEN_LABEL]],
     ['label_added', superevents.EARLY_WARNING_LABEL, 'CBC', 'AllSky',
         'gstlal', False, 1.e-10, ['H1', 'L1', 'V1'], 'S1234', []],
     ['label_added', superevents.EARLY_WARNING_LABEL, 'CBC', 'AllSky',
         'gstlal', False, 1.e-10, ['H1', 'L1', 'V1'], 'S1234',
         [superevents.SIGNIFICANT_LABEL]],
     ['label_added', superevents.SIGNIFICANT_LABEL, 'CBC', 'AllSky', 'gstlal',
         False, 1.e-10, ['H1', 'L1', 'V1'], 'S1234', []]])
def test_handle_superevent(monkeypatch, toy_3d_fits_filecontents,  # noqa: F811
                           alert_type, alert_label, group, search, pipeline,
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
                superevents.EARLY_WARNING_SEARCH_NAME if alert_label ==
                superevents.EARLY_WARNING_LABEL else 'AllSky'
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
            return json.dumps({'HasNS': 0.0, 'HasRemnant': 0.0,
                               'HasMassGap': 0.0})
        elif filename == 'psd.xml.gz':
            return str(resources.files().joinpath('psd.xml.gz'))
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
        superevents.SIGNIFICANT_LABEL in superevent_labels else \
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

    elif alert_label == superevents.SIGNIFICANT_LABEL:
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

    elif alert_label == superevents.EARLY_WARNING_LABEL:
        if superevents.SIGNIFICANT_LABEL in superevent_labels:
            expose.assert_not_called()
            alerts_send.assert_not_called()
            gcn_send.assert_not_called()
            create_tag.assert_not_called()
            create_initial_circular.assert_not_called()
            check_high_profile.assert_not_called()
            create_label.assert_not_called()
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
            create_label.assert_has_calls([call('DQR_REQUEST', 'S1234')])
    elif alert_label == superevents.FROZEN_LABEL:
        if (superevents.SIGNIFICANT_LABEL in superevent_labels) or \
                (superevents.EARLY_WARNING_LABEL in superevent_labels):
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
        if superevents.SIGNIFICANT_LABEL in superevent_labels:
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
            select_pipeline_preferred_event_task.assert_called_once()
            assert add_pipeline_preferred_event_task.call_count == len(
                select_pipeline_preferred_event_task.return_value
            )

    if alert_type == 'new' and group == 'CBC':
        threshold = (
            app.conf['preliminary_alert_far_threshold']['cbc'][search.lower()]
            /
            app.conf['preliminary_alert_trials_factor']['cbc'][search.lower()]
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


@patch("gwcelery.tasks.skymaps.annotate_fits")
@patch(
    "gwcelery.tasks.gracedb.create_voevent._orig_run",
    return_value="S1234-1-Preliminary.xml,0")
@patch("gwcelery.tasks.alerts.send.run")
@patch("gwcelery.tasks.gcn.send.run")
@patch("gwcelery.tasks.circulars.create_initial_circular.run")
@pytest.mark.parametrize(
    "alert_label,rapidpe_pastro_available,"
    "preferred_event_updated, rapidpe_included_in_second_prelim",
    [
        ("GCN_PRELIM_SENT", False, False, None),
        ("GCN_PRELIM_SENT", True, True, None),
        ("GCN_PRELIM_SENT", True, False, None),
        ("ADVOK", False, False, None),
        ("ADVOK", True, True, True),
        ("ADVOK", True, False, True),
        ("ADVOK", True, True, False),
        ("ADVOK", True, False, False),
    ],
)
def test_update_rapidpe_pastro(
    mock_create_initial_circular,
    mock_gcn_send,
    mock_alerts_send,
    mock_create_voevent,
    mock_annotate_fits,
    alert_label,
    rapidpe_pastro_available,
    preferred_event_updated,
    rapidpe_included_in_second_prelim,
    monkeypatch,
):
    labels = ["PASTRO_READY", "EMBRIGHT_READY", alert_label]

    renormalise_rapidpe = rapidpe_pastro_available * preferred_event_updated

    pipeline = 'gstlal'
    event = {
        "group": "CBC",
        "pipeline": pipeline,
        "search": "AllSky",
        "far": 1e-10,
        "graceid": "G1234",
        "gpstime": 1234.0,
        "labels": labels,
        "instruments": "H1,L1,V1",
        "offline": False,
    }
    superevent = {
        "gw_events": [event["graceid"]],
        "preferred_event_data": event,
        "category": "Production",
        "superevent_id": "S1234",
        "labels": labels,
    }

    alert = {
        "alert_type": "label_added",
        "uid": superevent["superevent_id"],
        "object": superevent,
        "data": {"name": alert_label},
    }

    mock_get_event = Mock(return_value=event)
    monkeypatch.setattr(
        "gwcelery.tasks.gracedb.get_event._orig_run",
        mock_get_event,
    )

    mock_get_superevent = Mock(return_value=superevent)
    monkeypatch.setattr(
        "gwcelery.tasks.gracedb.get_superevent._orig_run",
        mock_get_superevent,
    )

    mock_select_preferred_event = Mock(return_value=event)
    monkeypatch.setattr(
        "gwcelery.tasks.superevents.select_preferred_event.run",
        mock_select_preferred_event,
    )

    pipeline_p_terr_initial = 0.01
    pipeline_pastro_dict_initial = json.dumps(
        {
            "BNS": 0.94,
            "NSBH": 0.03,
            "BBH": 0.02,
            "Terrestrial": pipeline_p_terr_initial,
        }
    )

    mock_file_data = {
        "S1234": {
            "bayestar.multiorder.fits": resources.read_binary(
                data, "MS220722v_bayestar.multiorder.fits"
            ),
            "em_bright.json": json.dumps({"HasNS": 0.0, "HasRemnant": 0.0,
                                          "HasMassGap": 0.0}),
            f"{pipeline}.p_astro.json": pipeline_pastro_dict_initial,
            "S1234-1-Preliminary.xml": b"fake VOEvent file contents",
        }
    }
    mock_log_data = [
        {
            "comment": "source probabilities",
            "filename": f"{pipeline}.p_astro.json",
            "file_version": 0,
            "tag_names": ["public", "p_astro"],
        },
        {
            "comment": "em bright complete",
            "filename": "em_bright.json",
            "file_version": 0,
            "tag_names": ["public", "em_bright"],
        },
        {
            "comment": "New VOEvent",
            "filename": "S1234-1-Preliminary.xml",
            "file_version": 0,
            "tag_names": ["public", "em_follow", "gcn_received"],
        },
        {
            "comment": "sky localization complete",
            "filename": "bayestar.multiorder.fits",
            "file_version": 0,
            "tag_names": ["sky_loc", "public"],
        },
    ]
    if rapidpe_pastro_available:
        rapidpe_pastro_dict_initial = json.dumps(
            {
                "BNS": 0.95,
                "NSBH": 0.02,
                "BBH": 0.02,
                "Terrestrial": pipeline_p_terr_initial,
            }
        )

        mock_file_data["S1234"][
            "RapidPE_RIFT.p_astro.json"
        ] = rapidpe_pastro_dict_initial

        tag_names = ["p_astro", "pe", "public"]
        mock_log_data.append(
            {
                "comment": "RapidPE-RIFT Pastro results",
                "filename": "RapidPE_RIFT.p_astro.json",
                "file_version": 0,
                "tag_names": tag_names,
            }
        )

    if preferred_event_updated:
        pipeline_p_terr_updated = 0.02
        pipeline_pastro_dict_updated = json.dumps(
            {
                "BNS": 0.94,
                "NSBH": 0.03,
                "BBH": 0.01,
                "Terrestrial": pipeline_p_terr_updated,
            }
        )
        mock_file_data["S1234"][
            f"{pipeline}.p_astro.json"
        ] = pipeline_pastro_dict_updated

        mock_log_data.append(
            {
                "comment": "source probabilities",
                "filename": f"{pipeline}.p_astro.json",
                "file_version": 1,
                "tag_names": ["public", "p_astro"],
            }
        )

        expected_rapidpe_pastro_dict = json.dumps(
            read_json(data, 'updated_RapidPE_RIFT.p_astro.json')
        )
    mock_file_data["G1234"] = mock_file_data["S1234"]

    def download(filename, graceid):
        filename = filename.split(",")[0]
        if filename in mock_file_data[graceid].keys():
            f_data = mock_file_data[graceid][filename]
        else:
            f_data = b"some mock content"
            mock_file_data[graceid][filename] = f_data
        return f_data

    mock_download = Mock()
    mock_download.side_effect = download

    monkeypatch.setattr("gwcelery.tasks.gracedb.download.run", mock_download)

    def get_element_from_log(fname):
        # this function retuns most recent info associated with the
        # fname from mock_log_data
        file_version = None
        file_index_in_log = len(mock_log_data)
        if fname is not None:
            fname = fname.split(",")[0]
        for i_compliment, mock_log_data_i_c in enumerate(
            reversed(mock_log_data)
        ):
            i = len(mock_log_data) - i_compliment - 1
            if mock_log_data_i_c['filename'] == fname:
                file_version = mock_log_data_i_c['file_version']
                file_index_in_log = i
                break
        return fname, file_version, file_index_in_log

    def upload(filecontents, filename, graceid, message, tags=()):
        _, current_file_version, _ = get_element_from_log(filename)
        if current_file_version is not None:
            file_version = current_file_version + 1
        else:
            file_version = 0

        mock_file_data[graceid][filename] = filecontents
        mock_log_data.append(
            {
                "comment": message,
                "filename": filename,
                "file_version": file_version,
                "tag_names": tags,
            }
        )

        return f"{filename},{file_version}"

    mock_upload = Mock()
    mock_upload.side_effect = upload
    monkeypatch.setattr("gwcelery.tasks.gracedb.upload._orig_run", mock_upload)

    def get_log(graceid):
        return mock_log_data

    mock_get_log = Mock()
    mock_get_log.side_effect = get_log

    monkeypatch.setattr("gwcelery.tasks.gracedb.get_log", mock_get_log)

    def create_tag(filename, tag, graceid):
        *_, file_N = get_element_from_log(filename)
        mock_log_data[file_N]["tag_names"] += [tag]

    mock_create_tag = Mock()
    mock_create_tag.side_effect = create_tag
    monkeypatch.setattr(
        "gwcelery.tasks.gracedb.create_tag.run", mock_create_tag
    )

    orchestrator.handle_superevent(alert)
    mock_upload_call_arg_list = mock_upload.call_args_list
    if alert_label == "ADVOK":
        list_of_expected_filename = ["initial-circular.txt"]
    else:
        list_of_expected_filename = [
            f"{event['pipeline']}.p_astro.json",
            "em_bright.json",
            "bayestar.multiorder.fits",
            "preliminary-circular.txt",
        ]
    if renormalise_rapidpe:
        list_of_expected_filename += [
            "RapidPE_RIFT.p_astro.json",
            "RapidPE_RIFT.p_astro.png",
        ]
    for call_arg, _ in mock_upload_call_arg_list:
        _, fname, _, *extra = call_arg
        if "Superevent cleaned up after first preliminary alert" not in extra:
            assert fname in list_of_expected_filename
            if fname in [
                "RapidPE_RIFT.p_astro.json",
                "RapidPE_RIFT.p_astro.png",
            ]:
                _, file_version, file_N = get_element_from_log(fname)
                assert file_version == 1 if renormalise_rapidpe else 0

    alert_type = (
        "preliminary" if alert_label == "GCN_PRELIM_SENT" else "initial"
    )

    len_of_extra = len(mock_alerts_send.call_args[0][0]) - 3
    expected_input_list = (
        mock_file_data["S1234"]["bayestar.multiorder.fits"],
        mock_file_data["S1234"]["em_bright.json"],
    )
    if rapidpe_pastro_available:
        if not renormalise_rapidpe:
            expected_input_list += (rapidpe_pastro_dict_initial,)
        else:
            expected_input_list += (expected_rapidpe_pastro_dict,)
    else:
        if not preferred_event_updated:
            expected_input_list += (pipeline_pastro_dict_initial,)
        else:
            expected_input_list += (pipeline_pastro_dict_updated,)
    if len_of_extra != 0:
        expected_input_list += (*([None] * (len_of_extra)),)
    if alert_type == "initial":
        mock_alerts_send.assert_called_once_with(
            expected_input_list,
            superevent,
            alert_type,
            raven_coinc=False,
            combined_skymap_filename=None,
        )
    else:
        mock_alerts_send.assert_called_once_with(
            expected_input_list,
            superevent,
            alert_type,
            raven_coinc=False,
        )


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
        return json.dumps({'HasNS': 0.0, 'HasRemnant': 0.0,
                           'HasMassGap': 0.0})
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
    'labels,superevent_id,ext_id,search',
    [[[], 'S1234', 'E1', 'AllSky'],
     [[], 'S1234', 'E1', 'SSM'],
     [['EM_COINC', 'RAVEN_ALERT'], 'S1234', 'E2', 'AllSky'],
     [['EM_COINC', 'RAVEN_ALERT', 'COMBINEDSKYMAP_READY'], 'S1234',
        'E3', 'AllSky'],
     [['EM_COINC', 'RAVEN_ALERT', 'COMBINEDSKYMAP_READY'], 'S2468',
        'E3', 'AllSky']])
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
                                         superevent_id, ext_id, search):
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
            'preferred_event_data': dict(search=search),
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

    voevent_kwargs = dict(
        ProbHasRemnant=0.0, ProbHasNS=0.0, internal=False, open_alert=True,
        skymap_filename='foobar.multiorder.fits,0', skymap_type='foobar',
        raven_coinc='RAVEN_ALERT' in labels, HasMassGap=0.0,
        Significant=1,  # this field is true for initial alerts
        combined_skymap_filename=(combined_skymap_filename if
                                  combined_skymap_needed else None)
    )
    download_calls = (
        superevent_initial_alert_download(
            'foobar.multiorder.fits,0',
            superevent_id
        ),
        superevent_initial_alert_download('em_bright.json,0', superevent_id)
    )
    public_tag_calls = [
        call('foobar.multiorder.fits,0', 'public', superevent_id),
        call('em_bright.json,0', 'public', superevent_id),
        call('S1234-Initial-1.xml', 'public', superevent_id)
    ]
    if search != "SSM":
        # For SSM alerts there is no p-astro information
        voevent_kwargs.update(
            dict(BBH=0.02, BNS=0.94, NSBH=0.03, Terrestrial=0.01)
        )
        download_calls += (
            superevent_initial_alert_download(
                'p_astro.json,0',
                superevent_id
            ),
        )
        public_tag_calls.append(
            call('p_astro.json,0', 'public', superevent_id),
        )
    mock_create_voevent.assert_called_once()
    assert mock_create_voevent.call_args.args == (superevent_id, 'initial')
    assert set(voevent_kwargs) == set(mock_create_voevent.call_args.kwargs)
    mock_alerts_send.assert_called_once_with(
        download_calls +
        ((6 if combined_skymap_needed else 4) * (None,)),
        alert['object'], 'initial', raven_coinc='RAVEN_ALERT' in labels,
        combined_skymap_filename=combined_skymap_filename)
    mock_gcn_send.assert_called_once_with('contents of S1234-Initial-1.xml')
    if 'RAVEN_ALERT' in labels:
        mock_create_emcoinc_circular.assert_called_once_with(superevent_id)
    else:
        mock_create_initial_circular.assert_called_once_with(superevent_id)
    mock_create_tag.assert_has_calls(public_tag_calls, any_order=True)
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
    return read_binary(data, filenames[filename])


@pytest.mark.parametrize(
    'alert_type,filename',
    [['new', ''], ['log', 'psd.xml.gz'],
     ['log', 'test.posterior_samples.hdf5']])
def test_handle_posterior_samples(monkeypatch, alert_type, filename):
    alert = {
        'alert_type': alert_type,
        'uid': 'S1234',
        'data': {'comment': 'samples', 'filename': filename},
        'object': {'preferred_event_data': {'extra_attributes': {
            'SingleInspiral': [{'ifo': 'H1'}, {'ifo': 'L1'}]
        }}}
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
                          for search in ['earlywarning', 'mdc',
                                         'allsky', 'ssm', 'vtinjection']
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
    if alert_search == 'vtinjection':
        mock_source_properties.assert_not_called()
        mock_p_astro.assert_not_called()
        mock_localize.assert_not_called()
    elif (alert_pipeline, alert_search) in pipelines_stock_p_astro:
        mock_p_astro.assert_called_once()
    else:
        mock_source_properties.assert_called_once()
        mock_localize.assert_called_once()
        mock_p_astro.assert_not_called()


@pytest.mark.parametrize("alert_search,alert_pipeline",
                         [('allsky', 'cwb'), ('bbh', 'cwb'),
                          ('allsky', 'olib'), ('allsky', 'mly')])
@patch('gwcelery.tasks.gracedb.download._orig_run', mock_download)
@patch('gwcelery.tasks.em_bright.source_properties.run')
def test_handle_burst_event_new_event(mock_source_properties,
                                      alert_search, alert_pipeline):
    alert = read_json(data, 'lvalert_event_creation.json')
    # FIXME lvalert_event_creation.json contains CBC events. Either burst
    # events should be added to it, or a burst event should be hardcoded here
    alert['object']['group'] = 'Burst'

    alert['object']['search'] = alert_search
    alert['object']['pipeline'] = alert_pipeline
    if alert_pipeline == 'cwb':
        alert['object']['extra_attributes'] = {
            'MultiBurst': {'snr': 10.0, 'mchirp': 0.0}}
    orchestrator.handle_burst_event(alert)
    if (alert_pipeline, alert_search) in [('cwb', 'bbh')]:
        mock_source_properties.assert_called_once()
    else:
        mock_source_properties.assert_not_called()


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
    'far_event,pe_pipeline,pe_required',
    [
        [(1e-30, {'search': 'AllSky'}), 'bilby', True],  # significant CBC
        [(1e-30, {'search': 'AllSky'}), 'rapidpe', True],  # another pipeline
        [(1, {'search': 'AllSky'}), 'bilby', False],  # low significance
        [None, 'bilby', False],  # No CBC triggers
        [(1e-30, {'search': 'wormhole'}), 'bilby', False],  # unknown search
        [(1e-30, {'search': 'MDC'}), 'bilby', False],  # MDC upload
        [(1e-30, {'search': 'offline'}), 'bilby', False],  # Offline upload
        [(1e-30, {'search': 'earlywarning'}), 'rapidpe', False],
        # rapidpe + earlywarning
    ]
)
def test_parameter_estimation(
    monkeypatch, far_event, pe_pipeline, pe_required
):
    superevent_id = 'S1234'
    mock_upload = Mock()
    mock_start_pe = Mock()
    monkeypatch.setattr('gwcelery.tasks.gracedb.upload.run', mock_upload)
    monkeypatch.setattr('gwcelery.tasks.inference.start_pe.run', mock_start_pe)

    orchestrator.parameter_estimation.delay(
        far_event=far_event, superevent_id=superevent_id,
        pe_pipeline=pe_pipeline)

    if pe_required:
        _, event = far_event
        mock_start_pe.assert_any_call(
            event, superevent_id, pe_pipeline)
        assert mock_start_pe.call_count == 1
    else:
        mock_upload.assert_called_once()
        assert mock_start_pe.call_count == 0


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
    'gevents_dict,preferred_event_id,answer_far,answer_event_id',
    [
        [
            {
                "G1": {"group": "cbc", "search": "allsky", "far": 1e-10}
            },
            "G1",
            1e-10,
            "G1"
        ],  # one CBC trigger
        [
            {
                "G1": {"group": "cbc", "search": "allsky", "far": 1},
                "G2": {"group": "cbc", "search": "allsky", "far": 1e-10},
                "G3": {"group": "cbc", "search": "allsky", "far": 1e-5}
            },
            "G2",
            1e-10,
            "G2"
        ],  # multiple CBC triggers
        [
            {
                "G1": {"group": "cbc", "search": "allsky", "far": 1e-10},
                "G2": {"group": "burst", "search": "bbh", "far": 1e-12}
            },
            "G1",
            1e-12,
            "G1"
        ],  # significant CBC + significant Burst-BBH
        [
            {
                "G1": {"group": "cbc", "search": "allsky", "far": 1e-10},
                "G2": {"group": "burst", "search": "allsky", "far": 1e-12}
            },
            "G1",
            1e-10,
            "G1"
        ],  # significant CBC + significant unmodeled Burst
        [
            {
                "G1": {"group": "burst", "search": "bbh", "far": 1e-12},
                "G2": {"group": "cbc", "search": "allsky", "far": 1},
                "G3": {"group": "cbc", "search": "allsky", "far": 1e-5},
                "G4": {"group": "cbc", "search": "allsky", "far": 1e-3},
            },
            "G1",
            1e-12,
            "G3"
        ],  # low-significance CBC + significant Burst-BBH
        [
            {
                "G1": {"group": "burst", "search": "allsky", "far": 1e-10},
            },
            "G1",
            None,
            None
        ],  # only a cWB trigger
    ]
)
def test_get_pe_far_and_event(
    monkeypatch, gevents_dict, preferred_event_id, answer_far, answer_event_id
):
    superevent = {}
    superevent["gw_events"] = list(gevents_dict.keys())
    superevent["preferred_event_data"] = gevents_dict[preferred_event_id]

    def _get_event(graceid):
        return gevents_dict[graceid]

    monkeypatch.setattr(
        'gwcelery.tasks.gracedb.get_event._orig_run',
        _get_event
    )

    ans = orchestrator._get_pe_far_and_event(superevent)
    if answer_event_id is None:
        assert ans is None
    else:
        far, event = ans
        assert far == answer_far
        assert event == gevents_dict[answer_event_id]


@pytest.mark.parametrize(
    "superevent_labels,block_by_labels",
    [
        [[superevents.FROZEN_LABEL], set()],
        [[superevents.FROZEN_LABEL], {"ADVOK", "ADVNO"}],
        [[superevents.SIGNIFICANT_LABEL, "ADVOK"], {"ADVOK", "ADVNO"}]
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


def test_cwb_bbh_notice_is_cbc(monkeypatch):
    """Test that cWB BBH notices list CBC as the group instead of Burst. Can be
    removed once cWB BBH starts uploading to the CBC group on gracedb.
    """
    labels = ['PASTRO_READY', 'EMBRIGHT_READY', 'SKYMAP_READY']

    def get_event(graceid):
        assert graceid == 'G1234'
        return {
            'group': 'Burst',
            'pipeline': 'CWB',
            'search': 'BBH',
            'graceid': 'G1234',
            'offline': False,
            'far': 1e-10,
            'instruments': 'H1,L1',
            'gpstime': 1234.,
            'extra_attributes': {},
            'labels': labels
        }

    def download(filename, graceid):
        if '.fits' in filename:
            return toy_3d_fits_filecontents
        elif filename == 'em_bright.json':
            return json.dumps({'HasNS': 0.0, 'HasRemnant': 0.0,
                               'HasMassGap': 0.0})
        elif filename == 'psd.xml.gz':
            return str(resources.files().joinpath('psd.xml.gz'))
        elif filename == 'S1234-1-Preliminary.xml':
            return b'fake VOEvent file contents'
        elif filename == 'cwb.p_astro.json':
            return json.dumps(
                dict(BNS=0.94, NSBH=0.03, BBH=0.02, Terrestrial=0.01))
        else:
            raise ValueError
    superevent_id = 'S1234'
    superevent_labels = [superevents.FROZEN_LABEL]
    alert = {
        'alert_type': 'label_added',
        'uid': superevent_id,
        'data': {'name': superevents.SIGNIFICANT_LABEL},
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
    create_initial_circular = Mock()
    create_emcoinc_circular = Mock()
    expose = Mock()
    rrt_channel_creation = Mock()
    check_high_profile = Mock()
    annotate_fits = Mock(return_value=None)
    # FIXME: remove logic of mocking return value
    # when live worker testing is enabled
    proceed_if_not_blocked_by = Mock(return_value=(
        ('skymap', 'skymap-filename'),
        (download('em_bright.json', 'G1234'), 'em-bright-filename'),
        (download('cwb.p_astro.json', 'G1234'), 'p-astro-filename'))
    )
    gcn_send = Mock()
    mock_upload = Mock()
    setup_dag = Mock()
    start_pe = Mock()
    create_voevent = Mock(return_value='S1234-1-Preliminary.xml')
    create_label = Mock()
    create_tag = Mock()
    select_preferred_event_task = Mock(return_value=get_event('G1234'))
    update_superevent_task = Mock()
    select_pipeline_preferred_event_task = Mock()
    select_pipeline_preferred_event_task.return_value = {
        'cwb': {'graceid': 'G5'}
    }
    add_pipeline_preferred_event_task = Mock()
    omegascan = Mock()
    check_vectors = Mock(return_value=get_event('G1234'))

    monkeypatch.setattr('gwcelery.tasks.gcn.send.run', gcn_send)
    monkeypatch.setattr('gwcelery.tasks.alerts._upload_notice.run',
                        mock_upload)

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
    mock_get_labels = Mock(return_value=labels)
    monkeypatch.setattr('gwcelery.tasks.gracedb.get_labels', mock_get_labels)
    # FIXME: should test gracedb.create_voevent instead
    monkeypatch.setattr('gwcelery.tasks.orchestrator._create_voevent.run',
                        create_voevent)
    mock_get_superevent = Mock(return_value={
        'preferred_event': 'G1234',
        'gw_events': ['G1234'],
        'preferred_event_data': get_event('G1234'),
        'category': "Production",
        't_0': 1214714162,
        'far': 1e-10,
        'labels': superevent_labels,
        'superevent_id': superevent_id,
        'em_type': None if superevent_id == 'S1234' else 'E1234',
        'links': {'self': 'http://fake-gracedb.ligo.org/api'}
    })

    monkeypatch.setattr('gwcelery.tasks.gracedb.get_superevent._orig_run',
                        mock_get_superevent)
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

    mock_stream = Mock()
    mock_stream.write = Mock()
    mock_stream.serialization_model = AvroBlobWrapper
    monkeypatch.setitem(app.conf, 'kafka_streams', {'scimma': mock_stream})

    # Run function under test
    orchestrator.handle_superevent(alert)

    mock_upload.assert_called_once()
    alert_blob, _, _ = mock_upload.call_args[0]
    assert alert_blob.content[0]['event']['group'] == 'CBC'
