import configparser
import glob
import json
import os
import subprocess
from contextlib import nullcontext as does_not_raise
from itertools import product
from unittest.mock import Mock

import pytest
from celery.exceptions import Ignore

from .. import app
from ..conf import playground as conf_playground
from ..conf import production as conf_production
from ..conf import test as conf_test
from ..tasks import condor, inference


def test_find_appropriate_cal_env(tmp_path):
    filepath = tmp_path / 'H_CalEnvs.txt'
    t1, cal1 = 1126259462, 'O1_calibration.txt'
    t2, cal2 = 1187008882, 'O2_calibration.txt'
    filepath.write_text('{t1} {cal1}\n{t2} {cal2}'.format(
            t1=t1, cal1=cal1, t2=t2, cal2=cal2))
    dir_name = str(tmp_path)
    trigtime_answer = [
        (t1 - 1, cal1), ((t1 + t2) / 2., cal1), (t2 + 1, cal2)]
    for trigtime, answer in trigtime_answer:
        assert os.path.basename(
            inference._find_appropriate_cal_env(trigtime, dir_name)) == answer


@pytest.mark.parametrize(
    'mc,q,answers',
    [[1., 1.,
      [('analysis', 'engine', 'lalinferencenest'),
       ('analysis', 'nparallel', '4'),
       ('analysis', 'roq', 'True'),
       ('paths', 'roq_b_matrix_directory',
        '/home/cbc/ROQ_data/IMRPhenomPv2/'),
       ('engine', 'approx', 'IMRPhenomPv2pseudoFourPN'),
       ('bayeswave', 'Nchain', '10'),
       ('bayeswave', 'Niter', '100000'),
       ('condor', 'bayeswave_request_memory', '16000'),
       ('condor', 'bayeswavepost_request_memory', '16000'),
       ('bayeswave', 'bw_srate', '4096')]],
     [10., 1.,
      [('condor', 'bayeswave_request_memory', '1000'),
       ('condor', 'bayeswavepost_request_memory', '4000')]],
     [1., 1. / 10.,
      [('analysis', 'engine', 'lalinferencenest'),
       ('analysis', 'nparallel', '4'),
       ('analysis', 'roq', 'True'),
       ('paths', 'roq_b_matrix_directory',
        '/home/cbc/ROQ_data/SEOBNRv4ROQ/'),
       ('engine', 'approx', 'SEOBNRv4_ROMpseudoFourPN')]],
     [1., 1. / 20.,
      [('analysis', 'engine', 'lalinferencemcmc'),
       ('analysis', 'nparallel', '10'),
       ('analysis', 'roq', 'False'),
       ('engine', 'approx', 'SEOBNRv4_ROMpseudoFourPN'),
       ('engine', 'neff', '300')]]]
)
def test_prepare_lalinference_ini(monkeypatch, mc, q, answers):

    def mock_calenv(trigtime, path):
        return 'path_to_calenv'

    monkeypatch.setattr(
        'gwcelery.tasks.inference._find_appropriate_cal_env',
        mock_calenv)
    monkeypatch.setitem(app.conf, 'gracedb_host', 'gracedb.ligo.org')

    m1 = mc * q**(-3. / 5.) * (1 + q)**(1. / 5.)
    m2 = q * m1
    event = {
        'gpstime': 1187008882,
        'graceid': 'G1234',
        'extra_attributes':
            {'SingleInspiral': [{'mass1': m1, 'mass2': m2, 'mchirp': mc}]}
    }
    config = configparser.ConfigParser()
    config.read_string(
        inference.prepare_lalinference_ini(event, 'S1234'))
    for section, key, answer in answers:
        assert config[section][key] == answer


def test_setup_dag_for_lalinference(monkeypatch, tmp_path):
    coinc = b'coinc'
    rundir = str(tmp_path)
    ini = 'ini'
    dag = 'lalinference dag'

    def _subprocess_run(cmd, **kwargs):
        assert cmd[2] == rundir

        path_to_coinc = cmd[4]
        assert os.path.exists(path_to_coinc)
        with open(path_to_coinc, 'rb') as f:
            assert f.read() == coinc

        path_to_ini = cmd[5]
        assert os.path.exists(path_to_ini)
        with open(path_to_ini, 'r') as f:
            assert f.read() == ini

        path_to_psd = cmd[7]
        assert os.path.exists(path_to_psd)
        with open(path_to_psd, 'rb') as f:
            assert f.read() == coinc

        with open(os.path.join(rundir, 'multidag.dag'), 'w') as f:
            f.write(dag)

    upload = Mock()
    monkeypatch.setattr('subprocess.run', _subprocess_run)
    monkeypatch.setattr(
        'gwcelery.tasks.inference.prepare_lalinference_ini',
        Mock(return_value=ini))
    monkeypatch.setattr('gwcelery.tasks.gracedb.upload.run', upload)

    path_to_dag = inference._setup_dag_for_lalinference(
        coinc, rundir, {}, 'S1234')

    assert os.path.exists(path_to_dag)
    with open(path_to_dag, 'r') as f:
        assert f.read() == dag
    upload.assert_called_once()


@pytest.mark.parametrize(
    "host,mode,mc",
    product(
        ['gracedb-playground.ligo.org', 'gracedb.ligo.org'],
        ['production', 'fast_test'],
        [30, 20, 15, 10, 3, 2, 1, 0.1]
    )
)
def test_setup_dag_for_bilby(monkeypatch, tmp_path, host, mode, mc):
    psd = b'psd'
    bayestar = b'bayestar'
    rundir = str(tmp_path)
    event = {'gpstime': 1187008882, 'graceid': 'G1234',
             'extra_attributes': {'CoincInspiral': {'mchirp': mc}}}
    sid = 'S1234'
    ini = 'bilby configuration ini file'
    dag = 'bilby dag'
    monkeypatch.setitem(app.conf, 'gracedb_host', host)

    def _subprocess_run(cmd, **kwargs):
        if host != 'gracedb.ligo.org':
            assert 'o3replay' in cmd

        assert cmd[2] == os.path.join(
            app.conf['pe_results_path'], sid, 'bilby', mode)

        assert cmd[4] == rundir

        path_to_json = cmd[6]
        assert os.path.exists(path_to_json)
        with open(path_to_json, 'r') as f:
            assert event == json.load(f)

        path_to_psd = cmd[8]
        assert os.path.exists(path_to_psd)
        with open(path_to_psd, 'rb') as f:
            assert f.read() == psd

        path_to_bayestar = cmd[10]
        assert os.path.exists(path_to_bayestar)
        with open(path_to_bayestar, 'rb') as f:
            assert f.read() == bayestar

        if mode == "fast_test":
            assert "FastTest" in cmd

        path_to_settings = cmd[12]
        assert os.path.exists(path_to_settings)
        ans = {
            'summarypages_arguments': {'gracedb': event['graceid'],
                                       'no_ligo_skymap': True},
            'queue': 'Online_PE',
            'accounting_user': 'soichiro.morisaki',
            'tukey_roll_off': 1.0
        }
        if host != 'gracedb.ligo.org':
            ans['queue'] = 'Online_PE_MDC'
        else:
            ans['accounting'] = 'ligo.prod.o4.cbc.pe.bilby'
        if mode == 'production':
            ans.update(
                {'sampler_kwargs': {'naccept': 60, 'nlive': 500,
                                    'npool': 24, 'sample': 'acceptance-walk'},
                 'n_parallel': 3,
                 'request_cpus': 24,
                 'spline_calibration_nodes': 10,
                 'request_memory_generation': 8.0}
            )
            if 0.6 < mc < 1.465:
                assert 'lowspin_phenomd_fhigh1024_roq' in cmd
                ans['sampler_kwargs']['naccept'] = 10
            elif mc < 2.243:
                assert 'phenompv2_bns_roq' in cmd
            elif mc < 12:
                assert 'low_q_phenompv2_roq' in cmd
            else:
                assert 'phenomxphm_roq' in cmd
                if mc > 16:
                    ans['request_memory_generation'] = 36.0
                else:
                    ans['request_memory_generation'] = 50.0
                if mc > 25:
                    ans['request_memory'] = 16.0
                elif mc > 16:
                    ans['request_memory'] = 24.0
                else:
                    ans['request_memory'] = 36.0
        elif mode == 'fast_test' and mc < 3.9:
            ans['request_memory_generation'] = 8.0
        with open(path_to_settings, 'r') as f:
            assert json.load(f) == ans

        with open(os.path.join(rundir, 'bilby_config.ini'), 'w') as f:
            f.write(ini)
        dir = os.path.join(rundir, 'submit')
        os.mkdir(dir)
        with open(os.path.join(dir, 'dag_bilby.submit'), 'w') as f:
            f.write(dag)

    def _upload(filecontents, filename, graceid, message, tags):
        assert filecontents == ini

    upload = Mock(side_effect=_upload)
    monkeypatch.setattr('subprocess.run', _subprocess_run)
    monkeypatch.setattr('gwcelery.tasks.gracedb.upload.run', upload)

    if mc < 0.6:
        with pytest.raises(ValueError):
            inference._setup_dag_for_bilby(psd, rundir, event, sid, mode)
    else:
        path_to_dag = inference._setup_dag_for_bilby(
            (psd, bayestar), rundir, event, sid, mode
        )

        assert os.path.exists(path_to_dag)
        with open(path_to_dag, 'r') as f:
            assert f.read() == dag
        upload.assert_called_once()


def test_setup_dag_for_bilby_unknown_mode(tmp_path):
    with pytest.raises(ValueError):
        inference._setup_dag_for_bilby(
            b'psd', str(tmp_path),
            {'gpstime': 1187008882, 'graceid': 'G1234',
             'extra_attributes': {'CoincInspiral': {'mchirp': 1}}},
            'S1234', 'unknown'
        )


@pytest.mark.parametrize(
    'returncode,expectation',
    [
        (0, does_not_raise()),
        (1, pytest.raises(subprocess.CalledProcessError)),
        (inference._RAPIDPE_NO_GSTLAL_TRIGGER_EXIT_CODE,
         pytest.raises(Ignore)),
    ],
)
@pytest.mark.parametrize(
    'host',
    [
         'gracedb.ligo.org',
         'gracedb-playground.ligo.org',
         'gracedb-test.ligo.org',
    ]
)
def test_setup_dag_for_rapidpe(
        returncode, expectation, host, monkeypatch, tmp_path):
    rundir = str(tmp_path)
    dag_filename = 'event_all_iterations.dag'
    dag_content = 'rapidpe dag'
    event = {'superevent_id': 'S1234',
             'extra_attributes': {'CoincInspiral': {'snr': 10}}}

    def _subprocess_run(args, *, capture_output=False, check=False):
        # Simulate failed subprocess
        if returncode != 0:
            raise subprocess.CalledProcessError(
                returncode, args,
                output=b"STDOUT", stderr=b"STDERR",
            )

        path_to_ini = args[1]
        assert os.path.exists(path_to_ini)
        with open(os.path.join(rundir, dag_filename), 'w') as f:
            f.write(dag_content)

    upload = Mock()
    monkeypatch.setattr('subprocess.run', _subprocess_run)
    monkeypatch.setattr('gwcelery.tasks.gracedb.upload.run', upload)
    monkeypatch.setitem(app.conf, 'gracedb_host', host)
    with expectation:
        path_to_dag = inference._setup_dag_for_rapidpe(rundir, 'S1234', event)

        if returncode == 0:
            # The DAG should be generated
            with open(path_to_dag, 'r') as f:
                assert f.read() == dag_content
            # The ini file should be uploaded
            upload.assert_called_once()
        else:
            # The ini file and error log should be uploaded
            assert upload.call_count == 2


@pytest.mark.parametrize('pipeline', ['lalinference', 'bilby', 'rapidpe'])
def test_setup_dag_for_failure(monkeypatch, tmp_path, pipeline):
    upload = Mock()
    monkeypatch.setattr('gwcelery.tasks.gracedb.upload.run', upload)
    monkeypatch.setattr('subprocess.run',
                        Mock(side_effect=subprocess.CalledProcessError(
                            1, ['cmd'], output=b'out', stderr=b'err')))
    monkeypatch.setattr(
        'gwcelery.tasks.inference._find_appropriate_cal_env',
        Mock(return_value='./cal_file.txt'))

    rundir = str(tmp_path)
    event = {
        'gpstime': 1187008882,
        'graceid': 'G1234',
        'extra_attributes': {'SingleInspiral':
                             [{'mass1': 1.4, 'mass2': 1.4, 'mchirp': 1.2}],
                             'CoincInspiral': {'mchirp': 1, 'snr': 10}}
    }
    with pytest.raises(subprocess.CalledProcessError):
        if pipeline == 'lalinference':
            inference._setup_dag_for_lalinference(
                b'coinc', rundir, event, 'S1234')
        elif pipeline == 'bilby':
            inference._setup_dag_for_bilby(
                (b'psd', b'bayestar'), rundir, event, 'S1234', 'production')
        elif pipeline == 'rapidpe':
            inference._setup_dag_for_rapidpe(rundir, 'S1234', event)
    if pipeline == 'bilby':
        assert upload.call_count == 1
    else:
        # For pipelines except for bilby, an ini file is uploaded before dag
        # generation, and hence the call count is 2.
        assert upload.call_count == 2


@pytest.mark.parametrize(
    'pipeline', ['lalinference', 'bilby', 'rapidpe', 'my_awesome_pipeline'])
def test_dag_prepare_task(monkeypatch, pipeline):
    sid = 'S1234'
    coinc = b'coinc'
    bayestar = b'bayestar'
    event = {
            'gpstime': 1187008882,
            'graceid': 'G1234',
            'extra_attributes': {
                'CoincInspiral': {'snr': 10}
            }
    }
    rundir = 'rundir'
    path_to_dag = os.path.join(rundir, 'parameter_estimation.dag')
    kwargs = {'bilby_mode': 'production'}

    def mock_download(filename, gid):
        if filename == 'coinc.xml':
            return coinc
        elif filename == 'bayestar.multiorder.fits':
            return bayestar

    def _setup_dag_for_lalinference(c, r, e, s):
        assert (c == coinc and r == rundir and e == event and s == sid)
        return path_to_dag

    def _setup_dag_for_bilby(c_b, r, e, s, m):
        c, b = c_b
        assert c == coinc and b == bayestar and r == rundir and e == event \
            and s == sid and m == kwargs['bilby_mode']
        return path_to_dag

    def _setup_dag_for_rapidpe(r, s, e):
        assert r == rundir and s == sid and e == event
        return path_to_dag

    def _subprocess_run(cmd, **kwargs):
        expected_cmd = ['condor_submit_dag']

        if pipeline == 'rapidpe':
            expected_cmd += ['-include_env',
                             ','.join(inference.RAPIDPE_GETENV)]

        expected_cmd += ['-no_submit', path_to_dag]

        assert cmd == expected_cmd

    mock_setup_dag_for_lalinference = \
        Mock(side_effect=_setup_dag_for_lalinference)
    mock_setup_dag_for_bilby = Mock(side_effect=_setup_dag_for_bilby)
    mock_setup_dag_for_rapidpe = Mock(side_effect=_setup_dag_for_rapidpe)
    monkeypatch.setattr('gwcelery.tasks.gracedb.download.run', mock_download)
    monkeypatch.setattr('gwcelery.tasks.gracedb.get_event.run',
                        Mock(return_value=event))
    monkeypatch.setattr(
        'gwcelery.tasks.inference._setup_dag_for_lalinference.run',
        mock_setup_dag_for_lalinference)
    monkeypatch.setattr('gwcelery.tasks.inference._setup_dag_for_bilby.run',
                        mock_setup_dag_for_bilby)
    monkeypatch.setattr('gwcelery.tasks.inference._setup_dag_for_rapidpe.run',
                        mock_setup_dag_for_rapidpe)
    monkeypatch.setattr('subprocess.run', _subprocess_run)
    if pipeline in ['lalinference', 'bilby', 'rapidpe']:
        inference.dag_prepare_task(
            rundir, event, sid, pipeline, **kwargs).delay()
        if pipeline == 'lalinference':
            mock_setup_dag_for_lalinference.assert_called_once()
        elif pipeline == 'bilby':
            mock_setup_dag_for_bilby.assert_called_once()
        elif pipeline == 'rapidpe':
            mock_setup_dag_for_rapidpe.assert_called_once()
    else:
        with pytest.raises(NotImplementedError):
            inference.dag_prepare_task(
                rundir, event, sid, pipeline).delay()


@pytest.mark.parametrize(
    'exc,host',
    product(
        [condor.JobAborted(1, 'test'),
         condor.JobFailed(1, 'test'),
         condor.JobRunning({'Cluster': 1234})],
        ['gracedb-playground.ligo.org', 'gracedb.ligo.org']
    )
)
def test_job_error_notification(monkeypatch, tmp_path, exc, host):
    monkeypatch.setitem(app.conf, 'gracedb_host', host)
    filenames = ['pe.log', 'pe.err', 'pe.out']
    for filename in filenames:
        with open(str(tmp_path / filename), 'w') as f:
            f.write('test')
    upload = Mock()
    cleanup = Mock()
    monkeypatch.setattr('gwcelery.tasks.gracedb.upload.run', upload)
    monkeypatch.setattr(
        'gwcelery.tasks.inference._clean_up_bilby.run', cleanup
    )
    monkeypatch.setattr('subprocess.run', Mock())
    inference.job_error_notification(
        None, exc, 'test', 'S1234', str(tmp_path), 'bilby')
    assert upload.call_count == len(filenames) + 1
    if host != 'gracedb.ligo.org':
        cleanup.assert_called_once()


@pytest.mark.parametrize(
    'pipeline,host',
    product(
        ['lalinference', 'bilby_production', 'bilby_fast_test', 'rapidpe',
         'my_awesome_pipeline'],
        ['gracedb-playground.ligo.org', 'gracedb.ligo.org']
    )
)
def test_dag_finished(monkeypatch, tmp_path, pipeline, host):
    monkeypatch.setitem(app.conf, 'gracedb_host', host)
    sid = 'S1234'
    rundir = str(tmp_path / 'rundir')
    resultdir = str(tmp_path / 'rundir/result')
    sampledir = str(tmp_path / 'rundir/final_result')
    datadir = str(tmp_path / 'rundir/data')
    pe_results_path = str(tmp_path / 'public_html/online_pe')
    monkeypatch.setitem(app.conf, 'pe_results_path', pe_results_path)
    pe_results_path = os.path.join(pe_results_path, sid, pipeline)
    os.makedirs(rundir)
    os.makedirs(resultdir)
    os.makedirs(sampledir)
    os.makedirs(pe_results_path)
    os.makedirs(datadir)

    upload = Mock()
    create_label = Mock()
    monkeypatch.setattr('gwcelery.tasks.gracedb.upload.run', upload)
    monkeypatch.setattr('gwcelery.tasks.gracedb.create_label.run',
                        create_label)

    if pipeline in [
        'lalinference', 'bilby_production', 'bilby_fast_test', 'rapidpe'
    ]:
        kwargs = {}
        if pipeline == 'lalinference':
            paths = [os.path.join(rundir,
                                  'lalinference_1187008756-1187008882.dag'),
                     os.path.join(rundir, 'glitch_median_PSD_forLI_H1.dat'),
                     os.path.join(rundir, 'glitch_median_PSD_forLI_L1.dat'),
                     os.path.join(rundir, 'glitch_median_PSD_forLI_V1.dat'),
                     os.path.join(rundir, 'posterior_samples.hdf5'),
                     os.path.join(pe_results_path, 'extrinsic.png'),
                     os.path.join(pe_results_path, 'intrinsic.png'),
                     os.path.join(pe_results_path, 'posplots.html')]
        elif pipeline in ['bilby_production', 'bilby_fast_test']:
            kwargs['bilby_mode'] = pipeline[6:]
            pipeline = 'bilby'

            input_sample = os.path.join(sampledir, "test_result.hdf5")
            with open(input_sample, 'wb') as f:
                f.write(b'result')

            samplename = f'Bilby.{kwargs["bilby_mode"]}.posterior_samples.hdf5'
            if kwargs["bilby_mode"] == "production":
                samplename = 'Bilby.posterior_samples.hdf5'
            paths = [os.path.join(sampledir, samplename)]

            def _subprocess_run(cmd):
                assert os.path.exists(cmd[1])
                assert cmd[3] == paths[0]

            monkeypatch.setattr(
                'subprocess.run', Mock(side_effect=_subprocess_run))

            path_to_bilby_config = os.path.join(rundir, "bilby_complete.ini")
            with open(path_to_bilby_config, "w") as f:
                f.write(
                    "spline-calibration-envelope-dict="
                    "{'H1': 'H1_cal.txt', 'L1': 'L1_cal.txt'}\n"
                    "psd-dict={'H1': 'H1_psd.txt', 'L1': 'L1_psd.txt'}\n"
                    "waveform-approximant=IMRPhenomPv2\n"
                    "minimum-frequency=20\n"
                    "reference-frequency=100\n"
                    "webdir=webdir"
                )

            def mock_check_output(args, **kwargs):
                ans = [
                    "summarypages", "--webdir", "webdir/pesummary",
                    "--samples", os.path.join(sampledir, 'test_result.hdf5'),
                    "--gw", "--no_ligo_skymap", "--multi_process", "6",
                    "--config", path_to_bilby_config, "--psd", "H1:H1_psd.txt",
                    "L1:L1_psd.txt", "--calibration", "H1:H1_cal.txt",
                    "L1:L1_cal.txt", "--approximant", "IMRPhenomPv2",
                    "--f_low", "20", "--f_ref", "100", "--label", "online"
                ]
                if host != 'gracedb.ligo.org':
                    ans += ["--disable_interactive", "--disable_expert"]
                else:
                    ans += [
                        "--redshift_method", "exact", "--evolve_spins_forwards"
                    ]
                assert args == ans

            monkeypatch.setattr(
                'gwcelery.tasks.condor.check_output.run', mock_check_output)

            with open(
                os.path.join(datadir, "test_generation_roq_weights.hdf5"),
                "wb"
            ) as f:
                f.write(b'data')
            with open(
                os.path.join(datadir, "test_generation_data_dump.pickle"),
                "wb"
            ) as f:
                f.write(b'data')
        elif pipeline == "rapidpe":
            summary_path = os.path.join(rundir, "summary")
            os.makedirs(summary_path, exist_ok=True)
            paths = [
                os.path.join(summary_path, "p_astro.json"),
            ]

        else:
            paths = []
        for path in paths:
            with open(path, 'wb') as f:
                f.write(b'result')

        if pipeline == 'rapidpe':
            for event_pipeline in ['gstlal', 'pycbc']:
                kwargs['event_pipeline'] = event_pipeline
                inference.dag_finished(rundir, sid, pipeline, **kwargs)
        else:
            inference.dag_finished(rundir, sid, pipeline, **kwargs)

        if pipeline == "rapidpe":
            # +1 corresponds to summary page link
            assert upload.call_count == 2 * (len(paths) + 1)
        elif pipeline == 'bilby':
            # +1 corresponds to pesummary link
            assert upload.call_count == len(paths) + 1
            n = len(glob.glob(os.path.join(datadir, "*")))
            if host == "gracedb.ligo.org":
                assert n == 2
            else:
                assert n == 0
        else:
            assert upload.call_count == len(paths)

        if pipeline == 'bilby':
            create_label.assert_called_once()
        else:
            create_label.assert_not_called()

    else:
        with pytest.raises(NotImplementedError):
            inference.dag_finished(rundir, sid, pipeline)


@pytest.mark.parametrize(
    'pipeline', ['lalinference', 'bilby', 'rapidpe'])
def test_start_pe(monkeypatch, tmp_path, pipeline):
    path_to_sub = 'pe.dag.condor.sub'

    @app.task
    def mock_task():
        return path_to_sub

    def mock_condor_submit(path):
        assert path == path_to_sub

    if pipeline == 'rapidpe':
        event_pipeline_info = {
            'gstlal': {'sid': 'S1234', 'graceid': 'G1234'},
            'pycbc': {'sid': 'S1235', 'graceid': 'G1235'}}
        for event_pipeline in event_pipeline_info:
            dag_prepare_task = Mock(return_value=mock_task.s())

            submit_rapidpe = Mock(side_effect=mock_condor_submit)
            dag_finished = Mock()
            monkeypatch.setattr('gwcelery.tasks.gracedb.upload.run', Mock())
            monkeypatch.setattr('platformdirs.user_cache_dir',
                                Mock(return_value=str(tmp_path)))
            monkeypatch.setattr('gwcelery.tasks.inference.dag_prepare_task',
                                dag_prepare_task)
            monkeypatch.setattr(
                    'gwcelery.tasks.inference.submit_rapidpe.run',
                    submit_rapidpe)
            monkeypatch.setattr(
                    'gwcelery.tasks.inference.dag_finished.run',
                    dag_finished)

            inference.start_pe({
                'graceid': event_pipeline_info[event_pipeline]['graceid'],
                'pipeline': event_pipeline,
                'extra_attributes': {'CoincInspiral': {'snr': 10}}},
                event_pipeline_info[event_pipeline]['sid'], pipeline)
            dag_prepare_task.assert_called_once()
            submit_rapidpe.assert_called_once()
            dag_finished.assert_called_once()

    else:
        dag_prepare_task = Mock(return_value=mock_task.si(),
                                name="dag_prepare_task")

        condor_submit = Mock(side_effect=mock_condor_submit,
                             name="condor_submit")
        dag_finished = Mock(name="dag_finished")
        monkeypatch.setattr('gwcelery.tasks.gracedb.upload.run', Mock())
        monkeypatch.setattr('platformdirs.user_cache_dir',
                            Mock(return_value=str(tmp_path)))
        monkeypatch.setattr('gwcelery.tasks.inference.dag_prepare_task',
                            dag_prepare_task)
        monkeypatch.setattr('gwcelery.tasks.condor.submit.run', condor_submit)
        monkeypatch.setattr(
                'gwcelery.tasks.inference.dag_finished.run',
                dag_finished)

        inference.start_pe({'graceid': 'G1234'}, 'S1234', pipeline)
        dag_prepare_task.assert_called_once()
        condor_submit.assert_called_once()
        dag_finished.assert_called_once()


@pytest.mark.parametrize(
    'host', ["gracedb-playground.ligo.org", "gracedb.ligo.org"])
def test_bbh_rate_limit(monkeypatch, tmp_path, host):
    monkeypatch.setitem(app.conf, 'gracedb_host', host)
    monkeypatch.setattr(
        'platformdirs.user_cache_dir', Mock(return_value=str(tmp_path)))
    event = {'gpstime': 1187008882, 'graceid': 'G1234',
             'extra_attributes': {'CoincInspiral': {'mchirp': 20}}}
    for i in range(6):
        rundir = os.path.join(str(tmp_path), f"{i}/production")
        os.makedirs(rundir)
        os.mkdir(os.path.join(rundir, "data"))
        with open(
            os.path.join(rundir, "data/pe_generation_data_dump.pickle"), "wb"
        ) as f:
            f.write(b"test")
        with open(os.path.join(rundir, "event.json"), "w") as f:
            json.dump(event, f, indent=2)

    path_to_sub = 'pe.dag.condor.sub'

    @app.task
    def mock_task():
        return path_to_sub

    dag_prepare_task = Mock(return_value=mock_task.s())

    def mock_condor_submit(path):
        assert path == path_to_sub

    condor_submit = Mock(side_effect=mock_condor_submit)
    dag_finished = Mock()
    monkeypatch.setattr('gwcelery.tasks.gracedb.upload.run', Mock())
    monkeypatch.setattr('platformdirs.user_cache_dir',
                        Mock(return_value=str(tmp_path)))
    monkeypatch.setattr('gwcelery.tasks.inference.dag_prepare_task',
                        dag_prepare_task)
    monkeypatch.setattr('gwcelery.tasks.condor.submit.run', condor_submit)
    monkeypatch.setattr('gwcelery.tasks.inference.dag_finished.run',
                        dag_finished)

    inference.start_pe(event, 'S1234', 'bilby')
    if host == "gracedb-playground.ligo.org":
        dag_prepare_task.assert_not_called()
        condor_submit.assert_not_called()
        dag_finished.assert_not_called()
    else:
        dag_prepare_task.assert_called_once()
        condor_submit.assert_called_once()
        dag_finished.assert_called_once()


_rapidpe_accounting_prod = 'ligo.prod.o4.cbc.pe.lalinferencerapid'
_rapidpe_accounting_dev = 'ligo.dev.o4.cbc.pe.lalinferencerapid'


@pytest.mark.parametrize(
    'conf',
    [
        conf_production, conf_playground,
        conf_test,
    ]
)
def test_accounting_group_rapidpe(monkeypatch, tmp_path, conf):
    rundir = tmp_path
    event = {'extra_attributes': {'CoincInspiral': {'snr': 10}}}
    superevent_id = 'S1234'
    pe_pipeline = 'rapidpe'
    config_path = os.path.join(rundir, 'rapidpe.ini')

    # You might want to use pytest.fixture to pass different conf
    # based on the host. For now, the gracedb_host and rapidpe_settings
    # are mapped to corresponing item in app.conf

    # Set the GraceDB host appropriately
    monkeypatch.setitem(app.conf, 'gracedb_host', conf.gracedb_host)
    # Set app.conf based on gracedb_host
    monkeypatch.setattr(
            app.conf, 'rapidpe_settings', conf.rapidpe_settings)

    # Confirm the config file doesn't exist yet
    assert not os.path.isfile(config_path)

    # Generates the config file
    inference.dag_prepare_task(rundir, event, superevent_id, pe_pipeline).run()

    # Confirm the config file now exists
    assert os.path.isfile(config_path)

    # Parse the config file to confirm the correct accounting group is set
    config = configparser.ConfigParser()
    config.read(config_path)

    accounting_group = conf.rapidpe_settings['accounting_group']
    assert config.get('General', 'accounting_group') == accounting_group


def test_environment_rapidpe(monkeypatch, tmp_path):
    rundir = tmp_path
    gracedb_host = 'gracedb-dev.ligo.org'
    superevent_id = 'S1234'
    pe_pipeline = 'rapidpe'
    config_path = os.path.join(rundir, 'rapidpe.ini')
    event = {
        'superevent_id': superevent_id,
        'extra_attributes': {'CoincInspiral': {'snr': 10}},
    }

    # Confirm the config file doesn't exist yet
    assert not os.path.isfile(config_path)

    # Set the GraceDB host appropriately
    monkeypatch.setitem(app.conf, 'gracedb_host', gracedb_host)

    # Generates the config file
    inference.dag_prepare_task(rundir, event, superevent_id, pe_pipeline).run()

    # Confirm the config file now exists
    assert os.path.isfile(config_path)

    # Parse the config file to confirm the correct accounting group is set
    config = configparser.ConfigParser()
    config.read(config_path)

    # Confirm 'getenv' and 'environment' values were set as expected
    assert (json.loads(config.get('General', 'getenv'))
            == inference.RAPIDPE_GETENV)
    assert (json.loads(config.get('General', 'environment'))
            == inference.RAPIDPE_ENVIRONMENT)
