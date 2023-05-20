"""Source Parameter Estimation with LALInference, Bilby, and RapidPE."""
from distutils.spawn import find_executable
from distutils.dir_util import mkpath
import glob
import json
import os
import subprocess
import urllib

from bilby_pipe.utils import convert_string_to_dict
from bilby_pipe.bilbyargparser import BilbyConfigFileParser
from celery import group
from gwdatafind import find_urls
import numpy as np

from .. import app
from ..jinja import env
from . import condor
from . import gracedb


def _data_exists(gpstime, frametype_dict):
    """Check whether data at input GPS time can be found with gwdatafind and
    return true if it is found.
    """
    return min(
        len(
            find_urls(ifo[0], frametype_dict[ifo], gpstime, gpstime + 1)
        ) for ifo in frametype_dict.keys()
    ) > 0


class NotEnoughData(Exception):
    """Raised if found data is not enough due to the latency of data
    transfer
    """


@app.task(bind=True, autoretry_for=(NotEnoughData, ),
          retry_backoff=True, retry_backoff_max=600, max_retries=16)
def query_data(self, trigtime):
    """Continues to query data until it is found with gwdatafind and return
    frametypes for the data. This query will be retried for ~4600 seconds,
    which is longer than the expected latency of transfer of high-latency data
    with length of ~4000 seconds.
    """
    end = trigtime + 2
    if _data_exists(end, app.conf['low_latency_frame_types']):
        return app.conf['low_latency_frame_types']
    elif _data_exists(end, app.conf['high_latency_frame_types']):
        return app.conf['high_latency_frame_types']
    else:
        raise NotEnoughData


@app.task(ignore_result=True, shared=False)
def upload_no_frame_files(request, exc, traceback, superevent_id):
    """Upload notification when no frame files are found.

    Parameters
    ----------
    request : Context (placeholder)
        Task request variables
    exc : Exception
        Exception raised by query_data
    traceback : str (placeholder)
        Traceback message from a task
    superevent_id : str
        The GraceDB ID of a target superevent

    """
    if isinstance(exc, NotEnoughData):
        gracedb.upload.delay(
            filecontents=None, filename=None,
            graceid=superevent_id,
            message='Frame files have not been found.',
            tags='pe'
        )


def _find_appropriate_cal_env(trigtime, dir_name):
    """Return the path to the calibration uncertainties estimated at the time
    before and closest to the trigger time. If there are no calibration
    uncertainties estimated before the trigger time, return the oldest one. The
    gpstimes at which the calibration uncertainties were estimated and the
    names of the files containing the uncertaintes are saved in
    [HLV]_CalEnvs.txt.

    Parameters
    ----------
    trigtime : float
        The trigger time of a target event
    dir_name : str
        The path to the directory where files containing calibration
        uncertainties exist

    Return
    ------
    path : str
        The path to the calibration uncertainties appropriate for a target
        event

    """
    filename, = glob.glob(os.path.join(dir_name, '[HLV]_CalEnvs.txt'))
    calibration_index = np.atleast_1d(
        np.recfromtxt(filename, names=['gpstime', 'filename'])
    )
    gpstimes = calibration_index['gpstime']
    candidate_gpstimes = gpstimes < trigtime
    if np.any(candidate_gpstimes):
        idx = np.argmax(gpstimes * candidate_gpstimes)
        appropriate_cal = calibration_index['filename'][idx]
    else:
        appropriate_cal = calibration_index['filename'][np.argmin(gpstimes)]
    return os.path.join(dir_name, appropriate_cal.decode('utf-8'))


def prepare_lalinference_ini(frametype_dict, event, superevent_id):
    """Determine LALInference configurations and return ini file content

    Parameters
    ----------
    frametype_dict : dict
        Dictionary whose keys are ifos and values are frame types
    event : dict
        The json contents of a target G event retrieved from
        gracedb.get_event(), whose mass and spin information are used to
        determine analysis settings.
    superevent_id : str
        The GraceDB ID of a target superevent

    Returns
    -------
    ini_contents : str

    """
    # Get template of .ini file
    ini_template = env.get_template('lalinference.jinja2')

    # fill out the ini template and return the resultant content
    singleinspiraltable = event['extra_attributes']['SingleInspiral']
    trigtime = event['gpstime']
    executables = {'datafind': 'gw_data_find',
                   'mergeNSscript': 'lalinference_nest2pos',
                   'mergeMCMCscript': 'cbcBayesMCMC2pos',
                   'combinePTMCMCh5script': 'cbcBayesCombinePTMCMCh5s',
                   'resultspage': 'cbcBayesPostProc',
                   'segfind': 'ligolw_segment_query',
                   'ligolw_print': 'ligolw_print',
                   'coherencetest': 'lalinference_coherence_test',
                   'lalinferencenest': 'lalinference_nest',
                   'lalinferencemcmc': 'lalinference_mcmc',
                   'lalinferencebambi': 'lalinference_bambi',
                   'lalinferencedatadump': 'lalinference_datadump',
                   'ligo-skymap-from-samples': 'true',
                   'ligo-skymap-plot': 'true',
                   'processareas': 'process_areas',
                   'computeroqweights': 'lalinference_compute_roq_weights',
                   'mpiwrapper': 'lalinference_mpi_wrapper',
                   'gracedb': 'gracedb',
                   'ppanalysis': 'cbcBayesPPAnalysis',
                   'pos_to_sim_inspiral': 'cbcBayesPosToSimInspiral',
                   'bayeswave': 'BayesWave',
                   'bayeswavepost': 'BayesWavePost'}
    ini_settings = {
        'gracedb_host': app.conf['gracedb_host'],
        'types': frametype_dict,
        'channels': app.conf['strain_channel_names'],
        'state_vector_channels': app.conf['state_vector_channel_names'],
        'webdir': os.path.join(
            app.conf['pe_results_path'], superevent_id, 'lalinference'
        ),
        'paths': [{'name': name, 'path': find_executable(executable)}
                  for name, executable in executables.items()],
        'h1_calibration': _find_appropriate_cal_env(
            trigtime,
            '/home/cbc/pe/O3/calibrationenvelopes/LIGO_Hanford'
        ),
        'l1_calibration': _find_appropriate_cal_env(
            trigtime,
            '/home/cbc/pe/O3/calibrationenvelopes/LIGO_Livingston'
        ),
        'v1_calibration': _find_appropriate_cal_env(
            trigtime,
            '/home/cbc/pe/O3/calibrationenvelopes/Virgo'
        ),
        'mc': min([sngl['mchirp'] for sngl in singleinspiraltable]),
        'q': min([sngl['mass2'] / sngl['mass1']
                  for sngl in singleinspiraltable]),
        'mpirun': find_executable('mpirun')
    }
    return ini_template.render(ini_settings)


@app.task(shared=False)
def _setup_dag_for_lalinference(coinc, rundir, event, superevent_id,
                                frametype_dict):
    """Create DAG for a lalinference run and return the path to DAG.

    Parameters
    ----------
    coinc : byte contents
        Byte contents of ``coinc.xml``. The PSD is expected to be embedded.
    rundir : str
        The path to a run directory where the DAG file is created.
    event : dict
        The json contents of a target G event retrieved from
        gracedb.get_event(), whose mass and spin information are used to
        determine analysis settings.
    superevent_id : str
        The GraceDB ID of a target superevent
    frametype_dict : dict
        Dictionary whose keys are ifos and values are frame types

    Returns
    -------
    path_to_dag : str
        The path to the .dag file

    """
    # write down coinc.xml in the run directory
    path_to_coinc = os.path.join(rundir, 'coinc.xml')
    with open(path_to_coinc, 'wb') as f:
        f.write(coinc)

    # write down and upload ini file
    ini_contents = prepare_lalinference_ini(
        frametype_dict, event, superevent_id)
    path_to_ini = os.path.join(rundir, 'online_lalinference_pe.ini')
    with open(path_to_ini, 'w') as f:
        f.write(ini_contents)
    gracedb.upload.delay(
        ini_contents, filename=os.path.basename(path_to_ini),
        graceid=superevent_id,
        message=('Automatically generated LALInference configuration file'
                 ' for this event.'),
        tags='pe')

    try:
        subprocess.run(
            ['lalinference_pipe', '--run-path', rundir,
             '--coinc', path_to_coinc, path_to_ini, '--psd', path_to_coinc],
            capture_output=True, check=True)
    except subprocess.CalledProcessError as e:
        contents = b'args:\n' + json.dumps(e.args[1]).encode('utf-8') + \
                   b'\n\nstdout:\n' + e.stdout + b'\n\nstderr:\n' + e.stderr
        gracedb.upload.delay(
            filecontents=contents, filename='lalinference_dag.log',
            graceid=superevent_id,
            message='Failed to prepare DAG for lalinference', tags='pe'
        )
        raise

    return os.path.join(rundir, 'multidag.dag')


def _dump_phenomd_settings(path):
    settings = {
        "likelihood_args": {
            "likelihood_type": "ROQGravitationalWaveTransient",
            "minimum_frequency": 20,
            "maximum_frequency": 1024,
            "roq_scale_factor": 1,
            "waveform_approximant": "IMRPhenomD",
        },
        "likelihood_parameter_bounds": {
            "mass_ratio_min": 0.125,
            "a_1_max": 0.05,
            "a_2_max": 0.05,
            "spin_template": "aligned",
        },
        "trigger_dependent": {
            "range": {
                "chirp_mass": [
                    [0.6, 1.012], [1.012, 1.54], [1.54, 2.31], [2.31, 4.0]
                ],
            },
            "likelihood_args": [
                {"roq_linear_matrix":
                    "/home/roq/IMRPhenomD/lowspin_fhigh1024/basis_512s.hdf5",
                 "roq_quadratic_matrix":
                    "/home/roq/IMRPhenomD/lowspin_fhigh1024/basis_512s.hdf5",
                 "duration": 512},
                {"roq_linear_matrix":
                    "/home/roq/IMRPhenomD/lowspin_fhigh1024/basis_256s.hdf5",
                 "roq_quadratic_matrix":
                    "/home/roq/IMRPhenomD/lowspin_fhigh1024/basis_256s.hdf5",
                 "duration": 256},
                {"roq_linear_matrix":
                    "/home/roq/IMRPhenomD/lowspin_fhigh1024/basis_128s.hdf5",
                 "roq_quadratic_matrix":
                    "/home/roq/IMRPhenomD/lowspin_fhigh1024/basis_128s.hdf5",
                 "duration": 128},
                {"roq_linear_matrix":
                    "/home/roq/IMRPhenomD/lowspin_fhigh1024/basis_64s.hdf5",
                 "roq_quadratic_matrix":
                    "/home/roq/IMRPhenomD/lowspin_fhigh1024/basis_64s.hdf5",
                 "duration": 64}
            ],
            "likelihood_parameter_bounds": [
                {"chirp_mass_min": 0.6, "chirp_mass_max": 1.1},
                {"chirp_mass_min": 0.92, "chirp_mass_max": 1.7},
                {"chirp_mass_min": 1.4, "chirp_mass_max": 2.6},
                {"chirp_mass_min": 2.1, "chirp_mass_max": 4.0}
            ],
        },
    }
    with open(path, 'w') as f:
        json.dump(settings, f, indent=2)


def _dump_high_mass_ratio_pv2_settings(path):
    settings = {
        "likelihood_args": {
            "likelihood_type": "ROQGravitationalWaveTransient",
            "minimum_frequency": 20,
            "maximum_frequency": 1024,
            "roq_scale_factor": 1,
            "waveform_approximant": "IMRPhenomPv2",
        },
        "likelihood_parameter_bounds": {
            "mass_ratio_min": 0.05,
            "a_1_max": 0.99,
            "a_2_max": 0.99,
            "spin_template": "precessing",
        },
        "trigger_dependent": {
            "range": {
                "chirp_mass": [[1.4, 2.31], [2.31, 3.63], [3.63, 5.72],
                               [5.72, 9.57], [9.57, 21]],
            },
            "likelihood_args": [
                {"roq_linear_matrix":
                    "/home/roq/IMRPhenomPv2/low_mass_ratio/basis_128s.hdf5",
                 "roq_quadratic_matrix":
                    "/home/roq/IMRPhenomPv2/low_mass_ratio/basis_128s.hdf5",
                 "duration": 128},
                {
                    "roq_linear_matrix":
                        "/home/roq/IMRPhenomPv2/low_mass_ratio/basis_64s.hdf5",
                    "roq_quadratic_matrix":
                        "/home/roq/IMRPhenomPv2/low_mass_ratio/basis_64s.hdf5",
                    "duration": 64,
                },
                {
                    "roq_linear_matrix":
                        "/home/roq/IMRPhenomPv2/low_mass_ratio/basis_32s.hdf5",
                    "roq_quadratic_matrix":
                        "/home/roq/IMRPhenomPv2/low_mass_ratio/basis_32s.hdf5",
                    "duration": 32,
                },
                {
                    "roq_linear_matrix":
                        "/home/roq/IMRPhenomPv2/low_mass_ratio/basis_16s.hdf5",
                    "roq_quadratic_matrix":
                        "/home/roq/IMRPhenomPv2/low_mass_ratio/basis_16s.hdf5",
                    "duration": 16,
                },
                {
                    "roq_linear_matrix":
                        "/home/roq/IMRPhenomPv2/low_mass_ratio/basis_8s.hdf5",
                    "roq_quadratic_matrix":
                        "/home/roq/IMRPhenomPv2/low_mass_ratio/basis_8s.hdf5",
                    "duration": 8,
                },
            ],
            "likelihood_parameter_bounds": [
                {"chirp_mass_min": 1.4, "chirp_mass_max": 2.6},
                {"chirp_mass_min": 2.1, "chirp_mass_max": 4.0},
                {"chirp_mass_min": 3.3, "chirp_mass_max": 6.3},
                {"chirp_mass_min": 5.2, "chirp_mass_max": 11.0},
                {"chirp_mass_min": 8.7, "chirp_mass_max": 21.0},
            ],
        },
    }
    with open(path, 'w') as f:
        json.dump(settings, f, indent=2)


def _dump_xphm_settings(path):
    settings = {
        "likelihood_args": {
            "likelihood_type": "ROQGravitationalWaveTransient",
            "minimum_frequency": 20,
            "maximum_frequency": 4096,
            "reference_frequency": 20,
            "roq_scale_factor": 1,
            "waveform_approximant": "IMRPhenomXPHM",
            "waveform_arguments_dict": {"PhenomXHMReleaseVersion": 122019},
            "phase_marginalization": False,
        },
        "likelihood_parameter_bounds": {
            "mass_ratio_min": 0.05,
            "a_1_max": 0.99,
            "a_2_max": 0.99,
            "spin_template": "precessing",
        },
        "trigger_dependent": {
            "range": {
                "chirp_mass": [[10.03, 16], [16, 25],
                               [25, 45], [45, np.inf]],
            },
            "likelihood_args": [
                {
                    "roq_linear_matrix":
                        "/home/roq/IMRPhenomXPHM/basis_32s.hdf5",
                    "roq_quadratic_matrix":
                        "/home/roq/IMRPhenomXPHM/basis_32s.hdf5",
                    "duration": 32,
                },
                {
                    "roq_linear_matrix":
                        "/home/roq/IMRPhenomXPHM/basis_16s.hdf5",
                    "roq_quadratic_matrix":
                        "/home/roq/IMRPhenomXPHM/basis_16s.hdf5",
                    "duration": 16,
                },
                {
                    "roq_linear_matrix":
                        "/home/roq/IMRPhenomXPHM/basis_8s.hdf5",
                    "roq_quadratic_matrix":
                        "/home/roq/IMRPhenomXPHM/basis_8s.hdf5",
                    "duration": 8,
                },
                {
                    "roq_linear_matrix":
                        "/home/roq/IMRPhenomXPHM/basis_4s.hdf5",
                    "roq_quadratic_matrix":
                        "/home/roq/IMRPhenomXPHM/basis_4s.hdf5",
                    "duration": 4,
                },
            ],
            "likelihood_parameter_bounds": [
                {"chirp_mass_min": 10.03, "chirp_mass_max": 19.04},
                {"chirp_mass_min": 13, "chirp_mass_max": 31.85},
                {"chirp_mass_min": 20, "chirp_mass_max": 62.86},
                {"chirp_mass_min": 30, "chirp_mass_max": 200},
            ],
        },
    }
    with open(path, 'w') as f:
        json.dump(settings, f, indent=2)


@app.task(shared=False)
def _setup_dag_for_bilby(
    coinc, rundir, event, superevent_id, mode="production"
):
    """Create DAG for a bilby run and return the path to DAG.

    Parameters
    ----------
    coinc : bytes
        Byte contents of ``coinc.xml``. The PSD is expected to be embedded.
    rundir : str
        The path to a run directory where the DAG file is created
    event : dict
        The json contents of a target G event retrieved from
        gracedb.get_event(), whose mass and spin information are used to
        determine analysis settings.
    superevent_id : str
        The GraceDB ID of a target superevent
    mode : str
        Analysis mode, allowed options are "production" and "fast_test",
        default is "production".

    Returns
    -------
    path_to_dag : str
        The path to the .dag file

    Notes
    -----
    `--channel-dict o3replay` is added to bilby_pipe_gracedb arguments when the
    gracedb host is different from `gracedb.ligo.org` or
    `gracedb-test.ligo.org`. Condor queue is set to `Online_PE` if gracedb host
    is `gracedb.ligo.org`, and `Online_PE_MDC` otherwise.

    """
    path_to_json = os.path.join(rundir, 'event.json')
    with open(path_to_json, 'w') as f:
        json.dump(event, f, indent=2)

    path_to_psd = os.path.join(rundir, 'coinc.xml')
    with open(path_to_psd, 'wb') as f:
        f.write(coinc)

    path_to_webdir = os.path.join(
        app.conf['pe_results_path'], superevent_id, 'bilby', mode
    )

    path_to_settings = os.path.join(rundir, 'settings.json')
    setup_arg = ['bilby_pipe_gracedb', '--webdir', path_to_webdir,
                 '--outdir', rundir, '--json', path_to_json,
                 '--psd-file', path_to_psd, '--settings', path_to_settings]
    settings = {'summarypages_arguments': {'gracedb': event['graceid'],
                                           'no_ligo_skymap': True},
                'accounting_user': 'soichiro.morisaki',
                'enforce_signal_duration': False}
    if app.conf['gracedb_host'] != 'gracedb.ligo.org':
        settings['queue'] = 'Online_PE_MDC'
    else:
        settings['queue'] = 'Online_PE'
    # FIXME: using live data for gracedb-test events should be reconsidered
    # when we have a better idea to differentiate MDC and real events.
    if app.conf['gracedb_host'] not in [
        'gracedb.ligo.org', 'gracedb-test.ligo.org'
    ]:
        setup_arg += ['--channel-dict', 'o3replay']

    trigger_chirp_mass = event['extra_attributes']['CoincInspiral']['mchirp']
    if trigger_chirp_mass < 0.6:
        raise ValueError(
            "No bilby settings available for trigger chirp mass of"
            f" {trigger_chirp_mass}Msun."
        )
    if mode == 'production':
        settings.update(
            {
                'sampler_kwargs': {'naccept': 60, 'nlive': 500,
                                   'npool': 24, 'sample': 'acceptance-walk'},
                'n_parallel': 2,
                'request_cpus': 24,
                'spline_calibration_nodes': 10,
                'request_memory_generation': 8.0
            }
        )
        # use low-spin IMRPhenomD below chirp mass of m1=3Msun, m2=1Msun
        # assuming binary neutron star
        if trigger_chirp_mass < 1.465:
            likelihood_mode = os.path.join(rundir, "likelihood_mode.json")
            _dump_phenomd_settings(likelihood_mode)
            settings['sampler_kwargs']['naccept'] = 10
        # use IMRPhenomPv2 with mass ratio upper bound of 8 below chirp mass of
        # m1=8Msun, m2=1Msun
        elif trigger_chirp_mass < 2.243:
            likelihood_mode = 'phenompv2_bns_roq'
        # use IMRPhenomPv2 with mass ratio upper bound of 20 in chirp-mass
        # range where IMRPhenomXPHM ROQ bases are not available
        elif trigger_chirp_mass < 12:
            likelihood_mode = os.path.join(rundir, "likelihood_mode.json")
            _dump_high_mass_ratio_pv2_settings(likelihood_mode)
        else:
            likelihood_mode = os.path.join(rundir, "likelihood_mode.json")
            _dump_xphm_settings(likelihood_mode)
            settings['request_memory_generation'] = 36.0
            settings['request_memory'] = 16.0
        setup_arg += ['--cbc-likelihood-mode', likelihood_mode]
    elif mode == 'fast_test':
        setup_arg += ["--sampler-kwargs", "FastTest"]
        if trigger_chirp_mass < 3.9:
            setup_arg += ['--cbc-likelihood-mode', 'phenompv2_bns_roq']
            settings['request_memory_generation'] = 8.0
    else:
        raise ValueError(f"mode: {mode} not recognized.")

    with open(path_to_settings, 'w') as f:
        json.dump(settings, f, indent=2)

    try:
        subprocess.run(setup_arg, capture_output=True, check=True)
    except subprocess.CalledProcessError as e:
        contents = b'args:\n' + json.dumps(e.args[1]).encode('utf-8') + \
                   b'\n\nstdout:\n' + e.stdout + b'\n\nstderr:\n' + e.stderr
        gracedb.upload.delay(
            filecontents=contents, filename='bilby_dag.log',
            graceid=superevent_id,
            message=f'Failed to prepare DAG for {mode}-mode bilby', tags='pe'
        )
        raise
    else:
        # Uploads bilby ini file to GraceDB
        with open(os.path.join(rundir, 'bilby_config.ini'), 'r') as f:
            ini_contents = f.read()
        if mode == 'production':
            filename = 'bilby_config.ini'
        else:
            filename = f'bilby_{mode}_config.ini'
        gracedb.upload.delay(
            ini_contents, filename=filename, graceid=superevent_id,
            message=(f'Automatically generated {mode}-mode Bilby configuration'
                     ' file for this event.'),
            tags='pe')

    path_to_dag, = glob.glob(os.path.join(rundir, 'submit/dag*.submit'))
    return path_to_dag


@app.task(shared=False)
def _setup_dag_for_rapidpe(rundir, superevent_id, frametype_dict):
    """Create DAG for a rapidpe run and return the path to DAG.

    Parameters
    ----------
    rundir : str
        The path to a run directory where the DAG file is created
    superevent_id : str
        The GraceDB ID of a target superevent
    frametype_dict : dict
        Dictionary whose keys are ifos and values are frame types

    Returns
    -------
    path_to_dag : str
        The path to the .dag file

    """
    if app.conf['gracedb_host'] not in {
        'gracedb.ligo.org', 'gracedb-test.ligo.org'
    }:
        run_mode = 'o3replay'
    else:
        run_mode = 'online'

    # dump ini file
    ini_template = env.get_template('rapidpe.jinja2')
    ini_contents = ini_template.render(
        {'rundir': rundir,
         'webdir': os.path.join(
             app.conf['pe_results_path'], superevent_id, 'rapidpe'
         ),
         'gracedb_url': f'https://{app.conf["gracedb_host"]}/api',
         'superevent_id': superevent_id,
         'run_mode': run_mode,
         'frame_data_types': frametype_dict})
    path_to_ini = os.path.join(rundir, 'rapidpe.ini')
    with open(path_to_ini, 'w') as f:
        f.write(ini_contents)
    gracedb.upload.delay(
        ini_contents, filename=os.path.basename(path_to_ini),
        graceid=superevent_id,
        message=('Automatically generated RapidPE-RIFT configuration file'
                 ' for this event.'),
        tags='pe')

    # set up dag
    try:
        subprocess.run(['rapidpe-rift-pipe', path_to_ini],
                       capture_output=True, check=True)
    except subprocess.CalledProcessError as e:
        contents = b'args:\n' + json.dumps(e.args[1]).encode('utf-8') + \
                   b'\n\nstdout:\n' + e.stdout + b'\n\nstderr:\n' + e.stderr
        gracedb.upload.delay(
            filecontents=contents, filename='rapidpe_dag.log',
            graceid=superevent_id,
            message='Failed to prepare DAG for Rapid PE', tags='pe'
        )
        raise

    # return path to dag
    dag = os.path.join(rundir, "event_all_iterations.dag")
    return dag


@app.task(shared=False)
def _condor_no_submit(path_to_dag):
    """Run 'condor_submit_dag -no_submit' and return the path to .sub file."""
    subprocess.run(['condor_submit_dag', '-no_submit', path_to_dag],
                   capture_output=True, check=True)
    return '{}.condor.sub'.format(path_to_dag)


def dag_prepare_task(rundir, event, superevent_id, pe_pipeline,
                     frametype_dict=None, **kwargs):
    """Return a canvas of tasks to prepare DAG.

    Parameters
    ----------
    rundir : str
        The path to a run directory where the DAG file is created
    event : dict
        The json contents of a target G event retrieved from
        gracedb.get_event(), whose mass and spin information are used to
        determine analysis settings.
    superevent_id : str
        The GraceDB ID of a target superevent
    pe_pipeline : str
        The parameter estimation pipeline used,
        lalinference, bilby, or rapidpe.
    frametype_dict : dict
        Dictionary whose keys are ifos and values are frame types

    Returns
    -------
    canvas : canvas of tasks
        The canvas of tasks to prepare DAG

    """
    if pe_pipeline == 'lalinference':
        canvas = gracedb.download.si('coinc.xml', event['graceid']) | \
            _setup_dag_for_lalinference.s(rundir, event, superevent_id,
                                          frametype_dict)
    elif pe_pipeline == 'bilby':
        canvas = gracedb.download.si('coinc.xml', event['graceid']) | \
            _setup_dag_for_bilby.s(
                rundir, event, superevent_id, kwargs['bilby_mode'])
    elif pe_pipeline == 'rapidpe':
        canvas = _setup_dag_for_rapidpe.s(
            rundir, superevent_id, frametype_dict)
    else:
        raise NotImplementedError(f'Unknown PE pipeline {pe_pipeline}.')
    canvas |= _condor_no_submit.s()
    return canvas


def _find_paths_from_name(directory, name):
    """Return the paths of files or directories with given name under the
    specfied directory

    Parameters
    ----------
    directory : string
        Name of directory under which the target file or directory is searched
        for.
    name : string
        Name of target files or directories

    Returns
    -------
    paths : generator
        Paths to the target files or directories

    """
    return glob.iglob(os.path.join(directory, '**', name), recursive=True)


@app.task(ignore_result=True, shared=False)
def _clean_up_bilby(rundir):
    """Remove large data products produced by bilby

    Parameters
    ----------
    rundir : str

    """
    for p in glob.glob(
        os.path.join(rundir, "data/*_generation_roq_weights.hdf5")
    ):
        os.remove(p)
    for p in glob.glob(
        os.path.join(rundir, "data/*_generation_data_dump.pickle")
    ):
        os.remove(p)


@app.task(ignore_result=True, shared=False)
def job_error_notification(request, exc, traceback,
                           superevent_id, rundir, analysis):
    """Upload notification when condor.submit terminates unexpectedly.

    Parameters
    ----------
    request : Context (placeholder)
        Task request variables
    exc : Exception
        Exception raised by condor.submit
    traceback : str (placeholder)
        Traceback message from a task
    superevent_id : str
        The GraceDB ID of a target superevent
    rundir : str
        The run directory for PE
    analysis : str
        Analysis name used as a label in uploaded messages

    Notes
    -----
    Some large bilby data products are cleaned up after the notification if the
    gracedb host is different from `gracedb.ligo.org`.

    """
    if isinstance(exc, condor.JobRunning):
        subprocess.run(['condor_rm', str(exc.args[0]['Cluster'])])
        canvas = gracedb.upload.si(
            filecontents=None, filename=None, graceid=superevent_id, tags='pe',
            message=f'The {analysis} condor job was aborted by gwcelery, '
                    'due to its long run time.'
        )
    elif isinstance(exc, condor.JobAborted):
        canvas = gracedb.upload.si(
            filecontents=None, filename=None, graceid=superevent_id, tags='pe',
            message=f'The {analysis} condor job was aborted.'
        )
    else:
        canvas = gracedb.upload.si(
            filecontents=None, filename=None, graceid=superevent_id, tags='pe',
            message=f'The {analysis} condor job failed.'
        )

    if analysis == "rapidpe":
        to_upload = [
            'event_all_iterations.dag.lib.err',
            'marginalize_extrinsic_parameters_iteration_*.dag.lib.err'
        ]
    else:
        to_upload = ['*.log', '*.err', '*.out']
    for filename in to_upload:
        tasks = []
        for path in _find_paths_from_name(rundir, filename):
            with open(path, 'rb') as f:
                contents = f.read()
            if contents:
                # put .log suffix in log file names so that users can directly
                # read the contents instead of downloading them when they click
                # file names
                tasks.append(gracedb.upload.si(
                    filecontents=contents,
                    filename=os.path.basename(path) + '.log',
                    graceid=superevent_id,
                    message=f'A log file for {analysis} condor job.',
                    tags='pe'
                ))
        canvas |= group(tasks)

    if "bilby" in analysis and app.conf['gracedb_host'] != 'gracedb.ligo.org':
        canvas |= _clean_up_bilby.si(rundir)

    canvas.delay()


def _upload_tasks_lalinference(rundir, superevent_id):
    """Return canvas of tasks to upload LALInference results

    Parameters
    ----------
    rundir : str
        The path to a run directory
    superevent_id : str
        The GraceDB ID of a target superevent

    Returns
    -------
    tasks : canvas
        The work-flow for uploading LALInference results

    """
    pe_results_path = os.path.join(
        app.conf['pe_results_path'], superevent_id, 'lalinference'
    )

    # posterior samples
    path, = glob.glob(
        os.path.join(rundir, '**', 'posterior*.hdf5'), recursive=True)
    with open(path, 'rb') as f:
        canvas = gracedb.upload.si(
            f.read(), 'LALInference.posterior_samples.hdf5',
            superevent_id, 'LALInference posterior samples', 'pe')

    # plots
    tasks = []
    for filename, message in [
        ('extrinsic.png', 'LALInference corner plot for extrinsic parameters'),
        ('intrinsic.png', 'LALInference corner plot for intrinsic parameters')
    ]:
        # Here it is not required that only a single png file exists, so that
        # posterior samples are uploaded whatever. This applies for the other
        # files.
        for path in _find_paths_from_name(pe_results_path, filename):
            with open(path, 'rb') as f:
                tasks.append(gracedb.upload.si(
                    f.read(), f'LALInference.{filename}', superevent_id,
                    message, 'pe'
                ))
    canvas |= group(tasks)

    # psd
    tasks = []
    for path in _find_paths_from_name(rundir, 'glitch_median_PSD_forLI_*.dat'):
        with open(path, 'r') as f:
            tasks.append(gracedb.upload.si(
                f.read(), os.path.basename(path), superevent_id,
                'Bayeswave PSD used for LALInference PE', 'pe'
            ))
    canvas |= group(tasks)

    # dag
    tasks = []
    for path in _find_paths_from_name(rundir, 'lalinference*.dag'):
        with open(path, 'r') as f:
            tasks.append(gracedb.upload.si(
                f.read(), os.path.basename(path), superevent_id,
                'LALInference DAG', 'pe'
            ))
    canvas |= group(tasks)

    # link to results page
    tasks = []
    for path in _find_paths_from_name(pe_results_path, 'posplots.html'):
        baseurl = urllib.parse.urljoin(
            app.conf['pe_results_url'],
            os.path.relpath(path, app.conf['pe_results_path'])
        )
        tasks.append(gracedb.upload.si(
            None, None, superevent_id,
            'Online lalinference parameter estimation finished. '
            f'<a href={baseurl}>results</a>'
        ))
    canvas |= group(tasks)

    return canvas


def _upload_tasks_bilby(rundir, superevent_id, mode):
    """Return canvas of tasks to upload Bilby results

    Parameters
    ----------
    rundir : str
        The path to a run directory
    superevent_id : str
        The GraceDB ID of a target superevent
    mode : str
        Analysis mode

    Returns
    -------
    tasks : canvas
        The work-flow for uploading Bilby results

    Notes
    -----
    Some large bilby data products are cleaned up after posteterior file is
    uploaded if the gracedb host is different from `gracedb.ligo.org`.

    """
    # convert bilby sample file into one compatible with ligo-skymap
    samples_dir = os.path.join(rundir, 'final_result')
    if mode == 'production':
        samples_filename = 'Bilby.posterior_samples.hdf5'
    else:
        samples_filename = f'Bilby.{mode}.posterior_samples.hdf5'
    out_samples = os.path.join(samples_dir, samples_filename)
    in_samples, = glob.glob(os.path.join(samples_dir, '*result.hdf5'))
    subprocess.run(
        ['bilby_pipe_to_ligo_skymap_samples', in_samples, '--out', out_samples]
    )

    with open(out_samples, 'rb') as f:
        canvas = gracedb.upload.si(
            f.read(), samples_filename,
            superevent_id, f'{mode}-mode Bilby posterior samples', 'pe')

    if app.conf['gracedb_host'] != 'gracedb.ligo.org':
        canvas |= _clean_up_bilby.si(rundir)

    # pesummary
    pesummary_kwargs = {}
    path_to_ini, = glob.glob(os.path.join(rundir, "*_complete.ini"))
    pesummary_kwargs["config"] = path_to_ini
    config_parser = BilbyConfigFileParser()
    with open(path_to_ini, "r") as f:
        config_content, _, _, _ = config_parser.parse(f)
    pesummary_kwargs["psd"] = convert_string_to_dict(
        config_content["psd-dict"]
    )
    pesummary_kwargs["calibration"] = convert_string_to_dict(
        config_content["spline-calibration-envelope-dict"]
    )
    pesummary_kwargs["approximant"] = config_content["waveform-approximant"]
    pesummary_kwargs["f_low"] = config_content["minimum-frequency"]
    pesummary_kwargs["f_ref"] = config_content["reference-frequency"]

    webdir = os.path.join(config_content["webdir"], 'pesummary')
    url = urllib.parse.urljoin(
        app.conf['pe_results_url'],
        os.path.relpath(
            os.path.join(webdir, 'home.html'),
            app.conf['pe_results_path']
        )
    )
    canvas = group(
        canvas,
        _pesummary_task(webdir, in_samples, **pesummary_kwargs)
        |
        gracedb.upload.si(
            None, None, superevent_id,
            f'PESummary page for {mode}-mode Bilby is available '
            f'<a href={url}>here</a>',
            'pe'
        )
    )

    return canvas


def _upload_tasks_rapidpe(rundir, superevent_id):
    summary_path = os.path.join(rundir, "summary")

    url = urllib.parse.urljoin(
        app.conf['pe_results_url'],
        os.path.join(superevent_id, 'rapidpe', 'summarypage.html')
    )
    canvas = gracedb.upload.si(
        None, None, superevent_id,
        f'Summary page for RapidPE-RIFT is available <a href={url}>here</a>',
        ('pe',))

    to_upload = [
        (
            "p_astro.json", "RapidPE_RIFT.p_astro.json",
            "RapidPE-RIFT Pastro results",
            ("pe", "lvem", "public", "p_astro"),
        ),
        (
            "p_astro.png", "RapidPE_RIFT.p_astro.png",
            "RapidPE-RIFT Pastro results",
            ("pe", "lvem", "public", "p_astro"),
        ),
    ]
    tasks = []
    for src_basename, dst_filename, description, tags in to_upload:
        src_filename = os.path.join(summary_path, src_basename)
        if os.path.isfile(src_filename):
            with open(src_filename, "rb") as f:
                tasks.append(
                    gracedb.upload.si(
                        f.read(), dst_filename,
                        superevent_id, description, tags))

    canvas |= group(tasks)

    return canvas


@app.task(ignore_result=True, shared=False)
def dag_finished(rundir, superevent_id, pe_pipeline, **kwargs):
    """Upload PE results

    Parameters
    ----------
    rundir : str
        The path to a run directory where the DAG file exits
    superevent_id : str
        The GraceDB ID of a target superevent
    pe_pipeline : str
        The parameter estimation pipeline used,
        lalinference, bilby, or rapidpe.

    """
    if pe_pipeline == 'lalinference':
        canvas = _upload_tasks_lalinference(rundir, superevent_id)
    elif pe_pipeline == 'bilby':
        canvas = _upload_tasks_bilby(
            rundir, superevent_id, kwargs['bilby_mode'])
    elif pe_pipeline == 'rapidpe':
        canvas = _upload_tasks_rapidpe(rundir, superevent_id)
    else:
        raise NotImplementedError(f'Unknown PE pipeline {pe_pipeline}.')

    canvas.delay()

    # NOTE: check if this should include rapidpe as well
    if pe_pipeline == 'bilby':
        gracedb.create_label.delay('PE_READY', superevent_id)


def _pesummary_task(webdir, samples, **pesummary_kwargs):
    """Return a celery task to submit a pesummary condor job.

    Parameters
    ----------
    webdir : str
        output directory
    samples : str
        path to posterior sample file
    **pesummary_kwargs
        Extra arguments of summarypages

    Returns
    -------
    celery task

    Notes
    -----
    `--disable_interactive --disable_expert` are added and `--redshift_method
    exact --evolve_spins_forwards` are not added to `summarypages` arguments
    when the gracedb host is different from `gracedb.ligo.org`. Condor queue is
    set to `Online_PE` if gracedb host is `gracedb.ligo.org`, and
    `Online_PE_MDC` otherwise.

    """
    args = [
        "summarypages", "--webdir", webdir, "--samples", samples, "--gw",
        "--no_ligo_skymap", "--multi_process", "6"
    ]
    for key in pesummary_kwargs:
        if key in ["psd", "calibration"]:
            args += [f"--{key}"]
            for ifo in pesummary_kwargs[key]:
                args += [f'{ifo}:{pesummary_kwargs[key][ifo]}']
        else:
            args += [f"--{key}", pesummary_kwargs[key]]
    condor_kwargs = dict(
        request_memory=16000, request_disk=5000, request_cpus=6,
        accounting_group_user='soichiro.morisaki'
    )
    if app.conf['gracedb_host'] != 'gracedb.ligo.org':
        condor_kwargs['accounting_group'] = 'ligo.dev.o4.cbc.pe.bilby'
        condor_kwargs['requirements'] = '((TARGET.Online_PE_MDC =?= True))'
        condor_kwargs['+Online_PE_MDC'] = True
        args += ["--disable_interactive", "--disable_expert"]
    else:
        condor_kwargs['accounting_group'] = 'ligo.prod.o4.cbc.pe.bilby'
        condor_kwargs['requirements'] = '((TARGET.Online_PE =?= True))'
        condor_kwargs['+Online_PE'] = True
        args += ["--redshift_method", "exact", "--evolve_spins_forwards"]
    return condor.check_output.si(args, **condor_kwargs)


@app.task(ignore_result=True, shared=False)
def start_pe(frametype_dict, event, superevent_id, pe_pipeline):
    """Run Parameter Estimation on a given event.

    Parameters
    ----------
    frametype_dict : dict
        Dictionary whose keys are ifos and values are frame types
    event : dict
        The json contents of a target G event retrieved from
        gracedb.get_event(), whose mass and spin information are used to
        determine analysis settings.
    superevent_id : str
        The GraceDB ID of a target superevent
    pe_pipeline : str
        The parameter estimation pipeline used,
        lalinference, bilby, or rapidpe.

    """
    # make an event directory
    pipeline_dir = os.path.expanduser('~/.cache/{}'.format(pe_pipeline))
    mkpath(pipeline_dir)
    event_dir = os.path.join(pipeline_dir, superevent_id)
    os.mkdir(event_dir)

    if pe_pipeline == 'bilby':
        modes = ["production"]
        rundirs = [os.path.join(event_dir, m) for m in modes]
        kwargs_list = [{'bilby_mode': m} for m in modes]
        analyses = [f'{m}-mode bilby' for m in modes]
    else:
        rundirs = [event_dir]
        kwargs_list = [{}]
        analyses = [pe_pipeline]

    for rundir, kwargs, analysis in zip(rundirs, kwargs_list, analyses):
        mkpath(rundir)

        gracedb.upload.delay(
            filecontents=None, filename=None, graceid=superevent_id,
            message=(f'Starting {analysis} parameter estimation '
                     f'for {event["graceid"]}'),
            tags='pe'
        )

        (
            dag_prepare_task(
                rundir, event, superevent_id, pe_pipeline, frametype_dict,
                **kwargs
            )
            |
            condor.submit.s().on_error(
                job_error_notification.s(superevent_id, rundir, analysis)
            )
            |
            dag_finished.si(rundir, superevent_id, pe_pipeline, **kwargs)
        ).delay()
