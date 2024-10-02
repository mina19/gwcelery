"""Source Parameter Estimation with LALInference, Bilby, and RapidPE."""
import glob
import json
import os
import subprocess
import urllib
from shutil import which

import numpy as np
import platformdirs
from bilby_pipe.bilbyargparser import BilbyConfigFileParser
from bilby_pipe.utils import convert_string_to_dict
from celery import group
from celery.exceptions import Ignore

from .. import app
from ..jinja import env
from . import condor, gracedb

_RAPIDPE_NO_GSTLAL_TRIGGER_EXIT_CODE = 100

RAPIDPE_GETENV = [
    "DEFAULT_SEGMENT_SERVER", "GWDATAFIND_SERVER",
    "LAL_DATA_PATH", "LD_LIBRARY_PATH", "LIBRARY_PATH",
    "NDSSERVER", "PATH", "PYTHONPATH",
]
"""Names of environment variables to include in RapidPE HTCondor submit
files."""

RAPIDPE_ENVIRONMENT = {
    "RIFT_LOWLATENCY": "True",
}
"""Names and values of environment variables to include in RapidPE HTCondor
submit files."""


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
        np.genfromtxt(filename, dtype='object', names=['gpstime', 'filename'])
    )
    gpstimes = calibration_index['gpstime'].astype(np.int32)
    candidate_gpstimes = gpstimes < trigtime
    if np.any(candidate_gpstimes):
        idx = np.argmax(gpstimes * candidate_gpstimes)
        appropriate_cal = calibration_index['filename'][idx]
    else:
        appropriate_cal = calibration_index['filename'][np.argmin(gpstimes)]
    return os.path.join(dir_name, appropriate_cal.decode('utf-8'))


def prepare_lalinference_ini(event, superevent_id):
    """Determine LALInference configurations and return ini file content

    Parameters
    ----------
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
        'types': app.conf['low_latency_frame_types'],
        'channels': app.conf['strain_channel_names'],
        'state_vector_channels': app.conf['state_vector_channel_names'],
        'webdir': os.path.join(
            app.conf['pe_results_path'], superevent_id, 'lalinference'
        ),
        'paths': [{'name': name, 'path': which(executable)}
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
        'mpirun': which('mpirun')
    }
    return ini_template.render(ini_settings)


@app.task(shared=False)
def _setup_dag_for_lalinference(coinc, rundir, event, superevent_id):
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
    ini_contents = prepare_lalinference_ini(event, superevent_id)
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


@app.task(shared=False)
def _setup_dag_for_bilby(
    coinc_bayestar, rundir, event, superevent_id, mode="production"
):
    """Create DAG for a bilby run and return the path to DAG.

    Parameters
    ----------
    coinc_bayestar : tuple
        Byte contents of ``coinc.xml`` and ``bayestar.multiorder.fits``.
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

    coinc, bayestar = coinc_bayestar
    path_to_psd = os.path.join(rundir, 'coinc.xml')
    with open(path_to_psd, 'wb') as f:
        f.write(coinc)
    path_to_bayestar = os.path.join(rundir, 'bayestar.multiorder.fits')
    with open(path_to_bayestar, 'wb') as f:
        f.write(bayestar)

    path_to_webdir = os.path.join(
        app.conf['pe_results_path'], superevent_id, 'bilby', mode
    )

    path_to_settings = os.path.join(rundir, 'settings.json')
    setup_arg = ['bilby_pipe_gracedb', '--webdir', path_to_webdir,
                 '--outdir', rundir, '--json', path_to_json,
                 '--psd-file', path_to_psd, '--skymap-file', path_to_bayestar,
                 '--settings', path_to_settings]
    settings = {'summarypages_arguments': {'gracedb': event['graceid'],
                                           'no_ligo_skymap': True},
                'accounting_user': 'soichiro.morisaki',
                'tukey_roll_off': 1.0}
    if app.conf['gracedb_host'] != 'gracedb.ligo.org':
        settings['queue'] = 'Online_PE_MDC'
    else:
        settings['queue'] = 'Online_PE'
        settings['accounting'] = 'ligo.prod.o4.cbc.pe.bilby'
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
                'n_parallel': 3,
                'request_cpus': 24,
                'spline_calibration_nodes': 10,
                'request_memory_generation': 8.0
            }
        )
        # use low-spin IMRPhenomD below chirp mass of m1=3Msun, m2=1Msun
        # assuming binary neutron star
        if trigger_chirp_mass < 1.465:
            likelihood_mode = 'lowspin_phenomd_fhigh1024_roq'
            settings['sampler_kwargs']['naccept'] = 10
        # use IMRPhenomPv2 with mass ratio upper bound of 8 below chirp mass of
        # m1=8Msun, m2=1Msun
        elif trigger_chirp_mass < 2.243:
            likelihood_mode = 'phenompv2_bns_roq'
        # use IMRPhenomPv2 with mass ratio upper bound of 20 in chirp-mass
        # range where IMRPhenomXPHM ROQ bases are not available
        elif trigger_chirp_mass < 12:
            likelihood_mode = 'low_q_phenompv2_roq'
        else:
            likelihood_mode = 'phenomxphm_roq'
            if trigger_chirp_mass > 16:
                settings['request_memory_generation'] = 36.0
            else:
                settings['request_memory_generation'] = 50.0
            if trigger_chirp_mass > 25:
                settings['request_memory'] = 16.0
            elif trigger_chirp_mass > 16:
                settings['request_memory'] = 24.0
            else:
                settings['request_memory'] = 36.0
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
def _setup_dag_for_rapidpe(rundir, superevent_id, event):
    """Create DAG for a rapidpe run and return the path to DAG.

    Parameters
    ----------
    rundir : str
        The path to a run directory where the DAG file is created
    superevent_id : str
        The GraceDB ID of a target superevent

    Returns
    -------
    path_to_dag : str
        The path to the .dag file

    """
    gracedb_host = app.conf['gracedb_host']

    settings = app.conf['rapidpe_settings']
    trigger_snr = event['extra_attributes']['CoincInspiral']['snr']
    high_snr_trigger = trigger_snr >= 37.5

    # dump ini file
    ini_template = env.get_template('rapidpe.jinja2')
    ini_contents = ini_template.render(
        {'rundir': rundir,
         'webdir': os.path.join(
             app.conf['pe_results_path'], superevent_id, 'rapidpe'
         ),
         'gracedb_url': f'https://{gracedb_host}/api',
         'superevent_id': superevent_id,
         'run_mode': settings['run_mode'],
         'frame_data_types': app.conf['low_latency_frame_types'],
         'accounting_group': settings['accounting_group'],
         'use_cprofile': settings['use_cprofile'],
         'gracedb_host': gracedb_host,
         'high_snr_trigger': high_snr_trigger,
         'getenv': RAPIDPE_GETENV,
         'environment': RAPIDPE_ENVIRONMENT})
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

        message = 'Failed to prepare DAG for Rapid PE'

        fail_gracefully = False
        if e.returncode == _RAPIDPE_NO_GSTLAL_TRIGGER_EXIT_CODE:
            fail_gracefully = True
            message += ": no GstLAL trigger available"

        gracedb.upload.delay(
            filecontents=contents, filename='rapidpe_dag.log',
            graceid=superevent_id,
            message=message, tags='pe'
        )

        if fail_gracefully:
            # Ends task but without logging as a failure
            raise Ignore()
        else:
            # Ends task with the unhandled error logged
            raise

    # return path to dag
    dag = os.path.join(rundir, "event_all_iterations.dag")
    return dag


@app.task(shared=False)
def _condor_no_submit(path_to_dag, include_env=None):
    """Run 'condor_submit_dag -no_submit' and return the path to .sub file."""
    args = ['condor_submit_dag']

    if include_env is not None:
        args += ['-include_env', ','.join(include_env)]

    args += ['-no_submit', path_to_dag]
    subprocess.run(args, capture_output=True, check=True)
    return '{}.condor.sub'.format(path_to_dag)


def dag_prepare_task(rundir, event, superevent_id, pe_pipeline, **kwargs):
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

    Returns
    -------
    canvas : canvas of tasks
        The canvas of tasks to prepare DAG

    """
    # List of environment variables `condor_submit_dag` should be aware of.
    include_env = None

    if pe_pipeline == 'lalinference':
        canvas = gracedb.download.si('coinc.xml', event['graceid']) | \
            _setup_dag_for_lalinference.s(rundir, event, superevent_id)
    elif pe_pipeline == 'bilby':
        canvas = group(
            gracedb.download.si('coinc.xml', event['graceid']),
            gracedb.download.si('bayestar.multiorder.fits', event['graceid'])
        ) | _setup_dag_for_bilby.s(
            rundir, event, superevent_id, kwargs['bilby_mode']
        )
    elif pe_pipeline == 'rapidpe':
        canvas = _setup_dag_for_rapidpe.s(rundir, superevent_id, event)
        include_env = RAPIDPE_GETENV
    else:
        raise NotImplementedError(f'Unknown PE pipeline {pe_pipeline}.')

    canvas |= _condor_no_submit.s(include_env=include_env)

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
    pesummary_kwargs["label"] = "online"

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
            ("pe", "p_astro", "public"),
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
            # these values can be none if calibration envelopes
            # or PSD files aren't passed to bilby_pipe
            # see https://git.ligo.org/emfollow/gwcelery/-/issues/852
            # and related issues
            if pesummary_kwargs[key] is None:
                continue
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


# Modified version of condor.submit task with retry kwargs overridden with
# RapidPE-specific settings.
submit_rapidpe = app.task(
    **condor.submit_kwargs,
    **app.conf['rapidpe_condor_retry_kwargs'],
)(condor.submit.run)


@app.task(ignore_result=True, shared=False)
def start_pe(event, superevent_id, pe_pipeline):
    """Run Parameter Estimation on a given event.

    Parameters
    ----------
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
    pipeline_dir = platformdirs.user_cache_dir(pe_pipeline, ensure_exists=True)
    event_dir = os.path.join(pipeline_dir, superevent_id)

    if pe_pipeline == 'bilby':
        if (
            app.conf['gracedb_host'] == 'gracedb-playground.ligo.org' and
            event['extra_attributes']['CoincInspiral']['mchirp'] >= 12
        ):
            # Count the number of BBH jobs and do not start a run if it exceeds
            # 5 so that we do not use up disk space. We assume that the job is
            # running if a data dump pickle file exists under the run
            # directory, which is the largest file produced by PE and removed
            # when the run completes.
            number_of_bbh_running = 0
            for p in glob.glob(
                os.path.join(
                    pipeline_dir,
                    "*/*/data/*_generation_data_dump.pickle"
                )
            ):
                path_to_ev = os.path.join(os.path.dirname(p), "../event.json")
                if os.path.exists(path_to_ev):
                    with open(path_to_ev, "r") as f:
                        ev = json.load(f)
                        mc = ev['extra_attributes']['CoincInspiral']['mchirp']
                    if mc >= 12:
                        number_of_bbh_running += 1
            if number_of_bbh_running > 5:
                gracedb.upload.delay(
                    filecontents=None, filename=None, graceid=superevent_id,
                    message='Parameter estimation will not start to save disk '
                            f'space (There are {number_of_bbh_running} BBH '
                            'jobs running).',
                    tags='pe'
                )
                return
        modes = ["production"]
        rundirs = [os.path.join(event_dir, m) for m in modes]
        kwargs_list = [{'bilby_mode': m} for m in modes]
        analyses = [f'{m}-mode bilby' for m in modes]
        condor_submit_task = condor.submit
    elif pe_pipeline == 'rapidpe':
        rundirs = [event_dir]
        kwargs_list = [{'event_pipeline': event["pipeline"]}]
        analyses = [pe_pipeline]
        condor_submit_task = submit_rapidpe
    else:
        rundirs = [event_dir]
        kwargs_list = [{}]
        analyses = [pe_pipeline]
        condor_submit_task = condor.submit

    for rundir, kwargs, analysis in zip(rundirs, kwargs_list, analyses):
        os.makedirs(rundir, exist_ok=True)

        gracedb.upload.delay(
            filecontents=None, filename=None, graceid=superevent_id,
            message=(f'Starting {analysis} parameter estimation '
                     f'for {event["graceid"]}'),
            tags='pe'
        )

        (
            dag_prepare_task(
                rundir, event, superevent_id, pe_pipeline, **kwargs
            )
            |
            condor_submit_task.s().on_error(
                job_error_notification.s(superevent_id, rundir, analysis)
            )
            |
            dag_finished.si(rundir, superevent_id, pe_pipeline, **kwargs)
        ).delay()
