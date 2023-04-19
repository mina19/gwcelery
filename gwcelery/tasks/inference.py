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


@app.task(shared=False)
def _setup_dag_for_bilby(coinc, rundir, event, superevent_id, mode):
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
        Analysis mode

    Returns
    -------
    path_to_dag : str
        The path to the .dag file

    Notes
    -----
    `--channel-dict o3replay` is added to bilby_pipe_gracedb arguments when the
    gracedb host is different from `gracedb.ligo.org`. Condor queue is set to
    `Online_PE` if gracedb host is `gracedb.ligo.org`, and `Online_PE_MDC`
    otherwise.

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
                'accounting_user': 'soichiro.morisaki'}
    if app.conf['gracedb_host'] != 'gracedb.ligo.org':
        settings['queue'] = 'Online_PE_MDC'
        setup_arg += ['--channel-dict', 'o3replay']
    else:
        settings['queue'] = 'Online_PE'

    if mode == 'quick_bns':
        setup_arg += ['--cbc-likelihood-mode', 'lowspin_phenomd_narrowmc_roq']
        settings.update(
            {'sampler_kwargs': {'naccept': 10, 'nlive': 500,
                                'npool': 24, 'sample': 'acceptance-walk'},
             'n_parallel': 2,
             'request_cpus': 24,
             'spline_calibration_nodes': 10,
             'request_memory_generation': 8.0}
        )
    elif mode == 'fast_test':
        # use pv2_nrtidalv2 below chirp mass of m1=3Msun, m2=1Msun
        if event['extra_attributes']['CoincInspiral']['mchirp'] < 1.465:
            setup_arg += ['--cbc-likelihood-mode', 'phenompv2nrtidalv2_roq']
            settings['request_memory_generation'] = 8.0
        # use bns-mass pv2 basis for chirp mass range where it is available
        elif event['extra_attributes']['CoincInspiral']['mchirp'] < 3.9:
            setup_arg += ['--cbc-likelihood-mode', 'phenompv2_bns_roq']
            settings['request_memory_generation'] = 8.0
        settings.update(
            {'sampler_kwargs': {'naccept': 60, 'nlive': 500,
                                'npool': 24, 'sample': 'acceptance-walk'},
             'n_parallel': 2,
             'request_cpus': 24,
             'spline_calibration_nodes': 10}
        )
    elif mode != 'production':
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
            message=f'Failed to prepare DAG for {mode} bilby', tags='pe'
        )
        raise
    else:
        # Uploads bilby ini file to GraceDB
        with open(os.path.join(rundir, 'bilby_config.ini'), 'r') as f:
            ini_contents = f.read()
        gracedb.upload.delay(
            ini_contents, filename=f'bilby_{mode}_config.ini',
            graceid=superevent_id,
            message=(f'Automatically generated {mode} Bilby configuration file'
                     ' for this event.'),
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
    # dump SVD depth file
    path_to_svd_file = os.path.join(rundir, 'svd_depth_methods.json')
    with open(path_to_svd_file, 'w') as f:
        json.dump(
            [{'bounds': {}, 'fudge_factors': {'mchirp': 0.2, 'eta': 0.1},
             'svd_depth': 1}],
            f)

    # dump ini file
    ini_template = env.get_template('rapidpe.jinja2')
    ini_contents = ini_template.render(
        {'rundir': rundir,
         'webdir': os.path.join(
             app.conf['pe_results_path'], superevent_id, 'rapidpe'
         ),
         'gracedb_url': f'https://{app.conf["gracedb_host"]}/api',
         'superevent_id': superevent_id,
         'frame_data_types': frametype_dict,
         'svd_depth_json': os.path.abspath(path_to_svd_file)})
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

    """
    # convert bilby sample file into one compatible with ligo-skymap
    samples_dir = os.path.join(rundir, 'final_result')
    samples_filename = f'Bilby.{mode}.posterior_samples.hdf5'
    out_samples = os.path.join(samples_dir, samples_filename)
    in_samples, = glob.glob(os.path.join(samples_dir, '*result.hdf5'))
    subprocess.run(
        ['bilby_pipe_to_ligo_skymap_samples', in_samples, '--out', out_samples]
    )

    with open(out_samples, 'rb') as f:
        canvas = gracedb.upload.si(
            f.read(), samples_filename,
            superevent_id, f'{mode} Bilby posterior samples', 'pe')

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
            'PESummary page for {mode} Bilby is available '
            f'<a href={url}>here</a>'
        )
    )

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
        # TODO: upload rapidpe posterior samples
        canvas = gracedb.upload.si(
            None, None, superevent_id,
            'Online RapidPE-RIFT parameter estimation finished.', 'pe')
    else:
        raise NotImplementedError(f'Unknown PE pipeline {pe_pipeline}.')

    canvas.delay()

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

    """
    args = [
        "summarypages", "--webdir", webdir, "--samples", samples,
        "--gw", "--redshift_method", "exact", "--evolve_spins_fowards"
    ]
    for key in pesummary_kwargs:
        if key in ["psd", "calibration"]:
            args += [f"--{key}"]
            for ifo in pesummary_kwargs[key]:
                args += [f'{ifo}:{pesummary_kwargs[key][ifo]}']
        else:
            args += [f"--{key}", pesummary_kwargs[key]]
    if app.conf['gracedb_host'] != 'gracedb.ligo.org':
        queue = 'Online_PE_MDC'
    else:
        queue = 'Online_PE'
    return condor.check_output.si(
        args, request_memory=16000, request_disk=5000, queue=queue,
        accounting_group="ligo.dev.o4.cbc.pe.bilby",
        accounting_group_user="soichiro.morisaki",
    )


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
        modes = [app.conf['bilby_default_mode']]
        # add quick-bns mode if chirp mass is lower than that of 3Msun-3Msun
        if event['extra_attributes']['CoincInspiral']['mchirp'] < 2.62:
            modes += ['quick_bns']
        rundirs = [os.path.join(event_dir, m) for m in modes]
        kwargs_list = [{'bilby_mode': m} for m in modes]
        analyses = [f'{m} bilby' for m in modes]
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
