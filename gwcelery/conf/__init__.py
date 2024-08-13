"""GWCelery application configuration.

This module defines configuration variables and default values, including both
:doc:`generic options for Celery <celery:userguide/configuration>` as well as
options that control the behavior of specific GWCelery :mod:`~gwcelery.tasks`.

To override the configuration, define the ``CELERY_CONFIG_MODULE`` environment
variable to the fully qualified name of any Python module that can be located
in :obj:`sys.path`, including any of the following presets:

 * :mod:`gwcelery.conf.playground`
 * :mod:`gwcelery.conf.production`
 * :mod:`gwcelery.conf.test`
 * :mod:`gwcelery.conf.dev` (the default)
"""

import getpass
import os

from igwn_alert.client import DEFAULT_SERVER as DEFAULT_IGWN_ALERT_SERVER

# Celery application settings.

# Task tombstones expire after 2 hours.
# Celery's default setting of 1 day could cause the Redis database to grow too
# large because we pass large byte strings as task arguments and return values.
result_expires = 7200

# Use pickle serializer, because it supports byte values.
accept_content = ['json', 'pickle']
event_serializer = 'json'
result_serializer = 'pickle'
task_serializer = 'pickle'

# Compress tasks to reduce bandwidth in and out of Redis.
result_compression = task_compression = 'zstandard'

# Task priority settings.
task_inherit_parent_priority = True
task_default_priority = 0
task_queue_max_priority = 1
broker_transport_options = {
    'priority_steps': list(range(task_queue_max_priority + 1))
}

worker_proc_alive_timeout = 8
"""The timeout when waiting for a new worker process to start up."""

worker_log_format = "[%(asctime)s: %(levelname)s/%(processName)s/%(threadName)s] %(message)s"  # noqa: E501
"""Custom worker log format that includes the thread name."""

# GWCelery-specific settings.

condor_accounting_group = 'ligo.dev.o4.cbc.pe.bayestar'
"""HTCondor accounting group for Celery workers launched with condor_submit."""

expose_to_public = False
"""Set to True if events meeting the public alert threshold really should be
exposed to the public."""

igwn_alert_server = DEFAULT_IGWN_ALERT_SERVER
"""IGWN alert server"""

igwn_alert_noauth = False
"""IGWN alert server no-authetication"""

igwn_alert_group = 'gracedb-test01'
"""IGWN alert group."""

gracedb_host = 'gracedb-test01.igwn.org'
"""GraceDB host."""

gwskynet_upper_far_threshold = 1 / 21600
"""GWSkyNet will only annotate events with FARs below this value."""

gwskynet_snr_threshold = 4.5
"""GWSkyNet will only annotate events with individual SNRs above this value."""

gwskynet_network_snr_threshold = 7.0
"""GWSkyNet will only annotate events with network SNRs above this value."""

create_mattermost_channel = False
"""Do not create Mattermost channel."""

kafka_consumer_config = {
    'fermi_gbm_alert': {'url': 'kafka://kafka.gcn.nasa.gov/'
                        'gcn.classic.voevent.FERMI_GBM_ALERT',
                        'suffix': 'xml'},
    'fermi_gbm_flt_pos': {'url': 'kafka://kafka.gcn.nasa.gov/'
                          'gcn.classic.voevent.FERMI_GBM_FLT_POS',
                          'suffix': 'xml'},
    'fermi_gbm_gnd_pos': {'url': 'kafka://kafka.gcn.nasa.gov/'
                          'gcn.classic.voevent.FERMI_GBM_GND_POS',
                          'suffix': 'xml'},
    'fermi_gbm_fin_pos': {'url': 'kafka://kafka.gcn.nasa.gov/'
                          'gcn.classic.voevent.FERMI_GBM_FIN_POS',
                          'suffix': 'xml'},
    'fermi_gbm_subthresh': {'url': 'kafka://kafka.gcn.nasa.gov/'
                            'gcn.classic.voevent.FERMI_GBM_SUBTHRESH',
                            'suffix': 'xml'},
    'swift_bat_grb_pos_ack': {'url': 'kafka://kafka.gcn.nasa.gov/'
                              'gcn.classic.voevent.SWIFT_BAT_GRB_POS_ACK',
                              'suffix': 'xml'},
    'integral_wakeup': {'url': 'kafka://kafka.gcn.nasa.gov/'
                        'gcn.classic.voevent.INTEGRAL_WAKEUP',
                        'suffix': 'xml'},
    'integral_refined': {'url': 'kafka://kafka.gcn.nasa.gov/'
                         'gcn.classic.voevent.INTEGRAL_REFINED',
                         'suffix': 'xml'},
    'integral_offline': {'url': 'kafka://kafka.gcn.nasa.gov/'
                         'gcn.classic.voevent.INTEGRAL_OFFLINE',
                         'suffix': 'xml'},
    'snews': {'url': 'kafka://kafka.gcn.nasa.gov/gcn.classic.voevent.SNEWS',
              'suffix': 'xml'},
    'fermi_targeted': {'url': 'kafka://kafka.test.gcn.nasa.gov/'
                       'fermi.gbm.targeted.private.igwn', 'suffix': 'json'},
    'swift_targeted': {'url': 'kafka://kafka.gcn.nasa.gov/'
                       'gcn.notices.swift.bat.guano', 'suffix': 'json'}
}
"""Kafka consumer configuration details. The keys describe the senders of the
messages to be consumed. The values are a dictionary of the URL to listen to
and information about the message serializer. NOTE: We will switch the Swift
URL to a test topic once the topic is regularly written to."""

views_manual_preferred_event_log_message = 'User {} queued a preferred event' \
                                           ' change to {}.'
"""Log message that is uploaded to GraceDB when a user manually changes the
preferred event in the dashboard."""

voevent_broadcaster_address = ':5342'
"""The VOEvent broker will bind to this address to send GCNs.
This should be a string of the form `host:port`. If `host` is empty,
then listen on all available interfaces."""

voevent_broadcaster_whitelist = []
"""List of hosts from which the broker will accept connections.
If empty, then completely disable the broker's broadcast capability."""

voevent_receiver_address = '68.169.57.253:8099'
"""The VOEvent listener will connect to this address to receive GCNs. For
options, see `GCN's list of available VOEvent servers
<https://gcn.gsfc.nasa.gov/voevent.html#tc2>`_. If this is an empty string,
then completely disable the GCN listener."""

email_host = 'imap.gmail.com'
"""IMAP hostname to receive the GCN e-mail notice formats."""

superevent_d_t_start = {'gstlal': 1.0,
                        'spiir': 1.0,
                        'pycbc': 1.0,
                        'mbta': 1.0}
"""Pipeline based lower extent of superevent segments.
For cwb and lib this is decided from extra attributes."""

superevent_d_t_end = {'gstlal': 1.0,
                      'spiir': 1.0,
                      'pycbc': 1.0,
                      'mbta': 1.0}
"""Pipeline based upper extent of superevent segments
For cwb and lib this is decided from extra attributes."""

superevent_query_d_t_start = 100.
"""Lower extent of superevents query"""

superevent_query_d_t_end = 100.
"""Upper extent of superevents query"""

superevent_default_d_t_start = 1.0
"""Default lower extent of superevent segments"""

superevent_default_d_t_end = 1.0
"""Default upper extent for superevent segments"""

superevent_far_threshold = 1 / 3600
"""Maximum false alarm rate to consider events superevents."""

superevent_candidate_preference = {
    'cbc': {'allsky': 2, 'earlywarning': 1, 'ssm': 1, 'mdc': 1},
    'burst': {'allsky': 0, 'bbh': 0, 'mdc': 0},
}
"""Group/search preference for individual candidates. This is
used by :meth:`gwcelery.tasks.superevents.keyfunc` to sort
candidates for the preferred event before a ranking statistic is used."""

significant_alert_far_threshold = {
    'cbc': {
        'allsky': 1 / (30 * 86400),
        'earlywarning': 1 / (30 * 86400),
        'mdc': 1 / (30 * 86400),
        'ssm': 1 / (365 * 86400),
    },
    'burst': {
        'allsky': 1 / (365 * 86400),
        'bbh': 1 / (30 * 86400)
    },
    'test': {
        'allsky': 1 / (30 * 86400),
        'bbh': 1 / (30 * 86400),
        'earlywarning': 1 / (30 * 86400),
        'ssm': 1 / (365 * 86400)
    }
}
"""Group and search specific maximum false alarm rate to consider sending
significant alerts. A threshold of negative infinity disables alerts."""

significant_alert_trials_factor = {
    'cbc': {'allsky': 6,
            'earlywarning': 4,
            'mdc': 6,
            'ssm': 2},
    'burst': {'allsky': 3,
              'bbh': 6}
}
"""Trials factor corresponding to trigger categories. The CBC AllSky and Burst
BBH searches are treated as one group with a common trials factor. CBC AllSky
pipelines are gstlal, pycbc, mbta, spiir, and raven. The Burst BBH pipeline
is cwb. CBC EarlyWarning pipelines are gstlal, pycbc, mbta, and spiir.
CBC SSM pipelines are gstlal and mbta.
The Burst AllSky searches are treated as one group with one
trials factor. The Burst AllSky piplines are cwb, mly, and raven."""

preliminary_alert_trials_factor = {
    'cbc': {'allsky': 7,
            'earlywarning': 4,
            'mdc': 4,
            'ssm': 2},
    'burst': {'allsky': 7,
              'bbh': 7}
}
"""Trials factor for less significant alert categories. The CBC AllSky, Burst
AllSky, and Burst BBH searches are all treated as one group with a shared
trials factor. CBC AllSky pipelines are gstlal, pycbc, mbta, and spiir.
Burst AllSky pipelines are cwb, and mly. The Burst BBH pipelines is cwb."""

preliminary_alert_far_threshold = {
    'cbc': {
        'allsky': 2 / (1 * 86400) * preliminary_alert_trials_factor['cbc']['allsky'],  # noqa: E501
        'earlywarning': -1 * float('inf'),
        'mdc': -1 * float('inf'),
        'ssm': -1 * float('inf')
    },
    'burst': {
        'allsky': 2 / (1 * 86400) * preliminary_alert_trials_factor
        ['burst']['allsky'],
        'bbh': 2 / (1 * 86400) * preliminary_alert_trials_factor
        ['burst']['bbh']
    },
    'test': {
        'allsky': 2 / (1 * 86400) * preliminary_alert_trials_factor
        ['cbc']['allsky'],
        'earlywarning': -1 * float('inf'),
        'ssm': -1 * float('inf')
    }
}
"""Group and search specific maximum false alarm rate to consider sending less
significant alerts. Trials factors are included here to ensure events are sent
with the false alarm rate initially listed and removing trials factors are from
the threshold calculation. A threshold of negative infinity disables alerts."""

snews_gw_far_threshold = 1 / (3600 * 24)
"""Maximum false alarm rate for a superevent to send out a coincidence alert
between an external SNEWS alert and the superevent."""

sog_paper_far_threshold = {'gw': 1 / (10 * 365 * 86400),
                           'joint': 1 / (10000 * 365 * 86400)}
"""False alarm rate thresholds for producing a manuscript of speed of gravity
measurement in low-latency."""

superevent_clean_up_timeout = 270.
"""The orchestrator will wait this many seconds from the time of the
application of the GCN_PRELIM_SENT label to revise the preferred
event out of the accumulated events."""

pe_timeout = 345.0
"""The orchestrator will wait this many seconds from the time of the
creation of a new superevent to the time that parameter estimation begins, in
case the preferred event is updated with high latency."""

rapidpe_timeout = 30.0
"""The orchestrator will wait this many seconds from the time of the
creation of a new superevent to the time rapidpe parameter estimation begins,
in case the preferred event is updated with high latency."""


check_vector_prepost = {'gstlal': [2, 2],
                        'spiir': [2, 2],
                        'pycbc': [2, 2],
                        'MBTA': [2, 2],
                        'oLIB': [1.5, 1.5],
                        'LIB': [1.5, 1.5],
                        'CWB': [1.5, 1.5],
                        'MLy': [1.5, 1.5],
                        'HardwareInjection': [2, 2],
                        'Swift': [2, 2],
                        'Fermi': [2, 2],
                        'INTEGRAL': [2, 2],
                        'SNEWS': [10, 10]}
"""Seconds before and after the superevent start and end times which the DQ
vector check will include in its check. Pipeline dependent."""

uses_gatedhoft = {'gstlal': True,
                  'spiir': True,
                  'pycbc': True,
                  'MBTA': True,
                  'oLIB': False,
                  'LIB': False,
                  'CWB': True,
                  'MLy': False,
                  'HardwareInjection': False,
                  'Swift': False,
                  'Fermi': False,
                  'INTEGRAL': False,
                  'SNEWS': False}
"""Whether or not a pipeline uses gated h(t). Determines whether or not
the DMT-DQ_VECTOR will be analyzed for data quality."""

llhoft_glob = '/dev/shm/kafka/{detector}_O3ReplayMDC/*.gwf'
"""File glob for playground low-latency h(t) frames. Currently points
to O3 MDC Mock Data Challange data.
See https://git.ligo.org/emfollow/mock-data-challenge"""

llhoft_channels = {
    'H1:DMT-DQ_VECTOR': 'dmt_dq_vector_bits',
    'L1:DMT-DQ_VECTOR': 'dmt_dq_vector_bits',
    'H1:GDS-CALIB_STATE_VECTOR': 'ligo_state_vector_bits',
    'L1:GDS-CALIB_STATE_VECTOR': 'ligo_state_vector_bits',
    'V1:DQ_ANALYSIS_STATE_VECTOR': 'virgo_state_vector_bits'}
"""Low-latency h(t) state vector configuration. This is a dictionary consisting
of a channel and its bitmask, as defined in :mod:`gwcelery.tasks.detchar`."""

idq_ok_channels = ['H1:IDQ-OK_OVL_10_2048',
                   'L1:IDQ-OK_OVL_10_2048']
"""Low-latency iDQ OK channel names for O4. High bit indicates iDQ is ok."""

idq_channels = ['H1:IDQ-FAP_OVL_10_2048',
                'L1:IDQ-FAP_OVL_10_2048']
"""Low-latency iDQ false alarm probability channel names for O4."""

idq_fap_thresh = 0.01
"""If FAP is below this threshold, and
:obj:`~gwcelery.conf.idq_veto` for the pipeline is true, DQV will be labeled
for the event.
"""

idq_veto = {'gstlal': False,
            'spiir': False,
            'pycbc': False,
            'MBTA': False,
            'oLIB': False,
            'LIB': False,
            'CWB': False,
            'MLy': False,
            'HardwareInjection': False,
            'Swift': False,
            'Fermi': False,
            'INTEGRAL': False,
            'SNEWS': False}
"""If true for a pipeline, iDQ values below the threshold defined in
:obj:`~gwcelery.conf.idq_fap_thresh` will cause DQV to be labeled.
Currently all False, pending iDQ review (should be done before O3).
"""

low_latency_frame_types = {'H1': 'H1_O3ReplayMDC_llhoft',
                           'L1': 'L1_O3ReplayMDC_llhoft',
                           'V1': 'V1_O3ReplayMDC_llhoft'}
"""Types of low latency frames used in Parameter Estimation (see
:mod:`gwcelery.tasks.inference`) and in cache creation for detchar
checks (see :mod:`gwcelery.tasks.detchar`).
"""

high_latency_frame_types = {'H1': None,
                            'L1': None,
                            'V1': None}
"""Types of high latency frames used in Parameter Estimation and in cache
creation for detchar checks. They do not exist for O3Replay data. (see
:mod:`gwcelery.tasks.inference` and :mod:`gwcelery.tasks.detchar`)
"""

strain_channel_names = {'H1': 'H1:GDS-CALIB_STRAIN_INJ1_O3Replay',
                        'L1': 'L1:GDS-CALIB_STRAIN_INJ1_O3Replay',
                        'V1': 'V1:Hrec_hoft_16384Hz_INJ1_O3Replay'}
"""Names of h(t) channels used in Parameter Estimation (see
:mod:`gwcelery.tasks.inference`) and in detchar omegascan creation
(see :mod:`gwcelery.tasks.detchar`)."""

state_vector_channel_names = {'H1': 'H1:GDS-CALIB_STATE_VECTOR',
                              'L1': 'L1:GDS-CALIB_STATE_VECTOR',
                              'V1': 'V1:DQ_ANALYSIS_STATE_VECTOR'}
"""Names of state vector channels used in Parameter Estimation (see
:mod:`gwcelery.tasks.inference`)"""

detchar_bit_definitions = {
    'dmt_dq_vector_bits': {
        'channel': 'DMT-DQ_VECTOR',
        'bits': {
            1: 'NO_OMC_DCPD_ADC_OVERFLOW',
            2: 'NO_DMT-ETMY_ESD_DAC_OVERFLOW'
        },
        'description': {
            'NO_OMC_DCPD_ADC_OVERFLOW': 'OMC DCPC ADC not overflowing',
            'NO_DMT-ETMY_ESD_DAC_OVERFLOW': 'ETMY ESD DAC not overflowing'
        }
    },
    'ligo_state_vector_bits': {
        'channel': 'GDS-CALIB_STATE_VECTOR',
        'bits': {
            0: 'HOFT_OK',
            1: 'OBSERVATION_INTENT',
            5: 'NO_STOCH_HW_INJ',
            6: 'NO_CBC_HW_INJ',
            7: 'NO_BURST_HW_INJ',
            8: 'NO_DETCHAR_HW_INJ'
        },
        'description': {
            'HOFT_OK': 'h(t) was successfully computed',
            'OBSERVATION_INTENT': '"observation intent" button is pushed',
            'NO_STOCH_HW_INJ': 'No stochastic HW injection',
            'NO_CBC_HW_INJ': 'No CBC HW injection',
            'NO_BURST_HW_INJ': 'No burst HW injection',
            'NO_DETCHAR_HW_INJ': 'No HW injections for detector characterization'  # noqa: E501
        }
    },
    'virgo_state_vector_bits': {
        'channel': 'DQ_ANALYSIS_STATE_VECTOR',
        'bits': {
            0: 'HOFT_OK',
            1: 'OBSERVATION_INTENT',
            5: 'NO_STOCH_HW_INJ',
            6: 'NO_CBC_HW_INJ',
            7: 'NO_BURST_HW_INJ',
            8: 'NO_DETCHAR_HW_INJ',
            10: 'GOOD_DATA_QUALITY_CAT1'
        },
        'description': {
            'HOFT_OK': 'h(t) was successfully computed',
            'OBSERVATION_INTENT': '"observation intent" button is pushed',
            'NO_STOCH_HW_INJ': 'No stochastic HW injection',
            'NO_CBC_HW_INJ': 'No CBC HW injection',
            'NO_BURST_HW_INJ': 'No burst HW injection',
            'NO_DETCHAR_HW_INJ': 'No HW injections for detector characterization',  # noqa: E501
            'GOOD_DATA_QUALITY_CAT1': 'Good data quality (CAT1 type)'
        }
    }
}
"""Bit definitions for detchar checks"""

omegascan_durations = [(0.75, 0.25), (1.5, 0.5), (7.5, 2.5)]
"""Durations for omegascans, before and after t0"""

pe_results_path = os.path.join(os.getenv('HOME'), 'public_html/online_pe')
"""Path to the results of Parameter Estimation (see
:mod:`gwcelery.tasks.inference`)"""

pe_results_url = ('https://ldas-jobs.ligo.caltech.edu/~{}/'
                  'online_pe/').format(getpass.getuser())
"""URL of page where all the results of Parameter Estimation are outputted
(see :mod:`gwcelery.tasks.inference`)"""

raven_coincidence_windows = {'GRB_CBC': [-5, 1],
                             'GRB_CBC_SubFermi': [-11, 1],
                             'GRB_CBC_SubSwift': [-20, 10],
                             'GRB_Burst': [-600, 60],
                             'SNEWS': [-10, 10]}
"""Time coincidence windows passed to ligo-raven. External events and
superevents of the appropriate type are considered to be coincident if
within time window of each other."""

raven_ext_rates = {
    'GRB': 305 / (3600 * 24 * 365),    # 305 / yr
    'MDC': 305 / (3600 * 24 * 365),    # 305 / yr
    'SubGRB': 370 / (3600 * 24 * 365)  # 370 / yr
}
"""Expected rates of astrophysical external triggers submitted for each search.
These potentially include multiple satellites and experiments (e.g. for GRBs,
Fermi+Swift+INTEGRAL), accounting for multiple detections of the same
astrophysical event. For more details on methodology, see:
https://dcc.ligo.org/T1900297."""

raven_targeted_far_thresholds = {
    'GW': {
        'Fermi': preliminary_alert_far_threshold['cbc']['allsky'],
        'Swift': preliminary_alert_far_threshold['cbc']['allsky']
    },
    'GRB': {
        'Fermi': 1 / 10000,
        'Swift': 1 / 1000
    }
}
"""Max FAR thresholds used for the subthreshold targeted searches with Fermi
and Swift. Since we only listen to CBC low significance alerts, we use that
FAR threshold for now. Note that Swift current listens to events with the
threshold before and Fermi after trials factors."""

external_search_preference = {
    'Supernova': 2,
    'GRB': 1,
    'FRB': 1,
    'HEN': 1,
    'SubGRB': 0,
    'SubGRBTargeted': 0
}
"""Group/search preference for individual candidates. This is
used by :meth:`gwcelery.tasks.raven.keyfunc` to sort
candidates for the preferred external event. We first prefer coincidences with
SNEWS Supernova since their significance is so high (far<1/100 yrs). Next, we
prefer events significant enough to be publishable on their own, which
includes gamma-ray bursts, high energy neutrinos, and fast radio bursts.
Lastly we prefer lower significance subthreshold events, some of which are not
publishable on their own."""

mock_events_simulate_multiple_uploads = False
"""If True, then upload each mock event several times in rapid succession with
random jitter in order to simulate multiple pipeline uploads."""

only_alert_for_mdc = False
"""If True, then only sends alerts for MDC events. Useful for times outside
of observing runs."""

joint_mdc_freq = 2
"""Determines how often an external MDC event will be created near an
MDC superevent to test the RAVEN alert pipeline, i.e for every x
MDC superevents an external MDC event is created."""

joint_O3_replay_freq = 10
"""Determines how often an external replay event will be created near an
superevent to test the RAVEN alert pipeline, i.e for every x
O3 replay superevents an external MDC event is created."""

condor_retry_kwargs = dict(
    max_retries=80, retry_backoff=True, retry_jitter=False,
    retry_backoff_max=600
)
"""Retry settings of condor.submit task. With these settings, a condor job is
no longer tracked ~12 hours after it starts."""

rapidpe_condor_retry_kwargs = dict(
    max_retries=125, retry_backoff=True, retry_jitter=False,
    retry_backoff_max=30
)
"""Retry settings of condor.submit_rapidpe task. With these settings, a
condor job is no longer tracked ~1 hours after it starts. This is used
for RapidPE."""

rapidpe_settings = {
    'run_mode': 'online',
    'accounting_group': 'ligo.dev.o4.cbc.pe.lalinferencerapid',
    'use_cprofile': True,
}
"""Config settings used for rapidpe"""

# Delete imported modules so that they do not pollute the config object
del os, getpass
