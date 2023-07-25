"""Application configuration for ``gracedb.ligo.org``.

Inherits all settings from :mod:`gwcelery.conf.playground`, with the exceptions
below.
"""

from base64 import b64encode

from . import *  # noqa: F401, F403

condor_accounting_group = 'ligo.prod.o4.cbc.pe.bayestar'
"""HTCondor accounting group for Celery workers launched with condor_submit."""

expose_to_public = True
"""Set to True if events meeting the public alert threshold really should be
exposed to the public."""

igwn_alert_group = 'gracedb'
"""IGWN alert group."""

gracedb_host = 'gracedb.ligo.org'
"""GraceDB host."""

create_mattermost_channel = True
"""Create Mattermost channel in production"""

kafka_alert_config = {
    'scimma': {'url': 'kafka://kafka.scimma.org/igwn.gwalert',
               'suffix': 'avro', 'skymap_encoder': lambda _: _},
    'gcn': {'url': 'kafka://kafka.gcn.nasa.gov/igwn.gwalert',
            'suffix': 'json', 'skymap_encoder': lambda b:
            b64encode(b).decode('utf-8')}
}
"""Kafka broker configuration details"""

kafka_consumer_config = {
    'fermi': {'url': 'kafka://kafka.gcn.nasa.gov/'
              'fermi.gbm.targeted.private.igwn', 'suffix': 'json'},
    'swift': {'url': 'kafka://kafka.gcn.nasa.gov/'
              'gcn.notices.swift.bat.guano', 'suffix': 'json'}
}
"""Kafka consumer configuration details. The keys describe the senders of the
messages to be consumed. The values are a dictionary of the URL to listen to
and information about the message serializer."""

voevent_broadcaster_address = ':5341'
"""The VOEvent broker will bind to this address to send GCNs.
This should be a string of the form `host:port`. If `host` is empty,
then listen on all available interfaces."""

voevent_broadcaster_whitelist = ['capella2.gsfc.nasa.gov']
"""List of hosts from which the broker will accept connections.
If empty, then completely disable the broker's broadcast capability."""

llhoft_glob = '/dev/shm/kafka/{detector}/*.gwf'
"""File glob for low-latency h(t) frames."""

low_latency_frame_types = {'H1': 'H1_llhoft',
                           'L1': 'L1_llhoft',
                           'V1': 'V1_llhoft'}
"""Types of frames used in Parameter Estimation (see
:mod:`gwcelery.tasks.inference`) and in cache creation for detchar
checks (see :mod:`gwcelery.tasks.detchar`).
"""

high_latency_frame_types = {'H1': 'H1_HOFT_C00',
                            'L1': 'L1_HOFT_C00',
                            'V1': 'V1Online'}
"""Types of high latency frames used in Parameter Estimation
(see :mod:`gwcelery.tasks.inference`) and in cache creation for detchar
checks (see :mod:`gwcelery.tasks.detchar`).
"""

idq_channels = ['H1:IDQ-FAP_OVL_10_2048',
                'L1:IDQ-FAP_OVL_10_2048']
"""Low-latency iDQ false alarm probability channel names from live O3 frames"""

strain_channel_names = {'H1': 'H1:GDS-CALIB_STRAIN_CLEAN',
                        'L1': 'L1:GDS-CALIB_STRAIN_CLEAN',
                        'V1': 'V1:Hrec_hoft_16384Hz'}
"""Names of h(t) channels used in Parameter Estimation (see
:mod:`gwcelery.tasks.inference`)"""

sentry_environment = 'production'
"""Record this `environment tag
<https://docs.sentry.io/enriching-error-data/environments/>`_ in Sentry log
messages."""

only_alert_for_mdc = False
"""If True, then only sends alerts for MDC events. Useful for times outside
of observing runs."""

condor_retry_kwargs = dict(
    max_retries=None, retry_backoff=True, retry_jitter=True,
    retry_backoff_max=600
)
"""Retry settings of condor.submit task."""
