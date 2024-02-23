"""Application configuration for ``gracedb-playground.ligo.org``."""

from base64 import b64encode

from . import *  # noqa: F401, F403

sentry_environment = 'playground'
"""Record this `environment tag
<https://docs.sentry.io/enriching-error-data/environments/>`_ in Sentry log
messages."""

igwn_alert_group = 'gracedb-playground'
"""IGWN alert group."""

gracedb_host = 'gracedb-playground.ligo.org'
"""GraceDB host."""

mock_events_simulate_multiple_uploads = True
"""If True, then upload each mock event several times in rapid succession with
random jitter in order to simulate multiple pipeline uploads."""

kafka_alert_config = {
    'scimma': {'url': 'kafka://kafka.scimma.org/igwn.gwalert-playground',
               'suffix': 'avro', 'skymap_encoder': lambda _: _},
    'gcn': {'url': 'kafka://kafka.test.gcn.nasa.gov/igwn.gwalert',
            'suffix': 'json', 'skymap_encoder': lambda b:
            b64encode(b).decode('utf-8')}
}
"""Kafka broker configuration details"""

voevent_broadcaster_address = ':5341'
"""The VOEvent broker will bind to this address to send GCNs.
This should be a string of the form `host:port`. If `host` is empty,
then listen on all available interfaces."""

voevent_broadcaster_whitelist = ['capella2.gsfc.nasa.gov']
"""List of hosts from which the broker will accept connections.
If empty, then completely disable the broker's broadcast capability."""

voevent_receiver_address = '50.116.49.68:8094'
"""The VOEvent listener will connect to this address to receive GCNs. For
options, see `GCN's list of available VOEvent servers
<https://gcn.gsfc.nasa.gov/voevent.html#tc2>`_. If this is an empty string,
then completely disable the GCN listener."""

idq_ok_channels = ['H1:IDQ-OK_OVL_16_4096',
                   'L1:IDQ-OK_OVL_16_4096']
"""Low-latency iDQ OK channel names for O3 replay."""

idq_channels = ['H1:IDQ-FAP_OVL_16_4096',
                'L1:IDQ-FAP_OVL_16_4096']
"""Low-latency iDQ false alarm probability channel names for O3 replay."""

rapidpe_settings = {
    'run_mode': 'o3replay',
    'accounting_group': 'ligo.dev.o4.cbc.pe.lalinferencerapid',
    'use_cprofile': False,
}
"""Config settings used for rapidpe"""
