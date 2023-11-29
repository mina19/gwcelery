"""Application configuration for ``gracedb-test01.ligo.org``.

Inherits all settings from :mod:`gwcelery.conf.playground`, with the exceptions
below.
"""

from . import *  # noqa: F401, F403

igwn_alert_group = 'gracedb-test01'
"""IGWN alert group."""

gracedb_host = 'gracedb-test01.igwn.org'
"""GraceDB host."""

kafka_alert_config = {
    'scimma': {'url': 'kafka://kafka.scimma.org/igwn.gwalert-test01',
               'suffix': 'avro', 'skymap_encoder': lambda _: _}
}
"""Kafka broker configuration details"""

sentry_environment = 'dev'
"""Record this `environment tag
<https://docs.sentry.io/enriching-error-data/environments/>`_ in Sentry log
messages."""

mock_events_simulate_multiple_uploads = True
"""If True, then upload each mock event several times in rapid succession with
random jitter in order to simulate multiple pipeline uploads."""

idq_channels = ['H1:IDQ-FAP_OVL_16_4096',
                'L1:IDQ-FAP_OVL_16_4096']
"""Low-latency iDQ false alarm probability channel names for O3 replay."""

rapidpe_settings = {
    'run_mode': 'o3replay',
    'accounting_group': 'ligo.dev.o4.cbc.pe.lalinferencerapid',
    'use_cprofile': True,
}
"""Config settings used for rapidpe"""
