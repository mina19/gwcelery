"""Application configuration for ``minikube`` local installation."""

from . import *  # noqa: F401, F403

expose_to_public = True
"""Set to True if events meeting the public alert threshold really should be
exposed to the public."""

igwn_alert_server = 'kafka://hopskotch-server'
"""IGWN alert server: None == DEFAULT_SERVER"""

igwn_alert_noauth = True
"""IGWN alert server no-authetication"""

igwn_alert_group = 'default'
"""IGWN alert group."""

gracedb_host = 'gracedb.default.svc.cluster.local'
"""GraceDB host."""

early_warning_alert_far_threshold = float('inf')
"""False alarm rate threshold for early warning alerts."""

mock_events_simulate_multiple_uploads = False
"""If True, then upload each mock event several times in rapid succession with
random jitter in order to simulate multiple pipeline uploads."""

kafka_alert_config = {
    'scimma': {'url': 'kafka://hopskotch-server/igwn.gwalert-minikube',
               'suffix': 'avro', 'skymap_encoder': lambda _: _}
}
"""Kafka broker configuration details"""
