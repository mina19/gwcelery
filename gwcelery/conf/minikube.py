"""Application configuration for ``minikube`` local installation."""

import os

from . import *  # noqa: F401, F403

expose_to_public = True
"""Set to True if events meeting the public alert threshold really should be
exposed to the public."""

gracedb_host = os.getenv('GRACEDB_HOSTNAME',
                         'gracedb.default.svc.cluster.local')
"""GraceDB host."""


igwn_alert_server = os.getenv('IGWN_HOSTNAME',
                              'kafka://hopskotch-server')
"""IGWN alert server: None == DEFAULT_SERVER"""

igwn_alert_noauth = True
"""IGWN alert server no-authetication"""

igwn_alert_group = 'default'
"""IGWN alert group."""

mock_events_simulate_multiple_uploads = False
"""If True, then upload each mock event several times in rapid succession with
random jitter in order to simulate multiple pipeline uploads."""

kafka_consumer_config = {
}
"""Kafka consumer configuration details. The keys describe the senders of the
messages to be consumed. The values are a dictionary of the URL to listen to
and information about the message serializer."""

kafka_alert_server = os.getenv('KAFKA_HOSTNAME',
                               'kafka://hopskotch-server')
kafka_alert_config = {
    'scimma': {'url': kafka_alert_server + '/igwn.gwalert-minikube',
               'suffix': 'avro', 'skymap_encoder': lambda _: _,
               'auth': False}
}
"""Kafka broker configuration details"""
