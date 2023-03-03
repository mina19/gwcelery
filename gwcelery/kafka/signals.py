"""Definitions of custom :doc:`Celery signals <celery:userguide/signals>`
related to Kafka messages.
"""
from celery.utils.dispatch import Signal

kafka_record_consumed = Signal(
    name='kafka_record_consumed', providing_args=('name', 'record'))
"""Fired whenever a Kafka record is received.

Parameters
----------
name : str
    The name (key) of the Kafka configuration associated with the topic the
    message was received from, e.g.  ``fermi_swift``.
record : dict
    The deserialized contents of the message.
"""
