"""Embed a Kafka producer into a Celery worker by
:doc:`extending Celery with bootsteps <celery:userguide/extending>`.
"""
from .bootsteps import Producer, Consumer


def install(app):
    """Register the Kafka subsystem in the application boot steps."""
    app.steps['consumer'] |= {Producer, Consumer}
