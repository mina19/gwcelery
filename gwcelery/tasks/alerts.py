import json

import numpy as np
from astropy import time
from celery import group
from celery.utils.log import get_logger

from .. import app
from ..kafka.signals import kafka_record_consumed
from . import gracedb
from .core import DispatchHandler

log = get_logger(__name__)

# FIXME Remove this once cwb bbh is uploading to cbc instead of burst
CUSTOM_EVENT_GROUP_TO_NOTICE_GROUP_MAP = {
    'Burst': {'BBH': 'CBC'}
}


class _KafkaDispatchHandler(DispatchHandler):

    def process_args(self, name, record):
        return name, (record,), {}

    def __call__(self, *keys, **kwargs):
        r"""Create a new task and register it as a callback for handling the
        given keys.

        Parameters
        ----------
        \*keys : list
            Keys to match
        \*\*kwargs
            Additional keyword arguments for `celery.Celery.task`.

        """
        def wrap(f):
            f = gracedb.task(ignore_result=True, **kwargs)(f)
            for key in keys:
                self.setdefault(key, []).append(f)
            return f

        return wrap


handler = _KafkaDispatchHandler()
r"""Function decorator to register a handler callback for specified Kafka URLs.
The decorated function is turned into a Celery task, which will be
automatically called whenever a message is received from a matching URL.

Parameters
----------
\*keys
    List of keys from :obj:`gwcelery.conf.kafka_consumer_config`
    associated with Kafka topics to listen to messages to.
\*\*kwargs
    Additional keyword arguments for :meth:`celery.Celery.task`.

Examples
--------
Declare a new handler like this::

    # Assumes kafka_consumer_config dictionary has 'fermi_swift' key
    @alerts.handler('fermi_swift')
    def handle_swift(record):
        # record is a dict that contains the contents of the message
        # do work here...
"""


@kafka_record_consumed.connect
def _on_kafka_record_consumed(name, record, **kwargs):
    handler.dispatch(name, record)


def _create_base_alert_dict(classification, superevent, alert_type):
    '''Create the base of the alert dictionary, with all contents except the
    skymap and the external coinc information.'''
    # NOTE Everything that comes through this code path will be marked as
    # public. However, MDC events with this flag are not made public on
    # GraceDB-playground and GraceDB-test.
    # Re time_created: Dont need better than second precision for alert times

    # FIXME Dont change alert types internally
    # NOTE less-significant alerts have alert_type as PRELIMINARY
    alert_type_kafka = 'preliminary' if alert_type == 'less-significant' \
        else alert_type
    # NOTE the alert group is usually the same as the g-event group. Exceptions
    # are recorded in the CUSTOM_EVENT_GROUP_TO_NOTICE_GROUP_MAP definition
    # above
    superevent_group = superevent['preferred_event_data']['group']
    superevent_search = superevent['preferred_event_data']['search']
    if superevent_group in CUSTOM_EVENT_GROUP_TO_NOTICE_GROUP_MAP and \
            superevent_search in \
            CUSTOM_EVENT_GROUP_TO_NOTICE_GROUP_MAP[superevent_group]:
        alert_group_kafka = \
            CUSTOM_EVENT_GROUP_TO_NOTICE_GROUP_MAP[
                superevent_group
            ][superevent_search]
    else:
        alert_group_kafka = superevent['preferred_event_data']['group']

    alert_dict = {
        'alert_type': alert_type_kafka.upper(),
        'time_created': time.Time.now().utc.isot.split('.')[0] + 'Z',
        'superevent_id': superevent['superevent_id'],
        'urls': {'gracedb': superevent['links']['self'].replace('api/', '') +
                 'view/'},
        'event': None,
        'external_coinc': None
    }

    if alert_type == 'retraction':
        return alert_dict

    if classification and classification[0] is not None:
        properties = json.loads(classification[0])
    else:
        properties = {}

    if classification and classification[1] is not None:
        classification = json.loads(classification[1])
    else:
        classification = {}

    duration = None
    central_frequency = None

    if alert_group_kafka == 'Burst':
        if superevent['preferred_event_data']['pipeline'].lower() == 'cwb':
            duration = \
                superevent['preferred_event_data']['extra_attributes'].get(
                    'MultiBurst', {}).get('duration', None)
            central_frequency = \
                superevent['preferred_event_data']['extra_attributes'].get(
                    'MultiBurst', {}).get('central_freq', None)
        elif superevent['preferred_event_data']['pipeline'].lower() == 'mly':
            duration = \
                superevent['preferred_event_data']['extra_attributes'].get(
                    'MLyBurst', {}).get('duration', None)
            central_frequency = \
                superevent['preferred_event_data']['extra_attributes'].get(
                    'MLyBurst', {}).get('central_freq', None)
        elif superevent['preferred_event_data']['pipeline'].lower() == 'olib':
            quality_mean = \
                superevent['preferred_event_data']['extra_attributes'].get(
                     'LalInferenceBurst', {}).get('quality_mean', None)
            frequency_mean = \
                superevent['preferred_event_data']['extra_attributes'].get(
                     'LalInferenceBurst', {}).get('frequency_mean', None)
            central_frequency = \
                superevent['preferred_event_data']['extra_attributes'].get(
                     'LalInferenceBurst', {}).get('frequency_mean', None)
            duration = quality_mean / (2 * np.pi * frequency_mean)
        else:
            raise NotImplementedError(
                'Duration and central_frequency not implemented for Burst '
                'pipeline {}'.format(
                    superevent['preferred_event_data']['pipeline'].lower()
                )
            )

    alert_dict['event'] = {
        # set 'significant' field based on
        # https://dcc.ligo.org/LIGO-G2300151/public
        'significant': False if alert_type == 'less-significant' else True,
        'time': time.Time(superevent['t_0'], format='gps').utc.isot + 'Z',
        'far': superevent['far'],
        'instruments': sorted(
            superevent['preferred_event_data']['instruments'].split(',')
        ),
        'group': alert_group_kafka,
        'pipeline': superevent['preferred_event_data']['pipeline'],
        'search': superevent_search,
        'properties': properties,
        'classification': classification,
        'duration': duration,
        'central_frequency': central_frequency
    }

    return alert_dict


@gracedb.task(shared=False)
def _add_external_coinc_to_alert(alert_dict, superevent,
                                 combined_skymap_filename):
    external_event = gracedb.get_event(superevent['em_type'])
    if combined_skymap_filename:
        combined_skymap = gracedb.download(combined_skymap_filename,
                                           superevent['superevent_id'])
    else:
        combined_skymap = None
    alert_dict['external_coinc'] = {
        'gcn_notice_id':
            int(external_event['extra_attributes']['GRB']['trigger_id']),
        'ivorn': external_event['extra_attributes']['GRB']['ivorn'],
        'observatory': external_event['pipeline'],
        'search': external_event['search'],
        'time_difference': round(external_event['gpstime'] -
                                 superevent['t_0'], 2),
        'time_coincidence_far': superevent['time_coinc_far'],
        'time_sky_position_coincidence_far': superevent['space_coinc_far']
    }

    return alert_dict, combined_skymap


@app.task(bind=True, shared=False, queue='kafka', ignore_result=True)
def _upload_notice(self, payload, brokerhost, superevent_id):
    '''
    Upload serialized alert notice to GraceDB
    '''
    config = self.app.conf['kafka_alert_config'][brokerhost]
    kafka_writer = self.app.conf['kafka_streams'][brokerhost]

    # FIXME Drop get_payload_content method once
    # https://github.com/scimma/hop-client/pull/190 is merged
    alert_dict = kafka_writer.get_payload_content(payload)
    message = 'Kafka alert notice sent to {}'.format(config['url'])

    filename = '{}-{}.{}'.format(
        alert_dict['superevent_id'],
        alert_dict['alert_type'].lower(),
        config['suffix']
    )

    gracedb.upload.delay(payload.serialize()['content'], filename,
                         superevent_id, message, tags=['public', 'em_follow'])


@app.task(bind=True, queue='kafka', shared=False)
def _send(self, alert_dict, skymap, brokerhost, combined_skymap=None):
    """Write the alert to the Kafka topic"""
    # Copy the alert dictionary so we dont modify the original
    payload_dict = alert_dict.copy()
    # Add skymap to alert_dict
    config = self.app.conf['kafka_alert_config'][brokerhost]
    if alert_dict['event'] is not None:
        # dict.copy is a shallow copy, so need to copy event dict as well since
        # we plan to modify it
        payload_dict['event'] = alert_dict['event'].copy()

        # Encode the skymap
        encoder = config['skymap_encoder']
        payload_dict['event']['skymap'] = encoder(skymap)

        if combined_skymap:
            payload_dict['external_coinc']['combined_skymap'] = \
                encoder(combined_skymap)

    # Write to kafka topic
    serialization_model = \
        self.app.conf['kafka_streams'][brokerhost].serialization_model
    payload = serialization_model(payload_dict)
    self.app.conf['kafka_streams'][brokerhost].write(payload)

    return payload


@app.task(bind=True, queue='kafka', shared=False)
def _send_with_combined(self, alert_dict_combined_skymap, skymap, brokerhost):
    alert_dict, combined_skymap = alert_dict_combined_skymap
    return _send(alert_dict, skymap, brokerhost,
                 combined_skymap=combined_skymap)


@app.task(bind=True, ignore_result=True, queue='kafka', shared=False)
def send(self, skymap_and_classification, superevent, alert_type,
         raven_coinc=False, combined_skymap_filename=None):
    """Send an public alert to all currently connected kafka brokers.

    Parameters
    ----------
    skymap_and_classification : tuple, None
        The filecontents of the skymap followed by a collection of JSON
        strings. The former generated by
        :meth:`gwcelery.tasks.gracedb.download`, the latter generated by
        :meth:`gwcelery.tasks.em_bright.classifier` and
        :meth:`gwcelery.tasks.p_astro.compute_p_astro` or content of
        ``{gstlal,mbta}.p_astro.json`` uploaded by {gstlal,mbta} respectively.
        Can also be None.
    superevent : dict
        The superevent dictionary, typically obtained from an IGWN Alert or
        from querying GraceDB.
    alert_type : str
        The alert type. Either of {`less-significant`, `earlywarning`,
        `preliminary`, `initial`, `update`}.
    raven_coinc: bool
        Is there a coincident external event processed by RAVEN?
    combined_skymap_filename : str
        Combined skymap filename. Default None.

    Notes
    -----
    The `alert_type` value is used to set the `significant` field in the
    alert dictionary.
    """

    if skymap_and_classification is not None:
        skymap, *classification = skymap_and_classification
    else:
        skymap = None
        classification = None

    alert_dict = _create_base_alert_dict(
        classification,
        superevent,
        alert_type
    )

    if raven_coinc and alert_type != 'retraction':
        canvas = (
            _add_external_coinc_to_alert.si(
                alert_dict,
                superevent,
                combined_skymap_filename
            )
            |
            group(
                (
                    _send_with_combined.s(skymap, brokerhost)
                    |
                    _upload_notice.s(brokerhost, superevent['superevent_id'])
                ) for brokerhost in self.app.conf['kafka_streams'].keys()
            )
        )
    else:
        canvas = (
            group(
                (
                    _send.s(alert_dict, skymap, brokerhost)
                    |
                    _upload_notice.s(brokerhost, superevent['superevent_id'])
                ) for brokerhost in self.app.conf['kafka_streams'].keys()
            )
        )

    canvas.apply_async()


@app.task(shared=False)
def _create_skymap_classification_tuple(skymap, classification):
    return (skymap, *classification)


@app.task(shared=False, ignore_result=True)
def download_skymap_and_send_alert(classification, superevent, alert_type,
                                   skymap_filename=None, raven_coinc=False,
                                   combined_skymap_filename=None):
    """Wrapper for send function when caller has not already downloaded the
    skymap.

    Parameters
    ----------
    classification : tuple, None
        A collection of JSON strings, generated by
        :meth:`gwcelery.tasks.em_bright.classifier` and
        :meth:`gwcelery.tasks.p_astro.compute_p_astro` or
        content of ``{gstlal,mbta}.p_astro.json`` uploaded by {gstlal,mbta}
        respectively; or None
    superevent : dict
        The superevent dictionary, typically obtained from an IGWN Alert or
        from querying GraceDB.
    alert_type : {'earlywarning', 'preliminary', 'initial', 'update'}
        The alert type.
    skymap_filename : string
        The skymap filename.
    raven_coinc: bool
        Is there a coincident external event processed by RAVEN?
    combined_skymap_filename : str
        The combined skymap filename. Default None
    """

    if skymap_filename is not None and alert_type != 'retraction':
        canvas = (
            gracedb.download.si(
                skymap_filename,
                superevent['superevent_id']
            )
            |
            _create_skymap_classification_tuple.s(classification)
            |
            send.s(superevent, alert_type, raven_coinc=raven_coinc,
                   combined_skymap_filename=combined_skymap_filename)
        )
    else:
        canvas = send.s(
            (None, classification),
            superevent,
            alert_type,
            raven_coinc=raven_coinc,
            combined_skymap_filename=combined_skymap_filename
        )

    canvas.apply_async()
