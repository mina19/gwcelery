import json
from functools import cache
from os import path
from threading import Thread

from celery import bootsteps
from celery.concurrency import solo
from celery.utils.log import get_logger
from confluent_kafka.error import KafkaException
from fastavro.schema import parse_schema
from hop import Stream, auth
from hop.io import list_topics
from hop.models import AvroBlob, JSONBlob, VOEvent
from xdg.BaseDirectory import xdg_config_home

from ..util import read_json
from .signals import kafka_record_consumed

__all__ = ('Producer', 'Consumer')

log = get_logger(__name__)


@cache
def schema():
    # The order does not matter other than the Alert schema must be loaded last
    # because it references the other schema. All of the schema are saved in
    # named_schemas, but we only need to save a reference to the the Alert
    # schema to write the packet.
    # NOTE Specifying expand=True when calling parse_schema is okay when only
    # one schema contains references to other schema, in our case only the
    # alerts schema contains references to other schema. More complicated
    # relationships between schema though can lead to behavior that does not
    # conform to the avro spec, and a different method will need to be used to
    # load the schema. See https://github.com/fastavro/fastavro/issues/624 for
    # more info.
    named_schemas = {}
    for s in ['igwn.alerts.v1_0.ExternalCoincInfo.avsc',
              'igwn.alerts.v1_0.EventInfo.avsc',
              'igwn.alerts.v1_0.AlertType.avsc',
              'igwn.alerts.v1_0.Alert.avsc']:
        schema = parse_schema(read_json('igwn_gwalert_schema', s),
                              named_schemas, expand=True)

    return schema


def _load_hopauth_map():
    hop_auth = auth.load_auth()
    with open(path.join(xdg_config_home,
                        'gwcelery/kafka_credential_map.json'),
              'r') as fo:
        kafka_credential_map = json.load(fo)

    return hop_auth, kafka_credential_map


class AvroBlobWrapper(AvroBlob):

    def __init__(self, payload):
        return super().__init__([payload], schema())


class KafkaBase:

    def __init__(self, name, config, prefix):
        self.name = name
        self._config = config
        if config.get('auth') is not False:
            # Users only add auth to config to disable authentication
            self._credential = self.get_auth(prefix)
        else:
            # Dont use credentials
            self._credential = False
        self._hop_stream = Stream(self._credential)

        # FIXME Drop get_payload_content method once
        # https://github.com/scimma/hop-client/pull/190 is merged
        if config['suffix'] == 'avro':
            self.serialization_model = AvroBlobWrapper
            self.get_payload_content = lambda payload: payload.content[0]
        elif config['suffix'] == 'json':
            self.serialization_model = JSONBlob
            self.get_payload_content = lambda payload: payload.content
        elif config['suffix'] == 'xml':
            self.serialization_model = VOEvent
            self.get_payload_content = lambda payload: payload.content
        else:
            raise NotImplementedError(
                'Supported serialization method required for alert notices'
            )

    def get_auth(self, prefix):
        hop_auth, kafka_credential_map = _load_hopauth_map()

        # kafka_credential_map contains map between logical name for broker
        # topic and username
        username = kafka_credential_map.get(prefix, {}).get(self.name)
        if username is None:
            raise ValueError('Unable to find {} entry in kafka credential map '
                             'for {}'.format(prefix, self.name))

        # hop auth contains map between username and password/hostname
        target_auth = None
        for cred in hop_auth:
            if cred.username != username:
                continue
            target_auth = cred
            break
        else:
            raise ValueError('Unable to find entry in hop auth file for '
                             'username {}'.format(username))
        return target_auth


class KafkaListener(KafkaBase):

    def __init__(self, name, config):
        super().__init__(name, config, 'consumer')
        self._open_hop_stream = None
        self.running = False
        # Don't kill worker if listener can't connect
        try:
            self._open_hop_stream = self._hop_stream.open(config['url'], 'r')
        except KafkaException:
            log.exception('Connection to %s failed', self._config["url"])
        except ValueError:
            # Hop client will return a ValueError if the topic doesn't exist on
            # the broker
            log.exception('Connection to %s failed', self._config["url"])

    def listen(self):
        self.running = True
        # Restart the consumer when non-fatal errors come up, similar to
        # gwcelery.igwn_alert.IGWNAlertClient
        while self.running:
            try:
                for message in self._open_hop_stream:
                    # Send signal
                    kafka_record_consumed.send(
                        None,
                        name=self.name,
                        record=self.get_payload_content(message)
                    )
            except KafkaException as exception:
                err = exception.args[0]
                if self.running is False:
                    # The close attempt in the KafkaListener stop method throws
                    # a KafkaException that's caught by this try except, so we
                    # just have to catch this case for the worker to shut down
                    # gracefully
                    pass
                elif err.fatal():
                    # stop running and close before raising error
                    self.running = False
                    self._open_hop_stream.close()
                    raise
                else:
                    log.warning(
                        "non-fatal error from kafka: {}".format(err.name))


class KafkaWriter(KafkaBase):
    """Write Kafka topics and monitor health."""

    def __init__(self, name, config):
        super().__init__(name, config, 'producer')
        self._open_hop_stream = self._hop_stream.open(
            config['url'], 'w',
            message_max_bytes=1024 * 1024 * 8,
            compression_type='zstd')

        # Set up flag for failed delivery of messages
        self.kafka_delivery_failures = False

    def kafka_topic_up(self):
        '''Check for problems in broker and topic. Returns True is broker and
        topic appear to be up, returns False otherwise.'''
        kafka_url = self._config['url']
        _, _, broker, topic = kafka_url.split('/')
        try:
            topics = list_topics(kafka_url, auth=self._credential, timeout=5)
            if topics[topic].error is None:
                log.info(f'{kafka_url} appears to be functioning properly')
                return True
            else:
                log.error(f'{topic} at {broker} appears to be down')
                return False
        except KafkaException:
            log.error(f'{broker} appears to be down')
            return False

    def _delivery_cb(self, kafka_error, message):
        # FIXME Get rid of if-else logic once
        # https://github.com/scimma/hop-client/pull/190 is merged
        if self._config['suffix'] == 'avro':
            record = AvroBlob.deserialize(message.value()).content[0]
        else:
            record = JSONBlob.deserialize(message.value()).content
        kafka_url = self._config['url']
        if kafka_error is None:
            self.kafka_delivery_failures = False
        else:
            log.error(f'Received error code {kafka_error.code()} '
                      f'({kafka_error.name()}, {kafka_error.str()}) '
                      f'when delivering {record["superevent_id"]} '
                      f'{record["alert_type"]} alert to {kafka_url}')
            self.kafka_delivery_failures = True

    def write(self, payload):
        self._open_hop_stream.write(payload,
                                    delivery_callback=self._delivery_cb)
        self._open_hop_stream.flush()


class KafkaBootStep(bootsteps.ConsumerStep):
    """Generic boot step to limit us to appropriate kinds of workers.

    Only include this bootstep in workers that are configured to listen to the
    ``kafka`` queue.
    """

    def create(self, consumer):
        if not isinstance(consumer.pool, solo.TaskPool):
            raise RuntimeError(
                'The Kafka broker only works with the "solo" task pool. '
                'Start the worker with "--queues=kafka --pool=solo".')


class Consumer(KafkaBootStep):
    """Run MOU Kafka consumers in background threads.
    """

    name = 'Kafka consumer'

    def include_if(self, consumer):
        return 'kafka' in consumer.app.amqp.queues

    def start(self, consumer):
        log.info(f'Starting {self.name}, topics: ' +
                 ' '.join(config['url'] for config in
                          consumer.app.conf['kafka_consumer_config'].values()))
        self._listeners = {
            key: KafkaListener(key, config) for key, config in
            consumer.app.conf['kafka_consumer_config'].items()
        }
        self.threads = [
            Thread(target=s.listen, name=f'{key}_KafkaConsumerThread') for key,
            s in self._listeners.items() if s._open_hop_stream is not None
        ]
        for thread in self.threads:
            thread.start()

    def stop(self, consumer):
        log.info('Closing connection to topics: ' +
                 ' '.join(listener._config['url'] for listener in
                          self._listeners.values() if listener._open_hop_stream
                          is not None))
        for s in self._listeners.values():
            s.running = False
            if s._open_hop_stream is not None:
                s._open_hop_stream.close()

        for thread in self.threads:
            thread.join()

    def info(self, consumer):
        return {
            'active_kafka_consumers': {listener.name for listener in
                                       self._listeners.values() if
                                       listener._open_hop_stream is not
                                       None}
        }


class Producer(KafkaBootStep):
    """Run the global Kafka producers in a background thread.

    Flags that document the health of the connections are made available
    :ref:`inspection <celery:worker-inspect>` with the ``gwcelery inspect
    stats`` command under the ``kafka_topic_up`` and
    ``kafka_delivery_failures`` keys.
    """

    name = 'Kafka producer'

    def include_if(self, consumer):
        return 'kafka' in consumer.app.amqp.queues

    def start(self, consumer):
        log.info(f'Starting {self.name}, topics: ' +
                 ' '.join(config['url'] for config in
                          consumer.app.conf['kafka_alert_config'].values()))
        consumer.app.conf['kafka_streams'] = self._writers = {
            brokerhost: KafkaWriter(brokerhost, config) for brokerhost, config
            in consumer.app.conf['kafka_alert_config'].items()
        }

    def stop(self, consumer):
        log.info('Closing connection to topics: ' +
                 ' '.join(config['url'] for config in
                          consumer.app.conf['kafka_alert_config'].values()))
        for s in self._writers.values():
            s._open_hop_stream.close()

    def info(self, consumer):
        return {'kafka_topic_up': {
                    brokerhost: writer.kafka_topic_up() for brokerhost,
                    writer in self._writers.items()
                },
                'kafka_delivery_failures': {
                    brokerhost: writer.kafka_delivery_failures for
                    brokerhost, writer in self._writers.items()
                }}
