from functools import cache

from celery import bootsteps
from celery.concurrency import solo
from celery.utils.log import get_logger
from confluent_kafka.error import KafkaException
from fastavro.schema import parse_schema
from hop import stream
from hop.io import list_topics
from hop.models import AvroBlob, JSONBlob

from ..util import read_json


__all__ = ('Producer',)

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


class AvroBlobWrapper(AvroBlob):

    def __init__(self, payload):
        return super().__init__([payload], schema())


class KafkaWriter:
    '''Class to write to kafka stream and monitor stream health.'''

    def __init__(self, config):
        self._config = config
        self._open_hop_stream = stream.open(
            config['url'], 'w',
            message_max_bytes=1024 * 1024 * 2,
            compression_type='zstd')

        # Set up flag for failed delivery of messages
        self.kafka_delivery_failures = False

        # FIXME Drop get_payload_input method once
        # https://github.com/scimma/hop-client/pull/190 is merged
        if config['suffix'] == 'avro':
            self.serialization_model = AvroBlobWrapper
            self.get_payload_input = lambda payload: payload.content[0]
        elif config['suffix'] == 'json':
            self.serialization_model = JSONBlob
            self.get_payload_input = lambda payload: payload.content
        else:
            raise NotImplementedError(
                'Supported serialization method required for alert notices'
            )

    def kafka_topic_up(self):
        '''Check for problems in broker and topic. Returns True is broker and
        topic appear to be up, returns False otherwise.'''
        kafka_url = self._config['url']
        _, _, broker, topic = kafka_url.split('/')
        try:
            topics = list_topics(kafka_url, timeout=5)
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
            log.error(f'Received error code {kafka_error.error_code} '
                      f'({kafka_error.reason}) when delivering '
                      f'{record["superevent_id"]} '
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

    def include_if(self, consumer):
        return 'kafka' in consumer.app.amqp.queues

    def create(self, consumer):
        if not isinstance(consumer.pool, solo.TaskPool):
            raise RuntimeError(
                'The Kafka broker only works with the "solo" task pool. '
                'Start the worker with "--queues=kafka --pool=solo".')

    def start(self, consumer):
        log.info(f'Starting {self.name}, topics: ' +
                 ' '.join(config['url'] for config in
                          consumer.app.conf['kafka_alert_config'].values()))

    def stop(self, consumer):
        log.info('Closing connection to topics: ' +
                 ' '.join(config['url'] for config in
                          consumer.app.conf['kafka_alert_config'].values()))


class Producer(KafkaBootStep):
    """Run the global Kafka producers in a background thread.

    Flags that document the health of the connections are made available
    :ref:`inspection <celery:worker-inspect>` with the ``gwcelery inspect
    stats`` command under the ``kafka_topic_up`` and
    ``kafka_delivery_failures`` keys.
    """

    name = 'Kafka producer'

    def start(self, consumer):
        super().start(consumer)
        consumer.app.conf['kafka_streams'] = self._writers = {
            brokerhost: KafkaWriter(config) for brokerhost, config in
            consumer.app.conf['kafka_alert_config'].items()
        }

    def stop(self, consumer):
        super().stop(consumer)
        for s in self._writers.values():
            s._open_hop_stream.close()

    def info(self, consumer):
        return {'kafka_topic_up': {
                    brokerhost: writer.kafka_topic_up() for brokerhost, writer
                    in self._writers.items()
                },
                'kafka_delivery_failures': {
                    brokerhost: writer.kafka_delivery_failures for
                    brokerhost, writer in self._writers.items()
                }}
