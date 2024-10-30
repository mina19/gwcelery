from pathlib import Path
from shutil import which
from unittest.mock import Mock

import lal
import pytest
from gwpy.time import tconvert

from .. import app, main
from ..tools import nagios


def test_nagios_unknown_error(monkeypatch, capsys):
    """Test that we generate the correct message when there is an unexpected
    exception.
    """
    monkeypatch.setattr('gwcelery.app.connection',
                        Mock(side_effect=RuntimeError))

    with pytest.raises(SystemExit) as excinfo:
        main(['gwcelery', 'nagios'])
    assert excinfo.value.code == nagios.NagiosPluginStatus.UNKNOWN
    out, err = capsys.readouterr()
    assert 'UNKNOWN: Unexpected error' in out


@pytest.fixture
def celery_worker_parameters():
    return dict(
        perform_ping_check=False,
        queues=['celery', 'exttrig', 'em-bright', 'highmem', 'kafka',
                'multiprocessing', 'openmp', 'superevent', 'voevent']
    )


def test_nagios(capsys, monkeypatch, request, socket_enabled, starter,
                tmp_path):
    mock_igwn_alert_client = Mock()
    mock_hop_stream_object = Mock()
    mock_hop_stream_object.configure_mock(**{'close.return_value': None})
    mock_hop_stream = Mock(return_value=mock_hop_stream_object)
    mock_get_auth = Mock()
    mock_list_topics = Mock()
    unix_socket = str(tmp_path / 'redis.sock')
    broker_url = f'redis+socket://{unix_socket}'
    orig_llhoft_path = app.conf['llhoft_glob']
    llhoft_path = str(
        Path(__file__).parent / 'data/llhoft/fail/{detector}/*.gwf')

    monkeypatch.setattr('hop.io.Stream.open', mock_hop_stream)
    monkeypatch.setattr('igwn_alert.client', mock_igwn_alert_client)
    monkeypatch.setattr('gwcelery.kafka.bootsteps.KafkaBase.get_auth',
                        mock_get_auth)
    monkeypatch.setattr('gwcelery.kafka.bootsteps.list_topics',
                        mock_list_topics)
    monkeypatch.setitem(app.conf, 'broker_url', broker_url)
    monkeypatch.setitem(app.conf, 'result_backend', broker_url)
    monkeypatch.setitem(app.conf, 'kafka_consumer_config', {
        'swift': {
            'url': 'kafka://kafka.gcn.nasa.gov/swift.bat.guano',
            'suffix': 'json'
        },
        'fermi_gbm_alert': {
            'url': ('kafka://kafka.gcn.nasa.gov/'
                    'gcn.classic.voevent.FERMI_GBM_ALERT'),
            'suffix': 'xml'
        }
    })
    monkeypatch.setitem(app.conf, 'llhoft_glob', llhoft_path)

    # no llhoft distributed in last 10 mins
    with pytest.raises(SystemExit) as excinfo:
        main(['gwcelery', 'nagios'])
    assert excinfo.value.code == nagios.NagiosPluginStatus.CRITICAL
    out, err = capsys.readouterr()
    assert 'CRITICAL: Low-latency hoft is not being streamed' in out
    monkeypatch.setitem(app.conf, 'llhoft_glob', orig_llhoft_path)

    # no broker

    with pytest.raises(SystemExit) as excinfo:
        main(['gwcelery', 'nagios'])
    assert excinfo.value.code == nagios.NagiosPluginStatus.CRITICAL
    out, err = capsys.readouterr()
    assert 'CRITICAL: No connection to broker' in out

    # broker, no worker

    redis_server = which('redis-server')
    if redis_server is None:
        pytest.skip('redis-server is not installed')
    starter.exec_process(
        [redis_server, '--port', '0', '--unixsocket', unix_socket,
         '--unixsocketperm', '700'], timeout=10,
        magic_words=b'The server is now ready to accept connections')

    with pytest.raises(SystemExit) as excinfo:
        main(['gwcelery', 'nagios'])
    assert excinfo.value.code == nagios.NagiosPluginStatus.CRITICAL
    out, err = capsys.readouterr()
    assert 'CRITICAL: Not all expected queues are active' in out

    # worker, no igwn_alert nodes

    request.getfixturevalue('celery_worker')

    mock_igwn_alert_client.configure_mock(**{
        'return_value.get_topics.return_value': {}})

    with pytest.raises(SystemExit) as excinfo:
        main(['gwcelery', 'nagios'])
    assert excinfo.value.code == nagios.NagiosPluginStatus.CRITICAL
    out, err = capsys.readouterr()
    assert 'CRITICAL: Not all IGWN alert topics are subscribed' in out

    # tasks, too many igwn_alert topics

    expected_igwn_alert_topics = nagios.get_expected_igwn_alert_topics(app)
    monkeypatch.setattr(
        'celery.app.control.Inspect.stats', Mock(return_value={'foo': {
            'igwn-alert-topics': expected_igwn_alert_topics | {'foobar'}}}))

    with pytest.raises(SystemExit) as excinfo:
        main(['gwcelery', 'nagios'])
    assert excinfo.value.code == nagios.NagiosPluginStatus.CRITICAL
    out, err = capsys.readouterr()
    assert 'CRITICAL: Too many IGWN alert topics are subscribed' in out

    # igwn_alert topics present, no VOEvent broker peers

    monkeypatch.setattr(
        'celery.app.control.Inspect.stats', Mock(return_value={'foo': {
            'igwn-alert-topics': expected_igwn_alert_topics}}))

    with pytest.raises(SystemExit) as excinfo:
        main(['gwcelery', 'nagios'])
    assert excinfo.value.code == nagios.NagiosPluginStatus.CRITICAL
    out, err = capsys.readouterr()
    assert 'CRITICAL: The VOEvent broker has no active connections' in out

    # VOEvent broker peers, no VOEvent receiver peers

    monkeypatch.setattr(
        'celery.app.control.Inspect.stats', Mock(return_value={'foo': {
            'voevent-broker-peers': ['127.0.0.1'],
            'igwn-alert-topics': expected_igwn_alert_topics}}))

    with pytest.raises(SystemExit) as excinfo:
        main(['gwcelery', 'nagios'])
    assert excinfo.value.code == nagios.NagiosPluginStatus.CRITICAL
    out, err = capsys.readouterr()
    assert 'CRITICAL: The VOEvent receiver has no active connections' in out

    monkeypatch.setattr(
        'celery.app.control.Inspect.stats', Mock(return_value={'foo': {
            'voevent-broker-peers': ['127.0.0.1'],
            'voevent-receiver-peers': ['127.0.0.1'],
            'igwn-alert-topics': expected_igwn_alert_topics}}))

    # tasks piled up in celery queue
    monkeypatch.setattr(
        'gwcelery.tools.nagios.get_celery_queue_length',
        Mock(return_value=51))
    with pytest.raises(SystemExit) as excinfo:
        main(['gwcelery', 'nagios'])
    assert excinfo.value.code == nagios.NagiosPluginStatus.CRITICAL
    out, err = capsys.readouterr()
    assert 'CRITICAL: Tasks are piled up in Celery queue' in out

    monkeypatch.setattr(
        'gwcelery.tools.nagios.get_celery_queue_length',
        Mock(return_value=0))

    # no superevents in last hour
    monkeypatch.setattr(
        'gwcelery.tools.nagios.get_recent_mdc_superevents',
        Mock(return_value=(
            [{'superevent_id': 'MS123456',
              'created': str(tconvert(lal.GPSTimeNow() - 7200))}],
            lal.GPSTimeNow() - 6 * 3600, lal.GPSTimeNow())))
    with pytest.raises(SystemExit) as excinfo:
        main(['gwcelery', 'nagios'])
    assert excinfo.value.code == nagios.NagiosPluginStatus.CRITICAL
    out, err = capsys.readouterr()
    assert 'CRITICAL: No MDC superevents found in last one hour' in out
    # reset to expected behavior for later checks
    monkeypatch.setattr(
        'gwcelery.tools.nagios.get_recent_mdc_superevents',
        Mock(return_value=(
            [{'superevent_id': 'MS123456',
              'created': str(tconvert(lal.GPSTimeNow() - 100))}],
            lal.GPSTimeNow() - 6 * 3600, lal.GPSTimeNow())))

    # Kafka broker, topic or broker are down
    monkeypatch.setattr(
        'celery.app.control.Inspect.stats',
        Mock(return_value={'foo': {'voevent-broker-peers': ['127.0.0.1'],
                                   'voevent-receiver-peers': ['127.0.0.1'],
                                   'igwn-alert-topics':
                                   expected_igwn_alert_topics,
                                   'kafka_topic_up': {'kafka://kafka.scimma.org/gwalert-test': False}}}))  # noqa: E501

    with pytest.raises(SystemExit) as excinfo:
        main(['gwcelery', 'nagios'])
    assert excinfo.value.code == nagios.NagiosPluginStatus.CRITICAL
    out, err = capsys.readouterr()
    assert 'CRITICAL: Not all Kafka bootstep URLs are active' in out

    # Kafka broker, message not delivered
    monkeypatch.setattr(
        'celery.app.control.Inspect.stats',
        Mock(return_value={'foo': {'voevent-broker-peers': ['127.0.0.1'],
                                   'voevent-receiver-peers': ['127.0.0.1'],
                                   'igwn-alert-topics':
                                   expected_igwn_alert_topics,
                                   'kafka_topic_up':
                                   {'kafka://kafka.scimma.org/gwalert-test':
                                    True},
                                   'kafka_delivery_failures':
                                   {'kafka://kafka.scimma.org/gwalert-test':
                                    True}}}))  # noqa: E501

    with pytest.raises(SystemExit) as excinfo:
        main(['gwcelery', 'nagios'])
    assert excinfo.value.code == nagios.NagiosPluginStatus.CRITICAL
    out, err = capsys.readouterr()
    assert 'CRITICAL: Not all Kafka messages have been succesfully delivered' \
           in out

    monkeypatch.setattr(
        'celery.app.control.Inspect.stats',
        Mock(return_value={'foo': {'voevent-broker-peers': ['127.0.0.1'],
                                   'voevent-receiver-peers': ['127.0.0.1'],
                                   'igwn-alert-topics':
                                   expected_igwn_alert_topics,
                                   'kafka_topic_up':
                                   {'kafka://kafka.scimma.org/gwalert-test':
                                    True},
                                   'kafka_delivery_failures':
                                   {'kafka://kafka.scimma.org/gwalert-test':
                                    False},
                                   'active_kafka_consumers': set()}}))

    with pytest.raises(SystemExit) as excinfo:
        main(['gwcelery', 'nagios'])
    assert excinfo.value.code == nagios.NagiosPluginStatus.CRITICAL
    out, err = capsys.readouterr()
    assert 'CRITICAL: Not all Kafka consumer bootstep topics are active' \
           in out

    expected_kafka_consumer_topics = \
        nagios.get_expected_kafka_consumer_bootstep_names(app)
    monkeypatch.setattr(
        'celery.app.control.Inspect.stats',
        Mock(return_value={'foo': {'voevent-broker-peers': ['127.0.0.1'],
                                   'voevent-receiver-peers': ['127.0.0.1'],
                                   'igwn-alert-topics':
                                   expected_igwn_alert_topics,
                                   'kafka_topic_up':
                                   {'kafka://kafka.scimma.org/gwalert-test':
                                    True},
                                   'kafka_delivery_failures':
                                   {'kafka://kafka.scimma.org/gwalert-test':
                                    False},
                                   'active_kafka_consumers':
                                   expected_kafka_consumer_topics}}))

    with pytest.raises(SystemExit) as excinfo:
        main(['gwcelery', 'nagios'])
    assert excinfo.value.code == nagios.NagiosPluginStatus.OK
    out, err = capsys.readouterr()
    assert 'OK: Running normally' in out
