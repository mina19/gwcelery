from distutils.spawn import find_executable
from unittest.mock import Mock

import pytest

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
        queues=['celery', 'exttrig', 'openmp', 'superevent', 'voevent']
    )


def test_nagios(capsys, monkeypatch, request, socket_enabled, starter,
                tmp_path):
    mock_igwn_alert_client = Mock()
    monkeypatch.setattr(
        'igwn_alert.client', mock_igwn_alert_client)
    unix_socket = str(tmp_path / 'redis.sock')
    broker_url = f'redis+socket://{unix_socket}'
    monkeypatch.setitem(app.conf, 'broker_url', broker_url)
    monkeypatch.setitem(app.conf, 'result_backend', broker_url)

    # no broker

    with pytest.raises(SystemExit) as excinfo:
        main(['gwcelery', 'nagios'])
    assert excinfo.value.code == nagios.NagiosPluginStatus.CRITICAL
    out, err = capsys.readouterr()
    assert 'CRITICAL: No connection to broker' in out

    # broker, no worker

    redis_server = find_executable('redis-server')
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

    # success

    monkeypatch.setattr(
        'celery.app.control.Inspect.stats',
        Mock(return_value={'foo': {'voevent-broker-peers': ['127.0.0.1'],
                                   'voevent-receiver-peers': ['127.0.0.1'],
                                   'igwn-alert-topics': expected_igwn_alert_topics}}))  # noqa: E501

    with pytest.raises(SystemExit) as excinfo:
        main(['gwcelery', 'nagios'])
    assert excinfo.value.code == nagios.NagiosPluginStatus.OK
    out, err = capsys.readouterr()
    assert 'OK: Running normally' in out
