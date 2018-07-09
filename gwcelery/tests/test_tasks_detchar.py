from pkg_resources import resource_filename
import pytest

from ..celery import app
from ..tasks import detchar


@pytest.fixture
def llhoft_glob_pass():
    old = app.conf['llhoft_glob']
    app.conf['llhoft_glob'] = resource_filename(
        __name__, 'data/llhoft/pass/{detector}/*.gwf')
    yield
    app.conf['llhoft_glob'] = old


@pytest.fixture
def llhoft_glob_fail():
    old = app.conf['llhoft_glob']
    app.conf['llhoft_glob'] = resource_filename(
        __name__, 'data/llhoft/fail/{detector}/*.gwf')
    yield
    app.conf['llhoft_glob'] = old


def test_read_gwf(llhoft_glob_fail):
    assert len(detchar.read_gwf('L1', 'GDS-CALIB_STATE_VECTOR',
                                0, 16384.0))/16 == 16384.0


def test_check_vector(llhoft_glob_pass):
    channel = 'H1:DMT-DQ_VECTOR'
    start = 1214714160
    end = 1214714164
    assert detchar.check_vector(channel, start, end, 0b11, 'any')
    assert not detchar.check_vector(channel, start, end, 0b1111, 'any')
