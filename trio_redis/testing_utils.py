"""
Testing utilities for trio_redis.
"""

import logging
import re
import random
import string
import subprocess
from contextlib import asynccontextmanager

import pytest
import trio

from ._redis import Redis, RedisPool


logger = logging.getLogger(__name__)


DEFAULT_REDIS_IMAGE = 'redis:5.0.7-alpine3.11'
DEFAULT_REDIS_URL = 'redis://localhost'


_RE_DOCKER_PORT_MAPPING = re.compile(b'\\d+/tcp -> .*?:(?P<host_port>\\d+)')


def pytest_addoption(parser):
    parser.addoption(
        '--redis-url',
        action='store',
        default=DEFAULT_REDIS_URL,
        help='URL to Redis, e.g. redis://<hostname>[:<port>]',
    )
    parser.addoption(
        '--docker-redis',
        action='store_true',
        default=False,
        help='Start a Redis container.',
    )


# NOTE: Trio fixtures must be function-scope. This makes the tests
# slower when --docker-redis is given, because a Redis container is
# started for every test.
@pytest.fixture(scope='function')
async def redis_server(request):
    docker_redis = request.config.getoption('--docker-redis')
    if docker_redis:
        async with _new_redis_server() as docker_redis_url:
            yield docker_redis_url
    else:
        yield None


@pytest.fixture(scope='function')
async def redis_url(request, redis_server):
    url = request.config.getoption('--redis-url')

    if redis_server is not None:
        yield redis_server
    else:
        yield url


@pytest.fixture(scope='function')
async def redis(redis_url):
    async with new_redis(redis_url) as redis:
        yield redis


@pytest.fixture(scope='function')
async def redis_pool(redis_url):
    async with new_redis_pool(redis_url) as redis:
        yield redis


@asynccontextmanager
async def new_redis(url):
    async with _new_x(Redis, url) as client:
        yield client


@asynccontextmanager
async def new_redis_pool(url):
    async with _new_x(RedisPool, url) as pool:
        yield pool


@asynccontextmanager
async def _new_x(cls, url):
    obj = cls.from_url(url)
    try:
        await obj.connect()
        yield obj
    finally:
        await obj.flushdb()
        await obj.script_flush()
        await obj.aclose()


@asynccontextmanager
async def _new_redis_server():
    container_name = _redis_container_name()
    kwargs = {
        'command': _redis_container_args(container_name),
        'stdout': subprocess.PIPE,
        'stderr': subprocess.PIPE,
    }

    async with await trio.open_process(**kwargs) as proc:
        has_error = False

        try:
            while True:
                if proc.poll() is not None:
                    raise RuntimeError('Redis server exited')
                stdout = await proc.stdout.receive_some()
                if b'Ready to accept connections' in stdout:
                    break
            host_port = await _redis_container_port(container_name)
            url = f'redis://localhost:{host_port}'
            yield url
        except Exception:
            has_error = True
            raise
        finally:
            if has_error:
                logger.error(f'STDERR: ' + (await _drain_stream(proc.stderr)).decode('utf-8'))
            proc.terminate()
            await proc.wait()


def _redis_container_name():
    return f'pytest-redis-{_random_string()}'


def _redis_container_args(container_name):
    return [
        'docker',
        'run',
        '-p', '6379',  # No host port means 'choose random port'.
        '--name', container_name,
        '--rm',
        DEFAULT_REDIS_IMAGE,
    ]


async def _redis_container_port(container_name):
    completed_proc = await trio.run_process(
        command=['docker', 'port', container_name],
        capture_stdout=True,
        stderr=subprocess.STDOUT,
    )
    port_mapping = completed_proc.stdout
    match = _RE_DOCKER_PORT_MAPPING.match(port_mapping)
    port_mapping = match.groupdict()
    if match is None:
        raise ValueError(f'cannot get host port from {port_mapping!r}')
    host_port = int(port_mapping['host_port'].decode('utf-8'))
    return host_port


def _random_string():
    return ''.join(random.choices(string.ascii_lowercase, k=6))


async def _drain_stream(stream):
    buf = []

    while True:
        data = await stream.receive_some()
        if not data:
            break
        buf.append(data)

    return b''.join(buf)


async def fail_if_not_between(after, before, coroutine):
    with trio.move_on_after(after):
        result = False
        while not result:
            result = await coroutine()
            await trio.sleep(0.01)

    assert not result

    with trio.fail_after(before - after):
        result = False
        while not result:
            result = await coroutine()
            await trio.sleep(0.001)

    assert result
