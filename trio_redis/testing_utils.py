"""
Testing utilities for trio_redis.
"""

import logging
import shutil
import subprocess
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

import pytest
import trio

from ._redis import Redis, RedisPool, DEFAULT_HOST, DEFAULT_PORT


logger = logging.getLogger(__name__)


DEFAULT_REDIS_URL = 'redis://localhost'


def pytest_addoption(parser):
    parser.addoption(
        '--redis-url',
        action='store',
        default=DEFAULT_REDIS_URL,
        help='URL to Redis, e.g. redis://<hostname>[:<port>]',
    )
    parser.addoption(
        '--local-redis',
        action='store_true',
        default=False,
        help='Start a Redis server.',
    )


@pytest.fixture(scope='function')
async def redis_server(request):
    local_server = request.config.getoption('--local-redis')
    if local_server:
        async with _new_redis_server() as local_server_url:
            yield local_server_url
    else:
        yield None


@pytest.fixture(scope='function')
async def redis_sentinel_cluster(request):
    try:
        m = RedisSentinelManager()
        await m.start()
        yield m
    finally:
        await m.stop()


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
    tmp = Path(tempfile.mkdtemp())
    cfg = tmp / 'redis.conf'
    cfg.write_text('appendonly yes')
    prc = RedisNodeProcess(tmp, DEFAULT_PORT)

    try:
        await prc.open()
        yield f'redis://localhost:{DEFAULT_PORT}'
    finally:
        await prc.terminate()
        shutil.rmtree(tmp)


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


class TCPProxy:
    """A TCP proxy for request-response protocols."""

    def __init__(self, target_address):
        parsed_url = urlparse(target_address)
        self.host = parsed_url.hostname or DEFAULT_HOST
        self.port = parsed_url.port or DEFAULT_PORT

    async def run_forever(self, task_status=trio.TASK_STATUS_IGNORED):
        async with trio.open_nursery() as nursery:
            listeners = await nursery.start(trio.serve_tcp, self.handle_client, 0)
            proxy_address = self._get_address_from_listeners(listeners)
            task_status.started(proxy_address)

    def _get_address_from_listeners(self, listeners):
        return listeners[0].socket.getsockname()

    async def handle_client(self, client_stream):
        target_stream = await trio.open_tcp_stream(self.host, self.port)

        try:
            while True:
                # NOTE: We're assuming the requests from the client and
                # the responses from the server are never bigger than
                # 64KiB (Trio's default). This assumption makes the code
                # below much simpler. There's no logic needed to detect
                # if a request or response is complete.
                request = await client_stream.receive_some()
                request = await self.handle_request(request)
                await target_stream.send_all(request)

                response = await target_stream.receive_some()
                response = await self.handle_response(response)
                await client_stream.send_all(response)
        except self.CloseClientConnection:
            pass
        finally:
            await target_stream.aclose()

    async def handle_request(self, request):
        return request

    async def handle_response(self, response):
        return response

    class CloseClientConnection(Exception):
        pass


class RedisSentinelManager:
    _SENTINEL_PORT = 26379
    _REDIS_PORT = 6379
    _MASTER_CONFIG = """\
appendonly yes
port {PORT}
"""
    _REPLICA_CONFIG = """\
appendonly yes
port {PORT}
replicaof 127.0.0.1 {MASTER_PORT}
"""
    _SENTINEL_CONFIG = """\
port {PORT}
sentinel monitor test_cluster 127.0.0.1 {MASTER_PORT} {QUORUM}
sentinel down-after-milliseconds test_cluster 5000
sentinel failover-timeout test_cluster 60000
sentinel parallel-syncs test_cluster 1
"""

    def __init__(self):
        self.sentinel_count = 3
        self.replica_count = 2
        self.logger = get_class_logger(self.__class__)

        self._tmp = None
        self._sentinels = []
        self._nodes = []

    async def start(self):
        self._tmp = Path(tempfile.mkdtemp())

        config = self._create_config(self._REDIS_PORT, self._MASTER_CONFIG, {
            'PORT': self._REDIS_PORT,
        })
        self._nodes.append(RedisNodeProcess(config.parent, self._REDIS_PORT))

        for n in range(1, self.replica_count + 1):
            port = self._REDIS_PORT + n
            config = self._create_config(port, self._REPLICA_CONFIG, {
                'PORT': port,
                'MASTER_PORT': self._REDIS_PORT,
            })
            self._nodes.append(RedisNodeProcess(config.parent, port))

        for n in range(self.sentinel_count):
            port = self._SENTINEL_PORT + n
            config = self._create_config(port, self._SENTINEL_CONFIG, {
                'PORT': port,
                'MASTER_PORT': self._REDIS_PORT,
                'QUORUM': self.sentinel_count - 1,
            })
            self._sentinels.append(RedisSentinelProcess(config.parent, port))

        for node in self._nodes:
            await node.open()
        for sentinel in self._sentinels:
            await sentinel.open()

    async def stop(self):
        if not self._tmp:
            return
        for sentinel in self._sentinels:
            await sentinel.terminate()
        for node in self._nodes:
            await node.terminate()
        shutil.rmtree(self._tmp)

    def _create_config(self, port, template, params):
        base = (self._tmp / str(port))
        base.mkdir()
        config = base / 'redis.conf'
        config.write_text(template.format(**params))
        return config


class _RedisProcess:
    def __init__(self, working_directory, port):
        self._proc = None
        self._kwargs = {
            'command': self.command(),
            'stdout': subprocess.PIPE,
            'stderr': subprocess.STDOUT,
            'cwd': str(working_directory),
        }

        self.host = '127.0.0.1'
        self.port = port
        self.addr = f'{self.host}:{self.port}'
        self.url = f'redis://{self.addr}'
        self.logger = get_class_logger(self.__class__, self.url)

    async def open(self):
        try:
            self._proc = await trio.open_process(**self._kwargs)
            with trio.fail_after(5.0):
                while True:
                    if self._proc.returncode is not None:
                        raise OSError(f'redis-server exited with {self._proc.returncode}')
                    stdout = await self._proc.stdout.receive_some()
                    self.logger.debug(stdout.decode('utf-8'))
                    if self.ready_message() in stdout:
                        break
        except Exception:
            await self.terminate()
            raise

    async def terminate(self):
        if self._proc is None:
            return
        self._proc.terminate()
        await self._proc.wait()
        stdout = await self._drain(self._proc.stdout)
        self.logger.debug(stdout)
        self._proc = None

    async def _drain(self, stream):
        return (await _drain_stream(stream)).decode('utf-8')

    def command(self):
        return [
            'redis-server',
            './redis.conf',
            '--loglevel', 'verbose',
        ]

    def ready_message(self):
        raise NotImplementedError


class RedisNodeProcess(_RedisProcess):
    def ready_message(self):
        return b'* Ready to accept connections'


class RedisSentinelProcess(_RedisProcess):
    def command(self):
        return super().command() + ['--sentinel']

    def ready_message(self):
        return b'* Running mode=sentinel, port='


def get_class_logger(cls, suffix=''):
    name = f'{cls.__module__}.{cls.__name__}'
    if suffix:
        name += f'({suffix})'
    return logging.getLogger()
