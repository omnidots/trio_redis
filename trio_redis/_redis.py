from itertools import repeat
from urllib.parse import urlparse, parse_qs

import hiredis
import trio

from . import _errors
from ._commands import (
    ConnectionCommands,
    KeysCommands,
    ScriptingCommands,
    ServerCommands,
    SortedSetCommands,
    StreamCommands,
    StringCommands,
)
from ._connection import Connection


__all__ = [
    'Redis',
    'RedisPool',
]


_commands = (
    ConnectionCommands,
    KeysCommands,
    ScriptingCommands,
    ServerCommands,
    SortedSetCommands,
    StreamCommands,
    StringCommands,
)


DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 6379
DEFAULT_DB = 0


class _BaseRedis:
    # Alias exceptions. No need to import them. :)
    BusyError = _errors.BusyError
    ClosedError = _errors.ClosedError
    ReplyError = _errors.ReplyError

    @classmethod
    def from_url(cls, url):
        kwargs = _parse_url(url)
        return cls(**kwargs)

    def __init__(self, host=None, port=None, db=None):
        self.host = host or DEFAULT_HOST
        self.port = DEFAULT_PORT if port is None else port
        self.db = DEFAULT_DB if db is None else db

    def pipeline(self):
        return Pipeline(self)


class RedisPool(_BaseRedis, *_commands):
    """A pool of Redis clients.

    It's not needed to explicitly borrow a client from the pool. All
    commands (except SELECT) are implemented, acquiring and releasing
    clients is done behind the scenes.

    ``minimum`` is the minimum amount of clients created. When needed
    new client instances are created until a maximum is reached (see
    ``maximum``).

    An instance of this class can be used concurrently. Instances of
    ``Redis`` cannot.
    """
    def __init__(self, host=None, port=None, db=None, minimum=1, maximum=10):
        super().__init__(host, port, db)
        self.minimum = minimum
        self.maximum = maximum
        self._limit = trio.Semaphore(maximum)

        self._free = []
        self._not_free = []

    async def connect(self):
        async def add():
            self._free.append(await self._new_redis())

        async with trio.open_nursery() as nursery:
            for n in range(self.minimum):
                nursery.start_soon(add)

    async def _new_redis(self):
        client = Redis(self.host, self.port, self.db)
        await client.connect()
        return client

    async def aclose(self):
        async with trio.open_nursery() as nursery:
            for client in self._free:
                nursery.start_soon(client.aclose)
            for client in self._not_free:
                nursery.start_soon(client.aclose)

        self._free = []
        self._not_free = []

    async def acquire(self):
        """Acquire a client from the pool.

        Blocks if no client if available.
        """
        await self._limit.acquire()

        try:
            client = self._free.pop()
        except IndexError:
            client = await self._new_redis()

        self._not_free.append(client)

        return client

    async def release(self, client):
        self._not_free.remove(client)
        self._free.append(client)
        self._limit.release()

    def use(self):
        """Explicitly borrow a client from the pool.

        For example::

            async with pool.use() as client:
                await client.set('a', 1)
                await client.set('b', 2)
                await client.set('c', 3)
        """
        return self.Context(self)

    async def execute(self, command, parse_callback=None):
        client = await self.acquire()
        try:
            return await client.execute(command, parse_callback)
        finally:
            await self.release(client)

    async def execute_many(self, commands, parse_callbacks=None):
        client = await self.acquire()
        try:
            return await client.execute_many(commands, parse_callbacks)
        finally:
            await self.release(client)

    def select(self, index):
        raise NotImplementedError('SELECT not implemented for RedisPool')

    class Context:
        def __init__(self, pool):
            self._pool = pool
            self._client = None

        async def __aenter__(self):
            self._client = await self._pool.acquire()
            return self._client

        async def __aexit__(self, exc_type, exc, tb):
            await self._pool.release(self._client)
            self._client = None


class Redis(_BaseRedis, *_commands):
    def __init__(self, host=None, port=None, db=None):
        super().__init__(host, port, db)
        self._conn = Connection(self.host, self.port)

    async def connect(self):
        await self._conn.connect()
        await self.select(self.db)

    async def aclose(self):
        await self._conn.aclose()

    async def execute(self, command, parse_callback=None):
        if not parse_callback:
            parse_callback = _noop

        reply = await self._conn.execute(command)
        reply = self._parse_reply(command[0], reply, parse_callback)
        if isinstance(reply, self.ReplyError):
            raise reply

        return reply

    async def execute_many(self, commands, parse_callbacks=None):
        if not parse_callbacks:
            parse_callbacks = repeat(_noop)

        replies = await self._conn.execute_many(commands)
        replies = [
            self._parse_reply(command[0], reply, cb)
            for command, reply, cb in zip(commands, replies, parse_callbacks)
        ]

        return replies

    def _parse_reply(self, command, reply, parse_callback):
        if isinstance(reply, hiredis.ReplyError):
            reply = self.ReplyError.from_error_reply(reply)
        else:
            reply = parse_callback(reply)
        return reply


class Pipeline(*_commands):
    def __init__(self, redis):
        self._redis = redis
        self._buffer = []
        self._callbacks = []

    def execute(self, command, parse_callback=None):
        if not parse_callback:
            parse_callback = _noop
        self._buffer.append(command)
        self._callbacks.append(parse_callback)
        return self

    def __await__(self):
        return self._redis.execute_many(
            self._buffer,
            self._callbacks,
        ).__await__()


def _parse_url(url):
    """Parse a Redis URL.

    This is only a partial implementation that only supports a hostname,
    port, and database. See the `URI scheme`_ at IANA for a complete
    description of the URI scheme.

    .. _URI scheme: https://www.iana.org/assignments/uri-schemes/prov/redis
    """
    kwargs = {
        'host': DEFAULT_HOST,
        'port': DEFAULT_PORT,
        'db': DEFAULT_DB,
    }

    url = urlparse(url)
    if not url.scheme:
        raise ValueError(f'missing scheme in {url!r}')
    if url.scheme != 'redis':
        raise ValueError(f'unsupported scheme in {url!r}')

    if url.hostname:
        kwargs['host'] = url.hostname
    if url.port:
        kwargs['port'] = url.port

    params = parse_qs(url.query)
    for k, v in params.items():
        if len(v) > 1:
            raise ValueError(f'parameter {k} set multiple times in URL')
        params[k] = v[0]

    kwargs.update(params)

    return kwargs


def _noop(value):
    return value
