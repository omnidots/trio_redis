from contextlib import asynccontextmanager
from itertools import repeat
from urllib.parse import urlparse, parse_qs

import hiredis
import trio

from . import _errors
from ._commands import (
    ConnectionCommands,
    KeysCommands,
    ScriptingCommands,
    SentinelCommands,
    ServerCommands,
    SortedSetCommands,
    StreamCommands,
    StringCommands,
)
from ._connection import Connection


__all__ = [
    'Redis',
    'RedisPool',
    'RedisSentinel',
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


class _BaseRedis:
    """Abstract class for Redis clients."""
    # Alias exceptions. No need to import them. :)
    ConnectError = _errors.ConnectError
    BusyError = _errors.BusyError
    ClosedError = _errors.ClosedError
    ReplyError = _errors.ReplyError
    ReadOnlyError = _errors.ReadOnlyError

    def pipeline(self):
        return Pipeline(self)

    async def execute(self, command, parse_callback=None):
        return (await self.execute_many([command], [parse_callback or _noop]))[0]

    async def execute_many(self, commands, parse_callbacks=None):
        raise NotImplementedError('please implement this')


class _BareRedis(_BaseRedis):
    """Redis client w/o command methods."""

    @classmethod
    def from_url(cls, url):
        kwargs = _parse_url(url)
        return cls(**kwargs)

    def __init__(self, host=None, port=None, db=None):
        self.host = host or DEFAULT_HOST
        self.port = DEFAULT_PORT if port is None else port
        self.db = db
        self._conn = Connection(self.host, self.port)

    async def connect(self):
        await self._conn.connect()
        if self.db is not None:
            await self.select(self.db)

    async def aclose(self):
        await self._conn.aclose()

    async def execute_many(self, commands, parse_callbacks=None):
        if not parse_callbacks:
            parse_callbacks = repeat(_noop)

        replies = await self._conn.execute_many(commands)
        replies = [
            self._parse_reply(reply, cb)
            for reply, cb in zip(replies, parse_callbacks)
        ]

        return replies

    def _parse_reply(self, reply, parse_callback):
        if isinstance(reply, hiredis.ReplyError):
            reply = _errors.create_error_from_reply(reply)
        else:
            reply = parse_callback(reply)
        return reply


class RedisPool(_BaseRedis, *_commands):
    """A pool of Redis clients.

    It's not needed to explicitly borrow a client from the pool. All
    commands (except SELECT) are implemented, acquiring and releasing
    clients is done behind the scenes.

    ``client_factory`` is a function that returns a new instance of a
    client class. E.g. an instance of Redis or RedisSentinel.

    ``minimum`` is the minimum amount of clients created. When needed
    new client instances are created until ``maximum`` is reached.

    An instance of this class can be used concurrently. Instances of
    ``Redis`` cannot.
    """
    def __init__(self, client_factory, minimum=1, maximum=10):
        self.client_factory = client_factory
        self.minimum = minimum
        self.maximum = maximum

        self._limit = trio.Semaphore(maximum)
        self._free = []
        self._not_free = []

    @classmethod
    def from_url(cls, client_factory, url):
        kwargs = _parse_url(url)
        return cls(client_factory, **kwargs)

    @classmethod
    def redis(cls, host=None, port=None, db=None, url=None, **pool_kwargs):
        if ((host or port) and url) or not ((host or port) or url):
            raise ValueError('either host and port OR url must be given')
        def redis_factory():
            if host or port:
                return Redis(host, port, db)
            else:
                return Redis.from_url(url)
        return cls(redis_factory, **pool_kwargs)

    @classmethod
    def redis_sentinel(cls, master_name, addresses=None, urls=None, **pool_kwargs):
        if (addresses and urls) or not (addresses or urls):
            raise ValueError('either addresses OR urls must be given')
        def redis_sentinel_factory():
            if addresses:
                return RedisSentinel(master_name, addresses)
            else:
                return RedisSentinel.from_url(master_name, urls)
        return cls(redis_sentinel_factory, **pool_kwargs)

    async def connect(self):
        async def add():
            self._free.append(await self._new_client())

        async with trio.open_nursery() as nursery:
            for n in range(self.minimum):
                nursery.start_soon(add)

    async def _new_client(self):
        client = self.client_factory()
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
            client = await self._new_client()

        self._not_free.append(client)

        return client

    async def release(self, client):
        self._not_free.remove(client)
        self._free.append(client)
        self._limit.release()

    @asynccontextmanager
    async def borrow(self):
        """Explicitly borrow a client from the pool.

        For example::

            async with pool.borrow() as client:
                await client.set('a', 1)
                await client.set('b', 2)
                await client.set('c', 3)
        """
        client = await self.acquire()
        try:
            yield client
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


class Redis(_BareRedis, *_commands):
    """Basic Redis client."""


class RedisSentinel(Redis):
    """Basic Redis client with Sentinel support."""
    RECONNECT_DELAY = 0.5  # Seconds.
    RECONNECT_DELAY_INCREASE = 1.5  # Multiplier.
    MAX_RECONNECT_TRIES = 10

    @classmethod
    def from_url(cls, master_name, urls):
        adresses = []

        for url in urls:
            tmp = _parse_url(url)
            adresses.append((tmp['host'], tmp['port']))

        return cls(master_name, adresses)

    def __init__(self, master_name, adresses):
        self.master_name = master_name
        self.sentinel = Sentinel(adresses)
        super().__init__()

    async def connect(self):
        addr = await self._get_current_master_addr()
        self.host = self._conn.host = addr[0]
        self.port = self._conn.port = addr[1]
        await super().connect()

    async def _reconnect(self):
        if self._conn._is_connected:
            await self._conn.aclose()
        self._conn = Connection(None, None)
        await self.connect()

    async def _get_current_master_addr(self):
        await self.sentinel.connect()
        try:
            return await self.sentinel.get_master_addr_by_name(self.master_name)
        finally:
            await self.sentinel.aclose()

    async def execute_many(self, commands, parse_callbacks=None):
        delay = self.RECONNECT_DELAY
        do_reconnect = False

        for n in range(self.MAX_RECONNECT_TRIES + 1):
            if do_reconnect:
                try:
                    await self._reconnect()
                except OSError:
                    await trio.sleep(delay)
                    delay *= self.RECONNECT_DELAY_INCREASE
                    continue

            try:
                return await super().execute_many(commands, parse_callbacks)
            except (self.ClosedError, self.ReadOnlyError):
                do_reconnect = True

        raise self.ConnectError(f'unable to connect; conn={self._conn}')


class Sentinel(_BareRedis, SentinelCommands):
    """Sentinel client."""
    CONNECT_TIMEOUT = 0.5

    @classmethod
    def from_url(cls, urls):
        adresses = []

        for url in urls:
            tmp = _parse_url(url)
            if 'db' in tmp:
                raise ValueError('sentinal client does not support db selection')
            adresses.append((tmp['host'], tmp['port']))

        return cls(adresses)

    def __init__(self, addresses):
        self._connections = [Connection(*addr) for addr in addresses]
        self._conn = None

    async def connect(self):
        # Behavior:
        # - Try to connect to a sentinel node.
        # - Abort if connect does not happen within CONNECT_TIMEOUT.
        # - Connection errors are handled by catching OSError.
        # - On success, move the connection to the beginning of the
        #   connection list and stop break loop.
        #
        # See: https://redis.io/topics/sentinel-clients
        if self._conn:
            raise self.BusyError('sentinel client already connected')
        for conn in self._connections[:]:
            with trio.move_on_after(self.CONNECT_TIMEOUT) as cancel_scope:
                try:
                    await conn.connect()
                except OSError:
                    continue

            if not cancel_scope.cancelled_caught:
                self._connections.remove(conn)
                self._connections.insert(0, conn)
                self._conn = conn
                return
            else:
                await conn.aclose()

        raise self.ConnectError('unable to connect to sentinel node')

    async def aclose(self):
        await super().aclose()
        self._conn = None


class Pipeline(*_commands):
    """Command pipeliner.

    A pipeline object batches commands and executes all commands when
    the object is awaited.

    An in-order list of replies is returned. If there are no batched
    commands, an empty list is returned.
    """
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
        if not self._buffer:
            async def return_empty_list():
                return []
            return return_empty_list().__await__()

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

    db = url.path[1:]
    if db:
        if not db.isdigit():
            raise ValueError(f'db must be a digit in {url!r}')
        kwargs['db'] = db

    params = parse_qs(url.query)
    for k, v in params.items():
        if len(v) > 1:
            raise ValueError(f'parameter {k} set multiple times in {url!r}')
        params[k] = v[0]

    kwargs.update(params)

    return kwargs


def _noop(value):
    return value
