from binascii import crc_hqx
from bisect import bisect
from contextlib import asynccontextmanager
from itertools import repeat
from random import randrange
from urllib.parse import urlparse, parse_qs

import hiredis
import trio

from . import _errors
from ._commands import (
    ClusterCommands,
    ConnectionCommands,
    KeysCommands,
    ScriptingCommands,
    ServerCommands,
    SortedSetCommands,
    StreamCommands,
    StringCommands,
    DisableSelectCommand,
)
from ._connection import Connection


__all__ = [
    'Redis',
    'RedisPool',
]


_commands = (
    ClusterCommands,
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

AVAILABLE_CLUSTER_SLOTS = 16384


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


class RedisPool(_BaseRedis, DisableSelectCommand, *_commands):
    """A pool of Redis clients.

    It's not needed to explicitly borrow a client from the pool. All
    commands (except SELECT) are implemented, acquiring and releasing
    clients is done behind the scenes.

    ``minimum`` is the minimum amount of clients created. When needed
    new client instances are created until ``maximum`` is reached.

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
        # TODO: Cleanup old connections.

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

    async def execute(self, command, parse_callback=None, **kwargs):
        client = await self.acquire()
        try:
            return await client.execute(command, parse_callback, **kwargs)
        finally:
            await self.release(client)

    async def execute_many(self, commands, parse_callbacks=None, multi_kwargs=None):
        client = await self.acquire()
        try:
            return await client.execute_many(commands, parse_callbacks, multi_kwargs)
        finally:
            await self.release(client)


class Redis(_BaseRedis, *_commands):
    def __init__(self, host=None, port=None, db=None):
        super().__init__(host, port, db)
        self._conn = Connection(self.host, self.port)

    async def connect(self):
        await self._conn.connect()
        await self.select(self.db)

    async def aclose(self):
        await self._conn.aclose()

    async def execute(self, command, parse_callback=None, **kwargs):
        if not parse_callback:
            parse_callback = _noop

        reply = await self._conn.execute(command)
        reply = self._parse_reply(reply, parse_callback)
        if isinstance(reply, self.ReplyError):
            raise reply

        return reply

    async def execute_many(self, commands, parse_callbacks=None, multi_kwargs=None):
        if not parse_callbacks:
            parse_callbacks = repeat(_noop)
        if not multi_kwargs:
            multi_kwargs = repeat({})

        replies = await self._conn.execute_many(commands)
        replies = [
            self._parse_reply(reply, cb, **kwargs)
            for reply, cb, kwargs in zip(replies, parse_callbacks, multi_kwargs)
        ]

        return replies

    # FIXME: Should this be in Redis?
    def _parse_reply(self, reply, parse_callback):
        if isinstance(reply, hiredis.ReplyError):
            reply = _errors.create_error_from_reply(reply)
        else:
            reply = parse_callback(reply)
        return reply


class RedisCluster(_BaseRedis, *_commands):
    ClusterStartupError = _errors.ClusterStartupError

    def __init__(self, startup_nodes):
        self.startup_nodes = startup_nodes
        self.slots = []
        self.masters = []
        self.clients = {}

    async def connect(self):
        client = await self._startup_client()
        try:
            await self._update_routes(client)
        finally:
            await client.aclose()

    async def aclose(self):
        for client in self.clients.values():
            await client.aclose()
        self.slots = []
        self.masters = []
        self.clients = {}

    async def _update_routes(self, client):
        slots = []
        masters = []
        clients = {}

        for entry in (await client.slots()):
            high, master = entry[1], entry[2]
            master = tuple(master)
            idx = bisect(slots, high)
            slots.insert(idx, high)
            masters.insert(idx, master)
            # FIXME: Reuse existing connections!
            client = RedisPool(master[0], master[1])
            await client.connect()
            clients[master] = client

        # FIXME: Concurrency bugs.
        old_clients = self.clients.copy()
        self.slots = slots
        self.masters = masters
        self.clients = clients

        for client in old_clients.values():
            await client.aclose()

    async def _startup_client(self):
        for url in self.startup_nodes:
            client = Redis.from_url(url)
            try:
                await client.connect()
                return client
            except OSError:
                pass

        raise _errors.ClusterStartupError

    async def execute(self, command, parse_callback=None, key=None, keys=None):
        slot = self._determine_slot(key, keys)

        # TODO:
        # - Handle ask. WIP
        # - Limit amount of tries.

        while True:
            master = self.masters[bisect(self.slots, slot)]
            client = self.clients[master]
            try:
                return await client.execute(command, parse_callback)
            except _errors.ClusterSlotMoved:
                await self._update_routes(client)
            except _errors.ClusterSlotMigrating:
                command = [b'ASKING'] + command

    def _determine_slot(self, key=None, keys=None):
        slot = None

        if key is not None:
            slot = hashslot(key)
        elif keys is not None:
            for k in keys:
                new_slot = hashslot(k)
                if new_slot != slot:
                    raise ValueError('all keys must resolve to the same slot')
                slot = new_slot

        if slot is None:
            slot = randrange(0, AVAILABLE_CLUSTER_SLOTS)

        return slot


class Pipeline(*_commands):
    def __init__(self, redis):
        self._redis = redis
        self._buffer = []
        self._callbacks = []

    def execute(self, command, parse_callback=None, **kwargs):
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


# FIXME: Accept /<db> not ?db=<db?
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


def hashslot(key):
    if not isinstance(key, bytes):
        key = key.encode('utf-8')

    s = key.find(b'{')
    if s != -1:
        e = key.find(b'}')
        if e > s + 1:
            key = key[s + 1:e]
    return crc_hqx(key, 0) % AVAILABLE_CLUSTER_SLOTS


def _noop(value):
    return value
