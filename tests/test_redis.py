import pytest
import trio

from trio_redis import Redis
from trio_redis._redis import Pipeline
from trio_redis.testing_utils import TCPProxy


async def test_concurrent_use_of_single_instance(redis):
    # Concurrent use of the same instance is not allowed, because
    # it'll possibly mess up the state of the instance.
    #
    # Why not use a lock and wait for the connection to become available?
    # There's no gain in waiting. Using a pipeline or the pool to execute
    # commands concurrently has more benefits (i.e. it's faster).
    #
    # By raising an exception it should become clear the way the
    # developer is doing things is not the right way.
    with pytest.raises(redis.BusyError):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(redis.set, 'x', 1)
            nursery.start_soon(redis.set, 'y', 1)

    # Sequential use is allowed.
    result = await redis.set('x', 1)
    assert result
    result = await redis.set('y', 1)
    assert result


async def test_double_connect(redis):
    with pytest.raises(redis.BusyError):
        await redis.connect()


async def test_double_aclose(redis_url):
    redis = Redis.from_url(redis_url)
    await redis.connect()
    await redis.aclose()

    with pytest.raises(redis.ClosedError):
        await redis.aclose()


async def test_pipeline(redis):
    pipeline = (redis.pipeline()
        .set('x', 'y')
        .get('x')
        .set('y', 'z')
        .get('y')
        .set('z', 'a')
        .get('z')
    )
    assert isinstance(pipeline, Pipeline)

    result = await pipeline
    assert result == [True, b'y', True, b'z', True, b'a']


async def test_redis_pool_acquire_release(redis_pool):
    c1 = await redis_pool.acquire()
    assert c1 not in redis_pool._free
    assert c1 in redis_pool._not_free

    c2 = await redis_pool.acquire()
    assert c2 not in redis_pool._free
    assert c2 in redis_pool._not_free

    await redis_pool.release(c1)
    assert c1 in redis_pool._free
    assert c1 not in redis_pool._not_free

    await redis_pool.release(c2)
    assert c2 in redis_pool._free
    assert c2 not in redis_pool._not_free


async def test_redis_pool_commands(redis_pool):
    result = await redis_pool.set('x', 1)
    assert result
    result = await redis_pool.get('x')
    assert result == b'1'


async def test_redis_pool_borrow(redis_pool):
    async with redis_pool.borrow() as client:
        await client.set('a', 1)


async def test_close_socket(nursery, redis_url):
    class ProxyUnexpectedClose(TCPProxy):
        async def handle_request(self, request):
            if b'GET' in request:
                raise self.CloseClientConnection
            return request

    proxy = ProxyUnexpectedClose(redis_url)
    address = await nursery.start(proxy.run_forever)

    redis = Redis(*address)
    await redis.connect()

    with pytest.raises(redis.ClosedError, match='connection unexpectedly closed'):
        await redis.get('x')

    await redis.aclose()
    nursery.cancel_scope.cancel()


async def test_connection_reset_after_cancelled(redis, redis_url):
    # NOTE: The redis fixture is needed to cleanup after this test ends.

    # r1 is for reading, r2 is for writing.
    r1 = Redis.from_url(redis_url)
    await r1.connect()
    r2 = Redis.from_url(redis_url)
    await r2.connect()

    async def read():
        with trio.move_on_after(0.5) as cancel_scope:
            await r1.xread({'x': '0-0'}, block=5000)

        # Assert that XREAD has been cancelled.
        assert cancel_scope.cancelled_caught

        # Sleep for a bit. In the meanwhile an XADD is executed.
        await trio.sleep(0.5)

        # Try to get a value from a non-existing key.
        # If the connection was not reset we'd get the message which
        # was added by XADD in the write() function.
        result = await r1.get('bleep')
        assert result is None

    async def write():
        # Sleep until the XREAD is cancelled, then do XADD.
        await trio.sleep(0.8)
        await r2.xadd('x', {'a': 1})

    async with trio.open_nursery() as nursery:
        nursery.start_soon(read)
        nursery.start_soon(write)
