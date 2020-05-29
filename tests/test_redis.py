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
