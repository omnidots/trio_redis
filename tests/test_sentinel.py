from trio_redis import RedisSentinel
from trio_redis._redis import Sentinel


async def test_simple(redis_sentinel_cluster):
    client = RedisSentinel.from_url(
        'test_cluster',
        redis_sentinel_cluster.sentinels(),
    )
    await client.connect()
    await client.set('x', 1)
    await client.aclose()


async def test_get_master_addr_by_name(redis_sentinel_cluster):
    client = Sentinel.from_url(redis_sentinel_cluster.sentinels())
    await client.connect()
    assert (await client.get_master_addr_by_name('test_cluster')) == ('127.0.0.1', 6379)
    await client.aclose()


async def test_failover(redis_sentinel_cluster):
    client = RedisSentinel.from_url(
        'test_cluster',
        redis_sentinel_cluster.sentinels(),
    )
    await client.connect()
    await client.set('x', 1)

    # Force fail=over by killing the current master.
    await redis_sentinel_cluster.kill_master()

    # This should block until the new master is available.
    assert await client.get('x') == b'1'

    await client.aclose()
