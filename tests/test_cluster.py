from trio_redis import RedisCluster


async def test_cluster(redis_cluster):
    redis = RedisCluster(redis_cluster)
    await redis.connect()
    await redis.aclose()
