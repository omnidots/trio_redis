from trio_redis import RedisCluster


async def test_cluster(redis_cluster):
    redis = RedisCluster(redis_cluster.nodes())
    await redis.connect()
    await redis.aclose()


async def test_cluster_asking(redis_cluster):
    # TODO:
    # - Choose key and get slot.
    # - Set slot to MIGRATING on node A, and IMPORTING on node B.
    #       https://redis.io/commands/cluster-setslot
    # - Exec command on key, expect ASK error to be handled.

    redis = RedisCluster(redis_cluster.nodes())
    await redis.connect()
    await redis.aclose()
