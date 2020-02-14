from trio_redis.testing_utils import fail_if_not_between


async def test_set(redis):
    result = await redis.set('x', 1)
    assert result
    result = await redis.set('x', 1)
    assert result


async def test_set_with_nx(redis):
    result = await redis.set('x', 1, nx=True)
    assert result
    result = await redis.set('x', 1, nx=True)
    assert not result


async def test_set_with_xx(redis):
    result = await redis.set('x', 1, xx=True)
    assert not result
    result = await redis.set('x', 1)
    assert result
    result = await redis.set('x', 1, xx=True)
    assert result


async def test_set_with_ex(redis):
    result = await redis.set('x', 1, ex=1)
    assert result

    async def set_key_if_not_exists():
        return await redis.set('x', 1, nx=True)

    await fail_if_not_between(
        after=1.0,
        before=1.1,
        coroutine=set_key_if_not_exists,
    )


async def test_set_with_px(redis):
    result = await redis.set('x', 1, px=500)
    assert result

    async def set_key_if_not_exists():
        return await redis.set('x', 1, nx=True)

    await fail_if_not_between(
        after=0.5,
        before=0.6,
        coroutine=set_key_if_not_exists,
    )


async def test_get(redis):
    result = await redis.set('x', 1)
    assert result
    result = await redis.get('x')
    assert result == b'1'


async def test_get_not_existing_key(redis):
    result = await redis.get('x')
    assert result is None
