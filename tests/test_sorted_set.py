import pytest


async def test_zadd_and_zcount(redis):
    result = await redis.zadd('x', [('foo', 1)])
    assert result == 1

    result = await redis.zcount('x', '-inf', '+inf')
    assert result == 1


async def test_zadd_with_incr(redis):
    result = await redis.zadd('x', mapping=[('a', 1)], incr=True)
    assert result == 1.0
    result = await redis.zadd('x', mapping=[('a', 1)], incr=True)
    assert result == 2.0
    result = await redis.zadd('x', mapping=[('a', 1)], incr=True)
    assert result == 3.0


async def test_zadd_with_empty_mapping(redis):
    with pytest.raises(ValueError):
        await redis.zadd('x', mapping=[])


async def test_zadd_with_nx_and_xx(redis):
    with pytest.raises(ValueError):
        await redis.zadd('x', mapping=[('a', 1)], nx=True, xx=True)


async def test_zadd_incr_with_multiple_mappings(redis):
    with pytest.raises(ValueError):
        await redis.zadd('x', mapping=[('a', 1), ('b', 2)], incr=True)
