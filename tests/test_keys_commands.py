import trio


async def test_exists(redis):
    result = await redis.set('x', 1)
    assert result
    result = await redis.exists('x')
    assert result == 1


async def test_exists_with_not_existing_key(redis):
    result = await redis.exists('x')
    assert result == 0


async def test_exists_with_multiple_keys(redis):
    result = (await redis.pipeline()
        .set('x', 1)
        .set('y', 1)
        .set('z', 1)
    )
    assert result

    # The first three keys exists, but the fourth not.
    # So we count 3 existing keys.
    result = await redis.exists('x', 'y', 'z', 'ō')
    assert result == 3


async def test_pttl(redis):
    result = await redis.set('x', 1, px=500)
    assert result

    # The lifetime of the key should be 500ms (10ms margin).
    result = await redis.pttl('x')
    assert 490 <= result <= 500

    await trio.sleep(0.1)

    # After 100ms it should be 400ms (10ms margin).
    result = await redis.pttl('x')
    assert 390 <= result <= 400


async def test_pttl_with_not_existing_key(redis):
    result = await redis.pttl('x')
    assert result == -2


async def test_pttl_with_no_expire_on_key(redis):
    result = await redis.set('x', 1)
    assert result
    result = await redis.pttl('x')
    assert result == -1


async def test_keys_with_no_keys(redis):
    result = await redis.keys()
    assert result == []


async def test_keys(redis):
    result = (await redis.pipeline()
        .set('x', 1)
        .set('y', 1)
        .set('z', 1)
    )
    assert result

    result = await redis.keys()
    assert {*result} == {*[b'z', b'y', b'x']}
