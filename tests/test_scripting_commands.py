from hashlib import sha1


async def test_script_eval(redis):
    result = await redis.eval('return 1 + 1;')
    assert result == 2


async def test_script_eval_with_keys_and_args(redis):
    result = await redis.eval(
        'return {KEYS[1], KEYS[2], ARGV[1]};',
        ['x', 'y'],
        [3],
    )
    assert result == [b'x', b'y', b'3']


async def test_script_evalsha(redis):
    sha = await redis.script_load('return 1 + 1;')
    assert sha
    result = await redis.evalsha(sha)
    assert result == 2


async def test_script_evalsha_with_keys_and_args(redis):
    sha = await redis.script_load('return {KEYS[1], KEYS[2], ARGV[1]};')
    assert sha
    result = await redis.evalsha(sha, ['x', 'y'], [3])
    assert result == [b'x', b'y', b'3']


async def test_script_exists(redis):
    source = b'return 1 + 1;'
    checksum = sha1(source).hexdigest()

    result = await redis.script_exists(checksum)
    assert result == [False]

    await redis.script_load(source)

    result = await redis.script_exists(checksum)
    assert result == [True]
