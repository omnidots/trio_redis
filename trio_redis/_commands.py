# Lots of checking and parsing code is from redis-py.
# See: https://github.com/andymccurdy/redis-py/blob/master/redis/client.py


from functools import partial
from io import BytesIO


__all__ = [
    'ConnectionCommands',
    'KeysCommands',
    'ScriptingCommands',
    'ServerCommands',
    'SortedSetCommands',
    'StreamCommands',
    'StringCommands',
]


_EMPTY_TUPLE = tuple()  # noqa: C408


def bool_ok(reply):
    return reply == b'OK'


def parse_int_bool_list(reply):
    return [r == 1 for r in reply]


def parse_xread(reply):
    if reply is None:
        return []
    return [[r[0], parse_stream_list(r[1])] for r in reply]


def parse_stream_list(reply):
    if reply is None:
        return None
    data = []
    for r in reply:
        if r is not None:
            data.append((r[0], pairs_to_dict(r[1])))
        else:
            data.append((None, None))
    return data


def parse_xinfo_stream(response):
    data = pairs_to_dict(response)

    for k in (b'first-entry', b'last-entry'):
        item = data[k]
        if item is not None:
            data[k] = (item[0], pairs_to_dict(item[1]))

    return data


def pairs_to_dict(reply):
    it = iter(reply)
    return dict(zip(it, it))


def parse_list_of_dicts(reply):
    return [pairs_to_dict(r) for r in reply]


def parse_xpending(response, **options):
    consumers = [{b'name': n, b'pending': p} for n, p in response[3] or []]
    return {
        b'pending': response[0],
        b'min': response[1],
        b'max': response[2],
        b'consumers': consumers
    }


def parse_xpending_range(reply):
    if not reply:
        return []

    return [
        {
            'message_id': r[0],
            'consumer': r[1],
            'time_since_delivered': int(r[2]),
            'times_delivered': int(r[3]),
        }
        for r in reply
    ]


def parse_zadd(reply, as_score=False):
    if reply is None:
        return None
    if as_score:
        return float(reply)
    return int(reply)


def parse_cluster_nodes(reply):
    nodes = []

    for row in BytesIO(reply):
        values = row.split(b' ')
        nodes.append({
            'id': values[0],
            'address': values[1],  # TODO: Parse address
            'flags': [f.decode('utf-8') for f in values[2].split(b',')],
            'master': values[3],
            'ping_sent': int(values[4]),
            'pong_recv': int(values[5]),
            'config_epoch': int(values[6]),
            'link_state': values[7].rstrip(b'\n'),
            'slots': [[int(n) for n in s.split(b'-')] for s in values[8:]],
        })

    return nodes


class ClusterCommands:
    def nodes(self):
        return self.execute([b'CLUSTER', b'NODES'], parse_cluster_nodes)

    def slots(self):
        return self.execute([b'CLUSTER', b'SLOTS'])

    def cluster_set_slot(self):
        pass


class ConnectionCommands:
    def select(self, index):
        return self.execute([b'SELECT', index])


class KeysCommands:
    def exists(self, *keys):
        return self.execute([b'EXISTS', *keys], keys=keys)

    def pttl(self, key):
        return self.execute([b'PTTL', key], key=key)

    def keys(self, pattern='*'):
        return self.execute([b'KEYS', pattern])


class ScriptingCommands:
    def eval(self, script, keys=[], args=[]):
        return self.execute([b'EVAL', script, len(keys), *keys, *args], keys=keys)

    def evalsha(self, sha1, keys=[], args=[]):
        return self.execute([b'EVALSHA', sha1, len(keys), *keys, *args], keys=keys)

    def script_load(self, script):
        return self.execute([b'SCRIPT', b'LOAD', script])

    def script_flush(self):
        return self.execute([b'SCRIPT', b'FLUSH'])

    def script_exists(self, *sha1):
        return self.execute([b'SCRIPT', b'EXISTS', *sha1], parse_int_bool_list)


class ServerCommands:
    def flushdb(self):
        return self.execute([b'FLUSHDB'])


class SortedSetCommands:
    def zadd(self, key, mapping, nx=False, xx=False, ch=False, incr=False):
        if not mapping:
            raise ValueError('ZADD requires at least one element/score pair')
        if nx and xx:
            raise ValueError("ZADD allows either 'nx' or 'xx', not both")
        if incr and len(mapping) != 1:
            raise ValueError("ZADD option 'incr' only works when passing a"
                             'single element/score pair')

        pieces = [b'ZADD', key]
        as_score = False

        if nx:
            pieces.append(b'NX')
        if xx:
            pieces.append(b'XX')
        if ch:
            pieces.append(b'CH')
        if incr:
            pieces.append(b'INCR')
            as_score = True

        # ((<member>, <score>), …) → ((<score>, <member>), …)
        for pair in mapping:
            pieces.extend([pair[1], pair[0]])

        return self.execute(pieces, partial(parse_zadd, as_score=as_score), key=key)

    def zcount(self, key, min='-inf', max='+inf'):
        return self.execute([b'ZCOUNT', key, min, max], key=key)


class StreamCommands:
    def xadd(self, key, fields, id=u'*', maxlen=None, approximate=True):
        pieces = [b'XADD', key]

        if maxlen is not None:
            if not isinstance(maxlen, int) or maxlen < 1:
                raise ValueError('XADD maxlen must be a positive integer')
            pieces.append(b'MAXLEN')
            if approximate:
                pieces.append(b'~')
            pieces.append(maxlen)

        pieces.append(id)

        if not isinstance(fields, dict) or len(fields) == 0:
            raise ValueError('XADD fields must be a non-empty dict')
        for pair in fields.items():
            pieces.extend(pair)

        return self.execute(pieces, key=key)

    def xread(self, streams, count=None, block=None):
        pieces = [b'XREAD']

        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise ValueError('XREADGROUP count must be a positive integer')
            pieces.extend([b'COUNT', count])

        if block is not None:
            if not isinstance(block, int) or block < 0:
                raise ValueError('XREADGROUP block must be a non-negative '
                                 'integer')
            pieces.extend([b'BLOCK', block])

        pieces.append(b'STREAMS')
        pieces.extend(streams.keys())
        pieces.extend(streams.values())

        return self.execute(pieces, parse_xread, keys=streams.keys())

    def xreadgroup(
        self,
        groupname,
        consumername,
        streams,
        count=None,
        block=None,
    ):
        pieces = [b'XREADGROUP', b'GROUP', groupname, consumername]

        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise ValueError('XREADGROUP count must be a positive integer')
            pieces.extend([b'COUNT', count])

        if block is not None:
            if not isinstance(block, int) or block < 0:
                raise ValueError('XREADGROUP block must be a non-negative '
                                 'integer')
            pieces.extend([b'BLOCK', block])

        if not isinstance(streams, dict) or not streams:
            raise ValueError('XREADGROUP streams must be a non empty dict')

        pieces.append(b'STREAMS')
        pieces.extend(streams.keys())
        pieces.extend(streams.values())

        return self.execute(pieces, parse_xread, keys=streams.keys())

    def xgroup_create(self, key, groupname, id=b'$', mkstream=False):
        pieces = [b'XGROUP', b'CREATE', key, groupname, id]
        if mkstream:
            pieces.append(b'MKSTREAM')
        return self.execute(pieces, bool_ok, key=key)

    def xack(self, key, groupname, *ids):
        return self.execute([b'XACK', key, groupname, *ids], int, key=key)

    def xpending(self, key, groupname):
        return self.execute([b'XPENDING', key, groupname], parse_xpending, key=key)

    def xpending_range(self, key, groupname, start, end, count, consumername=None):
        # NOTE: The start, end and count arguments are optional to
        # XPENDING, but we only use the command with these arguments.
        # So they are not optional here.

        pieces = [b'XPENDING', key, groupname, start, end, count]
        if consumername is not None:
            pieces.append(consumername)

        return self.execute(pieces, parse_xpending_range, key=key)

    def xclaim(
        self,
        key,
        groupname,
        consumername,
        min_idle_time,  # Milliseconds.
        message_ids,
    ):
        # NOTE: XCLAIM is partially implemented. The optional arguments
        # are not used and there's no benefit in implementing things
        # that are not used.

        if not isinstance(min_idle_time, int) or min_idle_time < 0:
            raise ValueError('XCLAIM min_idle_time must be a non negative '
                             'integer')
        if not isinstance(message_ids, (list, tuple)) or not message_ids:
            raise ValueError('XCLAIM message_ids must be a non empty list or '
                             'tuple of message IDs to claim')

        return self.execute([
            b'XCLAIM',
            key,
            groupname,
            consumername,
            min_idle_time,
            *message_ids
        ], parse_stream_list, key=key)

    def xlen(self, key):
        return self.execute([b'XLEN', key], int, key=key)

    def xrange(self, key, min='-', max='+', count=None):
        pieces = []
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise ValueError('XRANGE count must be a positive integer')
            pieces.extend([b'COUNT', str(count)])
        return self.execute([b'XRANGE', key, min, max, *pieces], parse_stream_list, key=key)

    def xinfo_groups(self, key):
        return self.execute([b'XINFO', b'GROUPS', key], parse_list_of_dicts, key=key)

    def xinfo_stream(self, key):
        return self.execute([b'XINFO', b'STREAM', key], parse_xinfo_stream, key=key)

    def xdel(self, key, *ids):
        return self.execute([b'XDEL', key, *ids], int, key=key)

    def xtrim(self, key, maxlen, approximate=True):
        pieces = [b'XTRIM', key, b'MAXLEN']

        if approximate:
            pieces.append(b'~')

        pieces.append(maxlen)
        return self.execute(pieces, int, key=key)


class StringCommands:
    def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        pieces = [b'SET', key, value]

        if ex is not None:
            pieces.extend([b'EX', ex])
        if px is not None:
            pieces.extend([b'PX', px])

        if nx:
            pieces.append(b'NX')
        if xx:
            pieces.append(b'XX')

        return self.execute(pieces, bool_ok, key=key)

    def get(self, key):
        return self.execute([b'GET', key], key=key)


class DisableSelectCommand:
    def select(self, *args, **kwargs):
        raise NotImplementedError('select is disabled')
