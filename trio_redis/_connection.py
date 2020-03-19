import hiredis
import trio

from ._errors import BusyError, ClosedError


__all__ = [
    'Connection',
]


class Connection:
    """A TCP connection to a Redis server."""
    def __init__(self, host, port):
        self.host = host
        self.port = port

        self._is_connected = False
        self._is_busy = False
        self._stream = None
        self._parser = hiredis.Reader()

    async def connect(self):
        if self._is_connected:
            raise BusyError('already connected')
        self._stream = await trio.open_tcp_stream(self.host, self.port)
        self._is_connected = True

    async def aclose(self):
        if not self._is_connected:
            raise ClosedError('already closed')
        try:
            await self._stream.aclose()
        finally:
            self._stream = None
            self._is_connected = False

    async def execute(self, command):
        if not self._is_connected:
            raise ClosedError('cannot execute command, connection is closed')
        if self._is_busy:
            raise BusyError('another task is currently executing a command')
        self._is_busy = True

        try:
            request = _build_request(command)
            await self._stream.send_all(request)
            return (await self._read_reply())[0]
        finally:
            self._is_busy = False

    async def execute_many(self, commands):
        if self._is_busy:
            raise BusyError('another task is currently executing a command')
        self._is_busy = True

        try:
            request = b''.join([_build_request(cmd) for cmd in commands])
            await self._stream.send_all(request)
            reply = await self._read_reply(expected=len(commands))
            return reply
        finally:
            self._is_busy = False

    async def _read_reply(self, expected=1):
        """Read and parse replies from connection.

        ``expected`` is the amount of expected replies. In case of
        pipelining this number is set to the amount of commands sent.
        """
        replies = []

        while True:
            self._parser.feed(await self._stream.receive_some())
            while True:
                reply = self._parser.gets()
                if reply is False:
                    break  # Read more data, go back to the outer loop.
                replies.append(reply)
                if len(replies) == expected:
                    return replies


def _build_request(args):
    """Build a RESP request.

    A request consists of a RESP array with RESP bulk strings::

        * [ARRAY LENGHT] \r\n [ARRAY ELEMENTS]

    ``ARRAY LENGHT`` is the length of the array, the number of bulk
    strings. And ``ARRAY ELEMENTS`` contains the bulk strings::

        $ [STRING LENGTH] \r\n [STRING DATA] \r\n

    Here's a real example of ``GET mykey``::

        *2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n

    See `Redis Protocol specification`_ for more information.

    .. _Redis Protocol specification: https://redis.io/topics/protocol

    The hiredis library only exposes and API for decoding responses,
    not building request strings. That's why this function is needed.
    """
    out = [b'*%d\r\n' % len(args)]

    for part in args:
        if isinstance(part, str):
            part = _str_to_bytes(part)
        elif isinstance(part, int):
            part = b'%d' % part
        out.append(b'$%d\r\n%s\r\n' % (len(part), part))

    return b''.join(out)


def _str_to_bytes(v):
    return bytes(v, encoding='utf-8')
