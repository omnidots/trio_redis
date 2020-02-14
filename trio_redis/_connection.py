import hiredis
import trio

from ._errors import BusyError, ClosedError


__all__ = [
    'Connection',
]


class Connection:
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
        replys = []

        while True:
            self._parser.feed(await self._stream.receive_some())
            while True:
                reply = self._parser.gets()
                if reply is False:
                    break  # Read more data, go back to the outer loop.
                replys.append(reply)
                if len(replys) == expected:
                    return replys


def _build_request(args):
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
