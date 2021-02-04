class ConnectError(Exception):
    pass


class BusyError(Exception):
    pass


class ClosedError(Exception):
    pass


def create_error_from_reply(obj):
    msg = str(obj)

    if msg.startswith('READONLY'):
        cls = ReadOnlyError
    else:
        cls = ReplyError

    return cls(str(obj))


class ReplyError(Exception):
    pass


class ReadOnlyError(ReplyError):
    pass
