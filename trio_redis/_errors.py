class ConnectError(Exception):
    pass


class BusyError(Exception):
    pass


class ClosedError(Exception):
    pass


def create_error_from_reply(obj):
    return ReplyError(str(obj))


class ReplyError(Exception):
    pass
