class BusyError(Exception):
    pass


class ClosedError(Exception):
    pass


class ClusterStartupError(Exception):
    pass


def create_error_from_reply(obj):
    msg = str(obj)

    if msg.startswith('MOVED'):
        err = ClusterSlotMoved
    else:
        err = ReplyError

    return err(msg)


class ReplyError(Exception):
    pass


class ClusterSlotMoved(ReplyError):
    pass
