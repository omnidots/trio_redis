class BusyError(Exception):
    pass


class ClosedError(Exception):
    pass


class ReplyError(Exception):
    @classmethod
    def from_error_reply(cls, reply):
        return cls(str(reply))
