import json
import http.client
from http import HTTPStatus
import os

from .consts import Pid, Seqno
from .logging import log, log_duration
from .protocol import Request


RPC_HTTP_TIMEOUT = float(os.environ["RPC_HTTP_TIMEOUT"])  # Set by the coordinator.


class WouldBlock(Exception):
    """Exception signifying that a coordinator call is blocking"""
    pass


class RPCError(Exception):
    """Exception indicating that RPC has failed."""
    def __init__(self, status: str, message: str) -> None:
        super(Exception, self).__init__(f"{status}: {message}")


def rpc(addr: str, req: Request, pid: Pid, seqno: Seqno):
    """Issues a coordinator call asynchronously.  If the call is blocking, raises WouldBlock."""
    assert addr, "The RPC server address must not be empty."

    with log_duration(pid, seqno, "rpc"):
        conn = http.client.HTTPConnection(addr, timeout=RPC_HTTP_TIMEOUT)  # type: ignore
        # mypy thinks `timeout` has to be an `int`, but passing a `float` doesn't seem to be a problem.

        req_str = str(req)
        log(pid, seqno, f"rpc size: {len(req_str)}")

        conn.request("POST", "", str(req))
        res = conn.getresponse()
        body = res.read()

    if res.status == HTTPStatus.OK:
        return json.loads(body)
    elif res.status == HTTPStatus.ACCEPTED:  # Coordinator call is blocking.
        raise WouldBlock()

    raise RPCError(status=HTTPStatus(res.status).description, message=body.decode("utf-8"))
