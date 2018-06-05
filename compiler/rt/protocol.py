"""Protocol for contacting the coordinator to make coordinator calls."""
import json
from typing import Dict, NamedTuple, Iterable, Optional

from .chk_manager import NULL_CHK_ID
from .consts import CheckpointID, Pid, Seqno

ParamDict = Dict[str, object]


class FinalizedCoordinatorCall(NamedTuple):
    """Represents the request sent to the coordinator for a single coordinator call."""
    seqno: Seqno
    op: str
    params: ParamDict


class Request(NamedTuple):
    """Represents a request to send to the coordinator when making a batch of coordinator calls."""
    pid: Pid
    seqno: Seqno
    chk_id: CheckpointID
    calls: Iterable[FinalizedCoordinatorCall]
    blocked: bool = False  # True if the lambda has terminated because it's blocked on a coordinator call.
    err: Optional[str] = None

    def __str__(self) -> str:
        return json.dumps({
            "pid": self.pid,
            "seqno": self.seqno,
            "chk_id": self.chk_id,
            "calls": [call._asdict() for call in self.calls],
            "blocked": self.blocked,
            "err": self.err,
        })

    @staticmethod
    def make_blocked(pid: Pid, seqno: Seqno) -> "Request":
        """Returns an empty request."""
        return Request(pid=pid, seqno=seqno, chk_id=NULL_CHK_ID, calls=[], blocked=True)
