"""Custom logging function to ensure format conformity."""
from contextlib import contextmanager
import sys
import time
from typing import Generator, Optional

from .consts import Pid, Seqno


def log(pid: Pid, seqno: Seqno, msg: str, *, timestamp: Optional[float] = None) -> None:
    """Writes a log entry to stderr."""
    timestamp = timestamp or time.time()
    time_micro = int(timestamp * 1e6)
    print(f"[{pid}, seqno={seqno}, time={time_micro}] {msg}", file=sys.stderr)
    sys.stderr.flush()


def log_begin(pid: Pid, seqno: Seqno, event: str, *, timestamp: Optional[float] = None) -> None:
    """Logs the start of an event."""
    return log(pid, seqno, "begin: " + event, timestamp=timestamp)


def log_end(pid: Pid, seqno: Seqno, event: str, *, timestamp: Optional[float] = None) -> None:
    """Logs the end of an event."""
    return log(pid, seqno, "end: " + event, timestamp=timestamp)


@contextmanager
def log_duration(pid: Pid, seqno: Seqno, event: str) -> Generator[None, None, None]:
    """Logs the start and end of an event executed inside this context manager."""
    log_begin(pid, seqno, event)
    try:
        yield
    finally:
        log_end(pid, seqno, event)


@contextmanager
def log_at_end(pid: Pid, seqno: Seqno, event: str) -> Generator[None, None, None]:
    """Logs the end of an event after this context manager finishes."""
    try:
        yield
    finally:
        log_end(pid, seqno, event)
