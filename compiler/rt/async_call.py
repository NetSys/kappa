from io import BytesIO
from multiprocessing import Process
from typing import Optional, Sequence

from .chk_manager import CheckpointManager
from .consts import Seqno, Pid, Continuations
from .logging import log
from .protocol import Request, FinalizedCoordinatorCall
from .rpc import rpc, WouldBlock


class AsyncCallsNotSupported(Exception):
    """Raised by constructor of AsyncCaller if async calls are not supported."""
    pass


class _AsyncWorker(Process):
    """Worker process."""
    def __init__(self, rpc_addr: str, calls: Sequence[FinalizedCoordinatorCall], seqno: Seqno,
                 chk_manager: CheckpointManager, chk_file: BytesIO, lambda_pid: Pid) -> None:
        """Initializes a worker.  The `run` method is implicitly invoked when the worker process is started."""
        assert all(call.seqno <= seqno for call in calls)

        super(_AsyncWorker, self).__init__(name=f"async-worker-{seqno}", daemon=True)
        self.rpc_addr = rpc_addr
        self.calls = calls
        self.seqno = seqno
        self.chk_manager = chk_manager
        self.chk_file = chk_file
        self.lambda_pid = lambda_pid  # The Process class already has a "pid" attribute.

    def run(self) -> None:
        """Issues RPC for the calls.

        Returns normally if RPC succeeds (even if calls are blocking).  Otherwise, raises an exception, in which case
        this subprocess would exit abnormally.
        """
        log(self.lambda_pid, self.seqno, "RPC worker started")
        chk_id = self.chk_manager.save_from_file(self.chk_file, self.lambda_pid, self.seqno)
        req = Request(pid=self.lambda_pid, seqno=self.seqno, chk_id=chk_id, calls=self.calls)

        try:
            rpc(self.rpc_addr, req, self.lambda_pid, self.seqno)
        except WouldBlock:
            # TODO(zhangwen): make sure call can't fail after this.
            pass


class AsyncCaller(object):
    """
    Makes coordinator calls asynchronously (in the background).

    When a coordinator call is to be made in the background, its corresponding checkpoint is saved asynchronously,
    and then the call is made through RPC.
    """

    FAILURE_THRESHOLD = 3  # Give up on async calls if there have been >= this number of failures.

    def __init__(self, rpc_addr: Optional[str], chk_manager: CheckpointManager, pid: Pid) -> None:
        """
        Initializes AsyncCaller.  Runs in the main process.

        Raises `AsyncCallsNotSupported` if asynchronous calls are not supported.
        """
        if not rpc_addr:
            raise AsyncCallsNotSupported

        self.rpc_addr = rpc_addr
        self.chk_manager = chk_manager
        self.pid = pid

        self.next_seqno = Seqno(0)  # All calls with seqno less than this have finished.
        self.worker_process: Optional[_AsyncWorker] = None  # Worker currently running (at most one).

        self.num_failures = 0  # Number of consecutive failures.
        self.has_given_up = False  # If too many failures have happened, give up on background calls.

    def _update_worker_state(self, *, terminate_worker: bool) -> None:
        """Updates internal state to reflect all completed coordinator calls.

        If `terminate_worker` is set, terminates any outstanding worker.
        """
        if self.worker_process is None:  # There's nothing to update.
            return

        if self.worker_process.is_alive():  # Previous worker hasn't finished.
            if terminate_worker:
                self.worker_process.terminate()
                # TODO(zhangwen): should join worker?
                self.worker_process = None
                self.num_failures += 1
                log(self.pid, self.next_seqno, f"RPC worker (seqno={self.next_seqno} killed")
        else:  # Previous worker has finished...
            exit_code = self.worker_process.exitcode
            if exit_code == 0:  # ... and succeeded.
                self.next_seqno = Seqno(self.worker_process.seqno + 1)
                self.num_failures = 0
                log(self.pid, self.next_seqno, f"async RPC finished: seqno={self.next_seqno}")
            else:
                self.num_failures += 1
                log(self.pid, self.next_seqno,
                    f"RPC worker (seqno={self.worker_process.seqno}) exited abnormally (code {exit_code})")

            self.worker_process = None

        if self.num_failures >= self.FAILURE_THRESHOLD:
            log(self.pid, self.next_seqno, f"RPC failures exceeded threshold: {self.FAILURE_THRESHOLD}")
            self.has_given_up = True

    def get_next_seqno(self, *, terminate_worker: bool = False) -> Seqno:
        """Returns the smallest seqno for which a coordinator call hasn't been made.

        If `terminate_worker` is set, terminates any outstanding worker.
        """
        self._update_worker_state(terminate_worker=terminate_worker)
        return self.next_seqno

    def call(self, calls: Sequence[FinalizedCoordinatorCall], conts: Continuations, seqno: Seqno) -> bool:
        """Asynchronously issues coordinator calls.

        Returns True if calls have been successfully scheduled.
        """
        if self.has_given_up:
            return False

        self._update_worker_state(terminate_worker=True)

        f = BytesIO()
        self.chk_manager.serialize(conts, f)
        f.seek(0)

        self.worker_process = _AsyncWorker(rpc_addr=self.rpc_addr, calls=calls, seqno=seqno,
                                           chk_manager=self.chk_manager, chk_file=f, lambda_pid=self.pid)
        self.worker_process.start()
        return True
