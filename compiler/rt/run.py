"""Runs transformed lambda code."""
import functools
import itertools
import logging
import os
from pathlib import Path
from typing import Callable, Dict, List, Optional

from .async_call import AsyncCaller, AsyncCallsNotSupported
from .chk_manager import CheckpointManager, NULL_CHK_ID
from .consts import Continuations, Seqno, Pid, CheckpointID, MAIN_PID, INITIAL_SEQNO
from .coordinator_call import CoordinatorCall, exit_process, Exit, spawn
from .global_state import pause_ctrl
from .logging import log, log_begin, log_duration, log_at_end
from .protocol import Request, FinalizedCoordinatorCall
from .rpc import rpc, WouldBlock

logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)


class _CoordinatorCallBacklog(List[FinalizedCoordinatorCall]):
    """Represents coordinator calls yet to be made."""
    def prune(self, next_seqno: Seqno) -> None:
        """Removes calls with seqno less than `next_seqno`."""
        for i in reversed(range(len(self))):
            if self[i].seqno < next_seqno:
                del self[i]


def run(entry_point: Callable, *args, **kwargs):
    """Calls _run() with sensible defaults."""
    return _run(select_chk_manager("command_line"), MAIN_PID, INITIAL_SEQNO, NULL_CHK_ID, None, None,
                entry_point, *args, **kwargs)


def _run(chk_manager: CheckpointManager, pid: Pid, start_seqno: Seqno, start_chk_id: CheckpointID, return_value,
         rpc_addr: Optional[str], entry_point: Callable, *args, **kwargs) -> Request:
    """
    Resumes execution from saved state.  If no saved state is found, invokes the provided entry-point callable.

    If not in CPS mode, skips loading checkpoint and directly invokes the provided entry point (to keep
    un-transformed code functional).

    :param return_value: return value of the previous coordinator call.
    """
    # If not previous checkpoint is found, start fresh.
    with log_duration(pid, start_seqno, "load_chk"):
        continuations: Continuations = chk_manager.load(start_chk_id) or [lambda _: entry_point(*args, **kwargs)]

    async_caller: Optional[AsyncCaller] = None
    try:
        async_caller = AsyncCaller(rpc_addr, chk_manager, pid)
    except AsyncCallsNotSupported:
        pass

    cc_backlog = _CoordinatorCallBacklog()

    for _seqno in itertools.count(start=start_seqno):
        seqno = Seqno(_seqno)  # Wrap in Seqno for stricter type checking.

        i = 0
        try:
            with log_duration(pid, seqno, "compute"):
                for cont in continuations:
                    return_value = cont(return_value)
                    i += 1

                exit_process(return_value)
        except Exit as cc:  # Special-case the "exit" coordinator call.
            finalized = cc.finalize(chk_manager, pid, seqno)
            return Request(pid=pid, seqno=seqno, chk_id=NULL_CHK_ID, calls=[finalized])
        except CoordinatorCall as cc:  # Other coordinator calls.
            if cc.is_async:
                log_type = "async coordinator call"
            else:
                log_type = "coordinator call"

            log_begin(pid, seqno, log_type, timestamp=cc.start_time)
            with log_at_end(pid, seqno, log_type):
                # The saved continuations include the ones generated during this execution, and the ones left unrun from
                # the previous execution.
                continuations = cc.continuations + continuations[i+1:]

                if async_caller:
                    cc_backlog.prune(async_caller.get_next_seqno(terminate_worker=True))
                cc_backlog.append(cc.finalize(chk_manager, pid, seqno))

                if cc.is_async and async_caller and async_caller.call(cc_backlog, continuations, seqno):
                    continue

                # If we're here, we're doing the call synchronously.
                chk_id = chk_manager.save(continuations, pid, seqno)
                req = Request(pid=pid, seqno=seqno, chk_id=chk_id, calls=cc_backlog)
                log(pid, seqno, f"sending request with {len(cc_backlog)} coordinator calls")
                if rpc_addr:
                    try:
                        return_value = rpc(rpc_addr, req, pid, seqno)
                        cc_backlog.clear()
                        continue
                    except WouldBlock:
                        log(pid, seqno, "rpc blocked")
                        return Request.make_blocked(pid, seqno)
                    except Exception as e:
                        log(pid, seqno, f"rpc: {e}; falling back to synchronous")
                        # RPC failed; fall back to quitting lambda with coordinator call.
                        req = req._replace(err=f"RPC: {e}, falling back to synchronous (is your coordinator machine "
                                               "publicly accessible?)")

                return req
        finally:
            pause_ctrl.record_pause()

    assert False  # Unreachable.


def select_chk_manager(platform: str) -> CheckpointManager:
    """Returns a checkpoint manager corresponding to the platform.  Raises ValueError if platform is not recognized."""
    # Import locally so that the irrelevant checkpoint manager classes don't need to be importable.
    if platform == "local":
        from .chk_manager import LocalCheckpointManager
        return LocalCheckpointManager(Path(os.environ["CHECKPOINT_DIR"]))
    elif platform == "aws":
        from .chk_manager import S3CheckpointManager
        return S3CheckpointManager(bucket_name=os.environ["CHECKPOINT_BUCKET"])

    # TODO(zhangwen): signal that this error is fatal?
    raise ValueError("No checkpoint manager for platform: {}".format(platform))


def lambda_handler(handler):
    """
    Decorator to apply to a lambda handler.

    This decorator extracts Kappa runtime parameters from the `event` structure and passes any user-defined
    parameters to the handler.
    """
    @functools.wraps(handler)
    def decorated_handler(event: Dict[str, object], context) -> str:
        # TODO(zhangwen): add a way to induce artificial failures.
        # FIXME(zhangwen): maybe just don't pass context to user handler...  e.g., we don't want the context pickled.
        context = None

        _pid, _seqno, _chk_id, = event["pid"], event["seqno"], event["chk_id"]
        assert isinstance(_pid, int)
        pid = Pid(_pid)
        assert isinstance(_seqno, int)
        seqno = Seqno(_seqno)

        log(pid, seqno, f"lambda started!")

        assert isinstance(_chk_id, str)
        chk_id = CheckpointID(_chk_id)

        platform = os.environ["PLATFORM"]

        rpc_addr = None
        rpc_ip = os.environ.get("RPC_IP")
        if rpc_ip is not None:
            if os.environ["WHERE"] == "coordinator":
                rpc_ip = "127.0.0.1"  # If task is running on the coordinator machine, issue RPCs to localhost.
            rpc_port = os.environ["RPC_PORT"]  # RPC_PORT should be present in the environment iff RPC_IP is.
            rpc_addr = f"{rpc_ip}:{rpc_port}"

        app_event = event["app_event"]
        chk_manager = select_chk_manager(platform)

        last_return_value = event["coord_call_result"]

        def entry_point():
            return spawn(handler, (app_event, context), blocking=True)

        return str(_run(chk_manager, pid, seqno, chk_id, last_return_value, rpc_addr, entry_point))

    return decorated_handler
