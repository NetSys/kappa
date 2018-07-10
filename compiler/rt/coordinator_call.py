"""
Interface for making coordinator calls.

To ensure the accuracy of logging timestamps, any coordinator call wrapper function (e.g., `exit_process`) should do
nothing but raise a corresponding CoordinatorCall instance.
"""
from abc import ABCMeta, abstractmethod
from base64 import b64encode, b64decode
import logging
import pickle
from typing import Callable, List, Any, Sequence, Optional, TYPE_CHECKING, Iterable
import time

from .chk_manager import CheckpointManager
from .continuation import Continuation
from .consts import ContinuationT, Seqno, Pid, INITIAL_SEQNO, NEW_PID
from .protocol import FinalizedCoordinatorCall, ParamDict

if TYPE_CHECKING:
    # We don't want lambdas to need to install this module.
    from typing_extensions import NoReturn


def _serialize_object(obj: object) -> str:
    """Serializes a Python object to an ASCII string; involves calling pickle on the object."""
    return b64encode(pickle.dumps(obj)).decode("ascii")


def _deserialize_result(serialization: str) -> object:
    """Deserialize a serialization of a Python object."""
    return pickle.loads(b64decode(serialization))


class CoordinatorCall(Exception, metaclass=ABCMeta):
    """
    Thrown to take a checkpoint and make a coordinator call.

    To implement a coordinator call, inherit this class and implement all abstract methods.  The "op-code" of a
    coordinator call is simply the class name turned into snake case.
    """
    logger = logging.getLogger(__name__)

    def __init__(self, op: str, params: Optional[ParamDict], *, is_async: bool = False) -> None:
        """
        Initializes a coordinator call.

        :param params: parameters of the coordinator call; if cannot be determined at initialization time, pass `None`
            and override the `finalize` method.
        """
        super(CoordinatorCall, self).__init__(params, is_async)
        self.op = op
        self.start_time = time.time()
        self.continuations: List[ContinuationT] = [self.continuation]
        self.params = params
        self.is_async = is_async

    def add_continuation(self, cont: ContinuationT) -> None:
        """Adds a continuation to the list."""
        self.continuations.append(cont)

    @staticmethod
    @abstractmethod
    def continuation(result: object) -> Any:
        """This method is invoked with the result of the coordinator call when control returns to the lambda."""
        pass

    def _finalize_params(self, chk_manager: CheckpointManager, pid: Pid, seqno: Seqno) -> ParamDict:
        """
        Finalizes a coordinator call's params using global parameters.

        The global runtime parameters (e.g., the checkpoint manager) may not available at CoordinatorCall
        initialization time.  Overriding this method allows the CoordinatorCall to use these parameters.

        The default implementation, which simply returns `self.params`, suffices when all coordinator call arguments are
        known at initialization time and no extra actions are needed involving global runtime parameters.
        """
        assert self.params is not None, "The `_finalize_params` method must be overridden if `self.params` is None."
        return self.params

    def finalize(self, chk_manager: CheckpointManager, pid: Pid, seqno: Seqno) -> FinalizedCoordinatorCall:
        params = self._finalize_params(chk_manager, pid, seqno)
        return FinalizedCoordinatorCall(seqno=seqno, op=self.op, params=params)


# Coordinator call: checkpoint.
class Checkpoint(CoordinatorCall):
    """The "checkpoint" coordinator call creates a new checkpoint."""
    def __init__(self, is_async: bool) -> None:
        super(Checkpoint, self).__init__(op="checkpoint", params={}, is_async=is_async)

    @staticmethod
    def continuation(_result: object) -> None:
        """This coordinator call has no return value."""
        return None


def pause(is_async: bool) -> "NoReturn":
    """Makes the "checkpoint" coordinator call."""
    raise Checkpoint(is_async)


# Coordinator call: exit.
class Exit(CoordinatorCall):
    """The "exit" coordinator call signals that a process has completed with a return value."""
    def __init__(self, result: object) -> None:
        serialized_result = _serialize_object(result)
        super(Exit, self).__init__(op="exit", params={"result": serialized_result})

    @staticmethod
    def continuation(_result: object) -> "NoReturn":
        assert False, 'The "exit" coordinator call should never return.'


def exit_process(result: object) -> "NoReturn":
    """Exits the current process with a return value.  Automatically called at the end of a process."""
    raise Exit(result)


# Coordinator calls related to processes.
class _ProcessStart(Continuation):
    """A starting checkpoint for a subprocess."""
    def __init__(self, f: Callable, args: Sequence[Any]) -> None:
        """Initializes a continuation that runs `f(*args)`."""
        super(_ProcessStart, self).__init__(f, *args)

    @staticmethod
    def run(*args):
        pred_res, f, *args = args  # `pred_res` is a dict containing return values of futures contained in `args`.

        # Materialize any `Future` objects in `args`.
        converted_args = []
        for arg in args:
            if isinstance(arg, Future):
                value = _deserialize_result(pred_res[str(arg.pid)])  # JSON dicts can only have string keys.
                converted_args.append(value)
            else:
                converted_args.append(arg)

        return f(*converted_args)


class Future(object):
    """Represents the result of another process."""
    def __init__(self, pid: Pid) -> None:
        """Initializes a Future object for the process identified by `pid`."""
        self.pid = pid

    def wait(self) -> "NoReturn":
        """Returns the process' result; blocks if the process hasn't finished."""
        raise Wait(self.pid)


class Spawn(CoordinatorCall):
    """The "spawn" coordinator call spawns, on a lambda, a subprocess that runs a function."""
    def __init__(self, f: Callable, args: Sequence[Any], awaits: Iterable[Future], name: Optional[str],
                 blocking: bool, copies: int) -> None:
        """Initializes a coordinator call that launches `copies` subprocesses, named `name`, running `f(*args)`."""
        super(Spawn, self).__init__(op="spawn", params=None)  # Parameters are determined in `_finalize_params`.
        self.name = name or getattr(f, "__name__", "unnamed")
        self.copies = copies

        # Other processes whose return values the subprocess depends on.
        self.future_pids = list(set(arg.pid for arg in args if isinstance(arg, Future)))

        # Other processes that this subprocess waits for.
        self.await_pids = list(set(f.pid for f in awaits))

        self.blocking = blocking
        self.on_coordinator = bool(getattr(f, "on_coordinator", False))
        self.child_cont = _ProcessStart(f, args)

    @staticmethod
    def continuation(result: object):
        """Continuation for after the child has been spawned."""
        assert isinstance(result, dict), f"{result} is not a dict"

        child_pids = result.get("child_pids")
        if child_pids is not None:  # Non-blocking call => return future for child result.
            assert child_pids, "list of child pids must not be empty"
            return [Future(Pid(pid)) for pid in child_pids]

        # Blocking call => return result immediately.
        return [_deserialize_result(ret) for ret in result["rets"]]

    def _finalize_params(self, chk_manager: CheckpointManager, pid: Pid, seqno: Seqno) -> ParamDict:
        # It's fine to use the same PID for all spawns because the checkpoint manager generates a unique checkpoint ID.
        child_chk_id = chk_manager.save([self.child_cont], NEW_PID, INITIAL_SEQNO)
        params: ParamDict = {
            "name": self.name,
            "child_chk_id": child_chk_id,
            "future_pids": self.future_pids,
            "await_pids": self.await_pids,
            "blocking": self.blocking,
            "on_coordinator": self.on_coordinator,
            "copies": self.copies
        }
        return params


class SpawnOne(Spawn):
    """Spawns a subprocess that runs a function."""
    def __init__(self, f: Callable, args: Sequence[Any], awaits: Iterable[Future], name: Optional[str],
                 blocking: bool) -> None:
        super(SpawnOne, self).__init__(f, args, awaits, name, blocking, copies=1)

    @staticmethod
    def continuation(result: object):
        return Spawn.continuation(result)[0]


def spawn(f: Callable, args: Sequence[object], *, awaits: Iterable[Future] = (), name: Optional[str] = None,
          blocking: bool = False) -> "NoReturn":
    """
    Launches a subprocess that runs `f(*args)`.

    If one subprocess is launched, returns the child's future.  If multiple are launched, returns a list of futures.

    If `blocking` is set, blocks until the subprocess finishes and returns the result.  Otherwise, returns immediately
    with a `Future` for the subprocess' result.

    Because `f`, and `args` will be sent to the coordinator, they must be supported by pickle.  Specifically, `f` can be
    a module-level function, a built-in function, a method of a module-level class, etc.

    If any `Future` objects are passed in as arguments, the subprocess will not start until the futures are done, and
    `f` will be provided with the futures' values, instead of the `Future` objects themselves.

    If `name` is not specified, uses heuristics to generate a name for the subprocess.

    If a sequence of futures are passed in as `awaits`, each child does not start until all these futures are complete.
    """
    raise SpawnOne(f, args, awaits=awaits, name=name, blocking=blocking)


def spawn_many(f: Callable, args: Sequence[object], copies: int, *, awaits: Iterable[Future] = (),
               name: Optional[str] = None, blocking: bool = False) -> "NoReturn":
    """Launches `copies` subprocesses that run `f(*args)`."""
    raise Spawn(f, args, awaits=awaits, name=name, blocking=blocking, copies=copies)


def on_coordinator(f):
    """A decorator that, when applied to a function, makes a spawn of that function happen on the coordinator."""
    f.on_coordinator = True
    return f


class _MapProcessStart(Continuation):
    """A starting checkpoint for a subprocess spawned using `map_spawn`."""
    def __init__(self, f: Callable, extra_args: Sequence[object]) -> None:
        super(_MapProcessStart, self).__init__(f, *extra_args)

    @staticmethod
    def run(*args):
        (pred_res, serialized_elem), f, *args = args
        assert isinstance(pred_res, dict)
        assert isinstance(serialized_elem, str)
        elem = _deserialize_result(serialized_elem)

        # Materialize any `Future` objects in `args`.
        converted_args = []
        for arg in args:
            if isinstance(arg, Future):
                value = _deserialize_result(pred_res[str(arg.pid)])  # JSON dicts can only have string keys.
                converted_args.append(value)
            else:
                converted_args.append(arg)

        return f(elem, *converted_args)


# TODO(zhangwen): spawn and spawn_one can be implemented in terms of map_spawn.
# Not re-implementing them right now to keep prior experiment results valid.
class MapSpawn(CoordinatorCall):
    """The "map_spawn" coordinator call spawns lambdas to run the same function on different objects."""
    def __init__(self, f: Callable, elems: Iterable[object], extra_args: Iterable[object], awaits: Iterable[Future],
                 name: Optional[str]) -> None:
        super(MapSpawn, self).__init__(op="map_spawn", params=None)  # Parameters are determined in `_finalize_params`.
        self.name = name or getattr(f, "__name__", "unnamed")
        self.elems = list(map(_serialize_object, elems))

        extra_args = list(extra_args)
        # Other processes whose return values the subprocess depends on.
        self.future_pids = list(set(arg.pid for arg in extra_args if isinstance(arg, Future)))

        # Other processes that this subprocess waits for.
        self.await_pids = list(set(f.pid for f in awaits))

        self.on_coordinator = bool(getattr(f, "on_coordinator", False))
        self.child_cont = _MapProcessStart(f, extra_args)

    @staticmethod
    def continuation(children_pids: object):
        """Continuation for after the children have been spawned."""
        assert isinstance(children_pids, list), f"{children_pids} is not a list"
        return [Future(Pid(pid)) for pid in children_pids]

    def _finalize_params(self, chk_manager: CheckpointManager, pid: Pid, seqno: Seqno) -> ParamDict:
        # It's fine to use the same PID for all spawns because the checkpoint manager generates a unique checkpoint ID.
        child_chk_id = chk_manager.save([self.child_cont], NEW_PID, INITIAL_SEQNO)
        params: ParamDict = {
            "name": self.name,
            "child_chk_id": child_chk_id,
            "future_pids": self.future_pids,
            "elems": self.elems,
            "await_pids": self.await_pids,
            "on_coordinator": self.on_coordinator,
        }
        return params


def map_spawn(f: Callable, elems: Iterable[object], *, extra_args: Iterable[object] = (), awaits: Iterable[Future] = (),
              name: Optional[str] = None) -> "NoReturn":
    """Runs `f` on each of `elems` on a separate worker."""
    raise MapSpawn(f, elems, awaits=awaits, name=name, extra_args=extra_args)


class Wait(CoordinatorCall):
    """The "wait" coordinator call blocks until a process completes, then returns the process' result."""
    def __init__(self, pid: Pid) -> None:
        """Initializes a "wait" coordinator call for the process `pid`."""
        super(Wait, self).__init__(op="wait", params={"pid": pid})

    @staticmethod
    def continuation(result: object) -> Any:
        """
        Continuation for after the waited process has completed.
        :param result: Base64 encoding of the result's pickle.
        """
        assert isinstance(result, str)
        return _deserialize_result(result)


# Coordinator calls related to queues.
class Queue(object):
    """Represents a queue, allowing communication between processes."""
    def __init__(self, qid: int) -> None:
        self.qid = qid

    def enqueue(self, *objs, **kwargs) -> "NoReturn":
        """Pass in `is_async=True` as keyword argument for async enqueue."""
        is_async = kwargs.pop("is_async", False)
        if kwargs:
            raise ValueError(f"Extraneous keyword arguments: {kwargs}")

        raise Enqueue(qid=self.qid, objs=objs, is_async=is_async)

    def dequeue(self) -> "NoReturn":
        raise Dequeue(qid=self.qid)


class CreateQueue(CoordinatorCall):
    """The "create_queue" coordinator call creates a queue, which facilitates communication between processes."""
    def __init__(self, max_size: int, copies: int = -1) -> None:
        """
        Creates a queue with a specified maximum number of elements.

        An enqueue operation that causes the queue to go above the maximum size will block.  If maxsize == 0,
        an enqueue always blocks until the element is dequeued (usually by some other process).
        """
        # TODO(zhangwen): is maxsize a good idea?
        super(CreateQueue, self).__init__(op="create_queue", params={"max_size": max_size, "copies": copies})

    @staticmethod
    def continuation(result: object) -> Any:
        # `result`: the queue's id(s).
        if isinstance(result, int):
            return Queue(result)

        assert isinstance(result, list), f"{result} is not a list"
        return [Queue(qid) for qid in result]


def create_queue(max_size: int) -> "NoReturn":
    raise CreateQueue(max_size)


def create_queues(max_size: int, copies: int) -> "NoReturn":
    raise CreateQueue(max_size, copies)


class Enqueue(CoordinatorCall):
    """The "enqueue" coordinator call puts an object into a queue; blocks if the queue's max size will be exceeded."""
    def __init__(self, qid: int, objs: Sequence[object], is_async: bool) -> None:
        serialized_objs = list(map(_serialize_object, objs))
        self.logger.info("enqueue: total serialized length: %d", sum(len(m) for m in serialized_objs))
        super(Enqueue, self).__init__(op="enqueue", params={"qid": qid, "objs": serialized_objs}, is_async=is_async)

    @staticmethod
    def continuation(_: object) -> Any:
        pass  # Item has been enqueued; nothing needs to be done.


class Dequeue(CoordinatorCall):
    """The "dequeue" coordinator call retrieves an object from a queue; blocks if the queue is empty."""
    def __init__(self, qid: int) -> None:
        super(Dequeue, self).__init__(op="dequeue", params={"qid": qid})

    @staticmethod
    def continuation(result: object) -> Any:
        # `result`: the serialization of the dequeued object.
        assert isinstance(result, str)
        return _deserialize_result(result)


# (Pseudo) coordinator call: blocked.
class Blocked(CoordinatorCall):
    """
    The "blocked" coordinator call indicates that a previous asynchronous call has blocked.

    This call must be issued synchronously.  It's only meant to inform the coordinator; it should not be returned to.
    The seqno associated with this call indicates the seqno of the previous call on which the lambda is blocked.
    """
    def __init__(self) -> None:
        super(Blocked, self).__init__(op="blocked", params={})

    @staticmethod
    def continuation(result: object) -> "NoReturn":
        assert False, 'must not complete a "blocked" continuation call'


class RemapStore(CoordinatorCall):
    """
    The "remap_store" coordinator call makes the coordinator move `tmp_bucket/tmp_key` to `bucket/key`.
    """
    def __init__(self, tmp_bucket: str, tmp_key: str, bucket: str, key: str, is_async: bool) -> None:
        super(RemapStore, self).__init__(
            op="remap_store",
            params={"tmp_bucket": tmp_bucket, "tmp_key": tmp_key, "bucket": bucket, "key": key},
            is_async=is_async
        )

    @staticmethod
    def continuation(_result: object) -> None:
        """This coordinator call has no return value."""
        return None
