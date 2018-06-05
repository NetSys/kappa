"""
Kappa runtime library.

This module will run on lambdas, so we should be careful about dragging in external modules.
"""
import logging

from .run import run, lambda_handler
from .continuation import Continuation
from .coordinator_call import CoordinatorCall, pause as cc_pause
from .global_state import pause_ctrl

from .coordinator_call import exit_process, spawn, spawn_many, map_spawn, create_queue, create_queues, on_coordinator


def pause(is_async=False):
    """Pauses execution to take a checkpoint."""
    cc_pause(is_async=is_async)


def maybe_pause():
    """Pauses to take a checkpoint if deemed appropriate by policy."""
    if pause_ctrl.should_pause():
        cc_pause(is_async=True)


checkpoint = pause  # What "pause" should have been named...


def set_logging_level(level) -> None:
    """Sets the logging level for the runtime's logger."""
    logging.getLogger(__name__).setLevel(level)


# Support for classes whose __init__() method could pause.
class _InitContinuation(Continuation):
    """Continuation to run after __init__()."""
    @staticmethod
    def run(*args):
        result, obj = args  # `obj` is the object being initialized.
        if result is not None:
            raise TypeError("__init__() should return None, not '{}'.".format(result.__class__.__name__))
        return obj


class TransformedClassMeta(type):
    """Metaclass for classes whose __init__() method could pause."""
    def __call__(cls, *args, **kwargs):
        """Implement object creation in Python."""
        # TODO(zhangwen): this assumes that __new__ cannot pause.
        obj = cls.__new__(cls)

        try:
            obj.__init__(*args, **kwargs)
        except CoordinatorCall as cc:
            cc.add_continuation(_InitContinuation(obj))
            raise

        return obj


_reconstructor_supported_types = {str, bytes, int, float, list, dict, set}


def _reconstructor(func, args, kwargs):
    """Invoked during unpickling."""
    return reconstructor(func, *args, **kwargs)


def reconstructor(func, *args, **kwargs):
    """
    Invokes `func(*args)` and makes `func` the "deserializer" of its return value.

    Example use case: a `bytes` object backed by a persistent S3 object doesn't need to be serialized in a checkpoint
    because it can be reconstructed from said S3 object at deserialization time.

    May return a different object from the return value of `func`, so code that depends on identity checks may fail.

    Supports objects of types listed in `_reconstructor_supported_types` and of all user-defined classes.  In
    unsupported cases, the original function's functionality is unchanged.

    Function `func` MUST be visible at the module level (for it to be pickled).
    """
    obj = func(*args, **kwargs)
    reduce_tuple = (_reconstructor, (func, args, kwargs))

    try:
        # This works for objects of user-defined classes.
        obj.__reduce_ex__ = (lambda _self, _protocol: reduce_tuple).__get__(obj)
    except AttributeError:
        if type(obj) in _reconstructor_supported_types:
            class _Wrapper(type(obj)):
                def __reduce_ex__(self, _protocol):
                    return reduce_tuple

            obj = _Wrapper(obj)

    return obj
