"""Simulates running a Python script on a serverless platform (e.g., AWS Lambda).

This script, when invoked from the command line to run a handler:
  - if the handler finishes successfully, prints the handler's return value (in JSON) to standard output, and exits
    with status code 0;
  - if the handler fails (due to, e.g., a timeout or an uncaught exception), exits with non-zero status code and
    prints an error message to standard error; see top-level constants for exit codes in each case.

This script is used for:
  - implementing the "local" Kappa platform, which simulates a serverless environment on the local machine, and
  - running on-coordinator tasks for all platforms.

This script also supports "deploying" a handler to a local directory to speed up subsequent handler invocations.
The Kappa coordinator deploys the handler package at the beginning of a workload.  See help message for details.
"""
import argparse
import json
import multiprocessing
import os
from pathlib import Path
import shutil
import sys
import tempfile
import textwrap
import time
import traceback
from typing import Sequence


TIMEOUT_EXIT_CODE = 42
UNCAUGHT_EXCEPTION_EXIT_CODE = 43

ENTRY_MODULE_INDICATOR = ".entry_module"


class Context(object):
    def __init__(self, deadline: float) -> None:
        """Constructs a context to pass to a handler.
        :param deadline: the time at which the script will be killed (in UNIX time).
        """
        self.deadline = deadline

    def get_remaining_time_in_millis(self) -> float:
        return (time.time() - self.deadline) * 1000


class TimeLimitExceeded(Exception):
    """Raised when invoked script is killed due to the time limit."""
    def __init__(self, stdout_so_far):
        self.stdout = stdout_so_far


class WorkerRaisedException(Exception):
    """Raised when invoked script raises an uncaught exception."""
    def __init__(self, exc, tb, stdout_so_far):
        """
        Initializes a WorkerRaiseException instance.

        :param exc: the exception raised by the invoked script.
        :param tb: the traceback accompanying the exception.
        :param stdout_so_far: standard output produced by the invoked script till the exception.
        """
        self.exc = exc
        self.tb = tb
        self.stdout = stdout_so_far


class Tee(object):
    """
    Duplicates content written to stdout to another file.
    https://stackoverflow.com/questions/616645/how-do-i-duplicate-sys-stdout-to-a-log-file-in-python
    """

    def __init__(self, other_file):
        self.file = other_file
        self.stdout = sys.stdout
        sys.stdout = self

    def __del__(self):
        sys.stdout = self.stdout
        self.file.close()

    def write(self, data):
        self.file.write(data)
        self.stdout.write(data)
        self.flush()

    def flush(self):
        self.file.flush()
        self.stdout.flush()


def copy_file_or_dir(src: Path, dst_dir: Path) -> None:
    """
    Copies a file or directory from `src` to under the directory `dst_dir`.

    Adapted from: https://stackoverflow.com/questions/1994488/copy-file-or-directories-recursively-in-python.
    """
    if not dst_dir.is_dir():
        raise ValueError("dst_dir must be a directory.")

    try:
        # Assume `src` is an ordinary file.
        shutil.copy(src, dst_dir)
    except IsADirectoryError:
        # `src` is actually a directory.
        # https://stackoverflow.com/questions/3925096/how-to-get-only-the-last-part-of-a-path-in-python
        shutil.copytree(src, dst_dir / src.name)


def deploy(deploy_path: Path, script_paths: Sequence[Path]) -> str:
    """Deploys scripts to a path.  Returns the entry module name."""
    assert os.path.isdir(deploy_path), f"deploy path {deploy_path} is not an existent directory"

    # This is the entry point.  Has to be a Python source file.
    entry_module = script_paths[0].stem
    (deploy_path / ENTRY_MODULE_INDICATOR).write_text(entry_module)
    shutil.copy(script_paths[0], deploy_path)

    # Copy the rest of the bundle to the temporary directory.
    for script_path in script_paths[1:]:
        copy_file_or_dir(script_path, deploy_path)

    return entry_module


def _invoke(conn_retval, package_dir, entry_module_name, stdout_f, event, context, quiet):
    """Executes handler in another process and communicates back return value."""
    if quiet:
        devnull_f = open(os.devnull, "w")
        sys.stdout = devnull_f
        sys.stderr = devnull_f
    else:  # Redirect stdout to stderr so that invoker's stdout doesn't get cluttered.
        sys.stdout = sys.stderr

    Tee(stdout_f)

    # Import the lambda handler module and run the handler.
    # The handler module is imported by the subprocess, instead of the parent process, so that the parent can launch
    # multiple handlers in succession (e.g., by calling `invoke()` repeatedly) without having the handler modules
    # step on one another (e.g., importing a later module might fail due to an earlier module with the same name).
    sys.path.insert(0, str(package_dir))

    exception_occurred = False
    try:
        entry_module = __import__(entry_module_name)
        handler = getattr(entry_module, "rt_handler", entry_module.handler)
        retval = handler(event, context)
    except Exception as e:
        exception_occurred = True
        tb = traceback.format_exc()
        retval = (e, tb)

    conn_retval.send((exception_occurred, retval))
    conn_retval.close()


def invoke(package_dir, entry_module, event, timeout_secs, quiet=False):
    """
    Invokes a handler, passing the event structure and imposing a time limit.

    If `quiet` is `True`, stdout and stderr are suppressed in the script.
    :param package_dir: paths to deployment package to invoke.
    :param entry_module: name of the entry point module.
    :param event: event structure to pass to the handler
    :param timeout_secs: kill after this amount of time; pass `None` for no time limit.
    :param quiet: if True, stdout and stderr from the script are suppressed.
    :return: the handler's return value and the script's stdout, if the handler doesn't get killed due to timeout.
    :raises TimeLimitExceeded: if the handler is killed due to timeout.
    :raises WorkerRaisedException: if the handler has raised an uncaught exception.
    """
    # Spawn a subprocess to run the handler.
    retval_recv, retval_send = multiprocessing.Pipe(duplex=False)  # For communicating return value.
    context = Context(time.time() + (timeout_secs or float("inf")))
    with tempfile.NamedTemporaryFile("w") as stdout_f:
        p = multiprocessing.Process(target=_invoke,
                                    args=(retval_send, package_dir, entry_module, stdout_f, event, context, quiet))
        p.start()

        worker_result = None
        if retval_recv.poll(timeout=timeout_secs):
            worker_result = retval_recv.recv()

        p.terminate()
        handler_stdout = Path(stdout_f.name).read_text()

    if worker_result is None:  # Then the handler timed out.
        raise TimeLimitExceeded(handler_stdout)

    exception_occurred, retval = worker_result
    if exception_occurred:
        exc, tb = retval
        raise WorkerRaisedException(exc, tb, handler_stdout)

    return retval, handler_stdout


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("scripts", type=str, nargs="+",
                        help="path of the Python script to invoke, followed by any other files/directories to bundle "
                             "together; alternatively, a single path to a deployment package")
    parser.add_argument("--deploy", type=str, help="if specified, deploys package to this path without running it")
    parser.add_argument("--timeout-secs", type=float,
                        help="number of seconds the script is allowed to run for; if omitted, no time limit is imposed")
    parser.add_argument("--event-json", help="the event object, in JSON, to pass to the handler; {} if omitted")
    args = parser.parse_args()

    script_paths = [Path(p) for p in args.scripts]

    if args.deploy is not None:
        if args.timeout_secs is not None:
            sys.exit("Cannot specify timeout when --deploy is specified")
        if args.event_json is not None:
            sys.exit("Cannot specify event when --deploy is specified")

        deploy_path = Path(args.deploy)
        if not deploy_path.is_dir():
            sys.exit("Invalid deploy directory: {}".format(deploy_path))

        deploy(deploy_path, script_paths)
        sys.exit(0)

    timeout_secs = args.timeout_secs
    if timeout_secs is not None and not timeout_secs > 0:
        sys.exit("Invalid time limit: {}".format(timeout_secs))

    event = {}
    if args.event_json:
        event = json.loads(args.event_json)

    tempdir = None
    if script_paths[0].is_dir():  # A deployment directory is specified.
        if len(script_paths) > 1:
            sys.exit("When deployment path {} is specified, no other scripts can be specified".format(script_paths[0]))

        package_dir = script_paths[0]
        entry_module = (script_paths[0] / ENTRY_MODULE_INDICATOR).read_text()
    else:  # Scripts to deploy are specified.
        tempdir = tempfile.TemporaryDirectory()
        package_dir = Path(tempdir.name)
        entry_module = deploy(package_dir, script_paths)

    try:
        result, stdout_content = invoke(package_dir, entry_module, event, timeout_secs)
    except TimeLimitExceeded:
        print("Killed after {} seconds.".format(timeout_secs), file=sys.stderr)
        sys.exit(TIMEOUT_EXIT_CODE)
    except WorkerRaisedException as e:
        message = "Handler died after raising uncaught exception:\n\n" + textwrap.indent(e.tb, " " * 2)
        print(message, file=sys.stderr)
        sys.exit(UNCAUGHT_EXCEPTION_EXIT_CODE)
    finally:
        if tempdir:
            tempdir.cleanup()

    json.dump(result, sys.stdout)
    sys.exit(0)


if __name__ == '__main__':
    main()
