#!/usr/bin/env python3
"""
This script runs integration tests, i.e.,
  - transforms a lambda handler script using the compiler,
  - invokes the transformed script using the coordinator, and
  - verifies the final output.

Example usage:
    pytest test_integration.py -v -n 4                     # Run tests using 4 CPUs in verbose mode.
    pytest test_integration.py -v -n 4 --write-logs        # Run tests and gather logs into working directory.
    pytest test_integration.py -k "TestLocal and timeout"  # Run local timeout test.
"""
from enum import Enum, auto
import json
import logging
from math import factorial
from pathlib import Path
import random
import shutil
import subprocess
from typing import List, Iterable, Generator, Optional, Dict

import boto3
import pytest

s3 = boto3.resource("s3")


class RPCMode(Enum):
    """
    An enum for whether or not to enable RPC for an invocation.

    Using an Enum, instead of a bool, promotes code readability and lets the RPC mode show up nicely in test names
    when a test is parametrized on RPC mode.
    """
    USE_RPC = auto()
    NO_RPC = auto()

    def __str__(self):
        """Returns a shorter string representation of enum values (e.g., "USE_RPC" instead of "RPCMode.USE_RPC")."""
        return self.name


class Coordinator(object):
    COORDINATOR_PACKAGE = "coordinator/cmd/coordinator"

    def __init__(self, bin_path: Path, should_log: bool, no_build: bool) -> None:
        """
        Initializes a coordinator object; installs the coordinator.
        :param bin_path: where to install the coordinator binary.
        :param should_log: if True, coordinator & handler logs are written to working directory.
        """
        self.should_log = should_log

        if no_build:
            coordinator_path = shutil.which("coordinator")
            if coordinator_path is None:
                raise RuntimeError("coordinator cannot be found in PATH")
            self.path = coordinator_path
        else:
            # Install the coordinator.
            package_path = Path(__file__).parent / self.COORDINATOR_PACKAGE
            if not package_path.is_dir():
                raise RuntimeError(f"Cannot find coordinator package at {package_path}")

            try:
                subprocess.check_output(("go", "build", "-o", bin_path / "coordinator"), cwd=package_path,
                                        stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f"go install failed: f{e.output}")

            coordinator_path = bin_path / "coordinator"
            assert coordinator_path.is_file()
            logging.debug(f"coordinator installed at {coordinator_path}")

            self.path = coordinator_path

    def invoke(self, platform: str, event: object, package: Iterable[Path], rpc: RPCMode, timeout_secs: int,
               log_path: Path, config_path: Optional[Path], workload_name: Optional[str],
               env: Optional[Dict[str, str]]) -> bytes:

        command: List[str] = [str(self.path)]
        if not self.should_log:
            command += ["--no-logging"]

        assert isinstance(rpc, RPCMode)
        if rpc == RPCMode.USE_RPC:
            command += ["--rpc=true", "--rpc-port=0"]  # port=0 so that concurrent tests don't use conflicting ports.
        else:
            command += ["--rpc=false"]

        if config_path is not None:
            command += ["--config", str(config_path)]
        if workload_name is not None:
            command += ["--name", str(workload_name)]

        if env:
            for key, value in env.items():
                command += ["--env", f"{key}={value}"]

        command += [
            "--platform",   platform,
            "--event",      json.dumps(event),
            "--timeout",    str(timeout_secs),
        ]
        command.extend(str(p) for p in package)

        with log_path.open("w") as log_f:
            return subprocess.check_output(command, stderr=log_f)


class Executor(object):
    """Supports testing by invoking of the coordinator."""
    RT_PATH = Path("compiler/rt")
    TESTS_DIR = Path("compiler/tests")
    TRANSFORM_SCRIPT_PATH = Path("compiler/do_transform.py")

    def __init__(self, coord: Coordinator, platform: str, temp_dir: Path, config_path: Optional[Path] = None,
                 workload_name: Optional[str] = None) -> None:
        """Builds the coordinator."""
        self.coord = coord
        self.temp_dir = temp_dir
        self.log_path = self.temp_dir / "invoke.log"
        self.platform = platform
        self.config_path = config_path
        self.workload_name = workload_name

    def run(self, script: str, event: object, expected: object, rpc: RPCMode, timeout_secs: int = 300,
            env: Optional[Dict[str, str]] = None):
        """Transforms and invokes a script with the given parameters and verifies output."""
        # Transform the script.
        orig_path = self.TESTS_DIR / script
        transformed_path = self.temp_dir / "transformed.py"
        with orig_path.open("r") as in_f, transformed_path.open("w") as out_f:
            subprocess.check_call([str(self.TRANSFORM_SCRIPT_PATH)], stdin=in_f, stdout=out_f)

        # Invoke the transformed script.
        print(f"log file: {self.log_path}")
        output = self.coord.invoke(platform=self.platform, event=event, package=[transformed_path, self.RT_PATH],
                                   rpc=rpc, timeout_secs=timeout_secs, log_path=self.log_path,
                                   config_path=self.config_path, workload_name=self.workload_name, env=env)

        _assert_no_warning(self.log_path)

        assert output.decode("utf-8").strip() == repr(expected)


def rand_id() -> str:
    return "".join(random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(26))


def create_bucket(prefix: str):
    region_name = boto3.Session().region_name  # Use default region name.
    bucket_name = prefix + rand_id()
    bucket = s3.Bucket(bucket_name)
    bucket.create(CreateBucketConfiguration={"LocationConstraint": region_name})
    bucket.wait_until_exists()
    return bucket


def delete_bucket(bucket) -> None:
    bucket.objects.all().delete()
    bucket.delete()


@pytest.fixture(scope="session")
def coordinator(tmpdir_factory, should_log: bool, no_build: bool) -> Coordinator:
    """Creates and returns a coordinator object."""
    return Coordinator(bin_path=Path(tmpdir_factory.mktemp("bin")), should_log=should_log, no_build=no_build)


@pytest.fixture(scope="session")
def aws_config(tmpdir_factory) -> Generator[Path, None, None]:
    """Creates a temporary configuration file for the AWS platform; returns the file path."""
    bucket = create_bucket("test-")
    config_path = Path(tmpdir_factory.mktemp("config").join("aws_config.yml"))
    config_path.write_text(f"checkpoint_bucket: {bucket.name}")

    yield config_path

    delete_bucket(bucket)


# Common test results.
FACTORIAL_117 = factorial(117)
FIB_4 = 3
FIB_6 = 8

# A short hand for parametrizing a test by RPC mode.
parametrize_rpc = pytest.mark.parametrize("rpc", list(RPCMode))


def _num_times_blocked(log_path: Path):
    """Returns the number of times execution is blocked."""
    with log_path.open("r") as log_f:
        return sum(1 for line in log_f if "blocked" in line)


def _assert_no_warning(log_path: Path):
    """Asserts that the coordinator produced no warning."""
    with log_path.open("r") as log_f:
        for line in log_f:
            if "WARNING" in line:
                assert False, line


def _assert_timeout(log_path: Path):
    """Asserts that a timeout has occurred."""
    with log_path.open("r") as log_f:
        assert any("timed out" in line for line in log_f)


class TestLocal(object):
    """Local invocation tests."""
    @pytest.fixture()
    def ex(self, coordinator, tmpdir, request) -> Executor:
        """A fixture that creates a local executor."""
        chk_dir = Path(tmpdir.mkdir("chk"))
        print(f"checkpoint directory: {chk_dir}")

        config_path = Path(tmpdir) / "local_config.yml"
        config_path.write_text(f'checkpoint_dir: "{chk_dir}"')

        test_name = f"LOCAL_{request.node.name}"
        return Executor(coordinator, "local", Path(tmpdir), config_path=config_path, workload_name=test_name)

    @pytest.mark.parametrize("script", [
        "test_factorial.py",
        "test_factorial_for.py",
        "test_factorial_while.py",
        "test_factorial_comp.py",
    ])
    def test_factorial(self, ex: Executor, script: str):
        ex.run(script, event={"n": 117}, expected=FACTORIAL_117, rpc=RPCMode.NO_RPC)

    def test_factorial_for_rpc(self, ex: Executor):
        ex.run("test_factorial_for.py", event={"n": 117}, expected=FACTORIAL_117, rpc=RPCMode.USE_RPC)

    @parametrize_rpc
    def test_spawn_fib(self, ex: Executor, rpc: RPCMode):
        ex.run("test_spawn_fib.py", event={"n": 6}, expected=FIB_6, rpc=rpc)

    @parametrize_rpc
    def test_spawn_fib1(self, ex: Executor, rpc: RPCMode):
        ex.run("test_spawn_fib1.py", event={"n": 6}, expected=FIB_6, rpc=rpc)

    @parametrize_rpc
    def test_spawn_fib_blocking(self, ex: Executor, rpc: RPCMode):
        ex.run("test_spawn_fib_blocking.py", event={"n": 4}, expected=FIB_4, rpc=rpc)

    @pytest.mark.parametrize("qsize", [0, 1])
    @parametrize_rpc
    def test_queue(self, ex: Executor, qsize: int, rpc: RPCMode):
        ex.run("test_queue.py", event={"qsize": qsize}, expected="pingpong", rpc=rpc)

    def test_work_queue(self, ex: Executor):
        ex.run("test_work_queue.py", event={"num_workers": 10, "num_tasks": 1000},
               expected=sum(range(1000)), rpc=RPCMode.USE_RPC)

    def test_futures(self, ex: Executor):
        ex.run("test_futures_sum.py", event={"n": 20}, expected=210, rpc=RPCMode.USE_RPC)
        # Verify that execution blocked at most twice (once for spawning handler, another for waiting for sum).
        assert _num_times_blocked(ex.log_path) <= 2

    def test_map_spawn(self, ex: Executor):
        ex.run("test_map_spawn.py", event={"n": 5}, expected=25, rpc=RPCMode.USE_RPC)

    def test_timeout(self, ex: Executor):
        ex.run("test_factorial_for.py", event={"n": 20000}, expected=factorial(20000), rpc=RPCMode.USE_RPC,
               timeout_secs=1)
        _assert_timeout(ex.log_path)


class TestAWS(object):
    """AWS invocation tests."""
    @pytest.fixture()
    def ex(self, coordinator, tmpdir, aws_config, request) -> Executor:
        test_name = f"AWS_{request.node.name}"
        return Executor(coordinator, "aws", Path(tmpdir), config_path=aws_config, workload_name=test_name)

    @parametrize_rpc
    def test_factorial(self, ex: Executor, rpc: RPCMode):
        ex.run("test_factorial_for.py", event={"n": 117}, expected=FACTORIAL_117, rpc=rpc)

    @parametrize_rpc
    def test_spawn(self, ex: Executor, rpc: RPCMode):
        ex.run("test_spawn_fib.py", event={"n": 6}, expected=FIB_6, rpc=rpc)

    @parametrize_rpc
    def test_spawn_blocking(self, ex: Executor, rpc: RPCMode):
        ex.run("test_spawn_fib_blocking.py", event={"n": 4}, expected=FIB_4, rpc=rpc)

    def test_spawn_target(self, ex: Executor):
        ex.run("test_spawn_target.py", event={}, rpc=RPCMode.USE_RPC, expected=30)

    @pytest.mark.parametrize("qsize", [0, 1])
    @parametrize_rpc
    def test_queue(self, ex: Executor, qsize: int, rpc: RPCMode):
        ex.run("test_queue.py", event={"qsize": qsize}, expected="pingpong", rpc=rpc)

    def test_work_queue(self, ex: Executor):
        ex.run("test_work_queue.py", event={"num_workers": 10, "num_tasks": 1000},
               expected=sum(range(1000)), rpc=RPCMode.USE_RPC)

    def test_store(self, ex: Executor):
        trial_bucket = create_bucket("ts-")
        temp_bucket_name = "tmp-" + rand_id()
        try:
            ex.run("test_store.py", event={"bucket": trial_bucket.name, "key": "foo", "value": "bar"}, expected="bar",
                   rpc=RPCMode.USE_RPC, env={"TEMP_BUCKET": temp_bucket_name})

            obj = trial_bucket.Object("foo")
            assert obj.get()["Body"].read() == b"bar"

            assert len(list(s3.Bucket(temp_bucket_name).objects.all())) == 0
        finally:
            delete_bucket(trial_bucket)
            delete_bucket(s3.Bucket(temp_bucket_name))

    def test_timeout(self, ex: Executor):
        ex.run("test_factorial_for.py", event={"n": 1000}, expected=factorial(1000), rpc=RPCMode.USE_RPC,
               timeout_secs=3)

    def test_futures(self, ex: Executor):
        ex.run("test_futures_sum.py", event={"n": 20}, expected=210, rpc=RPCMode.USE_RPC)

    def test_map_spawn(self, ex: Executor):
        ex.run("test_map_spawn.py", event={"n": 5}, expected=25, rpc=RPCMode.USE_RPC)
