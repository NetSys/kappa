#!/usr/bin/env python3
"""
Runs the factorial example.

Example usage: ./run.py 100 0.5
"""
import argparse
from functools import reduce
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description="Runs the factorial example with auto-pause.")
    parser.add_argument("n", type=int, help="compute n!")
    parser.add_argument("pause_interval", type=float, help="auto-pause interval (in seconds)")
    parser.add_argument("coordinator_args", nargs="*", help="arguments to pass to the coordinator (specify after --)")
    args = parser.parse_args()

    # Make sure coordinator is installed.
    assert shutil.which("coordinator") is not None, "cannot find coordinator binary (is it installed?)"

    # Extract the modulus from the script (later used for correctness check).
    this_dir = Path(os.path.dirname(os.path.realpath(__file__)))  # The directory this script is in.
    script_path = this_dir / "factorial.py"
    match = re.search("^MODULUS\s*=\s*(?P<modulus>\d+)", script_path.read_text(), re.MULTILINE)
    if match is None:
        sys.exit("FATAL: can't find modulus in script")
    modulus = int(match.group("modulus"))

    # Transform the handler.
    compiler_dir = this_dir / ".." / ".." / "compiler"
    compiler_path = compiler_dir / "do_transform.py"
    transformed_path = this_dir / "factorial_transformed.py"
    with script_path.open("r") as fs, transformed_path.open("w") as ft:
        subprocess.check_call([compiler_path, "--auto-pause"], stdin=fs, stdout=ft)

    rt_path = compiler_dir / "rt"
    log_path = this_dir / f"factorial-{args.n}-{args.pause_interval}s-log"
    command = [
        "coordinator",
        "--name", "factorial", "--event", '{"n": %d}' % args.n,
        "--env", f"PAUSE_INTERVAL_SECS={args.pause_interval}",
        "--log-dir", str(log_path),
    ] + args.coordinator_args + [str(transformed_path), str(rt_path)]
    print(" ".join(command), file=sys.stderr)
    output = subprocess.check_output(command)

    # Assert that result is correct.
    result = output.rsplit(maxsplit=1)[-1].decode("utf-8").strip()
    expected = str(reduce(lambda x, y: (x * y) % modulus, range(1, args.n + 1)))
    if result != expected:
        sys.exit(f"WRONG RESULT -- expected: {expected}, actual: {result}")

    print(f"Success: {result}")


if __name__ == '__main__':
    main()
