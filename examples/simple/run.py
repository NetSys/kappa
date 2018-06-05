#!/usr/bin/env python3
"""
Runs the simple benchmark using various recursion depths.

Example usage:
    ./run.py 10 50 100  # Runs this simple benchmark with recursion depths 10, 50, and 100.
"""
import argparse
import os
import subprocess
import shutil
import sys


def main():
    parser = argparse.ArgumentParser(description="Runs simple benchmark using various recursion depths.")
    parser.add_argument("depths", type=int, nargs="+", help="recursion depths to run the benchmark with")
    parser.add_argument("coordinator_args", nargs=argparse.REMAINDER, help="arguments to pass to the coordinator")
    args = parser.parse_args()

    # Make sure coordinator binary can be found.
    assert shutil.which("coordinator") is not None, "cannot find coordinator binary (is it installed?)"

    # Transform the handler.
    this_dir = os.path.dirname(os.path.realpath(__file__))  # The directory this script is in.
    compiler_path = os.path.realpath(os.path.join(this_dir, "../../compiler/do_transform.py"))
    script_path = os.path.realpath(os.path.join(this_dir, "simple.py"))
    transformed_path = os.path.realpath(os.path.join(this_dir, "simple_transformed.py"))
    with open(script_path, "r") as fs, open(transformed_path, "w") as ft:
        subprocess.check_call([compiler_path], stdin=fs, stdout=ft)

    # Launch with different recursion depths.
    rt_path = os.path.realpath(os.path.join(this_dir, "../../compiler/rt"))
    for depth in args.depths:
        print(f"depth = {depth}", file=sys.stderr)
        print("----------------", file=sys.stderr)
        log_path = os.path.join(this_dir, f"simple-{depth}-log")
        command = [
            "coordinator",
            "--name", "simple", "--event", '{"depth": %d}' % depth,
            "--log-dir", log_path,
        ] + args.coordinator_args + [transformed_path, rt_path]
        print(" ".join(command), file=sys.stderr)
        subprocess.check_call(command)


if __name__ == '__main__':
    main()
