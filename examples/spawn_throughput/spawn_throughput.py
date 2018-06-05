"""
Benchmark for spawn throughput: how long does it take to launch x tasks, each with a specified initial checkpoint size.

Input arguments:
    - `num_spawns`: number of child tasks to spawn.
    - `chkpt_size`: initial checkpoint size for child tasks (MB).
"""
import time

from rt import on_coordinator, spawn_many


def child(_data):
    """kappa:ignore"""
    start_time = time.time()
    # Hopefully this sleep will have all child tasks running at once, instead of having earlier tasks die off before
    # later tasks are even launched.
    time.sleep(10)
    return start_time


@on_coordinator
def handler(event, _):
    """Prints spawn start time; returns list of child task start times."""
    num_spawns = event["num_spawns"]
    chkpt_size = int(event["chkpt_size"] * 2**20)
    data = b"x" * chkpt_size  # Passed to child tasks as argument to artificially set initial checkpoint size.

    print("Spawn: {}".format(time.time()))
    return spawn_many(child, (data,), copies=num_spawns, blocking=True)
