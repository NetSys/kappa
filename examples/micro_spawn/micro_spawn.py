"""
Microbenchmark for spawn: how long does it take for coordinator to spawn a child, varying init checkpoint size.

Parameters:
    - num_spawns: number of child tasks to spawn (sequentially).
    - chkpt_size: initial checkpoint size of child task (MB).
"""
import time

from rt import on_coordinator, spawn


def child(pid, _data):
    """kappa:ignore"""
    # Print out pid in order to correlate spawn timestamp (from coordinator log) with child start timestamp.
    print("Child started: {},{}".format(pid, time.time()))


@on_coordinator
def handler(event, _context):
    num_spawns = int(event["num_spawns"])
    chkpt_size = int(event["chkpt_size"] * 2**20)
    data = b"x" * chkpt_size

    for i in range(num_spawns):
        # TODO(zhangwen): should enable a process to find out its own pid.
        predicted_pid = i + 2
        fut = spawn(child, (predicted_pid, data))
        assert predicted_pid == fut.pid
        fut.wait()
