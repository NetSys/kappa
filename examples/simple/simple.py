"""A simple benchmark for checkpointing performance.

This application doesn't print out performance numbers.  Instead, checkpoint latency can be gathered from the log files.
"""
from rt import checkpoint


def foo(n):
    """Calls itself recursively n times and takes a checkpoint; each level contains a couple of live variables."""
    if n == 0:
        checkpoint()
        return

    x = "ghawoieth"
    y = [1, 2, 3, "hwe", "hwoiet", "Htwerht"]
    z = 2359025
    foo(n - 1)
    print(x, y, z)


def handler(event, _):
    depth = event["depth"]
    for _ in range(100):
        foo(depth)
