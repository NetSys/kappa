"""Sums the first `n` odd natural numbers."""
import time

import rt


def make_magic():
    time.sleep(1)
    return 42


def odd(i, magic):
    assert magic == 42
    return 2*i+1


def my_sum(*numbers):
    return sum(numbers)


def handler(event, _context):
    n = event["n"]
    magic_fut = rt.spawn(make_magic, ())
    futs = rt.map_spawn(odd, range(n), extra_args=(magic_fut,))
    return rt.spawn(my_sum, futs, blocking=True)


handler.on_coordinator = True
