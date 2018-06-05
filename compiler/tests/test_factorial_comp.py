import rt

from functools import reduce
from operator import mul


def my_range(n):
    l = []

    i = 0
    while i < n:
        print("my_range: i = ", i)
        l.append(i)
        i += 1

        if i % 20 == 0:
            rt.pause()

    return l


def return_true(i):
    print("return_true: i = ", i)
    if i % 20 == 0:
        rt.pause()

    return True


def factorial(n):
    return reduce(mul, [i + 1 for i in my_range(n) if return_true(i + 1)], 1)


def handler(event, context):
    return factorial(event["n"])
