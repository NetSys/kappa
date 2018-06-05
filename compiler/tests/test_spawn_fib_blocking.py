import rt


def fib(n):
    if n <= 1:
        return n
    else:
        return rt.spawn(fib, (n-1,), blocking=True) + rt.spawn(fib, (n-2,), blocking=True)


def handler(event, context):
    return fib(event["n"])
