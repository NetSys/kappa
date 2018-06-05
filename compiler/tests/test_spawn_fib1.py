import rt


def fib(n):
    rt.pause()
    if n <= 1:
        return n
    else:
        child = rt.spawn(fib, (n-2,))
        return fib(n-1) + child.wait()


def handler(event, context):
    return fib(event["n"])
