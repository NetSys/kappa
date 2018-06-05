import rt


def fib(n):
    if n <= 1:
        return n
    else:
        child1 = rt.spawn(fib, (n-1,))
        child2 = rt.spawn(fib, (n-2,))
        return child1.wait() + child2.wait()


def handler(event, context):
    return fib(event["n"])
