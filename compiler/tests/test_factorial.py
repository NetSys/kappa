import rt


def factorial(n):
    if n == 0:
        return 1
    else:
        print("n = %d" % n)
        if n % 10 == 0:
            rt.pause()
        return n * factorial(n - 1)


def handler(event, context):
    return factorial(event["n"])
