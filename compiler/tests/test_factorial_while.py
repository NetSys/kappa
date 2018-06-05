import rt


def factorial(n):
    steps = 0
    result = 1
    while n > 0:
        steps += 1
        if steps % 10 == 0:
            rt.pause()

        print("n = %d" % n)
        result = result * n
        n -= 1
    return result


def handler(event, context):
    return factorial(event["n"])
