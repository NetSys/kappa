import rt


def factorial(n):
    result = 1
    for i in range(1, n + 1):
        if i % 10 == 0:
            rt.pause()

        print("i = %d" % i)
        result *= i

    return result


def handler(event, context):
    return factorial(event["n"])
