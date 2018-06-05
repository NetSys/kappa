import rt


def main(n):
    """Prints out all odd numbers less than sqrt(n)."""
    for x in range(n):
        rt.pause()
        if x * x > n:
            break

        if x % 2 == 0:
            continue

        print(x)

    return {}


def handler(event, context):
    return main(event["n"])
