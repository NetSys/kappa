MODULUS = 5754853343


def factorial(n):
    result = 1
    for i in range(1, n + 1):
        print("i = %d" % i)
        result = (result * i) % MODULUS

    return result % MODULUS


def handler(event, _):
    return factorial(event["n"])
