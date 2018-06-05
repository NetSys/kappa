import rt


def main():
    a1, a2, a3, a4 = 1, 2, 3, 4
    rt.pause()
    [b1, b2, b3, b4] = 1, 2, 3, 4
    rt.pause()
    c1, c2, c3, c4 = [1, 2, 3, 4]
    rt.pause()
    [d1, d2, d3, d4] = [1, 2, 3, 4]
    rt.pause()
    print(a1, a2, a3, a4)
    print(b1, b2, b3, b4)
    print(c1, c2, c3, c4)
    print(d1, d2, d3, d4)


def handler(event, context):
    return main()
