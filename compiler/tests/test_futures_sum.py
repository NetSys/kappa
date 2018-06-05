import operator

import rt


def parallel_sum(l, r):
    """Computes (l + (l+1) + ... + r)."""
    # TODO(zhangwen): this function can either return an int or a future; this seems confusing...
    if l == r:
        return l

    m = (l + r) // 2
    sl = parallel_sum(l, m)
    sr = parallel_sum(m + 1, r)
    return rt.spawn(operator.add, (sl, sr))


def handler(event, context):
    n = event["n"]
    if n == 1:
        return 1
    else:
        return parallel_sum(1, n).wait()
