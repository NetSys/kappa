"""Spawns multiple identical workers to work on a queue of tasks, i.e., summing integers."""

import rt


def work(work_queue):
    accu = 0
    while True:
        n = work_queue.dequeue()
        if n is None:
            break

        accu += n

    return accu


def aggregate(*worker_results):
    return sum(worker_results)


def handler(event, context):
    num_tasks = event["num_tasks"]
    num_workers = event["num_workers"]

    work_queue = rt.create_queue(max_size=num_tasks + num_workers)
    futures = rt.spawn_many(work, (work_queue,), copies=num_workers)
    work_queue.enqueue(*range(num_tasks))
    work_queue.enqueue(*([None] * num_workers))  # Sentinels.

    return rt.spawn(aggregate, futures, blocking=True)


handler.on_coordinator = True
