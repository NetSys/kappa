"""
Counts, in parallel, all occurrences of a substring in another string, which is split into multiple S3 files.

This application launches worker tasks, each of which greps for the substring in a subset of input chunks.  To account
for substrings that straddle two consecutive input chunks, each worker task communicates with its predecessor and
successor workers through queues.

Parameters:
    - bucket: name of S3 bucket where input files are located.
    - word: substring to search for.
    - num_chunks: number of input chunks.
    - num_workers: number of workers to spawn.

Input chunks should be located at s3://bucket/i, where 0 <= i < num_chunks.
"""
import io
from time import time

import rt

import boto3

s3 = boto3.client("s3")


def get(worker_id: int, bucket: str, key: str) -> str:
    "kappa:ignore"
    start_time = time()

    f = io.BytesIO()
    s3.download_fileobj(bucket, key, f)
    data = f.getvalue().decode("utf-8")

    print("get:", worker_id, time() - start_time)

    return data


def count_occurrences(worker_id: int, text: str, word: str) -> int:
    "kappa:ignore"
    start_time = time()
    count = 0
    start = 0
    while True:
        index = text.find(word, start)
        if index == -1:
            break

        count += 1
        start = index + 1

    print("compute:", worker_id, time() - start_time)
    return count


def worker(e, chunks_per_worker, bucket, word) -> int:
    worker_id, next_queue, prev_queue = e
    print("start:", worker_id)

    count = 0
    word_len = len(word)

    tail = ""
    chunk_start = worker_id * chunks_per_worker
    chunk_end = (worker_id+1) * chunks_per_worker
    for chunk_id in range(chunk_start, chunk_end):
        print("Chunk:", chunk_id)
        text = rt.reconstructor(get, worker_id, bucket, str(chunk_id))
        if chunk_id == chunk_start and prev_queue:
            head = text[:word_len-1]
            start_time = time()
            prev_queue.enqueue(head, is_async=True)
            print("enqueue:", worker_id, time() - start_time)

        count += count_occurrences(worker_id, text, word)
        count += count_occurrences(worker_id, tail + text[:word_len-1], word)
        tail = text[-(word_len-1):]

    if next_queue:
        start_time = time()
        next_head = next_queue.dequeue()
        print("dequeue:", worker_id, time() - start_time)

        count += count_occurrences(worker_id, tail + next_head, word)

    return count


@rt.on_coordinator
def aggregate(*counts):
    """kappa:ignore"""
    return sum(counts)


@rt.on_coordinator
def handler(event, _):
    bucket = event["bucket"]
    word = event["word"]
    num_chunks = event["num_chunks"]
    num_workers = event["num_workers"]

    assert num_chunks % num_workers == 0
    chunks_per_worker = num_chunks // num_workers

    # Launch workers.
    next_queues = rt.create_queues(max_size=num_chunks, copies=num_workers-1) + [None]
    elems = [(i, next_queues[i], next_queues[i-1]) for i in range(num_workers)]

    workers = rt.map_spawn(worker, elems, extra_args=(chunks_per_worker, bucket, word))

    # Gather counts from workers.
    return rt.spawn(aggregate, workers, blocking=True)
