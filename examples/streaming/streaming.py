"""
Streaming example: counting average number of hashtags in a tweet on a 'stream' of tweets.

Specifically:
    - The main task builds a work queue containing input files.
    - Worker tasks are spawned to pull files from the queue and process them.
    - An aggregator task is spawned to receive updates from workers and print them.
    - Each worker periodically sends its average hashtag count so far to the aggregator.

Input parameters:
    - num_workers: number of workers to process tweets.
    - num_chunks: number of input files.
The input S3 bucket is passed through the environment variable `BUCKET`.

Input files should be located at s3://BUCKET/i, where 0 <= i < num_chunks.  Each input file should be a text file
where each line is a JSON object containing a tweet; each JSON objects should at least have a `text` field containing
the content of the tweet.

To generate input files, check out `uploader.py`.
"""
from io import BytesIO
import json
import os
import re

import boto3

from rt import create_queue, spawn, map_spawn, on_coordinator

BUCKET = os.environ["BUCKET"]

s3 = boto3.client("s3")


def count_hashtags(text):
    """kappa:ignore"""
    return len(re.findall(r"#\w+", text))


def process_chunk(chunk_id):
    """kappa:ignore"""
    buf = BytesIO()
    s3.download_fileobj(BUCKET, str(chunk_id), buf)
    lines = buf.getvalue().decode("utf-8").splitlines()

    hashtag_count = 0
    tweet_count = 0
    for line in lines:
        tweet = json.loads(line)
        if "text" not in tweet:
            continue

        hashtag_count += count_hashtags(tweet["text"])
        tweet_count += 1

    return hashtag_count, tweet_count


def worker(worker_id, work_queue, agg_queue):
    i = 0

    hashtag_count = 0
    tweet_count = 0

    while True:
        chunk_id = work_queue.dequeue()
        if chunk_id is None:
            break

        this_hashtag_count, this_tweet_count = process_chunk(chunk_id)
        hashtag_count += this_hashtag_count
        tweet_count += this_tweet_count
        i += 1

        if i % 2 == 0:  # Report stats every two chunks.
            agg_queue.enqueue((worker_id, hashtag_count, tweet_count), is_async=True)


@on_coordinator
def aggregate(num_workers, agg_queue):
    totals = [0] * num_workers
    counts = [0] * num_workers
    while True:
        worker_id, total, count = agg_queue.dequeue()
        if worker_id is None:
            break

        totals[worker_id] = total
        counts[worker_id] = count
        # TODO(zhangwen): this message is currently written to the log and does not appear on the terminal in real time.
        print("Average so far:", sum(totals) / sum(counts))


@on_coordinator
def waiter(*_):
    """kappa:ignore"""
    pass


@on_coordinator
def handler(event, _):
    num_workers = event["num_workers"]
    num_chunks = event["num_chunks"]

    work_queue = create_queue(max_size=num_workers*10)
    agg_queue = create_queue(max_size=num_workers*10)
    workers = map_spawn(worker, range(num_workers), extra_args=(work_queue, agg_queue))
    agg = spawn(aggregate, (num_workers, agg_queue,))

    work_queue.enqueue(*range(num_chunks))
    work_queue.enqueue(*([None] * num_workers))
    spawn(waiter, workers, blocking=True)  # Wait for all workers to complete.

    agg_queue.enqueue((None, None, None))
    agg.wait()
