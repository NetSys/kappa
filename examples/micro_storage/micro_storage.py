"""
Benchmark for a fault-tolerant S3 write through coordinator.

This benchmark consists of three sets of experiments:
    - Direct write to S3.
    - Synchronous fault-tolerant write.
    - Asynchronous fault-tolerant write (overlapped with foreground computation).
This program prints out write latencies for direct and synchronous writes; the write latency for asynchronous
writes should be computed in conjunction with the coordinator log (to determine when the write has complete).

Parameters:
    - bucket: bucket to write to.
    - write_size: file size to write (MB).
"""
from uuid import uuid4
from time import time

import boto3

from rt.storage import s3

client = boto3.client("s3")

MODULUS = 17771


def compute(secs):
    """Computes for roughly a number of seconds.

    kappa:ignore
    """
    accu = 2017
    for _ in range(int(secs * 10000000)):
        accu = (accu * accu) % MODULUS


def s3_put(bucket, key, value):
    """kappa:ignore"""
    start_time = time()
    client.put_object(Bucket=bucket, Key=key, Body=value)
    print("s3:", time() - start_time)


def run_s3(rounds, bucket, write_size):
    """kappa:ignore"""
    for _ in range(rounds):
        key = str(uuid4())
        s3_put(bucket, key, b"x" * write_size)


def run_sync(rounds, bucket, write_size):
    for _ in range(rounds):
        key = str(uuid4())
        start_time = time()
        s3.put(bucket, key, b"x" * write_size, is_async=False)
        print("sync:", time() - start_time)


def run_async(rounds, bucket, write_size):
    for _ in range(rounds):
        key = str(uuid4())
        print("async start:", time())
        s3.put(bucket, key, b"x" * write_size, is_async=True)
        compute(9)  # Overlap asynchronous write with foreground computation.


def handler(event, _):
    bucket = event["bucket"]
    write_size = int(event["write_size"] * 2**20)
    rounds = event["num_writes"]

    print("Write size = {}".format(write_size))
    print("Rounds = {}".format(rounds))

    run_s3(rounds, bucket, write_size)
    run_sync(rounds, bucket, write_size)
    run_async(rounds, bucket, write_size)
