"""
Word count (MapReduce): computes word frequencies for words with length >= 3 for multiple text files (case-insensitive).

This program mainly demonstrates Kappa's concurrency API and does not call `checkpoint()` to take checkpoints.
Each map and reduce job is assumed to finish within the lambda function time limit.

If you wish to take checkpoints, remove the `kappa:ignore` annotations from the functions and call `checkpoint()`.

All input, intermediate, and output files are stored on S3.

Input parameters:
    - num_chunks: number of input files.
    - num_mappers: number of workers for the map phase.
    - num_reducers: number of workers for the reduce phase.

The S3 buckets to use are passed through environment variables `INPUT_BUCKET`, `SHUFFLE_BUCKET`, and `OUTPUT_BUCKET`
(see run.sh).

Inputs should be text files located at: s3://INPUT_BUCKET/i, where 0 <= i < num_chunks.
Output files can be found at: s3://OUTPUT_BUCKET/i, where 0 <= i < num_reducers.
"""
import os
import re
from timeit import default_timer as timer
from typing import Dict, Counter, List, Any
import zlib

import boto3

from rt import spawn, map_spawn, on_coordinator

s3 = boto3.resource("s3")

INPUT_BUCKET = os.environ["INPUT_BUCKET"]
SHUFFLE_BUCKET = os.environ["SHUFFLE_BUCKET"]
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]


def get(bucket: str, key: str) -> bytes:
    "kappa:ignore"
    return s3.Object(bucket, key).get()["Body"].read()


def put(bucket: str, key: str, value: bytes) -> None:
    "kappa:ignore"
    s3.Object(bucket, key).put(Body=value)


def tokenize(text: str) -> List[str]:
    "kappa:ignore"
    return re.findall(r"\w{3,}", text.lower())


def hash_token(token: str) -> int:
    "kappa:ignore"
    return zlib.adler32(token.encode("utf-8"))


def serialize_counts(counter: Dict[str, int]) -> bytes:
    "kappa:ignore"
    return "\n".join(
        "{}\t{}".format(word, count)
        for word, count in counter.items()
    ).encode("utf-8")


def deserialize_counts(s: bytes) -> Counter[str]:
    "kappa:ignore"
    c: Counter[str] = Counter()
    for line in s.decode("utf-8").splitlines():
        word, count_str = line.rsplit("\t", maxsplit=1)
        c[word] = int(count_str)
    return c


def update_counters(counters, chunk_id):
    """kappa:ignore"""
    start = timer()

    content = get(INPUT_BUCKET, str(chunk_id)).decode("utf-8")
    for token in tokenize(content):
        reducer_id = hash_token(token) % len(counters)
        counters[reducer_id][token] += 1

    return timer() - start


def write_intermediate_results(mapper_id, counters):
    """kappa:ignore"""
    start = timer()

    for reducer_id, counter in enumerate(counters):
        key = "{}/{}".format(reducer_id, mapper_id)
        put(SHUFFLE_BUCKET, key, serialize_counts(counters[reducer_id]))

    return timer() - start


def mapper(e, num_reducers: int) -> float:
    """kappa:ignore"""
    # Count words in each object; one counter for each reducer.
    duration = 0.0

    mapper_id, mapper_range = e
    counters = [Counter() for _ in range(num_reducers)]
    for chunk_id in range(*mapper_range):
        duration += update_counters(counters, chunk_id)

    duration += write_intermediate_results(mapper_id, counters)
    return duration


def reducer(reducer_id: int, num_mappers: int, *prev_times) -> float:
    """kappa:ignore"""
    start = timer()

    c = Counter()  # type: Counter[str]
    for mapper_id in range(num_mappers):
        key = "{}/{}".format(reducer_id, mapper_id)
        s = get(SHUFFLE_BUCKET, key)
        c.update(deserialize_counts(s))
    put(OUTPUT_BUCKET, str(reducer_id), serialize_counts(c))

    duration = timer() - start
    return max(prev_times) + duration


def make_mapper_ranges(num_chunks, num_mappers):
    """kappa:ignore"""
    base = num_chunks // num_mappers
    extras = num_chunks % num_mappers

    mapper_ranges = []
    start = 0
    for i in range(num_mappers):
        chunks = base
        if i < extras:
            chunks += 1
        mapper_ranges.append((start, start + chunks))
        start += chunks

    assert start == num_chunks
    return mapper_ranges


@on_coordinator
def my_max(*values):
    """kappa:ignore"""
    return max(values)


@on_coordinator
def handler(event: Dict[str, Any], _):
    num_chunks = event["num_chunks"]
    num_mappers = event["num_mappers"]
    num_reducers = event["num_reducers"]

    mapper_ranges = make_mapper_ranges(num_chunks, num_mappers)
    mappers = map_spawn(mapper, zip(range(num_mappers), mapper_ranges), extra_args=(num_reducers,))
    reducers = map_spawn(reducer, range(num_reducers), extra_args=[num_mappers] + mappers)

    # I would just spawn `max` (Python built-in), but `max` throws an exception if it's only passed one value...
    return spawn(my_max, reducers, blocking=True)
