#!/usr/bin/env python3
"""
Generates input files from Archive Team JSON Download of Twitter Stream.

Usage:
    - Download twitter stream (tar file) from, say, https://archive.org/details/archiveteam-twitter-stream-2015-05.
    - Modify the CHUNK_SIZE and NUM_CHUNKS constants, if desired.
    - To upload using n parallel workers, run n copies of this script in parallel:
        `BUCKET=my-bucket ./uploader.py twitter-stream.tar i n &`, where 0 <= i < n.
      + Note that this script does not clear the bucket.
"""
import bz2
import io
import json
import os
import sys
import tarfile

import boto3

CHUNK_SIZE = 128 * 2**20
NUM_CHUNKS = 512

BUCKET = os.environ["BUCKET"]

s3 = boto3.client("s3")


def upload(key, buf):
    buf.seek(0)
    s3.upload_fileobj(buf, BUCKET, key)


def main():
    input_tar = sys.argv[1]
    worker_id = int(sys.argv[2])
    total_workers = int(sys.argv[3])

    buffer = io.BytesIO()
    buffer_size = 0

    total_chunks = int(NUM_CHUNKS / total_workers)
    chunk_id = int(NUM_CHUNKS / total_workers * worker_id)
    i = 0

    with tarfile.open(input_tar) as tar:
        members = tar.getmembers()
        start = int(len(members) / total_workers * worker_id)
        for member in tar.getmembers()[start:]:
            mf = tar.extractfile(member)
            if not mf:
                continue

            with bz2.open(mf, "rt") as bf:
                for line in bf:
                    try:
                        json.loads(line)
                    except ValueError:
                        continue

                    line_encoded = line.encode("utf-8")
                    buffer.write(line_encoded)
                    buffer_size += len(line_encoded)
                    if buffer_size >= CHUNK_SIZE:
                        buffer.seek(0)
                        s3.upload_fileobj(buffer, BUCKET, str(chunk_id))
                        print("{} / {}".format(i, total_chunks))

                        chunk_id += 1
                        i += 1
                        if i > total_chunks:
                            return

                        buffer = io.BytesIO()
                        buffer_size = 0


if __name__ == '__main__':
    main()
