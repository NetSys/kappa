"""
s3.py
-----
A key-value store that uses Amazon's S3 service.

Required environment variables:
  - TEMP_BUCKET: name of temporary bucket (will create if nonexistent).
  - AWS_REGION: region in which to create temporary bucket; is automatically set on AWS Lambda.
"""
import functools
import os
import uuid

import boto3

from ..coordinator_call import RemapStore

_TEMP_BUCKET = os.environ["TEMP_BUCKET"]
_TEMP_BUCKET_CREATED = False    # If the temp bucket has been created.
_s3 = boto3.client("s3")
_temp_keys = {}  # Maps (bucket, key) to tmp_key names.


def check_temp_bucket(fn):
    """Decorator tha creates the temporary bucket if necessary.

    Apply this decorator to every function that accesses the temporary bucket.
    """
    @functools.wraps(fn)
    def ret_fn(*args, **kwargs):
        global _TEMP_BUCKET_CREATED
        if not _TEMP_BUCKET_CREATED:
            create_bucket(_TEMP_BUCKET)
            _TEMP_BUCKET_CREATED = True
        return fn(*args, **kwargs)

    return ret_fn


def _make_temp_key(bucket, key):
    """
    Generates a temp key name for a given key and bucket pair.
    Saves mapping to `_temp_keys`.
    :param bucket: (string) A bucket name.
    :param key: (string) A key name.
    :return: (string) The temp key name.
    """
    tk = str(uuid.uuid4())
    _temp_keys[(bucket, key)] = tk
    return tk


def create_bucket(bucket):
    """
    Creates a bucket. Raises exception if bucket already exists and
    is not owned by you.
    :param bucket: (string) A bucket name.
    """
    try:
        _s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={
            'LocationConstraint': os.environ["AWS_REGION"],
        })
    except _s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    except _s3.exceptions.BucketAlreadyExists:
        raise ValueError("bucket '{}' already exists in global namespace".format(bucket))


@check_temp_bucket
def get(bucket, key):
    """
    Reads key-value pair from storage.
    :param bucket: (string) A bucket name.
    :param key: (string) A key name.
    :return: (bytes) The value mapped to the given key.
    """
    # First, try temp keys, then fall back to S3.
    try:
        tk = _temp_keys[(bucket, key)]
        return _s3.get_object(Bucket=_TEMP_BUCKET, Key=tk)['Body'].read()
    except (_s3.exceptions.NoSuchKey, KeyError):
        pass

    try:
        return _s3.get_object(Bucket=bucket, Key=key)['Body'].read()
    except _s3.exceptions.NoSuchBucket:
        raise ValueError("bucket '{}' does not exist".format(bucket))
    except _s3.exceptions.NoSuchKey:
        raise KeyError("key '{}' in bucket '{}' not found".format(key, bucket))


@check_temp_bucket
def put(bucket, key, val, is_async=True):
    """
    Writes key-value pair to storage.
    :param bucket: (string) A bucket name.
    :param key: (string) A key name.
    :param val: (bytes) Value to write.
    :param is_async: (bool) If true, issues rename coordinator call asynchronously.
    """
    tk = _make_temp_key(bucket, key)
    try:
        _s3.put_object(Bucket=_TEMP_BUCKET, Key=tk, Body=val)
    except _s3.exceptions.NoSuchBucket:
        raise RuntimeError("`TEMP_BUCKET` does not exist, was it deleted?")

    # TODO: What do we do when `bucket` does not exist?
    raise RemapStore(tmp_bucket=_TEMP_BUCKET, tmp_key=tk, bucket=bucket, key=key, is_async=is_async)


def delete(bucket, key):
    """
    Deletes `key` from `bucket`. No-op if the key does not exist.
    :param bucket: (string) A bucket name.
    :param key: (string) A key name.
    """
    # FIXME: this operation should also go through the coordinator---there might otherwise be a race between coordinator
    # committing an object and lambda deleting the object.
    try:
        _s3.delete_object(Bucket=bucket, Key=key)
    except _s3.exceptions.NoSuchBucket:
        raise ValueError("bucket '{}' does not exist".format(bucket))


def list_keys(bucket):
    """
    Lists all the keys in a bucket.
    :param bucket: (string) A bucket name.
    :return: (string list) Keys in the bucket.
    """
    try:
        resp = _s3.list_objects_v2(Bucket=bucket)
    except _s3.exceptions.NoSuchBucket:
        raise ValueError("bucket '{}' does not exist".format(bucket))

    keys = []
    for md in resp["Contents"]:
        keys.append(md["Key"])

    return keys


def list_buckets():
    """
    Returns the a list of bucket names associated with the user's AWS account.
    """
    buckets = []
    resp = _s3.list_buckets()
    for bucket in resp["Buckets"]:
        buckets.append(bucket["Name"])

    return buckets
