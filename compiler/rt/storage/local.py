"""
local.py
--------
A simple local key-value store.
** Keys are not case-sensitive. **
"""

import os
from pathlib import Path
import shutil
from tempfile import NamedTemporaryFile

STORAGE_DIR = Path(".kappa-store")


def _bucket_path(bucket):
    """
    Internal method, generates a Path to a bucket.
    :param bucket: (string) A bucket name.
    :return: Path object to the key.
    """
    return STORAGE_DIR / bucket


def _key_path(bucket, key):
    """
    Internal method, generates a Path to a key.
    :param bucket: (string) A bucket name.
    :param key: (string) A key name.
    :return: Path object to the key.
    """
    return _bucket_path(bucket) / key


def init():
    """
    Initializes the local key-value store.
    """
    STORAGE_DIR.mkdir(exist_ok=True)


def create_bucket(bucket):
    """
    Creates a bucket.  Is no-op if bucket already exists.
    :param bucket: (string) A bucket name.
    """
    _bucket_path(bucket).mkdir(exist_ok=True)


def _check_bucket(bucket):
    """
    Checks if a bucket exists. Raises a ValueError if it does not exist.
    :param bucket: (string) A bucket name.
    """
    if not _bucket_path(bucket).exists():
        raise ValueError("bucket '{}' does not exist".format(bucket))


def get(bucket, key):
    """
    Reads key-value pair from storage.
    :param bucket: (string) A bucket name.
    :param key: (string) A key name.
    :return: (bytes) The value mapped to the given key.
    """
    _check_bucket(bucket)
    key_path = _key_path(bucket, key)
    try:
        value = key_path.read_bytes()
    except FileNotFoundError:
        raise KeyError("key '{}' in bucket '{}' not found".format(key, bucket))

    return value


def put(bucket, key, val):
    """
    Writes key-value pair to storage.
    :param bucket: (string) A bucket name.
    :param key: (string) A key name.
    :param val: (bytes) Value to write.
    """
    _check_bucket(bucket)
    if not isinstance(val, bytes):
        raise TypeError("value should be of type bytes")

    key_path = _key_path(bucket, key)

    with NamedTemporaryFile("wb", delete=False) as f:
        tmp_key_path = Path(f.name)
        f.write(val)
        f.flush()
        os.fsync(f.fileno())

    tmp_key_path.rename(key_path)


def delete(bucket, key):
    """
    Deletes `key` from `bucket`.
    :param bucket: (string) A bucket name.
    :param key: (string) A key name.
    """
    _check_bucket(bucket)
    key_path = _key_path(bucket, key)
    try:
        key_path.unlink()
    except FileNotFoundError:
        raise KeyError("key '{}' in bucket '{}' not found".format(key, bucket))


def list_keys(bucket):
    """
    Lists all the keys in a bucket.
    :param bucket: (string) A bucket name.
    :return: (string list) Keys in the bucket.
    """
    _check_bucket(bucket)
    bucket_path = _bucket_path(bucket)
    keys = []
    for key_path in bucket_path.iterdir():
        keys.append(key_path.name)
    return keys


def list_buckets():
    """
    Searches STORAGE_DIR for buckets, returns the a list of bucket names.
    """
    buckets = []
    for bucket_path in STORAGE_DIR.iterdir():
        buckets.append(bucket_path.name)
    return buckets


def delete_bucket(bucket):
    """
    Deletes `bucket`.
    :param bucket: (string) Bucket to delete.
    """
    shutil.rmtree(_bucket_path(bucket))


def delete_all():
    """
    Delete all the buckets. `init()` to use key-value store again.
    """
    shutil.rmtree(STORAGE_DIR)
