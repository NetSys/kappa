"""Exports checkpoint managers, which are responsible for loading and storing checkpoints to/from storage."""
from abc import ABC, abstractmethod
import io
import logging
import os
from pathlib import Path
import pickle
import shutil
from typing import Optional, BinaryIO, cast
import uuid

from .consts import CheckpointID, Seqno, Pid, Continuations
from .logging import log, log_duration

NULL_CHK_ID = CheckpointID("")  # Signifies "no checkpoint".


class CheckpointManager(ABC):
    """Abstract base class for checkpoint managers."""
    logger = logging.getLogger(__name__)

    @abstractmethod
    def load(self, chk_id: CheckpointID) -> Optional[Continuations]:
        """If a checkpoint with the specified ID exists, loads and returns it; otherwise, returns None."""
        pass

    def _load_from_path(self, path: Path):
        """Loads and returns a checkpoint from a local disk path.  Raises `FileNotFoundError` if path doesn't exist."""
        with path.open("rb") as f:
            self.logger.info("Loading checkpoint from: %s.", path)
            return self._deserialize(cast(BinaryIO, f))

    @abstractmethod
    def save(self, conts: Continuations, pid: Pid, seqno: Seqno) -> CheckpointID:
        """
        Persists a checkpoint.
        :param conts: the continuations in the checkpoint.
        :param seqno: the sequence number of this checkpoint, unique to a process.
        :param pid: ID of the process that the checkpoint belongs to.
        :returns new checkpoint ID.
        """
        pass

    def save_from_file(self, f: BinaryIO, pid: Pid, seqno: Seqno) -> CheckpointID:
        """
        Persists a checkpoint previously serialized in a file object.

        Precondition: the file object has been populated using the `serialize_to_file` method.
        """
        raise NotImplementedError

    @classmethod
    def serialize(cls, conts: Continuations, f: BinaryIO) -> None:
        """Serializes a checkpoint to a file object."""
        pickle.dump(conts, f)

    @classmethod
    def _deserialize(cls, f: BinaryIO) -> Continuations:
        """Deserializes a checkpoint from a file object."""
        return pickle.load(f)

    @staticmethod
    def _make_chk_id(pid: Pid, seqno: Seqno) -> CheckpointID:
        """Helper function that constructs a unique checkpoint ID."""
        # Incorporate a random string into the checkpoint ID to avoid duplicate file names.
        return CheckpointID("p{}_{}_{}".format(pid, seqno, uuid.uuid4()))


class LocalCheckpointManager(CheckpointManager):
    """Checkpoint manager for a local invocation from the coordinator."""

    def __init__(self, checkpoint_dir: Path) -> None:
        super(LocalCheckpointManager, self).__init__()
        self.checkpoint_dir = checkpoint_dir

    def load(self, chk_id: CheckpointID) -> Optional[Continuations]:
        if chk_id == NULL_CHK_ID:
            return None

        # If a starting checkpoint ID is provided, the checkpoint must exist.
        path = self.checkpoint_dir / chk_id
        return self._load_from_path(path)

    def save(self, conts: Continuations, pid: Pid, seqno: Seqno) -> CheckpointID:
        chk_id = self._make_chk_id(pid, seqno)
        path = self.checkpoint_dir / chk_id
        with path.open("xb") as f:
            self.serialize(conts, cast(BinaryIO, f))
            f.flush()
            os.fsync(f.fileno())

        self.logger.info("Checkpoint saved to: %s.", path)
        return chk_id

    def save_from_file(self, f: BinaryIO, pid: Pid, seqno: Seqno) -> CheckpointID:
        chk_id = self._make_chk_id(pid, seqno)
        path = self.checkpoint_dir / chk_id
        with path.open("xb") as chk_f:
            # TODO(zhangwen): this is inefficient.
            shutil.copyfileobj(fsrc=f, fdst=chk_f)
        return chk_id


class S3CheckpointManager(CheckpointManager):
    """Checkpoint manager that stores checkpoints in an S3 bucket."""

    def __init__(self, bucket_name: str) -> None:
        """Initializes a checkpoint manager with the name of the bucket to store checkpoints in."""
        import boto3

        self.bucket_name = bucket_name
        self.s3_client = boto3.client("s3")

    def load(self, chk_id: CheckpointID) -> Optional[Continuations]:
        if chk_id == NULL_CHK_ID:
            return None

        f = io.BytesIO()
        self.s3_client.download_fileobj(self.bucket_name, chk_id, f)
        f.seek(0)  # Rewind to beginning.
        return self._deserialize(f)

    def save(self, conts: Continuations, pid: Pid, seqno: Seqno) -> CheckpointID:
        f = io.BytesIO()
        self.serialize(conts, f)
        size = f.tell()
        f.seek(0)

        chk_id = self._make_chk_id(pid, seqno)
        with log_duration(pid, seqno, "checkpoint s3"):
            self.s3_client.upload_fileobj(f, self.bucket_name, chk_id)

        log(pid, seqno, f"Checkpoint saved to: {self.bucket_name}/{chk_id} (size={size}).")
        return chk_id

    def save_from_file(self, f: BinaryIO, pid: Pid, seqno: Seqno) -> CheckpointID:
        chk_id = self._make_chk_id(pid, seqno)

        start_pos = f.tell()
        end_pos = f.seek(0, os.SEEK_END)
        size = end_pos - start_pos
        f.seek(start_pos, os.SEEK_SET)

        with log_duration(pid, seqno, "async checkpoint s3"):
            self.s3_client.upload_fileobj(f, self.bucket_name, chk_id)

        log(pid, seqno, f"Checkpoint saved (async) to: {self.bucket_name}/{chk_id} (size={size}).")
        return chk_id
