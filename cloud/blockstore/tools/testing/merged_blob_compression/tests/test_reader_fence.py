import hashlib
import os
from pathlib import Path
import tempfile

import pytest

from cloud.blockstore.tools.testing.merged_blob_compression.reader_fence import digest_fd, open_trusted, verify_policy


def policy():
    return {"format": 1, "minimum_blob_reader_version": 1,
            "cohort_id": "test-readers", "allowed_hosts": ["reader-1"],
            "reader_binary_sha256": ["a" * 64]}


def test_old_binary_and_unknown_host_are_denied():
    verify_policy(policy(), "a" * 64, "reader-1")
    with pytest.raises(RuntimeError, match="binary"):
        verify_policy(policy(), "b" * 64, "reader-1")
    with pytest.raises(RuntimeError, match="host"):
        verify_policy(policy(), "a" * 64, "old-host")
    p = policy()
    p["minimum_blob_reader_version"] = 0
    with pytest.raises(RuntimeError, match="floor"):
        verify_policy(p, "a" * 64, "reader-1")


def test_digest_binds_open_inode_and_rewinds():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "binary"
        path.write_bytes(b"approved reader image")
        fd = os.open(path, os.O_RDONLY)
        try:
            path.unlink()
            path.write_bytes(b"old image at the same name")
            expected = hashlib.sha256(b"approved reader image").hexdigest()
            assert digest_fd(fd) == expected
            assert os.read(fd, 4096) == b"approved reader image"
        finally:
            os.close(fd)


def test_writable_policy_is_denied():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "policy"
        path.write_text("{}")
        path.chmod(0o666)
        with pytest.raises(RuntimeError):
            open_trusted(path)
