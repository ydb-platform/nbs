"""Destructive writes are confined to two preflighted disposable disks."""

import hashlib
import json
import os
import stat
import subprocess
import time

from .config import Blocked


BLOCK = 1024 * 1024


def fixture(seed, index, case, length):
    if case == "zero" or (case in ("changed", "unchanged") and index % 7 == 0):
        return bytes(length)
    version = b"changed" if case in ("changed", "unchanged") and index % 8 == 0 else b"full"
    return hashlib.shake_256(seed + index.to_bytes(8, "little") + version).digest(length)


class Devices:
    def __init__(self, config):
        self.config = config

    def _open(self, path, write=False):
        try:
            # The configured by-id symlink is expected; fstat and lsblk match
            # the opened descriptor, avoiding a subsequent path-resolution race.
            descriptor = os.open(path, (os.O_RDWR if write else os.O_RDONLY) | os.O_CLOEXEC)
        except OSError:
            raise Blocked("Disposable device cannot be opened") from None
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISBLK(info.st_mode):
                raise Blocked("Test target is not a block device")
            result = subprocess.run(
                ["lsblk", "--json", "--bytes", "--output", "MAJ:MIN,SIZE,TYPE,MOUNTPOINTS"],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=False, timeout=10)
            if result.returncode:
                raise Blocked("Cannot inspect mounted devices")
            tree = json.loads(result.stdout)["blockdevices"]
            identity = f"{os.major(info.st_rdev)}:{os.minor(info.st_rdev)}"
            matches = []

            def visit(items):
                for item in items:
                    if item.get("maj:min") == identity:
                        matches.append(item)
                    visit(item.get("children", []))
            visit(tree)
            if len(matches) != 1:
                raise Blocked("Block device identity is ambiguous")
            item = matches[0]
            if (item.get("type") != "disk" or item.get("children")
                    or any(item.get("mountpoints") or [])
                    or int(item.get("size", -1)) != self.config.size_bytes):
                raise Blocked("Disk must have the expected size, no partitions and no mounts")
            return descriptor, info.st_rdev
        except Exception:
            os.close(descriptor)
            raise

    def validate(self):
        source, source_id = self._open(self.config.source_device)
        try:
            target, target_id = self._open(self.config.target_device)
            os.close(target)
            if source_id == target_id:
                raise Blocked("Both configured paths resolve to the same block device")
        finally:
            os.close(source)

    def fill(self, seed, case, deadline):
        descriptor, _ = self._open(self.config.source_device, write=True)
        digest = hashlib.sha256()
        with os.fdopen(descriptor, "wb", buffering=0) as stream:
            for offset in range(0, self.config.size_bytes, BLOCK):
                if time.monotonic() >= deadline:
                    raise Blocked("Source disk write deadline exceeded")
                data = fixture(seed, offset // BLOCK, case, min(BLOCK, self.config.size_bytes - offset))
                index = offset // BLOCK
                if case == "changed" and index % 7 != 0 and index % 8 != 0:
                    # Preserve the NBS dirty bitmap for unchanged ranges. Writing
                    # identical bytes would turn this into another full copy.
                    stream.seek(len(data), os.SEEK_CUR)
                elif stream.write(data) != len(data):
                    raise Blocked("Short source disk write")
                digest.update(data)
            os.fsync(stream.fileno())
        return digest.hexdigest()

    def restore_and_verify(self, image, expected, deadline):
        descriptor, _ = self._open(self.config.target_device, write=True)
        with os.fdopen(descriptor, "r+b", buffering=0) as target, open(image, "rb") as source:
            for offset in range(0, self.config.size_bytes, BLOCK):
                if time.monotonic() >= deadline:
                    raise Blocked("Restore disk write deadline exceeded")
                length = min(BLOCK, self.config.size_bytes - offset)
                data = source.read(length)
                if len(data) != length or target.write(data) != len(data):
                    raise Blocked("Short restore read/write")
            if source.read(1):
                raise Blocked("Restore image exceeds the disposable disk")
            os.fsync(target.fileno())
            target.seek(0)
            digest = hashlib.sha256()
            left = self.config.size_bytes
            while left:
                if time.monotonic() >= deadline:
                    raise Blocked("Restore verification deadline exceeded")
                data = target.read(min(BLOCK, left))
                if not data:
                    raise Blocked("Short restore disk read")
                digest.update(data)
                left -= len(data)
        if digest.hexdigest() != expected:
            from .transport import InvalidBackup
            raise InvalidBackup("Restored block device differs from the generated reference")
