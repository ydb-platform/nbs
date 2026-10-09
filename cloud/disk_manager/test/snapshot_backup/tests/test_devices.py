import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from cloud.disk_manager.test.snapshot_backup.config import Blocked
from cloud.disk_manager.test.snapshot_backup.devices import Devices, fixture
from cloud.disk_manager.test.snapshot_backup.tests.helpers import make_config
from cloud.disk_manager.test.snapshot_backup.transport import InvalidBackup


class DevicesTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.config = make_config(self.directory.name)
        self.devices = Devices(self.config)

    def inspect(self, tree, mode=stat.S_IFBLK):
        # No /dev path is opened: every OS descriptor/lsblk call is mocked.
        info = SimpleNamespace(st_mode=mode, st_rdev=os.makedev(8, 16))
        with mock.patch("os.open", return_value=123), mock.patch("os.fstat", return_value=info), \
                mock.patch("os.close"), mock.patch("subprocess.run", return_value=subprocess.CompletedProcess(
                    [], 0, json.dumps({"blockdevices": tree}).encode())):
            return self.devices._open(self.config.source_device)

    def test_only_an_unmounted_partitionless_correct_sized_block_disk_is_allowed(self):
        base = {"maj:min": "8:16", "type": "disk", "size": self.config.size_bytes, "mountpoints": [None]}
        self.assertEqual(self.inspect([base]), (123, os.makedev(8, 16)))
        for patch in ({"size": 1}, {"type": "part"}, {"mountpoints": ["/"]},
                      {"children": [{"maj:min": "8:17", "mountpoints": ["/mnt"]}]}):
            with self.subTest(patch=patch), self.assertRaises(Blocked):
                self.inspect([dict(base, **patch)])
        with self.assertRaises(Blocked):
            self.inspect([base], mode=stat.S_IFREG)
        with self.assertRaises(Blocked):
            self.inspect([base, base])

    def test_source_target_aliases_are_rejected(self):
        with mock.patch.object(self.devices, "_open", side_effect=[(123, 456), (124, 456)]), \
                mock.patch("os.close") as close, self.assertRaises(Blocked):
            self.devices.validate()
        self.assertEqual({c.args[0] for c in close.call_args_list}, {123, 124})

    def test_fixture_versions_and_zero_ranges_are_deterministic(self):
        seed = b"fixture-seed"
        self.assertEqual(fixture(seed, 1, "full", 512), fixture(seed, 1, "changed", 512))
        self.assertNotEqual(fixture(seed, 8, "full", 512), fixture(seed, 8, "changed", 512))
        for index in (0, 1, 7, 8, 15):
            self.assertEqual(fixture(seed, index, "changed", 512), fixture(seed, index, "unchanged", 512))
            self.assertEqual(fixture(seed, index, "zero", 512), bytes(512))
        self.assertEqual(fixture(seed, 7, "changed", 512), bytes(512))
        self.assertNotEqual(fixture(seed, 1, "full", 512), fixture(seed + b"other", 1, "full", 512))

    def test_fill_and_restore_use_only_private_regular_fixture_files(self):
        source = Path(self.directory.name) / "source-fixture"
        target = Path(self.directory.name) / "target-fixture"
        source.write_bytes(bytes(self.config.size_bytes))
        target.write_bytes(b"x" * self.config.size_bytes)

        def opened(path, write=False):
            selected = source if path == self.config.source_device else target
            return os.open(selected, os.O_RDWR if write else os.O_RDONLY), 123

        with mock.patch.object(self.devices, "_open", side_effect=opened):
            self.devices.fill(b"fixture", "full", time.monotonic() + 30)
            digest = self.devices.fill(b"fixture", "changed", time.monotonic() + 30)
            self.assertEqual(hashlib.sha256(source.read_bytes()).hexdigest(), digest)
            self.devices.restore_and_verify(source, digest, time.monotonic() + 30)
            self.assertEqual(source.read_bytes(), target.read_bytes())
            with self.assertRaises(InvalidBackup):
                self.devices.restore_and_verify(source, "0" * 64, time.monotonic() + 30)

    def test_changed_case_writes_only_modified_ranges_to_preserve_incremental_parent_chunks(self):
        path = Path(self.directory.name) / "source-fixture"
        path.write_bytes(bytes(self.config.size_bytes))

        def opened(*_args, **_kwargs):
            return os.open(path, os.O_RDWR), 1
        with mock.patch.object(self.devices, "_open", side_effect=opened):
            self.devices.fill(b"fixture", "full", time.monotonic() + 30)
        original_fdopen = os.fdopen
        writes = []

        class RecordingFile:
            def __init__(self, descriptor, *args, **kwargs):
                self.raw = original_fdopen(descriptor, *args, **kwargs)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.raw.close()

            def write(self, data):
                writes.append((self.raw.tell(), len(data)))
                return self.raw.write(data)

            def seek(self, *args):
                return self.raw.seek(*args)

            def fileno(self):
                return self.raw.fileno()
        with mock.patch.object(self.devices, "_open", side_effect=opened), \
                mock.patch("os.fdopen", side_effect=RecordingFile):
            digest = self.devices.fill(b"fixture", "changed", time.monotonic() + 30)
        self.assertEqual(writes, [(0, 1024**2)])
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), digest)
        self.assertEqual(path.read_bytes()[1024**2:2 * 1024**2], fixture(b"fixture", 1, "full", 1024**2))

    def test_expired_write_deadline_preserves_fixture(self):
        path = Path(self.directory.name) / "fixture"
        path.write_bytes(b"untouched")
        with mock.patch.object(self.devices, "_open", return_value=(os.open(path, os.O_RDWR), 1)):
            with self.assertRaises(Blocked):
                self.devices.fill(b"seed", "full", time.monotonic() - 1)
        self.assertEqual(path.read_bytes(), b"untouched")

    def test_short_restore_image_is_rejected(self):
        source = Path(self.directory.name) / "short-source"
        target = Path(self.directory.name) / "target-fixture"
        source.write_bytes(b"x" * (self.config.size_bytes - 1))
        target.write_bytes(b"x" * self.config.size_bytes)
        # Without an exact length check, a one-byte-short image passes when the
        # old tail happens to match the expected disk; this must never be green.
        expected = hashlib.sha256(target.read_bytes()).hexdigest()
        with mock.patch.object(self.devices, "_open", return_value=(os.open(target, os.O_RDWR), 1)):
            with self.assertRaises(Blocked):
                self.devices.restore_and_verify(source, expected, time.monotonic() + 30)


if __name__ == "__main__":
    unittest.main()
