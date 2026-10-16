import json
import os
import signal
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from cloud.disk_manager.test.snapshot_backup import transport as transport_module
from cloud.disk_manager.test.snapshot_backup.transport import (
    AuthError, BackupTimeout, InvalidBackup, ObjectNotFound, StoredObject,
    TransportError, PresignedObjectStore,
    require_read_only_trace,
)


def child_python():
    # PY3TEST's executable is a test binary; its default tool may be Python 2.
    if getattr(sys, "is_standalone_binary", False):
        import yatest.common
        return yatest.common.binary_path("contrib/tools/python3/bin/python3")
    return sys.executable


class TransportTests(unittest.TestCase):
    def setUp(self):
        guard = mock.patch("cloud.disk_manager.test.snapshot_backup.transport.require_read_only_trace")
        guard.start()
        self.addCleanup(guard.stop)
        self.store = PresignedObjectStore(
            "robot", "backup-bucket", "storage.example.test", "prefix",
            executable="/test/provider", config_path="/private/provider.json",
            trace_paths=["/var/lib/provider-trace"])
        self.url = "https://storage.example.test/backup-bucket/prefix/chunks/chunk?secret=never-print"
        self.calls = []
        self.directories = []
        self.status = b"200"
        self.returncode = 0
        self.payload = b"contents"
        self.headers = b"HTTP/1.1 200 OK\r\nx-amz-meta-Checksum: 123\r\nx-amz-meta-Compression: gzip\r\n\r\n"

    def command(self, command, **kwargs):
        self.calls.append((command, kwargs))
        if command == ["curl", "-q", "--version"]:
            return subprocess.CompletedProcess(command, 0, b"curl 8.5.0 (fixture)\n")
        if command[0] == "/test/provider":
            self.assertEqual(command, ["/test/provider", "--config", "/private/provider.json"])
            self.assertEqual(json.loads(kwargs["input_data"]), {
                "version": 1, "operation": "presign", "profile": "robot",
                "request": {"bucket": "backup-bucket", "host": "storage.example.test",
                            "key": "prefix/chunks/chunk", "method": "GET", "expires_seconds": 900},
            })
            return subprocess.CompletedProcess(command, 0, json.dumps({"url": self.url}).encode())
        self.directories.append(Path(command[command.index("--output") + 1]).parent)
        Path(command[command.index("--output") + 1]).write_bytes(self.payload)
        Path(command[command.index("--dump-header") + 1]).write_bytes(self.headers)
        self.assertNotIn(self.url, command)
        self.assertIn(self.url.encode(), kwargs["input_data"])
        self.assertEqual(command[1], "-q")
        self.assertNotIn("--location", command)
        self.assertEqual(command[command.index("--proto") + 1], "=https")
        self.assertLessEqual(kwargs["timeout"], 30)
        return subprocess.CompletedProcess(command, self.returncode, self.status)

    def get(self, **kwargs):
        with mock.patch.object(PresignedObjectStore, "_run", side_effect=self.command):
            return self.store.get("chunks/chunk", deadline=kwargs.pop("deadline", time.monotonic() + 60),
                                  max_bytes=kwargs.pop("max_bytes", 100))

    def test_signed_url_only_on_stdin_metadata_normalized_and_temp_removed(self):
        obj = self.get()
        self.assertEqual(obj.data, b"contents")
        self.assertEqual(dict(obj.metadata), {"checksum": "123", "compression": "gzip"})
        self.assertTrue(all(not directory.exists() for directory in self.directories))
        with self.assertRaises(TypeError):
            obj.metadata["x"] = "y"

    def test_storage_error_classification(self):
        for status, code, expected in ((b"404", 22, ObjectNotFound), (b"403", 22, AuthError),
                                       (b"401", 22, AuthError), (b"503", 22, TransportError),
                                       (b"301", 0, TransportError), (b"000", 28, BackupTimeout),
                                       (b"200", 63, InvalidBackup), (b"000", 7, TransportError)):
            with self.subTest(status=status, code=code):
                self.status, self.returncode = status, code
                with self.assertRaises(expected) as caught:
                    self.get()
                self.assertNotIn("never-print", str(caught.exception))
                self.assertTrue(all(not directory.exists() for directory in self.directories))

    def test_presign_wrong_host_object_scheme_and_userinfo_are_rejected(self):
        for url in ("https://other.example.test/backup-bucket/prefix/chunks/chunk?secret=never-print",
                    "https://storage.example.test/other?secret=never-print",
                    "http://storage.example.test/prefix/chunks/chunk?secret=never-print",
                    "https://user:password@storage.example.test/prefix/chunks/chunk?secret=never-print",
                    "https://storage.example.test:444/prefix/chunks/chunk?secret=never-print"):
            with self.subTest(url=url):
                self.url = url
                with self.assertRaises(TransportError) as caught:
                    self.get()
                self.assertNotIn("never-print", str(caught.exception))

    def test_oversized_body_and_duplicate_metadata_fail(self):
        with self.assertRaises(InvalidBackup):
            self.get(max_bytes=3)
        self.headers += b"x-amz-meta-checksum: 123\r\n"
        with self.assertRaises(InvalidBackup):
            self.get()

    def test_timeout_does_not_expose_command_stdout_or_stderr(self):
        with self.assertRaises(BackupTimeout) as caught:
            self.store._run([child_python(), "-c", "import time; print('SECRET'); time.sleep(10)"],
                            deadline=time.monotonic() + 5, timeout=0.1)
        self.assertNotIn("SECRET", str(caught.exception))

    def test_expired_deadline_and_bad_keys_do_not_run_commands(self):
        with mock.patch.object(PresignedObjectStore, "_run") as run:
            with self.assertRaises(BackupTimeout):
                self.store.get("chunks/chunk", deadline=time.monotonic() - 1, max_bytes=100)
            for key in ("../secret", "", "/absolute", "a//b", "a\nb", "a\\b"):
                with self.subTest(key=key), self.assertRaises(InvalidBackup):
                    self.store.get(key, deadline=time.monotonic() + 30, max_bytes=100)
            run.assert_not_called()

    def test_adapter_descendants_are_stopped_on_timeout_and_normal_exit(self):
        # The child ignores console output and would outlive its parent without
        # process-group cleanup. A delayed file write detects surviving work.
        for parent_waits in (True, False):
            with self.subTest(parent_waits=parent_waits), tempfile.TemporaryDirectory() as directory:
                marker = str(Path(directory) / "unexpected-child-write")
                ready = str(Path(directory) / "child-ready")
                child = ("import time; open(%r, 'w').close(); time.sleep(2); "
                         "open(%r, 'w').write('orphan')" % (ready, marker))
                parent = ("import subprocess, os, time; "
                          "subprocess.Popen([%r, '-c', %r], stdout=subprocess.DEVNULL, "
                          "stderr=subprocess.DEVNULL)\n"
                          "while not os.path.exists(%r): time.sleep(0.01)\n"
                          "time.sleep(%d)" % (child_python(), child, ready, 10 if parent_waits else 0))
                if parent_waits:
                    with self.assertRaises(BackupTimeout):
                        self.store._run([child_python(), "-c", parent],
                                        deadline=time.monotonic() + 5, timeout=1)
                else:
                    result = self.store._run([child_python(), "-c", parent],
                                             deadline=time.monotonic() + 5, timeout=5)
                    self.assertEqual(result.returncode, 0)
                self.assertTrue(Path(ready).exists(), "fixture child did not start")
                time.sleep(2.1)
                self.assertFalse(Path(marker).exists(), "adapter descendant survived its call")

    def test_stored_object_rejects_case_duplicate_metadata(self):
        with self.assertRaises(InvalidBackup):
            StoredObject(b"x", {"Checksum": "1", "checksum": "1"})

    def test_cycle_cancellation_unwinds_and_stops_active_adapter_group(self):
        source = transport_module.__file__
        if getattr(sys, "is_standalone_binary", False):
            import yatest.common
            source = yatest.common.source_path("cloud/disk_manager/test/snapshot_backup/transport.py")
        with tempfile.TemporaryDirectory() as directory:
            ready = str(Path(directory) / "adapter-ready")
            marker = str(Path(directory) / "unexpected-adapter-write")
            # Existence is the readiness signal: publish only a complete PID.
            adapter = ("import os, time\n"
                       "with open(%r, 'w') as ready_file:\n"
                       "    ready_file.write(str(os.getpid()))\n"
                       "os.replace(%r, %r)\n"
                       "time.sleep(2)\n"
                       "open(%r, 'w').write('orphan')\n"
                       % (ready + ".tmp", ready + ".tmp", ready, marker))
            child_code = (
                "import importlib.util, signal, sys, time\n"
                "spec = importlib.util.spec_from_file_location('transport_fixture', %r)\n"
                "module = importlib.util.module_from_spec(spec)\n"
                "sys.modules[spec.name] = module\n"
                "spec.loader.exec_module(module)\n"
                "signal.signal(signal.SIGTERM, module.interrupt_action)\n"
                "module.run_command([%r, '-c', %r], deadline=time.monotonic() + 30, timeout=30)\n"
                % (source, child_python(), adapter))
            child = subprocess.Popen([child_python(), "-c", child_code],
                                     stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL, start_new_session=True)
            try:
                end = time.monotonic() + 5
                while not Path(ready).exists() and child.poll() is None and time.monotonic() < end:
                    time.sleep(0.01)
                self.assertTrue(Path(ready).exists(), "fixture adapter did not start")
                os.killpg(child.pid, signal.SIGTERM)
                self.assertEqual(child.wait(timeout=5), 2)
                time.sleep(2.1)
                self.assertFalse(Path(marker).exists(), "adapter survived cycle cancellation")
            finally:
                if child.poll() is None:
                    os.killpg(child.pid, signal.SIGKILL)
                child.wait()
                if Path(ready).exists():
                    try:
                        os.killpg(int(Path(ready).read_text()), signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_old_curl_is_rejected_before_a_network_command(self):
        old_curl = subprocess.CompletedProcess([], 0, b"curl 7.81.0\n")
        with mock.patch.object(PresignedObjectStore, "_run", return_value=old_curl) as run:
            with self.assertRaisesRegex(TransportError, "curl >= 8.4"):
                self.store.get("chunks/chunk", deadline=time.monotonic() + 30, max_bytes=100)
            run.assert_called_once()

    def test_command_stdout_is_bounded_during_read(self):
        with self.assertRaisesRegex(TransportError, "Oversized"):
            self.store._run([child_python(), "-c", "import sys; sys.stdout.buffer.write(b'x' * 2097152)"],
                            deadline=time.monotonic() + 5, timeout=5)

    def test_command_stdin_stdout_and_exit_status(self):
        command = [child_python(), "-c",
                   "import sys; sys.stdout.buffer.write(sys.stdin.buffer.read()); sys.exit(7)"]
        result = self.store._run(
            command, deadline=time.monotonic() + 5, timeout=5, input_data=b"fixture")
        self.assertEqual(result.stdout, b"fixture")
        self.assertEqual(result.returncode, 7)

    def test_unprotected_trace_blocks_before_presigning(self):
        with mock.patch("cloud.disk_manager.test.snapshot_backup.transport.require_read_only_trace",
                        side_effect=AuthError("Use the hardened systemd unit")):
            with self.assertRaisesRegex(AuthError, "systemd"):
                self.get()
        self.assertEqual([command for command, _ in self.calls], [["curl", "-q", "--version"]])


class TraceGuardTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="snapshot-trace-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.home = self.root / "home"
        self.home.mkdir()
        self.paths = [str(self.home / ".config" / "provider" / "logs")]

    def filesystem(self, flags):
        return mock.patch("cloud.disk_manager.test.snapshot_backup.transport.os.statvfs",
                          return_value=mock.Mock(f_flag=flags))

    def test_empty_or_invalid_trace_inventory_is_rejected(self):
        for paths in (None, [], "", [None], [42]):
            with self.subTest(paths=paths), self.assertRaises(AuthError):
                require_read_only_trace(paths)

    def test_missing_directories_require_read_only_existing_parent(self):
        with self.filesystem(os.ST_RDONLY) as filesystem:
            require_read_only_trace(self.paths)
            filesystem.assert_called_once_with(self.home)
        with self.filesystem(0), self.assertRaisesRegex(AuthError, "hardened.*systemd"):
            require_read_only_trace(self.paths)

    def test_chmod_read_only_does_not_replace_read_only_mount(self):
        path = self.home / ".config" / "provider" / "logs"
        path.mkdir(parents=True)
        path.chmod(0o500)
        try:
            with self.filesystem(0), self.assertRaises(AuthError):
                require_read_only_trace(self.paths)
        finally:
            path.chmod(0o700)

    def test_existing_log_directory_mount_is_checked_not_home_mount(self):
        path = self.home / ".config" / "provider" / "logs"
        path.mkdir(parents=True)
        with self.filesystem(os.ST_RDONLY) as filesystem:
            require_read_only_trace(self.paths)
            filesystem.assert_called_once_with(path)

    def test_every_configured_path_is_checked(self):
        xdg = self.root / "xdg"
        xdg.mkdir()
        self.paths.append(str(xdg / "provider" / "logs"))
        with mock.patch("cloud.disk_manager.test.snapshot_backup.transport.os.statvfs",
                        side_effect=[mock.Mock(f_flag=os.ST_RDONLY), mock.Mock(f_flag=0)]) as filesystem:
            with self.assertRaises(AuthError):
                require_read_only_trace(self.paths)
            self.assertEqual(filesystem.call_args_list, [mock.call(self.home), mock.call(xdg)])

    def test_relative_or_escaping_trace_path_is_rejected(self):
        for path in ("relative", str(self.root / ".." / "escape"), "/tmp/line\nbreak"):
            with self.subTest(path=path), self.filesystem(os.ST_RDONLY), self.assertRaises(AuthError):
                require_read_only_trace([path])

    def test_symlink_components_and_dangling_redirects_are_rejected(self):
        other = self.root / "other"
        other.mkdir()
        config = self.home / ".config"
        for target in (other, self.root / "missing"):
            with self.subTest(target=target):
                config.symlink_to(target, target_is_directory=True)
                try:
                    with self.filesystem(os.ST_RDONLY), self.assertRaises(AuthError):
                        require_read_only_trace(self.paths)
                finally:
                    config.unlink()

    def test_inaccessible_children_require_read_only_visible_ancestor(self):
        original = Path.lstat

        def lstat(path):
            if path != self.home and self.home in path.parents:
                raise PermissionError("private fixture detail")
            return original(path)
        with mock.patch.object(Path, "lstat", lstat), self.filesystem(os.ST_RDONLY) as filesystem:
            require_read_only_trace(self.paths)
            filesystem.assert_called_once_with(self.home)

    def test_statvfs_failure_is_sanitized_and_blocks(self):
        with mock.patch("cloud.disk_manager.test.snapshot_backup.transport.os.statvfs",
                        side_effect=OSError("PRIVATE PATH")):
            with self.assertRaises(AuthError) as caught:
                require_read_only_trace(self.paths)
            self.assertNotIn("PRIVATE PATH", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
