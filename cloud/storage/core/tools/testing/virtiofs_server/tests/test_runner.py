import argparse
import signal
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from cloud.storage.core.tools.testing.virtiofs_server.lib import runner


class TestRunner(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.tmp_path = Path(temporary.name)

    def test_shared_root_and_restart(self):
        tmp_path = self.tmp_path
        shared = tmp_path / 'shared "directory'
        shared.mkdir()
        source = shared / "source"
        source.write_text("keep me")
        socket_path = str(tmp_path / "vhost.sock")
        state_dir = str(tmp_path / "private-state")
        config = runner.prepare_config(socket_path, str(shared), state_dir)
        root = config.parent / "root" / "nfs_shared"
        assert root.is_symlink()
        assert (root / "source").read_text() == "keep me"
        session = config.parent / "sessions" / "session"
        session.write_text("saved handles")
        assert runner.prepare_config(socket_path, str(shared), state_dir) == config
        assert session.read_text() == "saved handles"
        assert source.read_text() == "keep me"
        other = tmp_path / "other"
        other.mkdir()
        with self.assertRaisesRegex(ValueError, "another shared directory"):
            runner.prepare_config(socket_path, str(other), state_dir)

    def test_start_failure_reaps_vhost(self):
        tmp_path = self.tmp_path
        shared = tmp_path / "shared"
        shared.mkdir()
        args = argparse.Namespace(socket_path=str(tmp_path / "vhost.sock"),
                                  shared_dir=str(shared), vhost_binary="vhost",
                                  client_binary="client", state_dir=None)
        daemon = mock.Mock()
        daemon.poll.return_value = None
        daemon.wait.side_effect = [subprocess.TimeoutExpired("vhost", 5), 0]
        with mock.patch.object(runner.subprocess, "Popen", return_value=daemon), \
                mock.patch.object(runner, "run_client", side_effect=RuntimeError("failed")), \
                mock.patch.object(signal, "signal"), \
                mock.patch.object(runner.socket, "socket") as socket_mock:
            socket_mock.return_value.__enter__.return_value.getsockname.return_value = ("127.0.0.1", 1234)
            with self.assertRaisesRegex(RuntimeError, "failed"):
                runner.run(args)
        daemon.terminate.assert_called_once()
        daemon.kill.assert_called_once()
        assert daemon.wait.call_count == 2
        assert shared.is_dir()

    def test_client_checks_daemon_before_retry(self):
        daemon = mock.Mock()
        daemon.poll.return_value = 1
        daemon.returncode = 1
        with mock.patch.object(runner.subprocess, "run") as client:
            with self.assertRaisesRegex(RuntimeError, "exited with code 1"):
                runner.run_client("client", 1234, ["create"], daemon)
        client.assert_not_called()

    def test_client_retries_startup(self):
        daemon = mock.Mock()
        daemon.poll.return_value = None
        results = [subprocess.CompletedProcess([], 1, b"not ready"),
                   subprocess.CompletedProcess([], 0, b"")]
        with mock.patch.object(runner.subprocess, "run", side_effect=results) as client, \
                mock.patch.object(runner.time, "sleep"):
            runner.run_client("client", 1234, ["create"], daemon)
        assert client.call_count == 2
