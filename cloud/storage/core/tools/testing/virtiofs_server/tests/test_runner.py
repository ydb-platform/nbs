import subprocess
import tempfile
from pathlib import Path

import yatest.common as common


def test_reject_missing_share():
    runner = common.binary_path(
        "cloud/storage/core/tools/testing/virtiofs_server/bin/virtiofs-server")
    with tempfile.TemporaryDirectory(prefix="local-vfs-") as temp:
        root = Path(temp)
        result = subprocess.run([
            runner, "--socket-path", str(root / "vhost.sock"),
            "--shared-dir", str(root / "missing"),
        ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=15)
        assert result.returncode == 1, result.stdout.decode(errors="replace")
        assert b"missing" in result.stdout
        assert not (root / "vhost.sock").exists()
        assert not (root / "vhost.sock.state").exists()


def test_endpoint_start_failure():
    runner = common.binary_path(
        "cloud/storage/core/tools/testing/virtiofs_server/bin/virtiofs-server")
    with tempfile.TemporaryDirectory(prefix="local-vfs-") as temp:
        root = Path(temp)
        shared = root / "shared"
        shared.mkdir()
        (shared / "keep").write_text("keep me")
        # The parent of the socket does not exist. Fail after service startup,
        # exercising cleanup of the scheduler, worker pool and AIO thread.
        endpoint = root / "missing" / "vhost.sock"
        result = subprocess.run([
            runner, "--socket-path", str(endpoint),
            "--shared-dir", str(shared), "--state-dir", str(root / "state"),
        ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=15)
        assert result.returncode == 1, result.stdout.decode(errors="replace")
        assert not endpoint.exists()
        assert (shared / "keep").read_text() == "keep me"
