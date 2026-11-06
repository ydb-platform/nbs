import subprocess
import tempfile
from pathlib import Path

import pytest
import yatest.common as common

from cloud.storage.core.tools.testing.virtiofs_server.lib import VirtioFsServer


def test_local_vhost_restart():
    binary = common.binary_path("cloud/filestore/apps/vhost/filestore-vhost")
    client = common.binary_path("cloud/filestore/apps/client/filestore-client")
    with tempfile.TemporaryDirectory(prefix="local-vfs-") as temp:
        root = Path(temp)
        shared = root / 'shared "directory'
        shared.mkdir()
        (shared / "keep").write_text("shared contents")
        endpoint = str(root / "vhost.sock")
        for iteration in range(2):
            server = VirtioFsServer(binary, endpoint, str(shared))
            try:
                server.start(common.output_path(), str(iteration))
                server.wait_for_socket()
                assert subprocess.check_output([
                    client, "read", "--filesystem", "shared", "--path", "/keep",
                    "--server-port", str(server.local_port),
                ], timeout=30) == b"shared contents"
                other = root / "other"
                other.mkdir(exist_ok=True)
                with pytest.raises(ValueError, match="another shared directory"):
                    VirtioFsServer(binary, endpoint, str(other)).start(common.output_path(), "other")
                assert server.daemon.is_alive()
            finally:
                server.stop()
            assert not server.daemon.is_alive()
            assert (shared / "keep").read_text() == "shared contents"


def test_start_failure_stops_daemon():
    binary = common.binary_path("cloud/filestore/apps/vhost/filestore-vhost")
    with tempfile.TemporaryDirectory(prefix="local-vfs-") as temp:
        server = VirtioFsServer(binary, str(Path(temp) / "vhost.sock"), temp)

        def fail(*args):
            raise RuntimeError("startup failed")

        server._run_client = fail
        with pytest.raises(RuntimeError, match="startup failed"):
            server.start(common.output_path(), "failure")
        assert not server.daemon.is_alive()
