import subprocess
import tempfile
import time
from pathlib import Path

import yatest.common as common


def test_local_vhost_restart():
    runner = common.binary_path(
        "cloud/storage/core/tools/testing/virtiofs_server/bin/virtiofs-server")
    vhost = common.binary_path("cloud/filestore/apps/vhost/filestore-vhost")
    client = common.binary_path("cloud/filestore/apps/client/filestore-client")
    # Keep the Unix socket path below the kernel's length limit.
    with tempfile.TemporaryDirectory(prefix="local-vfs-") as temp:
        root = Path(temp)
        shared = root / "shared"
        shared.mkdir()
        (shared / "keep").write_text("shared contents")
        endpoint = root / "vhost.sock"
        for iteration in range(2):
            with open(common.output_path("local-vhost-{}.log".format(iteration)), "w+") as log:
                process = subprocess.Popen([
                    runner, "--socket-path", str(endpoint),
                    "--shared-dir", str(shared), "--vhost-binary", vhost,
                    "--client-binary", client,
                ], stdout=log, stderr=log)
                try:
                    deadline = time.monotonic() + 180
                    while not endpoint.is_socket():
                        if process.poll() is not None or time.monotonic() > deadline:
                            log.seek(0)
                            raise RuntimeError(log.read())
                        time.sleep(0.1)
                    children = Path("/proc/{0}/task/{0}/children".format(process.pid)).read_text().split()
                    vhost_pid = None
                    for child in children:
                        command = Path("/proc/{}/cmdline".format(child)).read_bytes().decode().split("\0")
                        if "--local-service-port" in command:
                            vhost_pid = child
                            port = command[command.index("--local-service-port") + 1]
                            break
                    assert vhost_pid is not None
                    contents = subprocess.check_output([
                        client, "read", "--filesystem", "shared",
                        "--path", "/keep", "--server-port", port,
                    ], timeout=30)
                    assert contents == b"shared contents"
                finally:
                    process.terminate()
                    try:
                        process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                assert process.returncode == 0
                assert not Path("/proc/" + vhost_pid).exists(), "vhost child survived shutdown"
                assert not endpoint.exists()
                assert (shared / "keep").read_text() == "shared contents"
