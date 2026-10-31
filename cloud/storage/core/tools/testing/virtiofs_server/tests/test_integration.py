import signal
import subprocess
import tempfile
import time
from pathlib import Path

import yatest.common as common


def binary():
    return common.binary_path(
        "cloud/storage/core/tools/testing/virtiofs_server/bin/virtiofs-server")


def wait_for_socket(process, endpoint, log):
    deadline = time.monotonic() + 60
    while not endpoint.is_socket():
        if process.poll() is not None or time.monotonic() > deadline:
            log.seek(0)
            raise RuntimeError(log.read())
        time.sleep(0.1)


def test_local_vhost_restart():
    # Keep the Unix socket path below the kernel's length limit.
    with tempfile.TemporaryDirectory(prefix="local-vfs-") as temp:
        root = Path(temp)
        shared = root / 'shared "directory'
        shared.mkdir()
        (shared / "keep").write_text("shared contents")
        endpoint = root / "vhost.sock"
        state = root / "private-state"
        command = [binary(), "--socket-path", str(endpoint),
                   "--shared-dir", str(shared), "--state-dir", str(state)]
        for iteration, stop_signal in enumerate((signal.SIGTERM, signal.SIGINT)):
            with open(common.output_path("local-vhost-{}.log".format(iteration)), "w+") as log:
                process = subprocess.Popen(command, stdout=log, stderr=log)
                try:
                    wait_for_socket(process, endpoint, log)
                    assert not Path("/proc/{0}/task/{0}/children".format(process.pid)).read_text().strip()
                    assert (state / "root" / "nfs_shared").resolve() == shared
                    assert (state / "root" / "nfs_shared" / "keep").read_text() == "shared contents"
                    # Reusing state with a different share must fail without disturbing this server.
                    other = root / "other"
                    other.mkdir(exist_ok=True)
                    rejected = subprocess.run(
                        command + ["--shared-dir", str(other)],
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=15)
                    assert rejected.returncode != 0
                    assert b"another shared directory" in rejected.stdout
                    assert process.poll() is None
                    assert endpoint.is_socket()
                finally:
                    process.send_signal(stop_signal)
                    try:
                        process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                assert process.returncode == 0
                assert not endpoint.exists()
                assert (shared / "keep").read_text() == "shared contents"
                assert (state / "sessions").is_dir()
