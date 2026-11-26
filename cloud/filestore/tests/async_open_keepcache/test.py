import logging
import os
import re

import yatest.common as common

from cloud.storage.core.tools.testing.qemu.lib.common import SshToGuest

BINARY = "cloud/filestore/tools/testing/stale_read_loop/stale-read-loop"
DURATION_SECONDS = 60
RACERS = 4
TIMEOUT_SECONDS = DURATION_SECONDS + 120


def test():
    ssh = SshToGuest(
        user="qemu",
        port=int(os.getenv("QEMU_FORWARDING_PORT")),
        key=os.getenv("QEMU_SSH_KEY"))
    path = os.path.join(os.getenv("NFS_MOUNT_PATH"), "stale_read_loop.txt")

    cmd = f"sudo {common.binary_path(BINARY)} {path}" \
        f" {DURATION_SECONDS} {RACERS}"
    result = common.execute(
        ssh.get_command(cmd, timeout=TIMEOUT_SECONDS),
        check_exit_code=False)
    out = result.stdout.decode("utf8")
    logging.info(out)

    # exit codes: 0 - clean, 1 - stale read, anything else - the tool failed
    assert "STALE READ" not in out, out
    assert result.exit_code == 0, out

    # make sure the race window was actually exercised, otherwise a clean run
    # proves nothing
    overlapped = int(re.search(r"overlapped=(\d+)", out).group(1))
    assert overlapped > 0, out
