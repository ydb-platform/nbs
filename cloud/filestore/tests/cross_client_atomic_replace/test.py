import errno
import json
import logging
import os

import yatest.common as common

from cloud.storage.core.tools.testing.qemu.lib.common import (
    env_with_guest_index,
    SshToGuest,
)

SCRIPT = "cloud/filestore/tests/cross_client_atomic_replace/script.py"
TIMEOUT_SECONDS = 300


def run(guest, role, root, wait):
    ssh = SshToGuest(
        user="qemu",
        port=int(
            os.getenv(env_with_guest_index("QEMU_FORWARDING_PORT", guest))),
        key=os.getenv("QEMU_SSH_KEY"))
    return common.execute(
        ssh.get_command(
            f"sudo python3 {common.source_path(SCRIPT)} {role} {root}",
            timeout=TIMEOUT_SECONDS),
        wait=wait)


def test():
    root = os.path.join(
        os.getenv("NFS_MOUNT_PATH"), "cross_client_atomic_replace")

    writer = run(0, "writer", root, wait=False)
    reader = run(1, "reader", root, wait=True)
    writer.wait(timeout=TIMEOUT_SECONDS)

    summary = json.loads(reader.stdout.decode("utf8").splitlines()[-1])
    with open(common.output_path("filestore-vhost.err")) as log:
        stale_opens = log.read().count("failed on a stale node")
    logging.info(f"reader: {summary}, stale opens: {stale_opens}")

    # the reader opened the replaced node through its cached dentry at least
    # once and the kernel recovered via ESTALE instead of reporting ENOENT
    assert stale_opens > 0
    # a replace landing inside the kernel's single ESTALE retry surfaces
    # ESTALE to the application; ENOENT must never be reported
    assert set(summary["errors"]) <= {str(errno.ESTALE)}, summary
