import json
import logging
import os

import cloud.filestore.tools.testing.profile_log.common as profile
import yatest.common as common

from cloud.filestore.tests.python.lib.common import flush_logs
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
    flush_logs()

    summary = json.loads(reader.stdout.decode("utf8").splitlines()[-1])

    profile_tool_bin_path = common.binary_path(
        "cloud/filestore/tools/analytics/profile_tool/filestore-profile-tool")
    events = profile.iter_profile_log_events(
        profile_tool_bin_path,
        common.output_path("vhost-profile.log"),
        "nfs_test")
    stale_opens = sum(
        event.request_type == "CreateHandle" and event.result == "E_FS_NOENT"
        for event in events)
    logging.info(f"reader: {summary}, stale opens: {stale_opens}")

    # the reader opened the replaced node through its cached dentry at least
    # once and the kernel recovered via ESTALE instead of reporting ENOENT
    assert stale_opens > 0

    # the file exists at all times, the reader must never see ENOENT
    errors = summary["errors"]
    assert "ENOENT" not in errors, errors
    # a replace landing inside the kernel's single ESTALE retry surfaces
    # ESTALE to the application; nothing else is expected
    unexpected_errors = set(errors) - {"ESTALE"}
    assert not unexpected_errors, errors
