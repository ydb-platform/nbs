import json
import logging
import os

import cloud.filestore.tools.testing.profile_log.common as profile
import yatest.common as common

from cloud.filestore.tests.python.lib.client import FilestoreCliClient
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


def find_garbage():
    fs = os.getenv("NFS_FILESYSTEM")
    shards = [
        f"{fs}_shard_{i}"
        for i in range(int(os.getenv("FILESTORE_SHARD_COUNT")))
    ]
    client = FilestoreCliClient(
        common.binary_path("cloud/filestore/apps/client/filestore-client"),
        os.getenv("NFS_SERVER_PORT"),
        cwd=common.output_path())
    out = client.find_garbage(
        fs,
        shards,
        page_size=1024,
        find_in_shards=True,
        find_in_leader=True)
    # progress is printed as json objects, garbage as tab-separated rows
    return [line for line in out.decode("utf8").splitlines() if "\t" in line]


def test_atomic_replace():
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


def test_create_handle_racing_with_unlink():
    root = os.path.join(
        os.getenv("NFS_MOUNT_PATH"), "cross_client_create_handle")

    toggler = run(0, "toggler", root, wait=False)
    creator = run(1, "creator", root, wait=True)
    toggler.wait(timeout=TIMEOUT_SECONDS)
    flush_logs()

    summary = json.loads(creator.stdout.decode("utf8").splitlines()[-1])
    garbage = find_garbage()
    logging.info(f"creator: {summary}, garbage: {garbage}")

    assert summary["created"] > 0
    unexpected_errors = set(summary["errors"]) - {"ESTALE"}
    assert not unexpected_errors, summary["errors"]

    # a node created by the shard phase of CreateHandle after the leader's
    # nodeRef was unlinked is reachable only through the handle: the data
    # written into it is lost
    assert not garbage, garbage
