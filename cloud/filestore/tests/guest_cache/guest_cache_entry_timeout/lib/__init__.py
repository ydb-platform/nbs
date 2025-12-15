import os
import time

from retrying import retry

import cloud.filestore.tools.testing.profile_log.common as profile
import yatest.common as common

from cloud.storage.core.tools.testing.qemu.lib.common import SshToGuest

RETRY_COUNT = 3
WAIT_TIMEOUT = 1000  # 1sec


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def mkdir(ssh: SshToGuest, dir: str):
    return ssh(f"sudo mkdir {dir}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def create_file(ssh: SshToGuest, dir: str, file_name: str):
    return ssh(f"sudo touch {dir}/{file_name}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def stat(ssh: SshToGuest, dir: str, file_name: str):
    return ssh(f"sudo stat {dir}/{file_name}")


def get_vhost_request_count_for_node(node_name: str) -> int:
    profile_tool_bin_path = common.binary_path(
        "cloud/filestore/tools/analytics/profile_tool/filestore-profile-tool"
    )
    return profile.analyze_profile_log(
        profile_tool_bin_path,
        common.output_path("vhost-profile.log"),
        "nfs_test",
        node_name
    ).get("GetNodeAttr", 0)


def test_guest_cache_enty_timeout(expected_foo: int, expected_bar: int):
    port = int(os.getenv("QEMU_FORWARDING_PORT"))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")

    ssh = SshToGuest(user="qemu", port=port, key=ssh_key)

    mkdir(ssh, f"{mount_dir}/foo")
    create_file(ssh, mount_dir, "foo/bar")

    # Sleep for a while to ensure that the profile log is flushed
    # before we start analyzing it
    # The default value of ProfileLogTimeThreshold for tests is 100ms
    time.sleep(2)

    # Collect counters after setup
    initial_foo = get_vhost_request_count_for_node("foo")
    initial_bar = get_vhost_request_count_for_node("bar")

    stat(ssh, mount_dir, "foo/bar")
    stat(ssh, mount_dir, "foo/bar")

    time.sleep(2)

    stat(ssh, mount_dir, "foo/bar")
    stat(ssh, mount_dir, "foo/bar")

    # Sleep for a while to ensure that the profile log is flushed
    time.sleep(2)

    # Collect counters for "foo" and "foo/bar"
    final_foo = get_vhost_request_count_for_node("foo")
    final_bar = get_vhost_request_count_for_node("bar")

    # In both tests EntryTimeout is set to 15 seconds
    # So "foo" should be always cached
    assert final_foo - initial_foo == expected_foo

    # Expect 0 GetNodeAttr for "bar" if RegularFileEntryTimeout is not set
    # And 2 GetNodeAttr for "bar" if RegularFileEntryTimeout is set to 1 second
    assert final_bar - initial_bar == expected_bar
