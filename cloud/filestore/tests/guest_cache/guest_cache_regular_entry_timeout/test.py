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


def get_server_request_count_for_entry(enty_name: str) -> int:
    profile_tool_bin_path = common.binary_path(
        "cloud/filestore/tools/analytics/profile_tool/filestore-profile-tool"
    )
    return profile.analyze_profile_log(
        profile_tool_bin_path, common.output_path("vhost-profile.log"), "nfs_test", enty_name
    ).get("GetNodeAttr", 0)


def test():

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
    initial_foo = get_server_request_count_for_entry("foo")
    initial_bar = get_server_request_count_for_entry("bar")

    stat(ssh, mount_dir, "foo/bar")  # bar is not uncached
    stat(ssh, mount_dir, "foo/bar")  # bar hit cache
    time.sleep(2)
    stat(ssh, mount_dir, "foo/bar")  # bar is not uncached
    stat(ssh, mount_dir, "foo/bar")  # bar hit cache

    # Sleep for a while to ensure that the profile log is flushed
    time.sleep(2)

    # Collect counters for "foo" and "foo/bar"
    final_foo = get_server_request_count_for_entry("foo")
    final_bar = get_server_request_count_for_entry("bar")

    # Expect 0 GetNodeAttr for the "foo" directory.
    # All stats should hit the cache.
    assert final_foo - initial_foo == 0

    # Expect 2 GetNodeAttr for "bar" regular file.
    # #Second requesr in every pair should hit the cache.
    assert final_bar - initial_bar == 2
