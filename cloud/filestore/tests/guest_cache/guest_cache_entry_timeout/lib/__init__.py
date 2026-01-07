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


def test_guest_cache_enty_timeout(expected_dir_stat_count: int,
                                  expected_file_stat_count: int):
    port = int(os.getenv("QEMU_FORWARDING_PORT"))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")

    ssh = SshToGuest(user="qemu", port=port, key=ssh_key)

    mkdir(ssh, f"{mount_dir}/dirname")
    create_file(ssh, mount_dir, "dirname/filename")

    # Sleep for a while to ensure that the profile log is flushed
    # before we start analyzing it
    # The default value of ProfileLogTimeThreshold for tests is 100ms
    time.sleep(2)

    # Collect counters after setup
    initial_dir_stat_count = get_vhost_request_count_for_node("dirname")
    initial_file_stat_count = get_vhost_request_count_for_node("filename")

    stat(ssh, mount_dir, "dirname/filename")

    time.sleep(2)

    # Count GetNodeAttr requests executed for the directory and the file
    final_dir_stat_count = get_vhost_request_count_for_node("dirname")
    final_file_stat_count = get_vhost_request_count_for_node("filename")

    # In both tests EntryTimeout is set to 15 seconds
    # final_dir_stat_count must be 0 for both tests
    assert final_dir_stat_count - initial_dir_stat_count == expected_dir_stat_count

    # expected values for final_file_stat_count:
    # 0 if RegularFileEntryTimeout is not set
    # 2 if RegularFileEntryTimeout is set to 1 second
    assert final_file_stat_count - initial_file_stat_count == expected_file_stat_count
