import os
import time
import re

from retrying import retry

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
def read_file(ssh: SshToGuest, dir: str, file_name: str):
    return ssh(f"sudo cat {dir}/{file_name}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def stat(ssh: SshToGuest, dir: str, file_name: str):
    return ssh(f"sudo stat {dir}/{file_name}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def list(ssh: SshToGuest, dir: str, file_name: str):
    return ssh(f"sudo ls {dir}/{file_name}")


def analyze_profile_log_for_name(profile_tool_bin_path, profile_log_path, node_name):
    proc = common.execute(
        [profile_tool_bin_path, "dumpevents",
         "--profile-log", profile_log_path])

    name_pattern = f"node_name={node_name}"

    type_dict = {}
    for line in proc.stdout.decode('utf-8').splitlines():
        request_type = re.split(r'\t+', line.rstrip())[2]
        if name_pattern in line:
            type_dict[request_type] = type_dict.get(request_type, 0) + 1

    return type_dict


def test():
    profile_tool_bin_path = common.binary_path(
        "cloud/filestore/tools/analytics/profile_tool/filestore-profile-tool"
    )

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
    initial_foo = analyze_profile_log_for_name(
        profile_tool_bin_path, common.output_path("vhost-profile.log"), "foo"
    ).get("GetNodeAttr", 0)

    initial_bar = analyze_profile_log_for_name(
        profile_tool_bin_path, common.output_path("vhost-profile.log"), "bar"
    ).get("GetNodeAttr", 0)

    # stat "foo/bar" for 60 times with 1 sec delay
    for i in range(60):
        stat(ssh, mount_dir, "foo/bar")
        time.sleep(1)

    # Sleep for a while to ensure that the profile log is flushed
    time.sleep(2)

    # Collect counters for "foo" and "foo/bar"
    final_foo = analyze_profile_log_for_name(
        profile_tool_bin_path, common.output_path("vhost-profile.log"), "foo"
    ).get("GetNodeAttr", 0)

    final_bar = analyze_profile_log_for_name(
        profile_tool_bin_path, common.output_path("vhost-profile.log"), "bar"
    ).get("GetNodeAttr", 0)

    # Expect nearly 4 GetNodeAttr for the "foo" directory. every 15 seconds
    assert final_foo - initial_foo >= 3
    assert final_foo - initial_foo <= 5

    # Expect 60 GetNodeAttr for "bar" regular file. Ensure cache was not used
    assert final_bar - initial_bar == 60
