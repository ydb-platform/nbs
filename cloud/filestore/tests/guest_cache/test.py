import os

from retrying import retry

import cloud.filestore.tools.testing.profile_log.common as profile
import yatest.common as common

from cloud.storage.core.tools.testing.qemu.lib.common import SshToGuest

RETRY_COUNT = 3
WAIT_TIMEOUT = 1000  # 1sec


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def create_file(ssh: SshToGuest, dir: str, file_name: str):
    return ssh(f"sudo touch {dir}/{file_name}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def write_to_file(ssh: SshToGuest, dir: str, file_name: str, data: str):
    return ssh(f"sudo bash -c 'echo {data} >> {dir}/{file_name}'")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def open_read_open_read(ssh: SshToGuest, dir: str, file_name: str):
    return ssh(
        f"sudo bash -c 'exec 3< {dir}/{file_name} && cat <&3 && cat {dir}/{file_name}'"
    )


def test():
    port = int(os.getenv("QEMU_FORWARDING_PORT"))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")

    ssh = SshToGuest(user="qemu", port=port, key=ssh_key)

    create_file(ssh, mount_dir, "test.txt")
    write_to_file(ssh, mount_dir, "test.txt", "test_data")
    open_read_open_read(ssh, mount_dir, "test.txt")

    fs_name = "nfs_test"
    profile_tool_bin_path = common.binary_path(
        "cloud/filestore/tools/analytics/profile_tool/filestore-profile-tool"
    )

    result = profile.analyze_profile_log(
        profile_tool_bin_path, common.output_path("vhost-profile.log"), fs_name
    )

    # With KeepCacheAllowed the second read will be cached and should neither be
    # sent by the kernel nor observed by the vhost
    assert result.get("ReadData", 0) == 1
