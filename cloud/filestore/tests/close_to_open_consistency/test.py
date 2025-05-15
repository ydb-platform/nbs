import os
import random
from retrying import retry
import string

from cloud.storage.core.tools.testing.qemu.lib.common import env_with_guest_index, SshToGuest

TEST_FILE_PREFIX = "close_to_open_consistency_test"
RETRY_COUNT = 3
WAIT_TIMEOUT = 1000  # 1sec


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def create_file(ssh: SshToGuest, dir: str, file_name: str):
    return ssh(f"sudo touch {dir}/{file_name}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def write_to_file(ssh: SshToGuest, dir: str, file_name: str, data: str):
    return ssh(f"sudo bash -c 'echo {data} >> {dir}/{file_name}'")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def read_from_file(ssh: SshToGuest, dir: str, file_name: str):
    return ssh(f"sudo bash -c 'cat {dir}/{file_name}'")


def test():
    port1 = int(os.getenv(env_with_guest_index("QEMU_FORWARDING_PORT", 0)))
    port2 = int(os.getenv(env_with_guest_index("QEMU_FORWARDING_PORT", 1)))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")

    ssh1 = SshToGuest(user="qemu", port=port1, key=ssh_key)
    ssh2 = SshToGuest(user="qemu", port=port2, key=ssh_key)

    for i in range(3):
        file_name = f"{TEST_FILE_PREFIX}_{i}.txt"
        res1 = create_file(ssh1, mount_dir, file_name)
        assert 0 == res1.returncode

        expected = ''
        for j in range(10):
            random_str = ''.join(
                random.choices(string.ascii_letters + string.digits, k=10))

            res1 = write_to_file(ssh1, mount_dir, file_name, random_str)
            assert 0 == res1.returncode

            expected += random_str + '\n'

            res1 = read_from_file(ssh1, mount_dir, file_name)
            res2 = read_from_file(ssh2, mount_dir, file_name)

            assert 0 == res1.returncode
            assert 0 == res2.returncode
            assert expected == res1.stdout.decode('utf8')
            assert expected == res2.stdout.decode('utf8')
