import os
import random
from retrying import retry
import string

import yatest.common as common

from cloud.storage.core.tools.testing.qemu.lib.common import env_with_guest_index, SshToGuest

TEST_FILE_PREFIX = "close_to_open_consistency_test"
RETRY_COUNT = 3
WAIT_TIMEOUT = 1000  # 1sec
STRACE_LOG_DIR = "strace_logs"


def strace_log_name(kind: str, i: int, vm_no: int, j: int):
    return f"{kind}_{i}_vm_{vm_no}_{j}.strace"


def run_under_strace(ssh: SshToGuest, command: str, log_name: str):
    # Traces the command in the guest into a guest-local file, then copies
    # that file into the test output dir. The ssh stderr is not used for the
    # trace since it also contains the ssh diagnostics. Returns the result of
    # the traced command itself, so its stdout and return code are intact.
    guest_log_path = f"/tmp/{log_name}"
    res = ssh(f"sudo strace -f -tt -o {guest_log_path} bash -c '{command}'")

    log = ssh(f"sudo cat {guest_log_path}; sudo rm -f {guest_log_path}")

    log_dir = common.output_path(STRACE_LOG_DIR)
    os.makedirs(log_dir, exist_ok=True)
    with open(os.path.join(log_dir, log_name), "wb") as f:
        f.write(log.stdout)

    return res


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def create_file(ssh: SshToGuest, dir: str, file_name: str):
    return ssh(f"sudo touch {dir}/{file_name}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def write_to_file(
        ssh: SshToGuest,
        dir: str,
        file_name: str,
        data: str,
        log_name: str):
    return run_under_strace(ssh, f"echo {data} >> {dir}/{file_name}", log_name)


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def read_from_file(ssh: SshToGuest, dir: str, file_name: str, log_name: str):
    return run_under_strace(ssh, f"cat {dir}/{file_name}", log_name)


def test():
    port1 = int(os.getenv(env_with_guest_index("QEMU_FORWARDING_PORT", 0)))
    port2 = int(os.getenv(env_with_guest_index("QEMU_FORWARDING_PORT", 1)))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")

    # VM numbers in the strace log names are the guest indices
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

            res1 = write_to_file(
                ssh1,
                mount_dir,
                file_name,
                random_str,
                strace_log_name("write", i, 0, j))
            assert 0 == res1.returncode

            expected += random_str + '\n'

            res1 = read_from_file(
                ssh1,
                mount_dir,
                file_name,
                strace_log_name("read", i, 0, j))
            res2 = read_from_file(
                ssh2,
                mount_dir,
                file_name,
                strace_log_name("read", i, 1, j))

            assert 0 == res1.returncode
            assert 0 == res2.returncode
            assert expected == res1.stdout.decode('utf8')
            assert expected == res2.stdout.decode('utf8')
