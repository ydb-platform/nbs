import os
import random
import string

from cloud.storage.core.tools.testing.qemu.lib.common import env_with_guest_index, SshToGuest

TEST_FILE_PREFIX = "test"


def createFile(ssh: SshToGuest, dir: str, fileName: str):
    return ssh(f"sudo touch {dir}/{fileName}")


def writeToFile(ssh: SshToGuest, dir: str, fileName: str, data: str):
    return ssh(f"sudo bash -c 'echo {data} >> {dir}/{fileName}'")


def readFromFile(ssh: SshToGuest, dir: str, fileName: str):
    return ssh(f"sudo bash -c 'cat {dir}/{fileName}'")


def test():
    port1 = int(os.getenv(env_with_guest_index("QEMU_FORWARDING_PORT", 0)))
    port2 = int(os.getenv(env_with_guest_index("QEMU_FORWARDING_PORT", 1)))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")

    ssh1 = SshToGuest(user="qemu", port=port1, key=ssh_key)
    ssh2 = SshToGuest(user="qemu", port=port2, key=ssh_key)

    for i in range(3):
        filename = f"{TEST_FILE_PREFIX}_{i}.txt"
        res1 = createFile(ssh1, mount_dir, filename)
        assert 0 == res1.returncode

        expected = ''
        for j in range(10):
            randomStr = ''.join(
                random.choices(string.ascii_letters + string.digits, k=10))

            res1 = writeToFile(ssh1, mount_dir, filename, randomStr)
            assert 0 == res1.returncode

            expected += randomStr + '\n'

            res1 = readFromFile(ssh1, mount_dir, filename)
            res2 = readFromFile(ssh2, mount_dir, filename)

            assert 0 == res1.returncode
            assert 0 == res2.returncode
            assert expected == res1.stdout.decode('utf8')
            assert expected == res2.stdout.decode('utf8')
