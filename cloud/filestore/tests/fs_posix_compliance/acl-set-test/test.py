import os

from retrying import retry

from cloud.storage.core.tools.testing.qemu.lib.common import SshToGuest

RETRY_COUNT = 3
WAIT_TIMEOUT = 1000  # 1sec


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def create_dir(ssh: SshToGuest, parent_dir: str, dir: str):
    return ssh(f"sudo mkdir {parent_dir}/{dir}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def set_default_acl(ssh: SshToGuest, parent_dir: str, dir: str):
    return ssh(f"sudo setfacl -d -m u::rwx,g::rwx,o::rwx {parent_dir}/{dir}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def get_acl(ssh: SshToGuest, parent_dir: str, dir: str):
    return ssh(f"sudo getfacl {parent_dir}/{dir}")


def test():
    port = int(os.getenv("QEMU_FORWARDING_PORT"))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")

    ssh = SshToGuest(user="qemu", port=port, key=ssh_key)

    create_dir(ssh, mount_dir, "test")
    set_default_acl(ssh, mount_dir, "test")
    result = get_acl(ssh, mount_dir, "test")
    out = str(result.stdout)

    assert out.find("default:user::rwx") != -1 and out.find("default:group::rwx") != -1 and out.find("default:other::rwx") != -1
