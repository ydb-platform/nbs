import os

from retrying import retry

from cloud.storage.core.tools.testing.qemu.lib.common import SshToGuest

RETRY_COUNT = 3
WAIT_TIMEOUT = 1000  # 1sec


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def create_dir(ssh: SshToGuest, dir: str):
    return ssh(f"mkdir {dir}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def set_default_acl(ssh: SshToGuest, dir: str):
    return ssh(f"setfacl -d -m u::rwx,g::rwx,o::rwx {dir}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def delete_all_acl(ssh: SshToGuest, file: str):
    return ssh(f"setfacl -b {file}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def get_acl(ssh: SshToGuest, file: str):
    return ssh(f"getfacl {file}")


def create_text_file(ssh: SshToGuest, file: str):
    return ssh(f"echo 'just text' > {file}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def set_xattr(ssh: SshToGuest, file: str, xattr_name: str, xattr_value: str):
    return str(ssh(f"xattr -w {xattr_name} {xattr_value} {file}").stdout)


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def list_xattr(ssh: SshToGuest, file: str):
    return ssh(f"xattr -l {file}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def delete_xattr(ssh: SshToGuest, file: str, xattr_name: str):
    return ssh(f"xattr -d {xattr_name} {file}")


def get_str(out):
    return str(out.stdout, 'utf-8')


# test setting and deleting defualt ACL for a directory
def check_dir_default_acl(ssh: SshToGuest, dir_name: str):
    create_dir(ssh, dir_name)
    set_default_acl(ssh, dir_name)
    out = get_str(get_acl(ssh, dir_name))
    assert out.find("default:user::rwx") != -1
    assert out.find("default:group::rwx") != -1
    assert out.find("default:other::rwx") != -1

    delete_all_acl(ssh, dir_name)
    out = get_str(get_acl(ssh, dir_name))
    assert out.find("default:user::rwx") == -1
    assert out.find("default:group::rwx") == -1
    assert out.find("default:other::rwx") == -1


# test setting, changing, deleting xattrs for a file
def check_xattrs(ssh: SshToGuest, file: str):
    # initially there are no xattrs
    xattrs = get_str(list_xattr(ssh, file))
    assert len(xattrs) == 0

    # set a couple of attributes
    xattr_names = ["user.xattr_0", "user.xattr_1"]
    xattr_values = ["xattr_0", "xattr_1"]
    set_xattr(ssh, file, xattr_names[0], xattr_values[0])
    set_xattr(ssh, file, xattr_names[1], xattr_values[1])
    out = get_str(list_xattr(ssh, file))
    assert out.find(f"{xattr_names[0]}: {xattr_values[0]}") != -1
    assert out.find(f"{xattr_names[1]}: {xattr_values[1]}") != -1

    # delete the first one
    delete_xattr(ssh, file, xattr_names[0])
    out = get_str(list_xattr(ssh, file))
    assert out.find(f"{xattr_names[0]}: {xattr_values[0]}") == -1
    assert out.find(f"{xattr_names[1]}: {xattr_values[1]}") != -1

    # change value
    set_xattr(ssh, file, xattr_names[1], xattr_values[0])
    out = get_str(list_xattr(ssh, file))
    assert out.find(f"{xattr_names[1]}: {xattr_values[0]}") != -1

    # and finally delete everything
    delete_xattr(ssh, file, xattr_names[1])
    out = get_str(list_xattr(ssh, file))
    assert len(out) == 0


def test_xattrs():
    port = int(os.getenv("QEMU_FORWARDING_PORT"))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")

    ssh = SshToGuest(user="qemu", port=port, key=ssh_key)

    # check that we can set and delete default acl for a directory
    dir_name = mount_dir + "/test"
    check_dir_default_acl(ssh, dir_name)

    # check that we can set and delete extended attributes of a file
    file_name = mount_dir + "/a.txt"
    create_text_file(ssh, file_name)
    check_xattrs(ssh, file_name)

    # check that we can set and delete extended attributes of a directory
    check_xattrs(ssh, dir_name)
