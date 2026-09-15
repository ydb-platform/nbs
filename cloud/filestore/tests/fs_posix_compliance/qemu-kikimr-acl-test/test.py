import contextlib
import logging
import os
import stat
import subprocess
import tempfile

from cloud.filestore.tests.python.lib.common import get_filestore_mount_path

_logger = logging.getLogger("test")


@contextlib.contextmanager
def become_fs_owner(mount_path):
    # In production filestore-vhost runs as root, but in unit tests it runs as a
    # normal user. The user inside the QEMU VM may have a different uid/gid, which
    # causes permission denied on the mount directory.  We switch the effective
    # uid/gid to match the owner of the filesystem (i.e. the user running
    # filestore-vhost) to avoid such conflicts.
    st = os.stat(mount_path)
    uid = st.st_uid
    gid = st.st_gid
    _logger.info("mount path %s uid=%d gid=%d", mount_path, uid, gid)
    saved_euid = os.geteuid()
    saved_egid = os.getegid()
    os.setegid(gid)
    os.seteuid(uid)
    _logger.info("effective uid=%d gid=%d", os.geteuid(), os.getegid())
    try:
        yield
    finally:
        os.seteuid(saved_euid)
        os.setegid(saved_egid)
        _logger.info("restored effective uid=%d gid=%d", os.geteuid(), os.getegid())


def _read_acl(path):
    out = subprocess.check_output(
        ["getfacl", "--omit-header", "--numeric", path],
        text=True,
    )
    entries = {}
    for line in out.splitlines():
        # Strip the optional "#effective:..." annotation. The ACL entry itself
        # keeps its original permissions; the mask is asserted separately.
        entry = line.split("#", 1)[0].strip()
        if entry:
            key, permissions = entry.rsplit(":", 1)
            entries[key] = permissions
    return entries


def _assert_acl(path, expected):
    actual = _read_acl(path)
    _logger.info("ACL for %s: %s", path, actual)
    for entry, permissions in expected.items():
        assert actual.get(entry) == permissions, (
            f"{path}: ACL entry {entry!r} is {actual.get(entry)!r}, "
            f"expected {permissions!r}; complete ACL: {actual}"
        )


def _check_default_acl_inheritance(test_dir):
    _logger.info(f"_check_default_acl_inheritance in {test_dir}")

    # FUSE_DONT_MASK passes the original modes to the server. Without a
    # default ACL, the server must still apply the process umask.
    ordinary_file = os.path.join(test_dir, "ordinary_file")
    ordinary_dir = os.path.join(test_dir, "ordinary_dir")
    with open(ordinary_file, "w", opener=lambda p, f: os.open(p, f, 0o666)):
        pass
    os.mkdir(ordinary_dir, mode=0o777)
    assert stat.S_IMODE(os.stat(ordinary_file).st_mode) == 0o644
    assert stat.S_IMODE(os.stat(ordinary_dir).st_mode) == 0o755

    named_uid = 12345
    named_gid = 12346
    default_acl = {
        "default:user:": "rwx",
        f"default:user:{named_uid}": "r-x",
        "default:group:": "r--",
        f"default:group:{named_gid}": "-wx",
        "default:mask:": "rwx",
        "default:other:": "---",
    }
    subprocess.check_call(
        [
            "setfacl",
            "-m",
            ",".join(
                f"{entry}:{permissions}"
                for entry, permissions in default_acl.items()
            ),
            test_dir,
        ]
    )
    _assert_acl(test_dir, default_acl)

    new_file = os.path.join(test_dir, "new_file")
    new_dir = os.path.join(test_dir, "new_dir")
    with open(new_file, "w", opener=lambda p, f: os.open(p, f, 0o666)):
        pass
    os.mkdir(new_dir, mode=0o777)

    # The named entries retain their permissions, while the mask is restricted
    # by the requested create mode. A default ACL replaces normal umask
    # processing.
    file_access_acl = {
        "user:": "rw-",
        f"user:{named_uid}": "r-x",
        "group:": "r--",
        f"group:{named_gid}": "-wx",
        "mask:": "rw-",
        "other:": "---",
    }
    _assert_acl(new_file, file_access_acl)

    dir_access_acl = {
        "user:": "rwx",
        f"user:{named_uid}": "r-x",
        "group:": "r--",
        f"group:{named_gid}": "-wx",
        "mask:": "rwx",
        "other:": "---",
    }
    _assert_acl(new_dir, dir_access_acl)
    _assert_acl(new_dir, default_acl)

    # Creating another generation verifies that the inherited default ACL was
    # persisted on the first directory and can produce a new ChildAccessAcl.
    nested_file = os.path.join(new_dir, "nested_file")
    with open(nested_file, "w", opener=lambda p, f: os.open(p, f, 0o666)):
        pass
    _assert_acl(nested_file, file_access_acl)

    _logger.info(
        "ACL inheritance passed for named user %d and named group %d",
        named_uid,
        named_gid,
    )


def test():
    mount_path = get_filestore_mount_path()
    previous_umask = os.umask(0o022)
    try:
        with become_fs_owner(mount_path):
            with tempfile.TemporaryDirectory(dir=mount_path) as test_dir:
                _check_default_acl_inheritance(test_dir)
    finally:
        os.umask(previous_umask)
