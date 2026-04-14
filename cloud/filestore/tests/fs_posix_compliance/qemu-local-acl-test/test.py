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


@contextlib.contextmanager
def _umask(mask):
    old = os.umask(mask)
    try:
        yield
    finally:
        os.umask(old)


def _acl_perm(path, entry):
    """Return the permission string for a named ACL entry
    (e.g. 'group', 'default:group')."""
    out = subprocess.check_output(["getfacl", "--omit-header", path], text=True)
    for line in out.splitlines():
        if line.startswith(entry + "::"):
            return line.split("::")[-1]
    return ""


def _check_umask_sanity(test_dir, umask_val):
    """Verify that mode bits on new files/dirs match the expected umask with no
    default ACL."""
    _logger.info("--- umask %04o: sanity check (no default ACL) ---", umask_val)

    with _umask(umask_val):
        new_file = os.path.join(test_dir, "ctrl_file")
        new_dir = os.path.join(test_dir, "ctrl_dir")
        with open(new_file, "w", opener=lambda p, f: os.open(p, f, 0o666)):
            pass
        os.mkdir(new_dir, mode=0o777)

        got_file = stat.S_IMODE(os.stat(new_file).st_mode)
        got_dir = stat.S_IMODE(os.stat(new_dir).st_mode)
        exp_file = 0o666 & ~umask_val
        exp_dir = 0o777 & ~umask_val

        _logger.info(
            "file mode %04o (expected %04o)  dir mode %04o (expected %04o)",
            got_file,
            exp_file,
            got_dir,
            exp_dir,
        )

        assert got_file == exp_file, (
            f"file mode {got_file:04o} != expected {exp_file:04o} for umask {umask_val:04o}"
        )
        assert got_dir == exp_dir, (
            f"dir mode {got_dir:04o} != expected {exp_dir:04o} for umask {umask_val:04o}"
        )

        _logger.info("PASS umask %04o sanity", umask_val)


def _check_default_acl_inheritance(test_dir, umask_val):
    _logger.info("--- umask %04o: applying default ACL to %s ---", umask_val, test_dir)

    with _umask(umask_val):
        subprocess.check_call(["setfacl", "-R", "-m", "g::rwX,d:g::rwX", test_dir])
        _logger.info(
            "parent default:group ACL: %s", _acl_perm(test_dir, "default:group")
        )

        new_file = os.path.join(test_dir, "new_file")
        new_dir = os.path.join(test_dir, "new_dir")
        with open(new_file, "w", opener=lambda p, f: os.open(p, f, 0o666)):
            pass
        os.mkdir(new_dir, mode=0o777)

        grp_file = _acl_perm(new_file, "group")
        grp_dir = _acl_perm(new_dir, "group")
        d_grp_dir = _acl_perm(new_dir, "default:group")
        _logger.info(
            "new_file group: %s  new_dir group: %s  new_dir default:group: %s",
            grp_file,
            grp_dir,
            d_grp_dir,
        )

        assert "w" in grp_file, (
            f"new file did not inherit g+w (umask {umask_val:04o}): got '{grp_file}'"
        )
        assert "w" in grp_dir, (
            f"new dir did not inherit g+w (umask {umask_val:04o}): got '{grp_dir}'"
        )
        assert "w" in d_grp_dir, (
            f"new dir missing default:group+w (umask {umask_val:04o}): got '{d_grp_dir}'"
        )

        _logger.info("PASS umask %04o", umask_val)


def test():
    mount_path = get_filestore_mount_path()
    with become_fs_owner(get_filestore_mount_path()):
        # Test with both 0022 (typical system default, strips g+w) and 0002
        # (group-collaborative).  umask 0022 is the interesting case: the kernel
        # applies the umask to the ACL mask of newly created entries, so g+w can
        # be silently suppressed even when default:group::rwx is correctly stored.
        for umask_val in (0o022, 0o002):
            with tempfile.TemporaryDirectory(dir=mount_path) as test_dir:
                _check_umask_sanity(test_dir, umask_val)
                _check_default_acl_inheritance(test_dir, umask_val)
