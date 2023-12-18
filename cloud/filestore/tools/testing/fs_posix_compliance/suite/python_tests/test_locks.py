import errno
import fcntl
import pytest
import struct


class FlockStruct:
    def __init__(self, l_type, l_whence=0, l_start=0, l_len=0, l_pid=0):
        self.l_type = l_type
        self.l_whence = l_whence
        self.l_start = l_start
        self.l_len = l_len
        self.l_pid = l_pid

    def pack(self):
        return struct.pack(
            "hhIllqq",
            self.l_type,
            self.l_whence,
            self.l_start,
            self.l_len,
            self.l_pid,
            0,
            0,
        )


def make_flock_pack(l_type, l_start=0, l_end=0):
    return FlockStruct(l_type, l_start, l_end).pack()


def test_flock_shared_on_read_fd(read_lock_file_descriptor):
    fcntl.flock(read_lock_file_descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
    fcntl.flock(read_lock_file_descriptor, fcntl.LOCK_UN | fcntl.LOCK_NB)


def test_flock_exclusive_on_read_fd(read_lock_file_descriptor):
    fcntl.flock(read_lock_file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    fcntl.flock(read_lock_file_descriptor, fcntl.LOCK_UN | fcntl.LOCK_NB)


def test_flock_upgrade_shared_to_exclusive_on_read_fd(read_lock_file_descriptor):
    fcntl.flock(read_lock_file_descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
    fcntl.flock(read_lock_file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    fcntl.flock(read_lock_file_descriptor, fcntl.LOCK_UN | fcntl.LOCK_NB)


def test_flock_downgrade_exclusive_to_shared_on_read_fd(read_lock_file_descriptor):
    fcntl.flock(read_lock_file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    fcntl.flock(read_lock_file_descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
    fcntl.flock(read_lock_file_descriptor, fcntl.LOCK_UN | fcntl.LOCK_NB)


def test_flock_shared_on_write_fd(write_lock_file_descriptor):
    fcntl.flock(write_lock_file_descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
    fcntl.flock(write_lock_file_descriptor, fcntl.LOCK_UN | fcntl.LOCK_NB)


def test_flock_exclusive_on_write_fd(write_lock_file_descriptor):
    fcntl.flock(write_lock_file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    fcntl.flock(write_lock_file_descriptor, fcntl.LOCK_UN | fcntl.LOCK_NB)


def test_flock_upgrade_shared_to_exclusive_on_write_fd(write_lock_file_descriptor):
    fcntl.flock(write_lock_file_descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
    fcntl.flock(write_lock_file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    fcntl.flock(write_lock_file_descriptor, fcntl.LOCK_UN | fcntl.LOCK_NB)


def test_flock_downgrade_exclusive_to_shared_on_write_fd(write_lock_file_descriptor):
    fcntl.flock(write_lock_file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    fcntl.flock(write_lock_file_descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
    fcntl.flock(write_lock_file_descriptor, fcntl.LOCK_UN | fcntl.LOCK_NB)


def test_fcntl_shared_lock_on_read_fd(read_lock_file_descriptor):
    fcntl.fcntl(
        read_lock_file_descriptor, fcntl.F_SETLK, make_flock_pack(fcntl.F_RDLCK)
    )
    fcntl.fcntl(
        read_lock_file_descriptor, fcntl.F_SETLK, make_flock_pack(fcntl.F_UNLCK)
    )


def test_fcntl_exclusive_lock_on_write_fd(write_lock_file_descriptor):
    fcntl.fcntl(
        write_lock_file_descriptor, fcntl.F_SETLK, make_flock_pack(fcntl.F_WRLCK)
    )
    fcntl.fcntl(
        write_lock_file_descriptor, fcntl.F_SETLK, make_flock_pack(fcntl.F_UNLCK)
    )


def test_fcntl_fail_exclusive_lock_on_read_fd(read_lock_file_descriptor):
    with pytest.raises(OSError) as e:
        fcntl.fcntl(
            read_lock_file_descriptor, fcntl.F_SETLK, make_flock_pack(fcntl.F_WRLCK)
        )
    assert e.value.errno == errno.EBADF


retcode = pytest.main()
