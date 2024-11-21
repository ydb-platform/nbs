from cloud.filestore.tests.python.lib.common import get_filestore_mount_path
import os
import pytest
import shutil
import tempfile


def pytest_addoption(parser):
    parser.addoption(
        "--target-dir",
        action="store",
        default="Path to target directory to run tests on",
    )


@pytest.fixture
def target_dir_path(pytestconfig):
    tmp_dir = None
    try:
        tmp_dir = tempfile.mkdtemp(dir=get_filestore_mount_path())
        yield tmp_dir
    finally:
        if tmp_dir is not None:
            shutil.rmtree(tmp_dir, ignore_errors=True)


def lock_file_descriptor(target_dir_path: str, flags: int):
    flags |= os.O_CREAT
    suffix = 'read' if flags & os.O_RDONLY else 'write'
    lock_file_path = os.path.join(target_dir_path, f'test_lockfile_{suffix}')
    if not os.path.exists(lock_file_path):
        os.mknod(lock_file_path)
    fd = os.open(lock_file_path, flags)
    assert fd > 0
    try:
        yield fd
    finally:
        os.close(fd)


@pytest.fixture()
def read_lock_file_descriptor(target_dir_path):
    yield from lock_file_descriptor(target_dir_path, os.O_RDONLY)


@pytest.fixture()
def read_lock_file_descriptor_second(target_dir_path):
    yield from lock_file_descriptor(target_dir_path, os.O_RDONLY)


@pytest.fixture()
def write_lock_file_descriptor(target_dir_path):
    yield from lock_file_descriptor(target_dir_path, os.O_WRONLY)
