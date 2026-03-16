import pytest

import cloud.storage.core.tools.testing.fio.lib as fio

from cloud.filestore.tests.python.lib.common import get_filestore_mount_path

# SCENARIOS = ["randrw"]
SCENARIOS = ["readwrite", "randrw"]

BLOCK_SIZE = 4096

SIZES = [
    1 * fio.MB,
    128 * fio.KB
]

UNALIGNED_SIZES = [
    1 * fio.MB,
    256 * fio.KB
]

DURATION = 60

UNALIGNED_TESTS = fio.generate_tests(
    offset=100,
    sizes=UNALIGNED_SIZES,
    iodepths=[12],
    numjobs=[4],
    scenarios=SCENARIOS,
    duration=DURATION,
)

ALIGNED_TESTS = fio.generate_tests(
    offset=0,
    sizes=SIZES,
    iodepths=[12],
    numjobs=[4],
    scenarios=SCENARIOS,
    duration=DURATION,
)


@pytest.mark.parametrize("name", UNALIGNED_TESTS.keys())
def test_fio_unaligned(name):
    mount_dir = get_filestore_mount_path()
    file_name = fio.get_file_name(mount_dir, name)

    fio.run_test(file_name, UNALIGNED_TESTS[name], fail_on_errors=True)


@pytest.mark.parametrize("name", ALIGNED_TESTS.keys())
def test_fio_aligned(name):
    mount_dir = get_filestore_mount_path()
    file_name = fio.get_file_name(mount_dir, name)

    fio.run_test(file_name, ALIGNED_TESTS[name], fail_on_errors=True)
