import pytest

import cloud.storage.core.tools.testing.fio.lib as fio

from cloud.filestore.tests.python.lib.common import get_filestore_mount_path

SCENARIOS = ["randwrite", "randrw"]

UNALIGNED_TESTS = fio.generate_tests(
    offset=100,
    sizes=[1 * fio.MB],
    iodepths=[1],
    numjobs=[4],
    scenarios=SCENARIOS)

ALIGNED_TESTS = fio.generate_tests(
    offset=0,
    sizes=[1 * fio.MB],
    iodepths=[1],
    numjobs=[4],
    scenarios=SCENARIOS)


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
