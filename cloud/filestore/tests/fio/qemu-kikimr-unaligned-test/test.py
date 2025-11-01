import pytest

import cloud.storage.core.tools.testing.fio.lib as fio

from cloud.filestore.tests.python.lib.common import get_filestore_mount_path


TESTS = fio.generate_tests()

SIZES = [1 * fio.MB]

UNALIGNED_TESTS = fio.generate_tests(
    offset=100,
    sizes=SIZES,
    iodepths=[1],
    numjobs=[1],
    scenarios=['randrw'],
    duration=None,
    io_size=2048 * fio.MB)


@pytest.mark.parametrize("name", UNALIGNED_TESTS.keys())
def test_unaligned_fio(name):
    mount_dir = get_filestore_mount_path()
    file_name = fio.get_file_name(mount_dir, name)

    fio.run_test(file_name, UNALIGNED_TESTS[name], fail_on_errors=True)
