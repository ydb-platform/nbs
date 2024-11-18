import pytest

import cloud.storage.core.tools.testing.fio.lib as fio

from cloud.filestore.tests.python.lib.common import get_filestore_mount_path


TESTS = fio.generate_tests()


@pytest.mark.parametrize("name", TESTS.keys())
def test_fio(name):
    mount_dir = get_filestore_mount_path()
    file_name = fio.get_file_name(mount_dir, name)

    fio.run_test(file_name, TESTS[name], fail_on_errors=True)
