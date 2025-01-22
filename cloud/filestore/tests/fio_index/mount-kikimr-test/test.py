import pytest

import cloud.storage.core.tools.testing.fio.lib as fio

from cloud.filestore.tests.python.lib.common import get_filestore_mount_path


TESTS = fio.generate_index_tests()


@pytest.mark.parametrize("name", TESTS.keys())
def test_fio(name):
    mount_dir = get_filestore_mount_path()
    dir_name = fio.get_dir_name(mount_dir, name)

    # TODO(#2831): remove this debug information
    fio.run_index_test(dir_name, TESTS[name], fail_on_errors=True, verbose=True)
    # assert False
