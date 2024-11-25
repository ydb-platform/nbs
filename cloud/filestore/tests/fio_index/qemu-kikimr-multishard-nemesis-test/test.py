import pytest
import os

import cloud.storage.core.tools.testing.fio.lib as fio

from cloud.filestore.tests.python.lib.common import get_filestore_mount_path

duration=30
if os.environ['SANITIZER_TYPE'] != '' :
    duration=5

TESTS = fio.generate_index_tests(duration=duration)

@pytest.mark.parametrize("name", TESTS.keys())
def test_fio(name):
    mount_dir = get_filestore_mount_path()
    dir_name = fio.get_dir_name(mount_dir, name)

    fio.run_index_test(dir_name, TESTS[name], fail_on_errors=True)
