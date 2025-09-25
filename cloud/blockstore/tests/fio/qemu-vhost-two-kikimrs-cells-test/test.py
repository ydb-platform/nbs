import pytest
import os

import cloud.storage.core.tools.testing.fio.lib as fio

from cloud.blockstore.tests.python.lib.test_base import get_nbs_device_path


KB = 1024
MB = 1024*KB

DEVICE_SIZE = 128*MB
TESTS = fio.generate_tests(size=DEVICE_SIZE, duration=60)


@pytest.mark.parametrize("name", TESTS.keys())
def test_fio(name):
    nbs_instance_count = int(os.getenv("NBS_INSTANCE_COUNT"))
    for index in range(nbs_instance_count):
        path = get_nbs_device_path(index=index)

        out = fio.run_test(path, TESTS[name])
        assert "errors: 0" == out
