import pytest

import cloud.storage.core.tools.testing.fio.lib as fio

from cloud.blockstore.tests.python.lib.test_base import get_nbs_device_path


TESTS = fio.generate_tests(verify=False)


@pytest.mark.parametrize("name", TESTS.keys())
def test_fio(name):
    device_path = get_nbs_device_path()

    out = fio.run_test(device_path, TESTS[name])
    assert "errors: 0" == out
