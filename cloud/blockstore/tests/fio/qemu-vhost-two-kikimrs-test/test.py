import pytest
import os
import logging

import cloud.storage.core.tools.testing.fio.lib as fio

from cloud.blockstore.tests.python.lib.test_base import get_all_nbs_paths, get_file_size


KB = 1024
MB = 1024*KB

logger = logging.getLogger(__name__)

DEVICE_SIZE = 128*MB
TESTS = fio.generate_tests(size=DEVICE_SIZE, duration=60)
nbs_instances_count = int(os.getenv("CLUSTERS_COUNT"))


@pytest.mark.parametrize("name", TESTS.keys())
def test_fio(name):
    device_paths = get_all_nbs_paths(nbs_instances_count)

    for path in device_paths:
        logger.info("device path: {}".format(path))
        device_size = get_file_size(path)
        if DEVICE_SIZE != device_size:
            raise RuntimeError("Invalid device size (expected: {}, actual: {})".format(
                DEVICE_SIZE,
                device_size))

        out = fio.run_test(path, TESTS[name])
        assert "errors: 0" == out
