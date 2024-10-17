import os
import pytest

from cloud.filestore.tests.python.lib.loadtest import run_load_test

import yatest.common as common


class Case(object):

    def __init__(self, name, config_path):
        self.name = name
        self.config_path = config_path


TESTS = [
    Case(
        "read-write-validation",
        "cloud/filestore/tests/loadtest/service-kikimr-emergency-test/read-write-validation.txt",
    ),
]


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_load(test_case):
    test_case.config_path = common.source_path(test_case.config_path)
    run_load_test(
        test_case.name,
        test_case.config_path,
        os.getenv("NFS_SERVER_PORT"),
        mon_port=os.getenv("NFS_MON_PORT"),
    )

    # TODO: destroy static BS group in order to emulate emergency
    return None
