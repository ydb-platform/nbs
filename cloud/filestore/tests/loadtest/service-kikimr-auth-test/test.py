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
        "index",
        "cloud/filestore/tests/loadtest/service-kikimr-auth-test/index.txt",
    ),
    Case(
        "read-write",
        "cloud/filestore/tests/loadtest/service-kikimr-auth-test/read-write.txt",
    ),
]


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_load(test_case):
    test_case.config_path = common.source_path(test_case.config_path)

    run_load_test(
        test_case.name,
        test_case.config_path,
        int(os.getenv("NFS_SERVER_SECURE_PORT")),
        auth=True,
        auth_token=os.getenv("ACCESS_SERVICE_TOKEN"),
    )

    return None
