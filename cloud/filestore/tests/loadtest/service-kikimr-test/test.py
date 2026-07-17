import os
import pytest

from cloud.filestore.tests.python.lib.loadtest import run_load_test
from cloud.filestore.tests.python.lib.client import FilestoreCliClient

import yatest.common as common


class Case(object):

    def __init__(self, name, config_path):
        self.name = name
        self.config_path = config_path


TESTS = [
    Case(
        "create-rename",
        "cloud/filestore/tests/loadtest/service-kikimr-test/create-rename.txt",
    ),
    Case(
        "read-write",
        "cloud/filestore/tests/loadtest/service-kikimr-test/read-write.txt",
    ),
    Case(
        "read-write-unaligned",
        "cloud/filestore/tests/loadtest/service-kikimr-test/read-write-unaligned.txt",
    ),
    Case(
        "read-write-validation",
        "cloud/filestore/tests/loadtest/service-kikimr-test/read-write-validation.txt",
    ),
    Case(
        "intrahost-migration",
        "cloud/filestore/tests/loadtest/service-kikimr-test/intrahost-migration.txt",
    ),
    Case(
        "get-node-attr.txt",
        "cloud/filestore/tests/loadtest/service-kikimr-test/get-node-attr.txt",
    )
]


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_load(test_case):
    test_case.config_path = common.source_path(test_case.config_path)
    run_load_test(
        test_case.name,
        test_case.config_path,
        os.getenv("NFS_SERVER_PORT"),
    )

    return None


def test_max_file_count():
    filesystem_id = "max-file-count"
    config_path = common.source_path(
        "cloud/filestore/tests/loadtest/service-kikimr-test/max-file-count.txt"
    )

    try:
        run_load_test(
            filesystem_id,
            config_path,
            os.getenv("NFS_SERVER_PORT"),
        )

        client = FilestoreCliClient(
            common.binary_path("cloud/filestore/apps/client/filestore-client"),
            os.getenv("NFS_SERVER_PORT"),
            cwd=common.output_path(),
        )
        entries = client.find(filesystem_id, depth=1).decode().splitlines()
        entries = [entry for entry in entries if entry]

        assert len(entries) == 3
    finally:
        client = FilestoreCliClient(
            common.binary_path("cloud/filestore/apps/client/filestore-client"),
            os.getenv("NFS_SERVER_PORT"),
            cwd=common.output_path(),
            check_exit_code=False,
        )
        client.destroy(filesystem_id)
