import os
import re
import yatest.common as common

from cloud.filestore.tests.python.lib.client import FilestoreCliClient
from cloud.filestore.tests.python.lib.loadtest import run_load_test


def test_max_file_count():
    filesystem_id = "max-file-count"
    results_path = common.output_path("results.txt")
    config_path = common.source_path(
        "cloud/filestore/tests/loadtest/max-file-count-test/max-file-count.txt"
    )
    client = FilestoreCliClient(
        common.binary_path("cloud/filestore/apps/client/filestore-client"),
        os.getenv("NFS_SERVER_PORT"),
        cwd=common.output_path())

    try:
        run_load_test(
            filesystem_id,
            config_path,
            os.getenv("NFS_SERVER_PORT"),
        )

        files = client.find(filesystem_id, depth=1).decode().splitlines()
        normalized_entries = [
            re.sub(
                r"(?<=max-file-count-loadtest:)[0-9a-f-]+$",
                "<random_guid>",
                file.rsplit("\t", 1)[0],
            )
            for file in files
        ]

        with open(results_path, "w") as results:
            results.write("\n".join(normalized_entries))

    finally:
        client.destroy(filesystem_id)

    return common.canonical_file(results_path, local=True)
