import yatest.common as common

import subprocess
import os


def test_patch():
    patcher = common.binary_path(
        "cloud/blockstore/tools/cms/patcher/blockstore-patcher")

    data = common.source_path("cloud/blockstore/tools/cms/patcher/tests/data")
    cluster_config = os.path.join(data, "cluster_config.json")
    patch = os.path.join(data, "patch.json")

    results_path = common.output_path() + "/results.txt"

    with open(results_path, "w") as out:
        result = subprocess.call(
            [
                patcher,
                "--cluster-config", cluster_config,
                "--patch", patch,
                "--commit-message", "test",
                "--test-data-dir", data,
                "--test-mode",
            ],
            stdout=out
        )

        assert result == 0

    ret = common.canonical_file(results_path, local=True)

    return ret
