import pytest
import subprocess

import yatest.common as common


@pytest.mark.parametrize("test_script", ["default.sh"])
@pytest.mark.parametrize("test_type", ["virtiofs"])
@pytest.mark.parametrize("cluster", ["cluster1"])
def test_xfs_test_suite(test_script, test_type, cluster):
    binary = common.binary_path("cloud/filestore/tests/xfs_suite/yc-nfs-ci-xfs-test-suite")

    results_path = "%s/%s_%s_results.txt" % (common.output_path(), test_type, cluster)

    with open(results_path, "w") as out:
        result = subprocess.call(
            [
                binary,
                "--cluster", cluster,
                "--zone-id", "zone1",
                "--test-type", test_type,
                "--test-device", "nfs-test",
                "--test-dir", "/mnt/test",
                "--scratch-type", test_type,
                "--scratch-device", "nfs-scratch",
                "--scratch-dir", "/mnt/scratch",
                "--script-name", test_script,
                "--dry-run",
                "--no-generate-ycp-config",
                "--ycp-requests-template-path", "/does/not/matter",
                "--cluster-config-path",
                f"{common.source_path()}/cloud/filestore/tests/xfs_suite/tests/test-configs"
            ],
            stdout=out
        )

        assert result == 0

    ret = common.canonical_file(results_path, local=True)

    return ret
