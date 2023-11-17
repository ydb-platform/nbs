import pytest
import subprocess

import yatest.common as common


@pytest.mark.parametrize("test_suite", ["512bytes-bs", "64MB-bs", "ranges-intersection"])
@pytest.mark.parametrize("cluster", ["cluster1"])
@pytest.mark.parametrize("service", ["nfs", "nbs"])
def test_corruption_test_suite(test_suite, cluster, service):
    binary = common.binary_path(
        "cloud/blockstore/tools/ci/corruption_test_suite/yc-nbs-ci-corruption-test-suite")

    results_path = "%s/%s_results.txt" % (common.output_path(), test_suite)
    results_dir = "%s/%s_results.dir" % (common.output_path(), test_suite)

    with open(results_path, "w") as out:
        result = subprocess.call(
            [
                binary,
                "--dry-run",
                "--teamcity",
                "--cluster", cluster,
                "--test-suite", test_suite,
                "--service", service,
                "--verify-test-path", "verify/test/path",
                "--no-generate-ycp-config",
                "--ycp-requests-template-path", "/does/not/matter",
                "--results-path", results_dir,
                "--cluster-config-path",
                f"{common.source_path()}/cloud/blockstore/tools/ci/corruption_test_suite/tests/test-configs"
            ],
            stdout=out
        )

        assert result == 0

    ret = common.canonical_file(results_path)

    return ret
