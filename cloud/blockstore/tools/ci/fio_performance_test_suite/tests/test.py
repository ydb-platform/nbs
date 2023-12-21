import pytest
import subprocess

import yatest.common as common


@pytest.mark.parametrize("test_suite", [
    "default",
    "max_iops",
    "max_bandwidth",
    "nrd",
    "nrd_iops",
    "nrd_throughput",
    "mirrored_iops",
    "mirrored_throughput",
    "large_block_size",
    "overlay_disk_max_count"])
@pytest.mark.parametrize(
    "cluster_and_platform",
    [
        ("cluster1", None),
        ("cluster1", "standard-v2"),
    ]
)
@pytest.mark.parametrize("service", ["nbs", "nfs"])
@pytest.mark.parametrize("in_parallel", [False, True])
def test_fio_performance_test_suite(test_suite, cluster_and_platform, service, in_parallel):
    binary = common.binary_path(
        "cloud/blockstore/tools/ci/fio_performance_test_suite/yc-nbs-ci-fio-performance-test-suite")

    results_path = f"{common.output_path()}/{test_suite}_results.txt"

    cluster = cluster_and_platform[0]
    platform = cluster_and_platform[1]

    with open(results_path, "w") as out:
        subprocess_args = [
            binary,
            "--dry-run",
            "--teamcity",
            "--ycp-requests-template-path", "/does/not/matter",
            "--service", service,
            "--cluster", cluster,
            "--test-suite", test_suite,
            "--no-generate-ycp-config",
            "--cluster-config-path", f"{common.source_path()}/cloud/blockstore/tools/ci/fio_performance_test_suite/tests/test-configs"
        ]
        if in_parallel:
            subprocess_args.append("--in-parallel")
        if platform is not None:
            subprocess_args += ["--platform-id", platform]

        result = subprocess.call(subprocess_args, stdout=out)

        if service == "nfs" and test_suite in [
            "nrd",
            "nrd_iops",
            "nrd_throughput",
            "mirrored_iops",
            "mirrored_throughput",
            "overlay_disk_max_count",
        ]:
            assert result != 0
        else:
            assert result == 0

    ret = common.canonical_file(results_path, local=True)

    return ret
