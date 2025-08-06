import pytest
import subprocess

import yatest.common as common


def get_binary_path():
    return common.binary_path(
        "cloud/blockstore/tools/ci/corruption_test_suite/yc-nbs-ci-corruption-test-suite")


def get_cluster_config_path():
    return (f"{common.source_path()}/"
            f"cloud/blockstore/tools/ci/corruption_test_suite/tests/test-configs")


def prepare_binary_args(
    results_dir,
    test_suite,
    cluster,
    service,
    compute_node=None,
    compute_nodes_list_path=None
):
    args = [
        get_binary_path(),
        "--dry-run",
        "--teamcity",
        "--cluster", cluster,
        "--test-suite", test_suite,
        "--service", service,
        "--verify-test-path", "verify/test/path",
        "--no-generate-ycp-config",
        "--ycp-requests-template-path", "/does/not/matter",
        "--results-path", results_dir,
        "--cluster-config-path", get_cluster_config_path()
    ]

    if compute_node is not None:
        args += ["--compute-node", compute_node]

    if compute_nodes_list_path is not None:
        args += ["--compute-nodes-list-path", compute_nodes_list_path]

    return args


@pytest.mark.parametrize("test_suite", ["512bytes-bs", "64MB-bs", "ranges-intersection"])
@pytest.mark.parametrize("cluster", ["cluster1"])
@pytest.mark.parametrize("service", ["nfs", "nbs"])
def test_corruption_test_suite(test_suite, cluster, service):
    results_path = "%s/%s_results.txt" % (common.output_path(), test_suite)
    results_dir = "%s/%s_results.dir" % (common.output_path(), test_suite)

    with open(results_path, "w") as out:
        result = subprocess.call(
            prepare_binary_args(results_dir, test_suite, cluster, service),
            stdout=out
        )

        assert result == 0

    ret = common.canonical_file(results_path, local=True)

    return ret


@pytest.mark.parametrize("compute_node", [None, "node.net"])
@pytest.mark.parametrize("compute_nodes_list_filename", [None, "nodes.txt"])
def test_corruption_test_suite_with_specified_compute_nodes(compute_node, compute_nodes_list_filename):
    results_path = "%s_results.txt" % (common.output_path())
    results_dir = "%s_results.dir" % (common.output_path())

    compute_nodes_list_path = None if compute_nodes_list_filename is None else \
        (f"{common.source_path()}/"
         f"cloud/blockstore/tools/ci/corruption_test_suite/tests/test-compute-nodes/"
         f"{compute_nodes_list_filename}")

    with open(results_path, "w") as out:
        result = subprocess.call(
            prepare_binary_args(
                results_dir,
                "512bytes-bs",
                "cluster1",
                "nbs",
                compute_node,
                compute_nodes_list_path),
            stdout=out
        )

        if compute_node is not None and compute_nodes_list_path is not None:
            assert result != 0
        else:
            assert result == 0

    ret = common.canonical_file(results_path, local=True)

    return ret
