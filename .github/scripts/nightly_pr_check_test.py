import pytest

from scripts.nightly_pr_check import get_summary_component, get_summary_label


@pytest.mark.parametrize("variant", ["", "variants/nebius-x86-64-release-asan/"])
def test_summary_component_accepts_legacy_and_variant_paths(variant):
    uri = (
        "s3://reports/reports/example/repo/nightly-asan.yaml/123/1/"
        f"{variant}cloud/blockstore/2/summary.json"
    )
    assert get_summary_component(uri) == "cloud/blockstore"
    assert (
        get_summary_label("nightly-asan.yaml", uri) == "release-asan/cloud/blockstore"
    )


def test_summary_without_a_component_keeps_workflow_label():
    uri = (
        "s3://reports/reports/example/repo/nightly-asan.yaml/123/1/"
        "variants/nebius-x86-64-release-asan/1/summary.json"
    )
    assert get_summary_component(uri) == ""
    assert get_summary_label("nightly-asan.yaml", uri) == "release-asan"
