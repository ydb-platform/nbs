import os
from argparse import Namespace
from pathlib import Path

from scripts.tracing.ya_trace_report import (
    _write_input_paths,
    build_resource_attributes,
)


def test_input_file_list_is_null_delimited_and_relative(tmp_path: Path) -> None:
    root = tmp_path / "out"
    paths = [
        root / "-suite\nname" / "test-results" / "ut" / "ytest.report.trace",
        root / "other" / "test-results" / "ut" / "ytest.report.trace",
    ]
    output = tmp_path / "paths"

    _write_input_paths(output, root, paths)

    assert output.read_bytes() == b"".join(
        os.fsencode(f"./{path.relative_to(root).as_posix()}") + b"\0" for path in paths
    )


def test_resource_attributes_are_built_locally() -> None:
    args = Namespace(
        operation="tests",
        component="cloud/tasks",
        build_preset="relwithdebinfo",
        build_target="",
        test_target="cloud/tasks/",
        test_type="unittest",
        test_size="small",
        retry=2,
        test_log_url_prefix="https://reports.example/logs/cloud/tasks/2",
        test_data_url_prefix="https://reports.example/test_data/cloud/tasks/2/workspace/",
    )

    resource = build_resource_attributes(args)

    assert resource["service.name"] == "nbs-ya-tests"
    assert resource["ci.component"] == "cloud/tasks"
    assert resource["ci.ya.retry"] == 2
    assert (
        resource["ci.artifact.test_log.url_prefix"]
        == "https://reports.example/logs/cloud/tasks/2/"
    )
    assert (
        resource["ci.artifact.test_data.url_prefix"]
        == "https://reports.example/test_data/cloud/tasks/2/workspace/"
    )


def test_unsafe_artifact_url_prefixes_are_not_stored() -> None:
    args = Namespace(
        operation="tests",
        component="cloud/tasks",
        build_preset="relwithdebinfo",
        build_target="",
        test_target="cloud/tasks/",
        test_type="unittest",
        test_size="small",
        retry=1,
        test_log_url_prefix="javascript:alert(1)",
        test_data_url_prefix="https://reports.example/data?token=secret",
    )

    resource = build_resource_attributes(args)

    assert "ci.artifact.test_log.url_prefix" not in resource
    assert "ci.artifact.test_data.url_prefix" not in resource
