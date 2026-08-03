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
    )

    resource = build_resource_attributes(args)

    assert resource["service.name"] == "nbs-ya-tests"
    assert resource["ci.component"] == "cloud/tasks"
    assert resource["ci.ya.retry"] == 2
