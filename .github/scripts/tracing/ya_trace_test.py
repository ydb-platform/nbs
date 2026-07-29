from __future__ import annotations

import json
from pathlib import Path

from scripts.tracing.ya_trace import YaTraceCollection, YaTraceInputs


def test_trace_collection_indexes_tests_and_disambiguates_chunks(
    tmp_path: Path,
) -> None:
    suite = "cloud/example/tests"
    for result_folder in ("py3test", "flake8"):
        trace_path = (
            tmp_path / suite / "test-results" / result_folder / "ytest.report.trace"
        )
        trace_path.parent.mkdir(parents=True)
        events = [
            {
                "name": "subtest-finished",
                "value": {
                    "class": "pkg::Suite",
                    "subtest": "test",
                    "logs": {
                        "stdout": (
                            f"$(BUILD_ROOT)/{suite}/test-results/"
                            f"{result_folder}/stdout.log"
                        ),
                        "logsdir": (
                            f"$(BUILD_ROOT)/{suite}/test-results/"
                            f"{result_folder}/testing_out_stuff"
                        ),
                    },
                },
            },
            {
                "name": "chunk-event",
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "logs": {
                        "log": (
                            f"$(BUILD_ROOT)/{suite}/test-results/"
                            f"{result_folder}/run_test.log"
                        )
                    },
                },
            },
        ]
        trace_path.write_text("".join(json.dumps(event) + "\n" for event in events))

    traces = YaTraceCollection.load(tmp_path)
    test_event = traces.finished_test(suite, "pkg.Suite", "test")

    assert test_event is not None
    assert test_event.logs_directory == (
        f"{suite}/test-results/py3test/testing_out_stuff"
    )
    assert test_event.resolved_logs(tmp_path)["stdout"] == str(
        tmp_path / suite / "test-results" / "py3test" / "stdout.log"
    )
    assert traces.select_chunk_event(suite, 0, 1) is None

    chunk_event = traces.select_chunk_event(
        suite,
        0,
        1,
        f"failure in {tmp_path}/{suite}/test-results/flake8/run_test.log",
    )
    assert chunk_event is not None
    assert "flake8" in chunk_event.log_paths["log"]


def test_trace_inputs_produce_manifest_and_file_list(
    tmp_path: Path,
) -> None:
    ya_out = tmp_path / "out"
    trace_path = (
        ya_out
        / "cloud/example/tests"
        / "test-results"
        / "unittest"
        / "ytest.report.trace"
    )
    trace_path.parent.mkdir(parents=True)
    trace_path.write_text('{"name":"chunk-event","value":{}}\n')
    evlog_path = tmp_path / "ya_evlog.jsonl"
    evlog_path.write_text('{"namespace":"stages","event":"stage-finished"}\n')

    inputs = YaTraceInputs.discover(
        ya_out,
        evlog_path=evlog_path,
    )
    manifest = inputs.bundle_manifest({"github.run.id": "123"})

    assert manifest == {
        "schema": "nbs-ya-trace-input-bundle",
        "schema_version": 1,
        "evlog_file": "ya_evlog.jsonl",
        "ya_out_dir": "ya-out",
        "ya_trace_file_count": 1,
        "metadata": {"github.run.id": "123"},
    }
    assert inputs.tar_file_list() == (
        b"./cloud/example/tests/test-results/unittest/ytest.report.trace\0"
    )
    assert len(inputs.parse().traces) == 1


def test_trace_inputs_skip_symlinks(tmp_path: Path) -> None:
    ya_out = tmp_path / "out"
    trace_path = (
        ya_out
        / "cloud/example/tests"
        / "test-results"
        / "unittest"
        / "ytest.report.trace"
    )
    trace_path.parent.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.write_text("must not be archived")
    trace_path.symlink_to(outside)
    evlog_path = tmp_path / "ya_evlog.jsonl"
    evlog_path.symlink_to(outside)

    inputs = YaTraceInputs.discover(ya_out, evlog_path=evlog_path)
    manifest = inputs.bundle_manifest()

    assert manifest["evlog_file"] is None
    assert manifest["ya_trace_file_count"] == 0
    assert inputs.tar_file_list() == b""


def test_trace_inputs_tar_file_list_handles_unusual_names(
    tmp_path: Path,
) -> None:
    ya_out = tmp_path / "out"
    trace_path = (
        ya_out
        / "-suite\nwith-newline"
        / "test-results"
        / "unittest"
        / "ytest.report.trace"
    )
    trace_path.parent.mkdir(parents=True)
    trace_path.write_text('{"name":"chunk-event","value":{}}\n')

    inputs = YaTraceInputs.discover(ya_out)

    assert inputs.tar_file_list() == (
        b"./-suite\nwith-newline/test-results/unittest/ytest.report.trace\0"
    )
