from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import scripts.tracing.ya_trace as ya_trace_module
from scripts.tracing.otlp import (
    ResourceAttributes,
    Ns,
    decode_attributes,
    span_duration_ns,
    span_status_code,
)
from scripts.tracing.ya_trace import (
    YaEvlog,
    YaEvlogRecord,
    YaTraceCollection,
    YaTraceInputs,
    load_ya_evlog,
    load_ya_traces,
)
from scripts.tracing.ya_trace_report import build_ya_trace


def _attributes(span) -> dict:
    return decode_attributes(span.attributes)


def _write_jsonl(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(event) + "\n" for event in events))


def _render_ya_trace(
    tmp_path: Path,
    events: list[dict],
    *,
    evlog_events: list[dict] | None = None,
    root_start_s: float = 0,
    root_end_s: float = 20,
    exit_code: int = 0,
):
    ya_out = tmp_path / "out"
    _write_jsonl(
        ya_out / "suite" / "test-results" / "unittest" / "ytest.report.trace",
        events,
    )
    evlog = None
    if evlog_events is not None:
        evlog_path = tmp_path / "ya_evlog.jsonl"
        _write_jsonl(evlog_path, evlog_events)
        evlog = load_ya_evlog(evlog_path)
    return build_ya_trace(
        load_ya_traces(ya_out),
        root_start_ns=Ns(round(root_start_s * 1_000_000_000)),
        root_end_ns=Ns(round(root_end_s * 1_000_000_000)),
        exit_code=exit_code,
        resource=ResourceAttributes(),
        evlog=evlog,
    )


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


def test_trace_inputs_exclude_files_older_than_attempt(
    tmp_path: Path,
) -> None:
    ya_out = tmp_path / "out"
    old_trace = (
        ya_out / "old-suite" / "test-results" / "unittest" / "ytest.report.trace"
    )
    current_trace = (
        ya_out / "current-suite" / "test-results" / "unittest" / "ytest.report.trace"
    )
    for trace_path in (old_trace, current_trace):
        trace_path.parent.mkdir(parents=True)
        trace_path.write_text('{"name":"chunk-event","value":{}}\n')

    os.utime(old_trace, (100, 100))
    os.utime(current_trace, (200, 200))

    inputs = YaTraceInputs.discover(ya_out, modified_since=150)

    assert inputs.trace_paths == [current_trace]


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


def test_finish_only_test_clamps_end_before_applying_duration(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "chunk-event",
                "timestamp": 110.9,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "metrics": {
                        "suite_start_timestamp": 100,
                        "suite_finish_timestamp": 110,
                    },
                },
            },
            {
                "name": "subtest-finished",
                "timestamp": 110.9,
                "value": {
                    "class": "Suite",
                    "subtest": "finish_only",
                    "status": "good",
                    "time": 0.5,
                    "chunk_index": 0,
                    "nchunks": 1,
                },
            },
        ],
        root_start_s=99,
        root_end_s=112,
    )

    test = next(trace.spans("ya.test"))
    assert test.start_time_unix_nano == 109_500_000_000
    assert test.end_time_unix_nano == 110_000_000_000
    assert span_duration_ns(test) == 500_000_000


def test_chunk_event_alias_and_real_error_schema_mark_chunk_failed(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "chunk_event",
                "timestamp": 5,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "errors": [["timeout", "test wrapper timed out"]],
                },
            }
        ],
        root_end_s=10,
        exit_code=1,
    )

    chunk = next(trace.spans("ya.chunk"))
    assert span_status_code(chunk) == 2
    assert _attributes(chunk)["ya.chunk.error.count"] == 1
    assert _attributes(chunk)["ya.chunk.error.statuses"] == ["timeout"]
    assert _attributes(chunk)["ya.chunk.error.messages"] == ["test wrapper timed out"]


def test_suite_error_has_localized_span_without_inventing_chunk(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "suite-event",
                "timestamp": 5,
                "value": {
                    "errors": [["fail", "recipe setup failed"]],
                },
            }
        ],
        root_end_s=10,
        exit_code=1,
    )

    assert not list(trace.spans("ya.chunk"))
    suite = next(trace.spans("ya.suite"))
    assert suite.start_time_unix_nano == 5_000_000_000
    assert suite.end_time_unix_nano == 5_000_000_000
    assert span_status_code(suite) == 2
    assert _attributes(suite)["ya.suite.error.count"] == 1
    assert _attributes(suite)["ya.suite.error.messages"] == ["recipe setup failed"]


@pytest.mark.parametrize(
    "status",
    ["good", "skipped", "xfail", "xpass", "deselected"],
)
def test_container_info_is_preserved_without_marking_span_failed(
    tmp_path: Path,
    status: str,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "chunk-event",
                "timestamp": 5,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "errors": [[status, "diagnostic information"]],
                },
            }
        ],
        root_end_s=10,
    )

    chunk = next(trace.spans("ya.chunk"))
    assert span_status_code(chunk) == 0
    assert _attributes(chunk)["ya.chunk.error.messages"] == ["diagnostic information"]


def test_invented_chunk_status_field_is_ignored(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "chunk-event",
                "timestamp": 5,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "status": "fail",
                },
            }
        ],
        root_end_s=10,
    )

    chunk = next(trace.spans("ya.chunk"))
    assert span_status_code(chunk) == 0
    assert "ya.chunk.status" not in _attributes(chunk)


def test_chunk_errors_take_priority_over_large_metric_sets(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "chunk-event",
                "timestamp": 5,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "errors": [["timeout", "wrapper timed out"]],
                    "metrics": {f"metric_{index}": index for index in range(256)},
                },
            }
        ],
        root_end_s=10,
        exit_code=1,
    )

    chunk = next(trace.spans("ya.chunk"))
    assert span_status_code(chunk) == 2
    assert _attributes(chunk)["ya.chunk.error.messages"] == ["wrapper timed out"]


@pytest.mark.parametrize("status", ["missing", "flaky", "diff"])
def test_yatool_failure_status_marks_test_and_chunk_failed(
    tmp_path: Path,
    status: str,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "subtest-finished",
                "timestamp": 5,
                "value": {
                    "class": "Suite",
                    "subtest": status,
                    "status": status,
                    "time": 1,
                },
            },
            {
                "name": "chunk-event",
                "timestamp": 6,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                },
            },
        ],
        root_end_s=10,
        exit_code=1,
    )

    assert span_status_code(next(trace.spans("ya.test"))) == 2
    assert span_status_code(next(trace.spans("ya.chunk"))) == 2


def test_worker_operations_do_not_claim_cache_hit_or_miss(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            {
                "namespace": "stages",
                "event": "stage-finished",
                "value": {
                    "name": "dispatch_build",
                    "tag": "dispatch_build",
                    "time": [1, 10],
                },
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "FromCache(uid$(BUILD_ROOT)/result.o)",
                    "tag": "restore[CC]",
                    "time": [2, 3],
                },
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "Run(uid$(BUILD_ROOT)/result.o)",
                    "tag": "CC",
                    "time": [3, 5],
                },
            },
            {
                "namespace": "dump_debug",
                "event": "log",
                "value": {
                    "key": "stats",
                    "value": {
                        "cache_hit": {
                            "cache_hit": 75,
                            "run_tasks": 4,
                            "executed_tasks": 4,
                            "cached_tasks": 3,
                            "not_cached_tasks": 1,
                        }
                    },
                },
            },
        ],
        root_end_s=11,
    )

    build = next(trace.spans("ya.build"))
    build_attributes = _attributes(build)
    node_attributes = [_attributes(span) for span in trace.spans("ya.build.node")]
    assert all("ya.build.cache.hit" not in item for item in node_attributes)
    assert not any(
        key.startswith("ya.build.cache.worker_node.")
        or key.startswith("ya.build.cache.tool.")
        for key in build_attributes
    )
    assert build_attributes["ya.build.worker.tool.cc.cache_restore.count"] == 1
    assert build_attributes["ya.build.worker.tool.cc.execute.count"] == 1


def test_failed_node_record_marks_matching_build_span_failed(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            {
                "namespace": "stages",
                "event": "stage-finished",
                "value": {
                    "name": "dispatch_build",
                    "tag": "dispatch_build",
                    "time": [1, 10],
                },
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "Run(failed-uid$(BUILD_ROOT)/result.o)",
                    "tag": "CC",
                    "time": [2, 5],
                },
            },
            {
                "namespace": "devtools.ya.build.reports.failed_node_info",
                "event": "node-failed",
                "value": {
                    "uid": "failed-uid",
                    "exit_code": 42,
                },
            },
        ],
        root_end_s=11,
        exit_code=1,
    )

    node = next(trace.spans("ya.build.node"))
    build = next(trace.spans("ya.build"))
    assert span_status_code(node) == 2
    assert span_status_code(build) == 2
    assert _attributes(node)["process.exit.code"] == 42
    assert _attributes(node)["ya.build.failed"] is True


def test_outputless_failed_node_is_joined_by_uid(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            {
                "namespace": "stages",
                "event": "stage-finished",
                "value": {
                    "name": "dispatch_build",
                    "tag": "dispatch_build",
                    "time": [1, 10],
                },
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "Run(failed-uid)",
                    "tag": "CC",
                    "time": [2, 5],
                },
            },
            {
                "namespace": "devtools.ya.build.reports.failed_node_info",
                "event": "node-failed",
                "value": {
                    "uid": "failed-uid",
                    "exit_code": 17,
                },
            },
        ],
        root_end_s=11,
        exit_code=1,
    )

    node = next(trace.spans("ya.build.node"))
    assert span_status_code(node) == 2
    assert _attributes(node)["process.exit.code"] == 17


def test_failed_node_is_protected_from_span_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ya_trace_module, "MAX_BUILD_NODE_SPANS", 1)
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            {
                "namespace": "stages",
                "event": "stage-finished",
                "value": {
                    "name": "dispatch_build",
                    "tag": "dispatch_build",
                    "time": [1, 10],
                },
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "Run(failed$(BUILD_ROOT)/failed.o)",
                    "tag": "CC",
                    "time": [2, 2.1],
                },
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "Run(long$(BUILD_ROOT)/long.o)",
                    "tag": "CC",
                    "time": [3, 9],
                },
            },
            {
                "namespace": "devtools.ya.build.reports.failed_node_info",
                "event": "node-failed",
                "value": {
                    "uid": "failed",
                    "exit_code": 1,
                },
            },
        ],
        root_end_s=11,
        exit_code=1,
    )

    node = next(trace.spans("ya.build.node"))
    assert _attributes(node)["ya.build.outputs"] == ["failed.o"]
    assert span_status_code(node) == 2


def test_failed_node_count_uses_unique_uids(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            {
                "namespace": "stages",
                "event": "stage-finished",
                "value": {
                    "name": "dispatch_build",
                    "tag": "dispatch_build",
                    "time": [1, 10],
                },
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "FromCache(failed$(BUILD_ROOT)/cached.o)",
                    "tag": "restore[CC]",
                    "time": [2, 3],
                },
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "Run(failed$(BUILD_ROOT)/built.o)",
                    "tag": "CC",
                    "time": [3, 5],
                },
            },
            {
                "namespace": "devtools.ya.build.reports.failed_node_info",
                "event": "node-failed",
                "value": {
                    "uid": "failed",
                    "exit_code": 1,
                },
            },
        ],
        root_end_s=11,
        exit_code=1,
    )

    build = next(trace.spans("ya.build"))
    assert _attributes(build)["ya.build.failed_node.count"] == 1
    assert (
        sum(span_status_code(node) == 2 for node in trace.spans("ya.build.node")) == 1
    )


@pytest.mark.parametrize(
    "task_type",
    ["TL", "TL-CACHED", "TS-DYN_UID_CACHE", "TM-CACHED-DYN_UID_CACHE"],
)
def test_all_yatool_test_types_are_recognized_on_critical_path(
    task_type: str,
) -> None:
    assert YaEvlog._is_test_critical_path_entry({"type": task_type})


def test_critical_path_keeps_all_entries_from_evlog(tmp_path: Path) -> None:
    evlog_path = tmp_path / "ya_evlog.jsonl"
    critical_path = [
        {
            "type": "CC",
            "uid": f"uid-{index}",
            "elapsed": 1,
            "start_ts": index,
            "end_ts": index + 1,
        }
        for index in range(129)
    ]
    critical_path.append(
        {
            "type": "TL",
            "uid": "terminal-test",
            "elapsed": 1,
            "start_ts": 129,
            "end_ts": 130,
        }
    )
    _write_jsonl(
        evlog_path,
        [
            {
                "namespace": "dump_debug",
                "event": "log",
                "value": {
                    "key": "stats",
                    "value": {
                        "critical_path": critical_path,
                    },
                },
            }
        ],
    )

    assert len(load_ya_evlog(evlog_path).statistics["critical_path"]) == 130


def test_critical_path_entry_after_128_is_rendered(tmp_path: Path) -> None:
    critical_path = [
        {
            "type": "CC",
            "uid": f"unmatched-{index}",
            "elapsed": 1,
            "start_ts": 0,
            "end_ts": 1,
        }
        for index in range(129)
    ]
    critical_path.append(
        {
            "type": "CC",
            "uid": "terminal-build",
            "elapsed": 1_000,
            "start_ts": 2_000,
            "end_ts": 3_000,
        }
    )
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            {
                "namespace": "stages",
                "event": "stage-finished",
                "value": {
                    "name": "dispatch_build",
                    "tag": "dispatch_build",
                    "time": [1, 5],
                },
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "Run(terminal-build$(BUILD_ROOT)/terminal.o)",
                    "tag": "CC",
                    "time": [2, 3],
                },
            },
            {
                "namespace": "dump_debug",
                "event": "log",
                "value": {
                    "key": "stats",
                    "value": {
                        "critical_path": critical_path,
                    },
                },
            },
        ],
        root_end_s=6,
    )

    node_attributes = _attributes(next(trace.spans("ya.build.node")))
    assert node_attributes["ya.build.critical_path"] is True
    assert node_attributes["ya.build.critical_path.index"] == 129


def test_critical_path_preserves_indices_and_matches_duplicate_uids(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            {
                "namespace": "stages",
                "event": "stage-finished",
                "value": {
                    "name": "dispatch_build",
                    "tag": "dispatch_build",
                    "time": [0, 10],
                },
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "Run(shared$(BUILD_ROOT)/first.o)",
                    "tag": "CC",
                    "time": [1, 2],
                },
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "Run(shared$(BUILD_ROOT)/second.o)",
                    "tag": "LD",
                    "time": [3, 5],
                },
            },
            {
                "namespace": "dump_debug",
                "event": "log",
                "value": {
                    "key": "stats",
                    "value": {
                        "critical_path": [
                            {
                                "type": "TM",
                                "uid": "test",
                                "elapsed": 1_000,
                                "start_ts": 0,
                                "end_ts": 1_000,
                            },
                            {
                                "type": "CC",
                                "uid": "shared",
                                "elapsed": 1_000,
                                "start_ts": 1_000,
                                "end_ts": 2_000,
                            },
                            {
                                "type": "LD",
                                "uid": "shared",
                                "elapsed": 2_000,
                                "start_ts": 3_000,
                                "end_ts": 5_000,
                            },
                        ]
                    },
                },
            },
        ],
        root_end_s=11,
    )

    nodes = {
        _attributes(span)["ya.build.outputs"][0]: _attributes(span)
        for span in trace.spans("ya.build.node")
    }
    assert nodes["first.o"]["ya.build.critical_path.index"] == 1
    assert nodes["first.o"]["ya.build.critical_path.reported_seconds"] == 1
    assert nodes["second.o"]["ya.build.critical_path.index"] == 2
    assert nodes["second.o"]["ya.build.critical_path.reported_seconds"] == 2


def test_critical_path_node_is_protected_from_span_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ya_trace_module, "MAX_BUILD_NODE_SPANS", 1)
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            {
                "namespace": "stages",
                "event": "stage-finished",
                "value": {
                    "name": "dispatch_build",
                    "tag": "dispatch_build",
                    "time": [0, 10],
                },
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "Run(critical$(BUILD_ROOT)/critical.o)",
                    "tag": "CC",
                    "time": [1, 1.1],
                },
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "Run(long$(BUILD_ROOT)/long.o)",
                    "tag": "CC",
                    "time": [2, 8],
                },
            },
            {
                "namespace": "dump_debug",
                "event": "log",
                "value": {
                    "key": "stats",
                    "value": {
                        "critical_path": [
                            {
                                "type": "CC",
                                "uid": "critical",
                                "elapsed": 100,
                                "start_ts": 1_000,
                                "end_ts": 1_100,
                            }
                        ]
                    },
                },
            },
        ],
        root_end_s=11,
    )

    node = next(trace.spans("ya.build.node"))
    assert _attributes(node)["ya.build.outputs"] == ["critical.o"]
    assert _attributes(node)["ya.build.critical_path"] is True


def test_one_critical_entry_marks_only_one_matching_worker_operation(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            {
                "namespace": "stages",
                "event": "stage-finished",
                "value": {
                    "name": "dispatch_build",
                    "tag": "dispatch_build",
                    "time": [0, 10],
                },
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "FromCache(shared$(BUILD_ROOT)/cached.o)",
                    "tag": "restore[CC]",
                    "time": [1, 2],
                },
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "Run(shared$(BUILD_ROOT)/built.o)",
                    "tag": "CC",
                    "time": [3, 5],
                },
            },
            {
                "namespace": "dump_debug",
                "event": "log",
                "value": {
                    "key": "stats",
                    "value": {
                        "critical_path": [
                            {
                                "type": "CC",
                                "uid": "shared",
                                "elapsed": 2_000,
                                "start_ts": 3_000,
                                "end_ts": 5_000,
                            }
                        ]
                    },
                },
            },
        ],
        root_end_s=11,
    )

    critical_nodes = [
        span
        for span in trace.spans("ya.build.node")
        if _attributes(span).get("ya.build.critical_path")
    ]
    assert len(critical_nodes) == 1
    assert _attributes(critical_nodes[0])["ya.build.outputs"] == ["built.o"]


def test_critical_entry_without_uid_matches_worker_by_time_and_type(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            {
                "namespace": "stages",
                "event": "stage-finished",
                "value": {
                    "name": "dispatch_build",
                    "tag": "dispatch_build",
                    "time": [0, 10],
                },
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "Run(worker$(BUILD_ROOT)/built.o)",
                    "tag": "CC",
                    "time": [3, 5],
                },
            },
            {
                "namespace": "dump_debug",
                "event": "log",
                "value": {
                    "key": "stats",
                    "value": {
                        "critical_path": [
                            {
                                "type": "CC",
                                "elapsed": 2_000,
                                "start_ts": 3_000,
                                "end_ts": 5_000,
                            }
                        ]
                    },
                },
            },
        ],
        root_end_s=11,
    )

    node = next(trace.spans("ya.build.node"))
    assert _attributes(node)["ya.build.critical_path"] is True


def test_build_node_span_limit_is_hard_for_critical_nodes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ya_trace_module, "MAX_BUILD_NODE_SPANS", 1)
    evlog_events = [
        {
            "namespace": "stages",
            "event": "stage-finished",
            "value": {
                "name": "dispatch_build",
                "tag": "dispatch_build",
                "time": [0, 10],
            },
        }
    ]
    for index in range(2):
        evlog_events.append(
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": f"Run(uid-{index}$(BUILD_ROOT)/{index}.o)",
                    "tag": "CC",
                    "time": [index + 1, index + 2],
                },
            }
        )
    evlog_events.append(
        {
            "namespace": "dump_debug",
            "event": "log",
            "value": {
                "key": "stats",
                "value": {
                    "critical_path": [
                        {
                            "type": "CC",
                            "uid": f"uid-{index}",
                            "elapsed": 1_000,
                            "start_ts": (index + 1) * 1_000,
                            "end_ts": (index + 2) * 1_000,
                        }
                        for index in range(2)
                    ]
                },
            },
        }
    )

    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=evlog_events,
        root_end_s=11,
    )

    assert len(list(trace.spans("ya.build.node"))) == 1
    build_attributes = _attributes(next(trace.spans("ya.build")))
    assert build_attributes["ya.build.critical_path.node_spans.dropped"] == 1


def test_graph_statistics_are_attached_to_dispatch_not_build_envelope(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            {
                "namespace": "stages",
                "event": "stage-finished",
                "value": {
                    "name": "dispatch_build",
                    "tag": "dispatch_build",
                    "time": [1, 10],
                },
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "Run(build$(BUILD_ROOT)/build.o)",
                    "tag": "CC",
                    "time": [2, 5],
                },
            },
            {
                "namespace": "dump_debug",
                "event": "log",
                "value": {
                    "key": "stats",
                    "value": {
                        "cache_hit": {
                            "tests_tasks": 2,
                            "ok_tasks": 3,
                        },
                        "execution_stages_msec": {
                            "build_only": 3_000,
                            "tests_only": 2_000,
                            "tests_with_other": 1_000,
                        },
                        "task_execution_msec": 6_000,
                    },
                },
            },
        ],
        root_end_s=11,
    )

    dispatch = next(
        span
        for span in trace.spans("ya.phase")
        if _attributes(span)["ya.stage.name"] == "dispatch_build"
    )
    build = next(trace.spans("ya.build"))
    dispatch_attributes = _attributes(dispatch)
    build_attributes = _attributes(build)
    assert dispatch_attributes["ya.build.execution.total.seconds"] == 6
    assert dispatch_attributes["ya.build.execution.stage.tests_only.seconds"] == 2
    assert dispatch_attributes["ya.build.task.uncached_test.count"] == 2
    assert dispatch_attributes["ya.build.task.uncached_non_test.count"] == 3
    assert "ya.build.execution.total.seconds" not in build_attributes
    assert "ya.build.execution.stage.tests_only.seconds" not in build_attributes
    assert "ya.build.task.test.count" not in dispatch_attributes
    assert "ya.build.task.ok.count" not in dispatch_attributes
    assert "ya.build.task.test.count" not in build_attributes
    assert "ya.build.task.ok.count" not in build_attributes


@pytest.mark.parametrize(
    ("tag", "expected_kind"),
    [
        ("restore_from_dist_cache[CC]", "cache_restore"),
        ("put_in_dist_cache[CC]", "cache_store"),
    ],
)
def test_distributed_cache_wrapper_tags_are_classified(
    tag: str,
    expected_kind: str,
) -> None:
    record = YaEvlogRecord(
        name="Cache(uid$(BUILD_ROOT)/output.o)",
        tag=tag,
        start_ns=Ns(1),
        end_ns=Ns(2),
    )
    assert record.kind_and_tool == (expected_kind, "CC")
    assert record.cache_source == "distributed"


def test_incremental_snapshots_merge_chunks_and_tests(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "subtest-started",
                "timestamp": 2,
                "value": {
                    "class": "Suite",
                    "subtest": "test",
                    "chunk_index": 0,
                    "nchunks": 1,
                    "chunk_filename": "chunk-a",
                },
            },
            {
                "name": "subtest-finished",
                "timestamp": 3,
                "value": {
                    "class": "Suite",
                    "subtest": "test",
                    "status": "crashed",
                    "time": 1,
                    "chunk_index": 0,
                    "nchunks": 1,
                    "chunk_filename": "chunk-a",
                },
            },
            {
                "name": "chunk-event",
                "timestamp": 3,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "chunk_filename": "chunk-a",
                    "metrics": {"snapshot": 1},
                    "errors": [["timeout", "intermediate timeout"]],
                },
            },
            {
                "name": "subtest-finished",
                "timestamp": 4,
                "value": {
                    "class": "Suite",
                    "subtest": "test",
                    "status": "good",
                    "time": 2,
                    "chunk_index": 0,
                    "nchunks": 1,
                    "chunk_filename": "chunk-a",
                },
            },
            {
                "name": "subtest-finished",
                "timestamp": 5,
                "value": {
                    "class": "Suite",
                    "subtest": "test",
                    "status": "deselected",
                    "time": 0,
                    "chunk_index": 0,
                    "nchunks": 1,
                    "chunk_filename": "chunk-a",
                },
            },
            {
                "name": "chunk-event",
                "timestamp": 6,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "chunk_filename": "chunk-a",
                    "metrics": {"snapshot": 2},
                },
            },
        ],
        root_end_s=10,
    )

    chunks = list(trace.spans("ya.chunk"))
    tests = list(trace.spans("ya.test"))
    assert len(chunks) == 1
    assert len(tests) == 1
    assert _attributes(chunks[0])["ya.chunk.metric.snapshot"] == 2
    assert _attributes(chunks[0])["ya.chunk.error.messages"] == ["intermediate timeout"]
    assert span_status_code(chunks[0]) == 2
    assert tests[0].start_time_unix_nano == 2_000_000_000
    assert tests[0].end_time_unix_nano == 4_000_000_000
    assert span_status_code(tests[0]) == 1


def test_chunk_filename_is_part_of_chunk_identity(tmp_path: Path) -> None:
    events = []
    for index, filename in enumerate(("first.cpp", "second.cpp"), 1):
        events.extend(
            [
                {
                    "name": "subtest-finished",
                    "timestamp": index,
                    "value": {
                        "class": "Suite",
                        "subtest": filename,
                        "status": "good",
                        "time": 0.1,
                        "chunk_index": 0,
                        "nchunks": 1,
                        "chunk_filename": filename,
                    },
                },
                {
                    "name": "chunk-event",
                    "timestamp": index,
                    "value": {
                        "chunk_index": 0,
                        "nchunks": 1,
                        "chunk_filename": filename,
                    },
                },
            ]
        )

    trace = _render_ya_trace(tmp_path, events, root_end_s=10)

    chunks = list(trace.spans("ya.chunk"))
    assert len(chunks) == 2
    assert {_attributes(chunk)["ya.chunk.filename"] for chunk in chunks} == {
        "first.cpp",
        "second.cpp",
    }
    tests = list(trace.spans("ya.test"))
    chunk_ids = {
        _attributes(chunk)["ya.chunk.filename"]: chunk.span_id for chunk in chunks
    }
    assert {
        _attributes(test)["test.name"]: test.parent_span_id for test in tests
    } == chunk_ids


def test_test_identity_includes_type_and_path(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "subtest-finished",
                "timestamp": index + 1,
                "value": {
                    "class": "Suite",
                    "subtest": "same_name",
                    "status": "good",
                    "time": 0.1,
                    "type": test_type,
                    "path": path,
                },
            }
            for index, (test_type, path) in enumerate(
                (("unit", "first.cpp"), ("integration", "second.cpp"))
            )
        ]
        + [
            {
                "name": "chunk-event",
                "timestamp": 3,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                },
            }
        ],
        root_end_s=10,
    )

    tests = list(trace.spans("ya.test"))
    assert len(tests) == 2
    assert {
        (_attributes(test)["test.type"], _attributes(test)["test.path"])
        for test in tests
    } == {
        ("unit", "first.cpp"),
        ("integration", "second.cpp"),
    }


def test_final_deselected_result_does_not_reuse_started_timestamp(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "subtest-started",
                "timestamp": 2,
                "value": {
                    "class": "Suite",
                    "subtest": "deselected",
                },
            },
            {
                "name": "subtest-finished",
                "timestamp": 5,
                "value": {
                    "class": "Suite",
                    "subtest": "deselected",
                    "status": "deselected",
                    "time": 0,
                },
            },
            {
                "name": "chunk-event",
                "timestamp": 6,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "metrics": {
                        "suite_start_timestamp": 1,
                        "suite_finish_timestamp": 6,
                        "suite_delay_until_first_test_secs": 2,
                    },
                },
            },
        ],
        root_end_s=10,
    )

    test = next(trace.spans("ya.test"))
    assert test.start_time_unix_nano == 5_000_000_000
    assert test.end_time_unix_nano == 5_000_000_000


def test_first_test_delay_excludes_binary_startup_and_uses_test_metric_prefix(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "chunk-event",
                "timestamp": 110,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "metrics": {
                        "suite_start_timestamp": 100,
                        "suite_finish_timestamp": 110,
                        "suite_delay_until_first_test_secs": 2,
                        "suite_binary_startup_secs": 1.5,
                    },
                },
            },
            {
                "name": "subtest-finished",
                "timestamp": 110,
                "value": {
                    "class": "Suite",
                    "subtest": "test",
                    "status": "good",
                    "time": 1,
                    "metrics": {"elapsed_time": 1},
                },
            },
        ],
        root_start_s=99,
        root_end_s=111,
    )

    test = next(trace.spans("ya.test"))
    assert test.start_time_unix_nano == 100_500_000_000
    test_attributes = _attributes(test)
    assert test_attributes["ya.test.metric.elapsed_time"] == 1
    assert "ya.chunk.metric.elapsed_time" not in test_attributes


def test_chunk_uses_precise_event_timestamp_when_consistent_with_metrics(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "chunk-event",
                "timestamp": 110.75,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "metrics": {
                        "suite_start_timestamp": 100,
                        "suite_finish_timestamp": 110,
                        "wall_time": 10.25,
                    },
                },
            }
        ],
        root_start_s=99,
        root_end_s=112,
    )

    chunk = next(trace.spans("ya.chunk"))
    assert chunk.start_time_unix_nano == 100_500_000_000
    assert chunk.end_time_unix_nano == 110_750_000_000


def test_chunk_uses_feasible_precise_wall_time_when_event_is_late(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "chunk-event",
                "timestamp": 120,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "metrics": {
                        "suite_start_timestamp": 100,
                        "suite_finish_timestamp": 110,
                        "wall_time": 10.25,
                    },
                },
            }
        ],
        root_start_s=99,
        root_end_s=121,
    )

    chunk = next(trace.spans("ya.chunk"))
    assert chunk.start_time_unix_nano == 100_000_000_000
    assert chunk.end_time_unix_nano == 110_250_000_000


def test_suite_span_is_parented_to_dispatch_phase(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "suite-event",
                "timestamp": 5,
                "value": {
                    "errors": [["fail", "setup failed"]],
                },
            }
        ],
        evlog_events=[
            {
                "namespace": "stages",
                "event": "stage-finished",
                "value": {
                    "name": "dispatch_build",
                    "tag": "dispatch_build",
                    "time": [1, 10],
                },
            }
        ],
        root_end_s=11,
        exit_code=1,
    )

    dispatch = next(
        span
        for span in trace.spans("ya.phase")
        if _attributes(span)["ya.stage.name"] == "dispatch_build"
    )
    assert next(trace.spans("ya.suite")).parent_span_id == dispatch.span_id
