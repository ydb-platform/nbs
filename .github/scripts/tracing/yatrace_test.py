from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

import scripts.tracing.ya_trace_report as ya_trace_report_module
import scripts.tracing.yatrace.limits as yatrace_limits
from scripts.tracing.otlp import (
    Interval,
    ResourceAttributes,
    Ns,
    decode_attributes,
    span_duration_ns,
    span_status_code,
)
from scripts.tracing.yatrace import (
    YaCriticalPathEntry,
    YaEvent,
    YaEvlog,
    YaEvlogRecord,
    YaTraceCollection,
    YaTraceInputs,
    load_ya_evlog,
)
from scripts.tracing.ya_trace_report import build_ya_trace


def _attributes(span) -> dict:
    return decode_attributes(span.attributes)


def _write_jsonl(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(event) + "\n" for event in events))


def _subtest(
    name: str,
    *,
    timestamp: float,
    chunk: tuple[int, int] | None = None,
    started: bool = False,
    status: str = "good",
    duration: float | None = None,
    logs: dict[str, str] | None = None,
) -> dict:
    value = {"class": "Suite", "subtest": name}
    if chunk is not None:
        value.update({"chunk_index": chunk[0], "nchunks": chunk[1]})
    if not started:
        value["status"] = status
    if duration is not None:
        value["time"] = duration
    if logs is not None:
        value["logs"] = logs
    return {
        "name": "subtest-started" if started else "subtest-finished",
        "timestamp": timestamp,
        "value": value,
    }


def _chunk(
    index: int,
    total: int,
    *,
    timestamp: float,
    metrics: dict[str, int | float] | None = None,
    logs: dict[str, str] | None = None,
) -> dict:
    return {
        "name": "chunk-event",
        "timestamp": timestamp,
        "value": {
            "chunk_index": index,
            "nchunks": total,
            "metrics": metrics or {},
            **({"logs": logs} if logs is not None else {}),
        },
    }


def _stage(name: str, start: float, end: float) -> dict:
    return {
        "namespace": "stages",
        "event": "stage-finished",
        "value": {"name": name, "tag": name, "time": [start, end]},
    }


def _worker(
    name: str,
    tag: str,
    start: float,
    end: float,
    *,
    thread_name: str = "",
) -> dict:
    event = {
        "namespace": "worker_threads",
        "event": "node-finished",
        "value": {"name": name, "tag": tag, "time": [start, end]},
    }
    if thread_name:
        event["thread_name"] = thread_name
    return event


def _worker_detail(
    tag: str,
    start: float,
    end: float,
    thread_name: str,
) -> dict:
    return {
        "thread_name": thread_name,
        "namespace": "worker_threads",
        "event": "node-detailed",
        "value": {"name": tag, "tag": tag, "time": [start, end]},
    }


def _run_worker(
    uid: str,
    output: str,
    start: float,
    end: float,
    thread_name: str = "",
) -> dict:
    return _worker(
        f"Run({uid}$(BUILD_ROOT)/{output})",
        "CC",
        start,
        end,
        thread_name=thread_name,
    )


def _node_started(thread_name: str, name: str) -> dict:
    return {
        "thread_name": thread_name,
        "namespace": "worker_threads",
        "event": "node-started",
        "value": {"name": name},
    }


def _statistics(value: dict) -> dict:
    return {
        "namespace": "dump_debug",
        "event": "log",
        "value": {"key": "stats", "value": value},
    }


def _failed_node(uid: str, exit_code: int) -> dict:
    return {
        "namespace": "devtools.ya.build.reports.failed_node_info",
        "event": "node-failed",
        "value": {"uid": uid, "exit_code": exit_code},
    }


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
        YaTraceCollection.load(ya_out).traces,
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


def test_log_attributes_keep_only_safe_build_root_relative_paths() -> None:
    event = YaEvent(
        name="subtest-finished",
        timestamp_ns=Ns(1),
        order=0,
        value={
            "logs": {
                "stdout": "$(BUILD_ROOT)/suite/output.log",
                "stderr": "$(BUILD_ROOT)/suite/../secret.log",
                "absolute": "$(BUILD_ROOT)//etc/passwd",
                "nul": "$(BUILD_ROOT)/suite/bad\x00name",
                "runner": "/home/runner/output.log",
            }
        },
    )

    assert event.log_attributes("ya.test") == {
        "ya.test.log.stdout.path": "suite/output.log"
    }


def test_raw_test_class_names_do_not_collapse_into_one_test(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "subtest-finished",
                "timestamp": 5,
                "value": {
                    "class": test_class,
                    "subtest": "same-name",
                    "status": "good",
                    "time": 1,
                },
            }
            for test_class in ("A::B", "A.B")
        ],
        root_end_s=10,
    )

    tests = list(trace.spans("ya.test"))
    assert len(tests) == 2
    assert {_attributes(test)["test.suite"] for test in tests} == {"A::B", "A.B"}


def test_test_operations_group_enriches_chunks_and_renders_inferred_stages(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            _subtest(
                "case",
                timestamp=8,
                duration=4,
                logs={
                    "stdout": (
                        "$(BUILD_ROOT)/suite/test-results/unittest/"
                        "testing_out_stuff/case.out"
                    ),
                    "logsdir": (
                        "$(BUILD_ROOT)/suite/test-results/unittest/" "testing_out_stuff"
                    ),
                },
            ),
            _chunk(
                0,
                1,
                timestamp=10,
                metrics={
                    "suite_start_timestamp": 2,
                    "suite_finish_timestamp": 10,
                    "suite_initial_(seconds)": 1,
                    "suite_prepare_recipes_(seconds)": 2,
                    "suite_wrapper_execution_(seconds)": 4,
                    "suite_stop_recipes_(seconds)": 1,
                },
                logs={
                    "log": "$(BUILD_ROOT)/suite/test-results/unittest/run_test.log",
                    "logsdir": (
                        "$(BUILD_ROOT)/suite/test-results/unittest/" "testing_out_stuff"
                    ),
                },
            ),
        ],
        evlog_events=[
            _stage("dispatch_build", 1, 11),
            _worker(
                "Run(testuid$(BUILD_ROOT)/suite/test-results/unittest/meta.json)",
                "TM",
                2,
                10,
                thread_name="Worker-1",
            ),
        ],
        root_end_s=12,
    )

    dispatch = next(trace.spans("ya.phase"))
    operations = next(trace.spans("ya.test.operations"))
    worker = next(trace.spans("ya.test.worker"))
    chunk = next(trace.spans("ya.chunk"))
    test = next(trace.spans("ya.test"))
    stages = list(trace.spans("ya.test.stage"))

    assert operations.parent_span_id == dispatch.span_id
    assert worker.parent_span_id == operations.span_id
    assert chunk.parent_span_id == worker.span_id
    assert span_duration_ns(operations) == Ns.from_s(8)
    assert _attributes(operations) == {
        "ya.test.wall_time.kind": "worker-node-execution-envelope",
        "ya.test.worker.execute.count": 1,
        "ya.test.worker.unmatched.count": 0,
        "ya.test.worker.cumulative_seconds": 8,
        "ya.test.chunk.count": 1,
    }
    assert _attributes(chunk)["test.size"] == "medium"
    assert _attributes(chunk)["ya.test.worker.uid"] == "testuid"
    assert _attributes(chunk)["ya.test.worker.thread"] == "Worker-1"
    assert _attributes(test)["test.size"] == "medium"
    assert _attributes(test)["ya.test.log.stdout.path"] == (
        "suite/test-results/unittest/testing_out_stuff/case.out"
    )
    assert _attributes(test)["ya.test.logs_directory.path"] == (
        "suite/test-results/unittest/testing_out_stuff"
    )
    assert _attributes(chunk)["ya.chunk.log.log.path"] == (
        "suite/test-results/unittest/run_test.log"
    )
    assert _attributes(chunk)["ya.chunk.logs_directory.path"] == (
        "suite/test-results/unittest/testing_out_stuff"
    )

    assert [stage.name for stage in stages] == [
        "test stage: initial",
        "test stage: prepare recipes",
        "test stage: wrapper execution",
        "test stage: stop recipes",
    ]
    assert [span_duration_ns(stage) for stage in stages] == [
        Ns.from_s(1),
        Ns.from_s(2),
        Ns.from_s(4),
        Ns.from_s(1),
    ]
    assert all(stage.parent_span_id == chunk.span_id for stage in stages)
    assert all(_attributes(stage)["test.timing.inferred"] is True for stage in stages)


def test_unmatched_failed_test_worker_is_preserved(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 1, 8),
            _worker(
                "Run(missing$(BUILD_ROOT)/suite/test-results/unittest/meta.json)",
                "TS",
                2,
                7,
                thread_name="Worker-1",
            ),
            _worker_detail("setup", 2, 3, "Worker-1"),
            _worker_detail("exec_cmd", 3, 7, "Worker-1"),
            _failed_node("missing", 17),
        ],
        root_end_s=9,
        exit_code=1,
    )

    operations = next(trace.spans("ya.test.operations"))
    node = next(trace.spans("ya.test.node"))
    assert node.parent_span_id == operations.span_id
    assert span_status_code(node) == 2
    assert _attributes(node)["test.size"] == "small"
    assert _attributes(node)["ya.test.worker.uid"] == "missing"
    assert _attributes(node)["process.exit.code"] == 17
    assert _attributes(operations)["ya.test.worker.unmatched.count"] == 1
    phases = list(trace.spans("ya.test.worker.phase"))
    assert [phase.name for phase in phases] == [
        "worker phase: setup",
        "worker phase: exec command",
    ]
    assert all(phase.parent_span_id == node.span_id for phase in phases)


def test_exact_test_worker_phases_are_children_of_the_matching_chunk(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            _chunk(
                0,
                1,
                timestamp=8.5,
                metrics={
                    "suite_start_timestamp": 1.5,
                    "suite_finish_timestamp": 8.5,
                },
            )
        ],
        evlog_events=[
            _stage("dispatch_build", 1, 9),
            _worker(
                "Run(testuid$(BUILD_ROOT)/suite/test-results/unittest/meta.json)",
                "TS",
                2,
                8,
                thread_name="Worker-1",
            ),
            _worker_detail("setup", 2, 2.5, "Worker-1"),
            _worker_detail("exec_cmd", 2.5, 7, "Worker-1"),
            _worker_detail("post_cmd", 7, 7.2, "Worker-1"),
            _worker_detail("node_result", 7.2, 7.5, "Worker-1"),
            _worker_detail("finalize", 7.5, 8, "Worker-1"),
        ],
        root_end_s=10,
    )

    chunk = next(trace.spans("ya.chunk"))
    worker = next(trace.spans("ya.test.worker"))
    phases = list(trace.spans("ya.test.worker.phase"))
    assert [phase.name for phase in phases] == [
        "worker phase: setup",
        "worker phase: exec command",
        "worker phase: post command",
        "worker phase: node result",
        "worker phase: finalize",
    ]
    assert chunk.parent_span_id == worker.span_id
    assert worker.start_time_unix_nano == Ns.from_s(1.5)
    assert worker.end_time_unix_nano == Ns.from_s(8.5)
    assert _attributes(worker)["ya.test.worker.reported_seconds"] == 6
    assert _attributes(worker)["ya.test.worker.timeline.adjusted"] is True
    assert all(phase.parent_span_id == worker.span_id for phase in phases)
    assert [_attributes(phase)["ya.test.worker.phase"] for phase in phases] == [
        "setup",
        "exec_cmd",
        "post_cmd",
        "node_result",
        "finalize",
    ]
    assert all(
        _attributes(phase)["ya.test.worker.timing.source"] == "ya-evlog"
        for phase in phases
    )


def test_test_result_processing_nodes_are_preserved_as_test_operations(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 1, 9),
            _worker(
                "Run(aggregate$(BUILD_ROOT)/suite/test-results/unittest/meta.json)",
                "TA",
                2,
                4,
            ),
            _worker(
                "Run(merge$(BUILD_ROOT)/suite/test-results/unittest/meta.json)",
                "TR",
                4,
                6,
            ),
            _worker(
                "Result(materialize$(BUILD_ROOT)/suite/test-results/unittest/meta.json)",
                "result[TM]",
                6,
                7,
            ),
            _worker(
                "Put(store$(BUILD_ROOT)/suite/test-results/unittest/meta.json)",
                "put_in_cache[TM]",
                7,
                8,
            ),
        ],
        root_end_s=10,
    )

    operations = next(trace.spans("ya.test.operations"))
    nodes = list(trace.spans("ya.test.node"))
    assert span_duration_ns(operations) == Ns.from_s(6)
    assert [_attributes(node)["ya.test.graph_node.kind"] for node in nodes] == [
        "test_aggregate",
        "test_merge",
        "test_materialize",
        "test_cache_store",
    ]
    assert [node.name for node in nodes] == [
        "test results aggregation: suite [unittest]",
        "test results merge: suite [unittest]",
        "test result materialization: suite [unittest]",
        "test result cache store: suite [unittest]",
    ]
    assert all(_attributes(node)["test.suite"] == "suite" for node in nodes)
    assert all(
        _attributes(node)["ya.test_results.folder"] == "unittest" for node in nodes
    )
    assert all(node.parent_span_id == operations.span_id for node in nodes)
    assert not any(trace.spans("ya.build"))
    assert _attributes(operations)["ya.test.graph_node.aggregate.count"] == 1
    assert _attributes(operations)["ya.test.graph_node.merge.count"] == 1
    assert _attributes(operations)["ya.test.graph_node.materialize.count"] == 1
    assert _attributes(operations)["ya.test.graph_node.cache_store.count"] == 1


def test_ten_longest_completed_tests_are_ranked_per_ya_trace(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            *[
                _subtest(
                    f"case-{duration}",
                    timestamp=20,
                    duration=duration,
                )
                for duration in range(1, 13)
            ],
            _subtest(
                "not-launched",
                timestamp=20,
                duration=100,
                status="not_launched",
            ),
            _chunk(
                0,
                1,
                timestamp=21,
                metrics={
                    "suite_start_timestamp": 0,
                    "suite_finish_timestamp": 21,
                },
            ),
        ],
        root_end_s=22,
        exit_code=1,
    )

    tests = {span.name: _attributes(span) for span in trace.spans("ya.test")}
    assert tests["Suite::case-12"]["ya.test.duration.rank"] == 1
    assert tests["Suite::case-3"]["ya.test.duration.rank"] == 10
    assert "ya.test.duration.rank" not in tests["Suite::case-2"]
    assert "ya.test.duration.rank" not in tests["Suite::case-1"]
    assert "ya.test.duration.rank" not in tests["Suite::not-launched"]


def test_finished_test_prefers_raw_class_then_uses_unique_normalized_fallback(
    tmp_path: Path,
) -> None:
    ya_out = tmp_path / "out"
    suite = "cloud/example/tests"
    _write_jsonl(
        ya_out / suite / "test-results" / "unittest" / "ytest.report.trace",
        [
            {
                "name": "subtest-finished",
                "value": {
                    "class": test_class,
                    "subtest": "same-name",
                    "marker": marker,
                },
            }
            for test_class, marker in (
                ("A.B", "dot"),
                ("A::B", "colon"),
                ("C::D", "fallback"),
                ("E::F.G", "ambiguous-one"),
                ("E.F::G", "ambiguous-two"),
            )
        ],
    )
    traces = YaTraceCollection.load(ya_out)

    exact = traces.finished_test(suite, "A.B", "same-name")
    fallback = traces.finished_test(suite, "C.D", "same-name")
    ambiguous = traces.finished_test(suite, "E.F.G", "same-name")
    assert exact is not None and exact.value["marker"] == "dot"
    assert fallback is not None and fallback.value["marker"] == "fallback"
    assert ambiguous is None


def test_yatool_nul_padding_is_accepted(tmp_path: Path) -> None:
    ya_out = tmp_path / "out"
    trace_path = ya_out / "suite" / "test-results" / "unittest" / "ytest.report.trace"
    trace_path.parent.mkdir(parents=True)
    event = {
        "name": "subtest-finished",
        "timestamp": 5,
        "value": {
            "class": "Suite",
            "subtest": "nul-padded",
            "status": "good",
            "time": 1,
        },
    }
    trace_path.write_bytes(b"\0 " + json.dumps(event).encode() + b"\0\r\n")

    trace = build_ya_trace(
        YaTraceCollection.load(ya_out).traces,
        root_start_ns=Ns(0),
        root_end_ns=Ns.from_s(10),
        exit_code=0,
        resource=ResourceAttributes(),
    )

    assert [span.name for span in trace.spans("ya.test")] == ["Suite::nul-padded"]


def test_malformed_json_is_counted_and_marks_input_incomplete(
    tmp_path: Path,
) -> None:
    ya_out = tmp_path / "out"
    trace_path = ya_out / "suite" / "test-results" / "unittest" / "ytest.report.trace"
    trace_path.parent.mkdir(parents=True)
    trace_path.write_text(
        '{"name":"subtest-finished","timestamp":5,'
        '"value":{"class":"Suite","subtest":"valid","status":"good","time":1}}\n'
        '{"name":"subtest-finished",broken-json}\n'
    )

    trace = build_ya_trace(
        YaTraceCollection.load(ya_out).traces,
        root_start_ns=Ns(0),
        root_end_ns=Ns.from_s(10),
        exit_code=0,
        resource=ResourceAttributes(),
    )

    assert [span.name for span in trace.spans("ya.test")] == ["Suite::valid"]
    attributes = _attributes(next(trace.spans("ya")))
    assert attributes["ya.trace.malformed_json_record.count"] == 1
    assert attributes["ya.trace.input.incomplete"] is True


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


def test_oversized_trace_collection_is_rejected_before_opening_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ya_out = tmp_path / "out"
    for suite in ("one", "two"):
        trace_path = ya_out / suite / "test-results" / "unittest" / "ytest.report.trace"
        trace_path.parent.mkdir(parents=True)
        trace_path.write_text("{}\n")

    inputs = YaTraceInputs.discover(ya_out)
    monkeypatch.setattr(yatrace_limits, "MAX_YA_TRACE_BYTES", 5)

    def reject_open(*args, **kwargs):
        raise AssertionError(
            f"oversized trace files must not be opened: {args!r}, {kwargs!r}"
        )

    monkeypatch.setattr(Path, "open", reject_open)
    with pytest.raises(ValueError, match="Ya traces exceed 5 bytes"):
        inputs.parse()


def test_trace_collection_above_old_512_mib_limit_is_parsed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ya_out = tmp_path / "out"
    trace_path = ya_out / "suite" / "test-results" / "unittest" / "ytest.report.trace"
    trace_path.parent.mkdir(parents=True)
    trace_path.write_text('{"name":"chunk-event","value":{}}\n')
    inputs = YaTraceInputs.discover(ya_out)
    actual_stat = Path.stat

    def inflated_stat(path: Path, *args, **kwargs):
        result = actual_stat(path, *args, **kwargs)
        if path == trace_path:
            return type(
                "InflatedStat",
                (),
                {"st_size": 512 * 1024**2 + 1},
            )()
        return result

    monkeypatch.setattr(Path, "stat", inflated_stat)

    assert len(inputs.parse().traces) == 1


def test_report_preserves_raw_input_list_when_trace_parsing_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
    trace_path.write_text("{}\n")
    output_dir = tmp_path / "summary"

    monkeypatch.setattr(yatrace_limits, "MAX_YA_TRACE_BYTES", 1)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ya_trace_report",
            "--ya-out",
            str(ya_out),
            "--output-dir",
            str(output_dir),
            "--attempt-start-ns",
            "0",
            "--attempt-end-ns",
            "1",
            "--exit-code",
            "0",
        ],
    )

    with pytest.raises(ValueError, match="Ya traces exceed 1 bytes"):
        ya_trace_report_module.main()

    manifest = json.loads((output_dir / "trace-inputs.manifest.json").read_text())
    assert manifest["ya_trace_file_count"] == 1
    assert (output_dir / "trace-inputs.files").read_bytes() == (
        b"./cloud/example/tests/test-results/unittest/ytest.report.trace\0"
    )


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


def test_report_loads_only_discovered_evlog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ya_out = tmp_path / "out"
    ya_out.mkdir()
    evlog_target = tmp_path / "outside.jsonl"
    evlog_target.write_text("{}\n")
    evlog_path = tmp_path / "ya_evlog.jsonl"
    evlog_path.symlink_to(evlog_target)
    loaded_paths = []

    def load_evlog(path):
        loaded_paths.append(path)
        return YaEvlog(stages=[], nodes=[])

    def write_bundle(output_dir, trace, **kwargs):
        assert output_dir == tmp_path / "summary"
        assert kwargs["title"]
        assert kwargs["test_log_url_prefix"] == "https://example.test/logs/1/"
        assert kwargs["test_data_url_prefix"] == (
            "https://example.test/test_data/1/workspace/"
        )
        return {"span_count": len(trace)}

    monkeypatch.setattr(ya_trace_report_module, "load_ya_evlog", load_evlog)
    monkeypatch.setattr(
        ya_trace_report_module,
        "write_trace_bundle",
        write_bundle,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ya_trace_report",
            "--ya-out",
            str(ya_out),
            "--evlog",
            str(evlog_path),
            "--output-dir",
            str(tmp_path / "summary"),
            "--attempt-start-ns",
            "0",
            "--attempt-end-ns",
            "1",
            "--exit-code",
            "0",
            "--test-log-url-prefix",
            "https://example.test/logs/1/",
            "--test-data-url-prefix",
            "https://example.test/test_data/1/workspace/",
        ],
    )

    ya_trace_report_module.main()

    assert loaded_paths == [None]


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


def test_test_timing_preserves_observed_inferred_and_incomplete_chunks(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            _subtest("observed", timestamp=101, chunk=(0, 4), started=True),
            _subtest("observed", timestamp=102.5, chunk=(0, 4), duration=1.5),
            _chunk(
                0,
                4,
                timestamp=105,
                metrics={
                    "suite_start_timestamp": 100,
                    "suite_finish_timestamp": 105,
                },
            ),
            _subtest("inferred", timestamp=120, chunk=(1, 4), duration=1),
            _chunk(
                1,
                4,
                timestamp=120,
                metrics={
                    "suite_finish_timestamp": 112,
                    "wall_time": 5,
                    "suite_delay_until_first_test_secs": 1,
                },
            ),
            _subtest("inferred_other", timestamp=120, chunk=(2, 4), duration=3),
            _chunk(
                2,
                4,
                timestamp=120,
                metrics={
                    "suite_finish_timestamp": 110,
                    "wall_time": 10,
                    "suite_delay_until_first_test_secs": 2,
                },
            ),
            _subtest("incomplete", timestamp=115, chunk=(3, 4), started=True),
        ],
        root_start_s=99,
        root_end_s=121,
        exit_code=1,
    )

    root = next(trace.spans("ya"))
    chunk_spans = list(trace.spans("ya.chunk"))
    chunks = {
        attributes["ya.chunk.chunk_index"]: span
        for span in chunk_spans
        if "ya.chunk.chunk_index" in (attributes := _attributes(span))
    }
    tests = {span.name: span for span in trace.spans("ya.test")}
    observed = tests["Suite::observed"]
    inferred = tests["Suite::inferred"]
    inferred_other = tests["Suite::inferred_other"]
    incomplete = tests["Suite::incomplete"]

    assert root.name == "ya make tests"
    assert span_status_code(root) == 2
    assert _attributes(root)["ya.chunk.count"] == 4
    assert span_duration_ns(observed) == Ns.from_s(1.5)
    assert "test.timing.inferred" not in _attributes(observed)
    assert inferred.start_time_unix_nano == Ns.from_s(108)
    assert span_duration_ns(inferred) == Ns.from_s(1)
    assert (
        _attributes(inferred)["test.timing.source"] == "chunk-delay-and-test-duration"
    )
    assert inferred_other.start_time_unix_nano == Ns.from_s(102)
    assert span_duration_ns(inferred_other) == Ns.from_s(3)
    assert incomplete.end_time_unix_nano == Ns.from_s(121)
    assert span_status_code(incomplete) == 2
    assert _attributes(incomplete)["test.incomplete"] is True
    assert observed.parent_span_id == chunks[0].span_id
    assert inferred.parent_span_id == chunks[1].span_id
    assert inferred_other.parent_span_id == chunks[2].span_id
    assert incomplete.parent_span_id in {span.span_id for span in chunk_spans}


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


@pytest.mark.parametrize("status", ["xfail", "xpass"])
def test_yatool_expected_status_marks_test_ok(
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
            }
        ],
        root_end_s=10,
    )

    assert span_status_code(next(trace.spans("ya.test"))) == 1


def test_yatool_internal_xfaildiff_status_marks_test_failed(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "subtest-finished",
                "timestamp": 5,
                "value": {
                    "class": "Suite",
                    "subtest": "xfaildiff",
                    "status": "xfaildiff",
                    "time": 1,
                },
            }
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
            _stage("dispatch_build", 1, 10),
            _worker(
                "FromCache(uid$(BUILD_ROOT)/result.o)",
                "restore[CC]",
                2,
                3,
            ),
            _worker("Run(uid$(BUILD_ROOT)/result.o)", "CC", 3, 5),
            _statistics(
                {
                    "cache_hit": {
                        "cache_hit": 75,
                        "run_tasks": 4,
                        "executed_tasks": 4,
                        "cached_tasks": 3,
                        "not_cached_tasks": 1,
                    }
                }
            ),
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
            _stage("dispatch_build", 1, 10),
            _worker(
                "Run(failed-uid$(BUILD_ROOT)/result.o)",
                "CC",
                2,
                5,
            ),
            _failed_node("failed-uid", 42),
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
            _stage("dispatch_build", 1, 10),
            _worker("Run(failed-uid)", "CC", 2, 5),
            _failed_node("failed-uid", 17),
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
    monkeypatch.setattr(yatrace_limits, "MAX_BUILD_NODE_SPANS", 1)
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 1, 10),
            _worker("Run(failed$(BUILD_ROOT)/failed.o)", "CC", 2, 2.1),
            _worker("Run(long$(BUILD_ROOT)/long.o)", "CC", 3, 9),
            _failed_node("failed", 1),
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
            _stage("dispatch_build", 1, 10),
            _worker(
                "FromCache(failed$(BUILD_ROOT)/cached.o)",
                "restore[CC]",
                2,
                3,
            ),
            _worker("Run(failed$(BUILD_ROOT)/built.o)", "CC", 3, 5),
            _failed_node("failed", 1),
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
    ("task_type", "base_type"),
    [
        ("TA", "TA"),
        ("TL", "TL"),
        ("TL-CACHED", "TL"),
        ("TS-DYN_UID_CACHE", "TS"),
        ("TM-CACHED-DYN_UID_CACHE", "TM"),
    ],
)
def test_all_yatool_test_types_are_recognized_on_critical_path(
    task_type: str,
    base_type: str,
) -> None:
    entry = YaCriticalPathEntry.from_raw(3, {"type": task_type})

    assert entry.index == 3
    assert entry.raw_type == task_type
    assert entry.base_type == base_type
    assert entry.is_test


def test_critical_path_entry_parses_timing_and_attributes() -> None:
    entry = YaCriticalPathEntry.from_raw(
        7,
        {
            "type": "CC-CACHED",
            "text": "compile source.cpp",
            "uid": "compile",
            "elapsed": 1_500,
            "start_ts": 2_000,
            "end_ts": 3_500,
        },
    )

    assert entry.base_type == "CC"
    assert entry.text == "compile source.cpp"
    assert entry.uid == "compile"
    assert entry.elapsed_ms == 1_500
    assert entry.start_ms == 2_000
    assert entry.end_ms == 3_500
    assert entry.interval == Interval(
        Ns(2_000_000_000),
        Ns(3_500_000_000),
    )
    assert entry.span_attributes(test=False) == {
        "ya.build.critical_path": True,
        "ya.build.critical_path.index": 7,
        "ya.build.critical_path.reported_seconds": 1.5,
    }


def test_critical_path_entries_follow_mutable_statistics() -> None:
    evlog = YaEvlog(
        stages=[],
        nodes=[],
        statistics={"critical_path": [{"type": "CC"}]},
    )

    assert evlog.critical_path_entries[0].raw_type == "CC"

    evlog.statistics["critical_path"] = [{"type": "LD"}]

    assert evlog.critical_path_entries[0].raw_type == "LD"


def test_test_critical_path_uid_matches_when_interval_is_malformed() -> None:
    short = YaEvlogRecord(
        name="Run(shared$(BUILD_ROOT)/suite/test-results/unit/short)",
        tag="TM",
        start_ns=Ns(1),
        end_ns=Ns(2),
    )
    long = YaEvlogRecord(
        name="Run(shared$(BUILD_ROOT)/suite/test-results/unit/long)",
        tag="TM",
        start_ns=Ns(3),
        end_ns=Ns(6),
    )
    entry = YaCriticalPathEntry.from_raw(
        0,
        {
            "type": "TM",
            "uid": "shared",
            "start_ts": 2,
            "end_ts": 1,
        },
    )
    evlog = YaEvlog(stages=[], nodes=[short, long])

    assert entry.interval is None
    assert (
        evlog._critical_test_node(
            entry,
            [short, long],
            {"shared": [short, long]},
        )
        is long
    )


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
        [_statistics({"critical_path": critical_path})],
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
            _stage("dispatch_build", 1, 5),
            _worker(
                "Run(terminal-build$(BUILD_ROOT)/terminal.o)",
                "CC",
                2,
                3,
            ),
            _statistics({"critical_path": critical_path}),
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
            _stage("dispatch_build", 0, 10),
            _worker("Run(shared$(BUILD_ROOT)/first.o)", "CC", 1, 2),
            _worker("Run(shared$(BUILD_ROOT)/second.o)", "LD", 3, 5),
            _statistics(
                {
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
                }
            ),
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
    monkeypatch.setattr(yatrace_limits, "MAX_BUILD_NODE_SPANS", 1)
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 0, 10),
            _worker("Run(critical$(BUILD_ROOT)/critical.o)", "CC", 1, 1.1),
            _worker("Run(long$(BUILD_ROOT)/long.o)", "CC", 2, 8),
            _statistics(
                {
                    "critical_path": [
                        {
                            "type": "CC",
                            "uid": "critical",
                            "elapsed": 100,
                            "start_ts": 1_000,
                            "end_ts": 1_100,
                        }
                    ]
                }
            ),
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
            _stage("dispatch_build", 0, 10),
            _worker(
                "FromCache(shared$(BUILD_ROOT)/cached.o)",
                "restore[CC]",
                1,
                2,
            ),
            _worker("Run(shared$(BUILD_ROOT)/built.o)", "CC", 3, 5),
            _statistics(
                {
                    "critical_path": [
                        {
                            "type": "CC",
                            "uid": "shared",
                            "elapsed": 2_000,
                            "start_ts": 3_000,
                            "end_ts": 5_000,
                        }
                    ]
                }
            ),
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
            _stage("dispatch_build", 0, 10),
            _worker("Run(worker$(BUILD_ROOT)/built.o)", "CC", 3, 5),
            _statistics(
                {
                    "critical_path": [
                        {
                            "type": "CC",
                            "elapsed": 2_000,
                            "start_ts": 3_000,
                            "end_ts": 5_000,
                        }
                    ]
                }
            ),
        ],
        root_end_s=11,
    )

    node = next(trace.spans("ya.build.node"))
    assert _attributes(node)["ya.build.critical_path"] is True


def test_build_node_span_limit_is_hard_for_critical_nodes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(yatrace_limits, "MAX_BUILD_NODE_SPANS", 1)
    evlog_events = [_stage("dispatch_build", 0, 10)]
    for index in range(2):
        evlog_events.append(
            _worker(
                f"Run(uid-{index}$(BUILD_ROOT)/{index}.o)",
                "CC",
                index + 1,
                index + 2,
            )
        )
    evlog_events.append(
        _statistics(
            {
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
            }
        )
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
            _stage("dispatch_build", 1, 10),
            _worker("Run(build$(BUILD_ROOT)/build.o)", "CC", 2, 5),
            _statistics(
                {
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
                }
            ),
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


def test_build_statistics_and_test_critical_path_are_preserved(
    tmp_path: Path,
) -> None:
    statistics = {
        "cache_hit": {
            "cache_hit": 75,
            "run_tasks": 100,
            "executed_tasks": 4,
            "cached_tasks": 3,
            "dyn_cached_tasks": 1,
            "not_cached_tasks": 1,
            "tests_tasks": 1,
            "failed_tasks": 0,
            "ok_tasks": 1,
            "avoided_tasks": 96,
        },
        "dist_cache_stat": {
            "get_count": 2,
            "get_data_size": 1_024,
            "put_count": 1,
            "put_data_size": 512,
        },
        "execution_stages_msec": {"build_only": 3_500, "tests_only": 2_000},
        "task_execution_msec": 5_500,
        "graph_lang_usage": {"cpp": 1},
        "critical_path": [
            {
                "type": "CC",
                "elapsed": 4_000,
                "start_ts": 13_000,
                "end_ts": 17_000,
                "text": "$(SOURCE_ROOT)/suite/main.cpp",
                "uid": "compileuid",
            },
            {
                "type": "TM",
                "elapsed": 2_000,
                "start_ts": 18_000,
                "end_ts": 20_000,
                "text": "suite/tests",
                "uid": "testuid",
            },
        ],
    }
    evlog_events = [
        _stage("build_graph_and_tests", 10, 12),
        _stage("dispatch_build", 12, 30),
        _worker(
            "FromCache(cacheuid$(BUILD_ROOT)/library/cached.a)",
            "restore[AR]",
            12.5,
            13,
        ),
        _worker(
            "FromCache(loweruid$(BUILD_ROOT)/library/lower.a)",
            "restore[ar]",
            12.6,
            12.9,
        ),
        _worker(
            "Run(compileuid$(BUILD_ROOT)/suite/main.cpp.o)",
            "CC",
            13,
            17,
        ),
        _worker("PutInCache(uid)", "put_in_cache[CC]", 16.9, 17),
        _worker(
            "Run(actual-test$(BUILD_ROOT)/suite/test-results/unittest/meta.json)",
            "TM",
            18,
            20,
        ),
        _statistics(statistics),
    ]
    trace = _render_ya_trace(
        tmp_path,
        [
            _subtest("critical", timestamp=19.5, duration=1),
            _chunk(
                0,
                1,
                timestamp=20,
                metrics={
                    "suite_start_timestamp": 18,
                    "suite_finish_timestamp": 20,
                },
            ),
        ],
        evlog_events=evlog_events,
        root_start_s=9,
        root_end_s=31,
    )

    root = next(trace.spans("ya"))
    phases = {
        _attributes(span)["ya.stage.name"]: span for span in trace.spans("ya.phase")
    }
    build = next(trace.spans("ya.build"))
    nodes = list(trace.spans("ya.build.node"))
    operations = next(trace.spans("ya.test.operations"))
    worker = next(trace.spans("ya.test.worker"))
    chunk = next(trace.spans("ya.chunk"))
    test = next(trace.spans("ya.test"))
    dispatch_attributes = _attributes(phases["dispatch_build"])
    build_attributes = _attributes(build)
    root_attributes = _attributes(root)

    assert set(phases) == {"build_graph_and_tests", "dispatch_build"}
    assert build.parent_span_id == phases["dispatch_build"].span_id
    assert operations.parent_span_id == phases["dispatch_build"].span_id
    assert worker.parent_span_id == operations.span_id
    assert chunk.parent_span_id == worker.span_id
    assert span_duration_ns(build) == Ns.from_s(4.5)
    assert build_attributes["ya.build.node.count"] == 4
    assert build_attributes["ya.build.node.cache_store.count"] == 1
    assert build_attributes["ya.build.first_test_node_offset_seconds"] == 5.5
    assert build_attributes["ya.build.worker.tool.ar.cache_restore.count"] == 2
    assert build_attributes["ya.build.worker.tool.cc.execute.count"] == 1
    assert build_attributes["ya.build.critical_path.node.count"] == 1
    assert build_attributes["ya.build.critical_path.work.seconds"] == 4
    assert len(nodes) == 3

    expected_dispatch = {
        "ya.build.cache.considered_task.hit.ratio": 0.75,
        "ya.build.cache.considered_task.hit.count": 3,
        "ya.build.cache.considered_task.miss.count": 1,
        "ya.build.task.avoided.ratio": 0.96,
        "ya.build.task.reused_or_avoided.ratio": 0.99,
        "ya.build.task.avoided.count": 96,
        "ya.build.dist_cache.get.bytes": 1_024,
        "ya.build.execution.stage.build_only.seconds": 3.5,
        "ya.build.execution.total.seconds": 5.5,
    }
    assert {
        key: dispatch_attributes[key] for key in expected_dispatch
    } == expected_dispatch

    cached = next(
        node for node in nodes if _attributes(node)["ya.build.kind"] == "cache_restore"
    )
    compiled = next(
        node for node in nodes if _attributes(node)["ya.build.kind"] == "execute"
    )
    assert _attributes(cached)["ya.build.cache.source"] == "local"
    assert _attributes(cached)["ya.build.outputs"] == ["library/cached.a"]
    assert _attributes(compiled)["ya.build.critical_path"] is True
    assert _attributes(compiled)["ya.build.critical_path.index"] == 0
    assert _attributes(compiled)["ya.build.critical_path.reported_seconds"] == 4
    assert root_attributes["ya.build.node.count"] == 4
    assert root_attributes["ya.build.node.span_count"] == 3
    assert root_attributes["ya.test.critical_path.entry.count"] == 1
    assert root_attributes["ya.test.critical_path.chunk.count"] == 1
    assert root_attributes["ya.test.critical_path.span.count"] == 1
    assert _attributes(chunk)["ya.test.critical_path"] is True
    assert _attributes(chunk)["ya.test.critical_path.granularity"] == "test-chunk"
    assert _attributes(test)["ya.test.critical_path"] is True
    assert _attributes(test)["ya.test.critical_path.inferred"] is True
    assert _attributes(test)["ya.test.critical_path.reported_seconds"] == 2

    build_only = build_ya_trace(
        [],
        root_start_ns=Ns.from_s(9),
        root_end_ns=Ns.from_s(31),
        exit_code=0,
        resource=ResourceAttributes(),
        evlog=load_ya_evlog(tmp_path / "ya_evlog.jsonl"),
        operation="build",
    )
    assert build_only[0].name == "ya make build"
    assert not any(build_only.spans("ya.chunk"))
    assert any(build_only.spans("ya.build"))


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


@pytest.mark.parametrize(
    ("tag", "expected_kind", "expected_tool"),
    [
        ("TS", "test_execute", "TS"),
        ("TM", "test_execute", "TM"),
        ("TL", "test_list", "TL"),
        ("YT", "test_execute", "YT"),
        ("TA", "test_aggregate", "TA"),
        ("TR", "test_merge", "TR"),
        ("restore[TS]", "test_cache_restore", "TS"),
        ("restore_from_dist_cache[TM]", "test_cache_restore", "TM"),
        ("result[TL]", "test_materialize", "TL"),
        ("put_in_cache[TS]", "test_cache_store", "TS"),
        ("put_in_dist_cache[TM]", "test_cache_store", "TM"),
    ],
)
def test_all_yatool_test_node_tags_are_classified(
    tag: str,
    expected_kind: str,
    expected_tool: str,
) -> None:
    record = YaEvlogRecord(
        name="Run(testuid)",
        tag=tag,
        start_ns=Ns(1),
        end_ns=Ns(2),
    )

    assert record.kind_and_tool == (expected_kind, expected_tool)


def test_large_test_runner_is_distinguished_from_test_list_node() -> None:
    record = YaEvlogRecord(
        name="Run(testuid$(BUILD_ROOT)/suite/test-results/unit/meta.json)",
        tag="TL",
        start_ns=Ns(1),
        end_ns=Ns(2),
    )

    assert record.kind_and_tool == ("test_execute", "TL")
    assert record.test_size == "large"


def test_test_result_path_does_not_override_a_worker_wrapper_kind() -> None:
    record = YaEvlogRecord(
        name="Result(uid$(BUILD_ROOT)/suite/test-results/unit/meta.json)",
        tag="result[CC]",
        start_ns=Ns(1),
        end_ns=Ns(2),
    )

    assert record.kind_and_tool == ("materialize", "CC")


def test_non_object_evlog_record_is_ignored(tmp_path: Path) -> None:
    evlog_path = tmp_path / "ya_evlog.jsonl"
    evlog_path.write_text(
        "[]\n" + json.dumps(_run_worker("uid", "output.o", 2, 3)) + "\n"
    )

    evlog = load_ya_evlog(evlog_path)
    assert [record.uid for record in evlog.nodes] == ["uid"]


def test_exec_cmd_interval_is_child_of_its_worker_node_with_interleaving(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 1, 10),
            _run_worker("firstuid", "first.o", 2, 6, "Worker-1"),
            _run_worker("seconduid", "second.o", 3, 7, "Worker-2"),
            _worker_detail("exec_cmd", 4, 5, "Worker-1"),
            _worker_detail("post_cmd", 5, 5.5, "Worker-1"),
        ],
        root_end_s=11,
    )

    nodes = list(trace.spans("ya.build.node"))
    nodes_by_id = {node.span_id: _attributes(node) for node in nodes}
    commands = list(trace.spans("ya.build.command"))
    assert len(commands) == 1
    command = commands[0]
    assert nodes_by_id[command.parent_span_id]["ya.build.node.uid"] == "firstuid"
    assert span_duration_ns(
        next(
            node
            for node in nodes
            if _attributes(node)["ya.build.node.uid"] == "firstuid"
        )
    ) == Ns.from_s(4)
    assert span_duration_ns(command) == Ns.from_s(1)
    assert _attributes(command)["ya.build.timing.scope"] == "command"
    assert (
        _attributes(next(trace.spans("ya.build")))[
            "ya.build.cumulative_command_seconds"
        ]
        == 1
    )


def test_node_started_clears_stale_command_detail_parent(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _run_worker("firstuid", "first.o", 2, 6, "Worker-1"),
            _node_started(
                "Worker-1",
                "Run(seconduid$(BUILD_ROOT)/second.o)",
            ),
            _worker_detail("exec_cmd", 4, 5, "Worker-1"),
        ],
        root_end_s=11,
    )

    assert not any(trace.spans("ya.build.command"))


def test_null_worker_thread_does_not_associate_command_detail(
    tmp_path: Path,
) -> None:
    worker = _run_worker("firstuid", "first.o", 2, 6)
    worker["thread_name"] = None
    detail = _worker_detail("exec_cmd", 3, 4, "unused")
    detail["thread_name"] = None

    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[worker, detail],
        root_end_s=11,
    )

    assert not any(trace.spans("ya.build.command"))


def test_failed_worker_does_not_imply_failed_command(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _run_worker("faileduid", "failed.o", 2, 6, "Worker-1"),
            _worker_detail("exec_cmd", 3, 4, "Worker-1"),
            _failed_node("faileduid", 1),
        ],
        root_end_s=11,
        exit_code=1,
    )

    assert span_status_code(next(trace.spans("ya.build.node"))) == 2
    command = next(trace.spans("ya.build.command"))
    assert span_status_code(command) == 0
    assert _attributes(command)["ya.build.node.failed"] is True


def test_build_command_span_limit_prefers_critical_parent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(yatrace_limits, "MAX_BUILD_COMMAND_SPANS", 1)
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _run_worker("firstuid", "first.o", 2, 6, "Worker-1"),
            _worker_detail("exec_cmd", 3, 4, "Worker-1"),
            _run_worker("criticaluid", "critical.o", 4, 8, "Worker-2"),
            _worker_detail("exec_cmd", 5, 6, "Worker-2"),
            _statistics(
                {
                    "critical_path": [
                        {
                            "type": "CC",
                            "uid": "criticaluid",
                            "elapsed": 4_000,
                            "start_ts": 4_000,
                            "end_ts": 8_000,
                        }
                    ]
                }
            ),
        ],
        root_end_s=11,
    )

    nodes_by_id = {
        node.span_id: _attributes(node) for node in trace.spans("ya.build.node")
    }
    command = next(trace.spans("ya.build.command"))
    assert nodes_by_id[command.parent_span_id]["ya.build.node.uid"] == "criticaluid"
    build_attributes = _attributes(next(trace.spans("ya.build")))
    assert build_attributes["ya.build.command_spans.rendered"] == 1
    assert build_attributes["ya.build.command_spans.dropped"] == 1


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
