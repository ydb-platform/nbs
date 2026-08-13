import json
from pathlib import Path

from scripts.tracing.otlp import (
    Interval,
    Ns,
    ResourceAttributes,
    decode_attributes,
    span_status_code,
    span_status_message,
)
from scripts.tracing.yatrace import YaTraceCollection, limits, load_ya_evlog
from scripts.tracing.yatrace.build_operations import YaBuildOperations
from scripts.tracing.yatrace.critical_path import YaCriticalPath
from scripts.tracing.yatrace.evlog_data import YaEvlog
from scripts.tracing.yatrace.node import YaNode
from scripts.tracing.yatrace.projection import build_ya_trace
from scripts.tracing.yatrace.statistics import YaBuildStatistics


def _write_jsonl(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{json.dumps(event)}\n" for event in events))


def test_complete_ya_projection_from_raw_inputs(tmp_path: Path) -> None:
    ya_out = tmp_path / "out"
    _write_jsonl(
        ya_out / "suite" / "test-results" / "unittest" / "ytest.report.trace",
        [
            {"name": "suite-event", "timestamp": 2, "value": {}},
            {
                "name": "subtest-finished",
                "timestamp": 8,
                "value": {
                    "class": "Suite",
                    "subtest": "case",
                    "status": "good",
                    "time": 4,
                },
            },
            {
                "name": "chunk-event",
                "timestamp": 10,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "metrics": {
                        "suite_start_timestamp": 2,
                        "suite_finish_timestamp": 10,
                        "suite_initial_(seconds)": 1,
                        "suite_prepare_recipes_(seconds)": 2,
                        "suite_wrapper_execution_(seconds)": 4,
                    },
                },
            },
        ],
    )
    evlog_path = tmp_path / "ya_evlog.jsonl"
    _write_jsonl(
        evlog_path,
        [
            {
                "namespace": "stages",
                "event": "stage-finished",
                "value": {"name": "get-tools", "tag": "get-tools", "time": [0.5, 1]},
            },
            {
                "namespace": "stages",
                "event": "stage-finished",
                "value": {
                    "name": "dispatch_build",
                    "tag": "dispatch_build",
                    "time": [1, 11],
                },
            },
            {
                "thread_name": "Worker-2",
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": "Run(builduid$(BUILD_ROOT)/result.o)",
                    "tag": "CC",
                    "time": [2, 6],
                },
            },
            {
                "thread_name": "Worker-2",
                "namespace": "worker_threads",
                "event": "node-detailed",
                "value": {"name": "exec_cmd", "tag": "exec_cmd", "time": [3, 5]},
            },
            {
                "thread_name": "Worker-1",
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {
                    "name": (
                        "Run(testuid$(BUILD_ROOT)/suite/test-results/"
                        "unittest/meta.json)"
                    ),
                    "tag": "TM",
                    "time": [2, 10],
                },
            },
            *[
                {
                    "thread_name": "Worker-1",
                    "namespace": "worker_threads",
                    "event": "node-detailed",
                    "value": {"name": tag, "tag": tag, "time": interval},
                }
                for tag, interval in (
                    ("setup", [2, 3]),
                    ("exec_cmd", [3, 9]),
                    ("node_result", [9, 10]),
                )
            ],
        ],
    )

    trace = build_ya_trace(
        YaTraceCollection.load(ya_out).traces,
        root_start_ns=Ns(0),
        root_end_ns=Ns.from_s_or_zero(12),
        exit_code=0,
        resource=ResourceAttributes(
            {
                "github.repository": "example/repo",
                "github.run.id": 123,
                "ci.component": "all",
            }
        ),
        evlog=load_ya_evlog(evlog_path),
    )

    assert len(trace) == 17
    assert {
        scope: len(list(trace.spans(scope)))
        for scope in (
            "ya.suite",
            "ya.chunk",
            "ya.test",
            "ya.test.stage",
            "ya.phase",
            "ya.test.operations",
            "ya.test.worker",
            "ya.test.worker.phase",
            "ya.build",
            "ya.build.node",
            "ya.build.command",
        )
    } == {
        "ya.suite": 1,
        "ya.chunk": 1,
        "ya.test": 1,
        "ya.test.stage": 3,
        "ya.phase": 2,
        "ya.test.operations": 1,
        "ya.test.worker": 1,
        "ya.test.worker.phase": 3,
        "ya.build": 1,
        "ya.build.node": 1,
        "ya.build.command": 1,
    }
    spans = {
        scope: next(trace.spans(scope))
        for scope in (
            "ya",
            "ya.chunk",
            "ya.test",
            "ya.test.operations",
            "ya.test.worker",
            "ya.build",
            "ya.build.node",
            "ya.build.command",
        )
    }
    assert decode_attributes(spans["ya"].attributes)["ya.chunk.count"] == 1
    chunk_attributes = decode_attributes(spans["ya.chunk"].attributes)
    assert chunk_attributes["test.size"] == "medium"
    assert chunk_attributes["ya.test.result.count"] == 1
    test_result = spans["ya.test"]
    assert test_result.name == "Suite::case"
    assert test_result.start_time_unix_nano == Ns.from_s_or_zero(6).value
    assert test_result.end_time_unix_nano == Ns.from_s_or_zero(10).value
    assert decode_attributes(test_result.attributes) == {
        "test.duration.reported_seconds": 4.0,
        "test.framework": "ya",
        "test.name": "case",
        "test.size": "medium",
        "test.status": "good",
        "test.suite": "Suite",
        "test.timing.inferred": True,
        "test.timing.source": "chunk-order-and-reported-duration",
        "ya.test.duration.rank": 1,
        "ya.test.result.timestamp.source": "subtest-finished.timestamp",
    }
    assert spans["ya.chunk"].parent_span_id == spans["ya.test.worker"].span_id
    assert spans["ya.test.worker"].parent_span_id == spans["ya.test.operations"].span_id
    assert spans["ya.test"].parent_span_id == spans["ya.chunk"].span_id
    assert spans["ya.build.node"].parent_span_id == spans["ya.build"].span_id
    assert spans["ya.build.command"].parent_span_id == spans["ya.build.node"].span_id


def test_single_test_is_right_aligned_without_start_evidence(
    tmp_path: Path,
) -> None:
    ya_out = tmp_path / "out"
    _write_jsonl(
        ya_out / "suite" / "test-results" / "unittest" / "ytest.report.trace",
        [
            {
                "name": "chunk-event",
                "timestamp": 12,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "metrics": {
                        "suite_start_timestamp": 10,
                        "suite_finish_timestamp": 12,
                        "wall_time": 2,
                    },
                },
            },
            {
                "name": "subtest-finished",
                "timestamp": 15,
                "value": {
                    "class": "Suite",
                    "subtest": "failed_case",
                    "status": "fail",
                    "time": 0.5,
                    "chunk_index": 0,
                    "nchunks": 1,
                },
            },
        ],
    )

    trace = build_ya_trace(
        YaTraceCollection.load(ya_out).traces,
        root_start_ns=Ns.from_s_or_zero(0),
        root_end_ns=Ns.from_s_or_zero(20),
        exit_code=1,
        resource=ResourceAttributes(),
    )

    chunk = next(trace.spans("ya.chunk"))
    result = next(trace.spans("ya.test"))
    assert chunk.start_time_unix_nano == Ns.from_s_or_zero(10).value
    assert chunk.end_time_unix_nano == Ns.from_s_or_zero(12).value
    assert span_status_code(chunk) == 2
    assert result.parent_span_id == chunk.span_id
    assert result.start_time_unix_nano == Ns.from_s_or_zero(11.5).value
    assert result.end_time_unix_nano == Ns.from_s_or_zero(12).value
    assert span_status_code(result) == 2
    attributes = decode_attributes(result.attributes)
    assert attributes["test.duration.reported_seconds"] == 0.5
    assert attributes["test.timing.inferred"] is True
    assert attributes["test.timing.source"] == "chunk-order-and-reported-duration"


def test_nested_results_remain_timestamp_markers(tmp_path: Path) -> None:
    ya_out = tmp_path / "out"
    _write_jsonl(
        ya_out / "suite" / "test-results" / "tests" / "ytest.report.trace",
        [
            {
                "name": "chunk-event",
                "timestamp": 20,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "metrics": {
                        "suite_start_timestamp": 10,
                        "suite_finish_timestamp": 20,
                        "wall_time": 10,
                    },
                },
            },
            {
                "name": "subtest-finished",
                "timestamp": 21,
                "value": {
                    "class": "tests",
                    "subtest": "TestParent",
                    "status": "good",
                    "time": 4,
                    "chunk_index": 0,
                    "nchunks": 1,
                },
            },
            {
                "name": "subtest-finished",
                "timestamp": 22,
                "value": {
                    "class": "tests",
                    "subtest": "TestParent/child",
                    "status": "good",
                    "time": 3,
                    "chunk_index": 0,
                    "nchunks": 1,
                },
            },
        ],
    )

    trace = build_ya_trace(
        YaTraceCollection.load(ya_out).traces,
        root_start_ns=Ns.from_s_or_zero(0),
        root_end_ns=Ns.from_s_or_zero(30),
        exit_code=0,
        resource=ResourceAttributes(),
    )

    tests = list(trace.spans("ya.test"))
    assert [
        (span.name, span.start_time_unix_nano, span.end_time_unix_nano)
        for span in tests
    ] == [
        (
            "tests::TestParent",
            Ns.from_s_or_zero(21).value,
            Ns.from_s_or_zero(21).value,
        ),
        (
            "tests::TestParent/child",
            Ns.from_s_or_zero(22).value,
            Ns.from_s_or_zero(22).value,
        ),
    ]
    assert all(
        "test.timing.inferred" not in decode_attributes(span.attributes)
        for span in tests
    )


def test_empty_uid_is_not_treated_as_a_failed_build_node() -> None:
    node = YaNode("Run(tool)", "", Interval(Ns(1), Ns(2)), "execute", "")
    operations = YaBuildOperations(
        [node],
        {"": 17},
        YaBuildStatistics.from_raw(),
        YaCriticalPath.from_evlog({}, []),
        [],
    )

    assert operations._failures([node]) == (set(), set())


def test_build_node_metrics_separate_policy_omission_from_limit_drop(
    monkeypatch,
) -> None:
    monkeypatch.setattr(limits, "MAX_BUILD_NODE_SPANS", 1)
    nodes = [
        YaNode("Run(a)", "CC", Interval(Ns(0), Ns(3)), "execute", "CC"),
        YaNode("Run(b)", "CC", Interval(Ns(1), Ns(2)), "execute", "CC"),
        YaNode(
            "put_in_cache[CC]",
            "put_in_cache[CC]",
            Interval(Ns(1), Ns(2)),
            "cache_store",
            "CC",
        ),
    ]
    trace = build_ya_trace(
        [],
        root_start_ns=Ns(0),
        root_end_ns=Ns(4),
        exit_code=0,
        resource=ResourceAttributes(),
        evlog=YaEvlog.from_raw(nodes=nodes),
    )
    root_attributes = decode_attributes(next(trace.spans("ya")).attributes)
    build_attributes = decode_attributes(next(trace.spans("ya.build")).attributes)

    assert build_attributes["ya.build.node.execute.count"] == 2
    assert build_attributes["ya.build.node.cache_store.count"] == 1
    assert build_attributes["ya.build.node_spans.rendered"] == 1
    assert build_attributes["ya.build.node_spans.policy_omitted"] == 1
    assert build_attributes["ya.build.node_spans.dropped"] == 1
    assert build_attributes["ya.build.node.count"] == sum(
        build_attributes[key]
        for key in (
            "ya.build.node_spans.rendered",
            "ya.build.node_spans.policy_omitted",
            "ya.build.node_spans.dropped",
        )
    )
    assert root_attributes["ya.build.node.count"] == 3
    assert root_attributes["ya.build.node.span_count"] == 1
    assert root_attributes["ya.build.node.span_policy_omitted_count"] == 1
    assert root_attributes["ya.build.node.span_dropped_count"] == 1
    assert root_attributes["ya.build.node.count"] == sum(
        root_attributes[key]
        for key in (
            "ya.build.node.span_count",
            "ya.build.node.span_policy_omitted_count",
            "ya.build.node.span_dropped_count",
        )
    )


def test_malformed_evlog_marks_trace_incomplete_without_losing_valid_records(
    tmp_path: Path,
) -> None:
    evlog_path = tmp_path / "ya_evlog.jsonl"
    valid_records = [
        {
            "namespace": "stages",
            "event": "stage-finished",
            "value": {"name": "configure", "tag": "configure", "time": [0, 1]},
        },
        {
            "namespace": "worker_threads",
            "event": "node-finished",
            "value": {"name": "Run(tool)", "tag": "CC", "time": [1, 2]},
        },
    ]
    evlog_path.write_text(
        f"{json.dumps(valid_records[0])}\n{{malformed\n"
        f"{json.dumps(valid_records[1])}\n"
    )

    evlog = load_ya_evlog(evlog_path)
    trace = build_ya_trace(
        [],
        root_start_ns=Ns(0),
        root_end_ns=Ns.from_s_or_zero(3),
        exit_code=0,
        resource=ResourceAttributes(),
        evlog=evlog,
    )
    root_attributes = decode_attributes(next(trace.spans("ya")).attributes)

    assert len(evlog.stages) == 1
    assert len(evlog.nodes) == 1
    assert evlog.malformed_json_record_count == 1
    assert root_attributes["ya.trace.malformed_json_record.count"] == 0
    assert root_attributes["ya.evlog.malformed_json_record.count"] == 1
    assert root_attributes["ya.trace.input.incomplete"] is True


def test_root_failure_message_names_ya_operation() -> None:
    trace = build_ya_trace(
        [],
        root_start_ns=Ns(1),
        root_end_ns=Ns(2),
        exit_code=0,
        result_code=17,
        resource=ResourceAttributes(),
        operation="tests",
    )

    assert span_status_message(next(trace.spans("ya"))) == (
        "ya make tests result code 17"
    )
