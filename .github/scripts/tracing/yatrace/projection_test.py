import json
from pathlib import Path

from scripts.tracing.otlp import (
    Interval,
    Ns,
    ResourceAttributes,
    decode_attributes,
    span_status_message,
)
from scripts.tracing.yatrace import YaTraceCollection, load_ya_evlog
from scripts.tracing.yatrace.build_operations import YaBuildOperations
from scripts.tracing.yatrace.critical_path import YaCriticalPath
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
    assert decode_attributes(spans["ya.test"].attributes)["test.size"] == "medium"
    assert spans["ya.chunk"].parent_span_id == spans["ya.test.worker"].span_id
    assert spans["ya.test.worker"].parent_span_id == spans["ya.test.operations"].span_id
    assert spans["ya.test"].parent_span_id == spans["ya.chunk"].span_id
    assert spans["ya.build.node"].parent_span_id == spans["ya.build"].span_id
    assert spans["ya.build.command"].parent_span_id == spans["ya.build.node"].span_id


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
