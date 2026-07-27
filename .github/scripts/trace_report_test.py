from __future__ import annotations

import base64
import gzip
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from github.WorkflowJob import WorkflowJob
from opentelemetry.proto_json.trace.v1.trace import TracesData

from scripts.tests.ya_trace_report import (
    build_resource_attributes,
    build_ya_trace,
)
from scripts.otlp import (
    ResourceAttributes,
    Span,
    Trace,
    decode_attributes,
    make_event,
    make_span as new_span,
    ns,
    span_duration_ns,
    span_status_code,
    stable_span_id,
    stable_trace_id,
)
from scripts.trace_report import (
    TRACE_HTML_TEMPLATE,
    TRACE_SCRIPT_TEMPLATE,
    _trace_model,
    read_otlp_jsonl,
    render_html,
    write_trace_bundle,
)
from scripts.tests.ya_trace import load_ya_evlog, load_ya_traces
from scripts.workflow_trace_report import (
    build_workflow_trace,
    download_s3_trace_inputs,
)


def make_span(**kwargs) -> Span:
    values = {
        "trace_id": bytes.fromhex("1" * 32),
        "span_id": bytes.fromhex("2" * 16),
        "name": "root",
        "start_ns": 1_000,
        "end_ns": 2_000,
    }
    values.update(kwargs)
    return new_span(**values)


def make_trace(
    *spans: Span,
    resource: ResourceAttributes | None = None,
    scope_name: str = "nbs.ci",
) -> Trace:
    trace = Trace()
    for span in spans:
        trace.add_span(
            span,
            resource=resource or ResourceAttributes(),
            scope_name=scope_name,
        )
    return trace


def attributes(span: Span) -> dict:
    return decode_attributes(span.attributes)


def test_otlp_round_trip_and_static_html_escaping(tmp_path: Path) -> None:
    trace = make_trace(
        make_span(
            attributes={
                "string": '<script>alert("attribute")</script>',
                "integer": 42,
                "boolean": True,
                "array": ["one", "two"],
            },
            events=[
                make_event(
                    name="<event>",
                    time_ns=1_500,
                    attributes={"detail": "<b>unsafe</b>"},
                )
            ],
            status_code=2,
            status_message="<failed>",
        ),
        resource=ResourceAttributes({"service.name": "test"}),
    )

    assert isinstance(trace.data, TracesData)
    encoded_span = trace.to_dict()["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
    assert encoded_span["traceId"] == "1" * 32
    assert encoded_span["spanId"] == "2" * 16
    assert encoded_span["startTimeUnixNano"] == "1000"

    manifest = write_trace_bundle(
        tmp_path,
        trace,
        title="<trace>",
        metadata={"github.sha": "<sha>"},
    )

    restored = read_otlp_jsonl([tmp_path / manifest["otlp_file"]])
    assert restored.to_dict() == trace.to_dict()
    assert isinstance(restored[0].start_time_unix_nano, ns)
    assert isinstance(restored[0].events[0].time_unix_nano, ns)

    report = (tmp_path / manifest["html_file"]).read_text()
    assert "<script>alert" not in report
    assert "&lt;sha&gt;" in report
    assert "<h1>&lt;trace&gt;</h1>" in report
    assert "default-src &#x27;none&#x27;" not in report
    payload = re.search(
        r'<script id="trace-data"[^>]*>([^<]+)</script>',
        report,
    )
    assert payload
    model = json.loads(gzip.decompress(base64.b64decode(payload.group(1))))
    assert model["s"][0][5]["string"] == '<script>alert("attribute")</script>'
    assert manifest["span_count"] == 1


def test_renderer_marks_error_spans_and_supports_empty_input() -> None:
    trace = make_trace(make_span(status_code=2))
    assert _trace_model(trace)["s"][0][7] == 2
    assert 'id="trace-data"' in render_html(trace)
    assert "No spans found" in render_html(Trace())


def test_resource_attributes_support_environment_and_workflow_run() -> None:
    environment = {
        "GITHUB_SERVER_URL": "https://github.example",
        "GITHUB_REPOSITORY": "example/repo",
        "GITHUB_WORKFLOW": "Build",
        "GITHUB_RUN_ID": "123",
        "GITHUB_SHA": "abc123",
    }
    current = ResourceAttributes.from_github(environment=environment)
    assert current["github.run.url"] == (
        "https://github.example/example/repo/actions/runs/123"
    )
    assert current["github.commit.url"] == (
        "https://github.example/example/repo/commit/abc123"
    )

    completed = ResourceAttributes.from_github(
        environment=environment,
        workflow_run={
            "id": 456,
            "name": "Nightly",
            "head_sha": "def456",
            "repository": {"full_name": "upstream/repo"},
            "pull_requests": [{"number": 42}],
        },
    )
    assert completed["github.run.id"] == 456
    assert completed["github.workflow"] == "Nightly"
    assert completed["github.pull_request.number"] == [42]
    assert completed["github.commit.url"] == (
        "https://github.example/upstream/repo/commit/def456"
    )


def test_ya_resource_attributes_extend_common_github_attributes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", "example/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_SHA", "abc123")
    attributes = build_resource_attributes(
        SimpleNamespace(
            operation="tests",
            component="tasks",
            build_preset="relwithdebinfo",
            build_target="",
            test_target="cloud/tasks",
            test_type="unittest",
            test_size="small",
            retry=2,
        )
    )
    assert attributes["service.name"] == "nbs-ya-tests"
    assert attributes["github.run.url"].endswith("/example/repo/actions/runs/123")
    assert attributes["ci.test.target"] == "cloud/tasks"


def test_renderer_collapses_build_and_test_groups_by_default() -> None:
    trace = Trace()
    for span, scope_name in (
        (make_span(), "ya.run"),
        (
            make_span(
                span_id=bytes.fromhex("3" * 16),
                parent_span_id=bytes.fromhex("2" * 16),
                name="build operations",
            ),
            "ya.build",
        ),
        (
            make_span(
                span_id=bytes.fromhex("4" * 16),
                parent_span_id=bytes.fromhex("3" * 16),
                name="compile target",
            ),
            "ya.build.node",
        ),
        (
            make_span(
                span_id=bytes.fromhex("5" * 16),
                parent_span_id=bytes.fromhex("2" * 16),
                name="cloud/tasks/storage/tests [tests chunk 1/1]",
            ),
            "ya.chunk",
        ),
        (
            make_span(
                span_id=bytes.fromhex("6" * 16),
                parent_span_id=bytes.fromhex("5" * 16),
                name="TestSuite::test_case",
            ),
            "ya.test",
        ),
    ):
        trace.add_span(
            span,
            resource=ResourceAttributes(),
            scope_name=scope_name,
        )

    model = _trace_model(trace)
    encoded = {span[2]: span for span in model["s"]}
    indexes = {span[2]: index for index, span in enumerate(model["s"])}
    report = render_html(trace)
    assert encoded["build operations"][1] == indexes["root"]
    assert encoded["compile target"][1] == indexes["build operations"]
    assert encoded["cloud/tasks/storage/tests [tests chunk 1/1]"][1] == indexes["root"]
    assert (
        encoded["TestSuite::test_case"][1]
        == indexes["cloud/tasks/storage/tests [tests chunk 1/1]"]
    )
    template = TRACE_HTML_TEMPLATE.read_text()
    script = TRACE_SCRIPT_TEMPLATE.read_text()
    assert "@@TRACE_SCRIPT@@" in template
    assert script in report
    assert "@@TRACE_" not in report
    assert "compile target" not in report


def test_reader_rejects_invalid_ids(tmp_path: Path) -> None:
    path = tmp_path / "bad.jsonl"
    path.write_text(
        json.dumps(
            {
                "resourceSpans": [
                    {
                        "scopeSpans": [
                            {
                                "spans": [
                                    {
                                        "traceId": "not-a-trace-id",
                                        "spanId": "2" * 16,
                                        "name": "bad",
                                        "startTimeUnixNano": "1",
                                        "endTimeUnixNano": "2",
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        )
        + "\n"
    )
    with pytest.raises(ValueError, match="Invalid OTLP JSON.*Invalid hex string"):
        read_otlp_jsonl([path])


def test_reader_limits_decompressed_input(tmp_path: Path) -> None:
    path = tmp_path / "compressed.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        stream.write(" " * 1_000 + "{}\n")
    assert path.stat().st_size < 200
    with pytest.raises(ValueError, match="Decoded OTLP input exceeds"):
        read_otlp_jsonl([path], max_input_bytes=200)


def test_span_ids_are_stable_and_have_otlp_lengths() -> None:
    assert stable_span_id("run", 1) == stable_span_id("run", 1)
    assert len(stable_span_id("run")) == 8
    assert len(stable_trace_id("run")) == 16


def test_ya_trace_produces_observed_and_inferred_test_spans(
    tmp_path: Path,
) -> None:
    trace_path = (
        tmp_path
        / "out"
        / "cloud/test/suite"
        / "test-results"
        / "py3test"
        / "ytest.report.trace"
    )
    trace_path.parent.mkdir(parents=True)
    events = [
        {
            "name": "subtest-started",
            "timestamp": 101.0,
            "value": {"class": "Suite", "subtest": "test_ok"},
        },
        {
            "name": "subtest-finished",
            "timestamp": 102.5,
            "value": {
                "class": "Suite",
                "subtest": "test_ok",
                "status": "good",
                "time": 1.5,
            },
        },
        {
            "name": "subtest-finished",
            "timestamp": 106.0,
            "value": {
                "class": "Suite",
                "subtest": "test_failed",
                "status": "fail",
                "time": 0.5,
            },
        },
        {
            "name": "chunk-event",
            "timestamp": 109.0,
            "value": {
                "chunk_index": 0,
                "nchunks": 1,
                "status": "fail",
                "metrics": {
                    "suite_start_timestamp": 100,
                    "suite_finish_timestamp": 110,
                    "suite_prepare_recipes_(seconds)": 2.25,
                },
            },
        },
    ]
    trace_path.write_text("".join(json.dumps(event) + "\n" for event in events))

    traces = load_ya_traces(tmp_path / "out")
    spans = build_ya_trace(
        traces,
        root_start_ns=ns(99_000_000_000),
        root_end_ns=ns(111_000_000_000),
        exit_code=1,
        resource=ResourceAttributes({"service.name": "test"}),
    )

    root, chunk = spans[:2]
    tests = {span.name: span for span in spans[2:]}
    observed = tests["Suite::test_ok"]
    inferred = tests["Suite::test_failed"]
    assert root.name == "ya make tests"
    assert span_status_code(root) == 2
    assert chunk.start_time_unix_nano == 100_000_000_000
    assert chunk.end_time_unix_nano == 110_000_000_000
    assert attributes(chunk)["ya.chunk.metric.suite_prepare_recipes_seconds"] == 2.25
    assert span_duration_ns(observed) == 1_500_000_000
    assert "test.timing.inferred" not in attributes(observed)
    assert span_duration_ns(inferred) == 500_000_000
    assert attributes(inferred)["test.timing.inferred"] is True
    assert span_status_code(inferred) == 2


def test_ya_trace_keeps_started_but_unfinished_test(
    tmp_path: Path,
) -> None:
    trace_path = tmp_path / "suite" / "test-results" / "runner" / "ytest.report.trace"
    trace_path.parent.mkdir(parents=True)
    trace_path.write_text(
        json.dumps(
            {
                "name": "subtest-started",
                "timestamp": 10,
                "value": {"class": "Suite", "subtest": "crashed"},
            }
        )
        + "\n"
    )
    spans = build_ya_trace(
        load_ya_traces(tmp_path),
        root_start_ns=ns(9_000_000_000),
        root_end_ns=ns(12_000_000_000),
        exit_code=137,
        resource=ResourceAttributes(),
    )
    test_span = spans[-1]
    assert test_span.end_time_unix_nano == 12_000_000_000
    assert span_status_code(test_span) == 2
    assert attributes(test_span)["test.incomplete"] is True


def test_ya_trace_preserves_chunks_and_anchors_finish_only_tests(
    tmp_path: Path,
) -> None:
    trace_path = tmp_path / "suite" / "test-results" / "unittest" / "ytest.report.trace"
    trace_path.parent.mkdir(parents=True)
    events = [
        {
            "name": "chunk-event",
            "timestamp": 120,
            "value": {
                "chunk_index": 0,
                "nchunks": 2,
                "metrics": {
                    "suite_finish_timestamp": 110,
                    "wall_time": 10,
                    "suite_delay_until_first_test_secs": 2,
                },
            },
        },
        {
            "name": "subtest-finished",
            "timestamp": 120,
            "value": {
                "class": "Suite",
                "subtest": "first",
                "status": "good",
                "time": 3,
                "chunk_index": 0,
                "nchunks": 2,
            },
        },
        {
            "name": "chunk-event",
            "timestamp": 120,
            "value": {
                "chunk_index": 1,
                "nchunks": 2,
                "metrics": {
                    "suite_finish_timestamp": 112,
                    "wall_time": 5,
                    "suite_delay_until_first_test_secs": 1,
                },
            },
        },
        {
            "name": "subtest-finished",
            "timestamp": 120,
            "value": {
                "class": "Suite",
                "subtest": "second",
                "status": "good",
                "time": 1,
                "chunk_index": 1,
                "nchunks": 2,
            },
        },
    ]
    trace_path.write_text("".join(json.dumps(event) + "\n" for event in events))

    spans = build_ya_trace(
        load_ya_traces(tmp_path),
        root_start_ns=ns(99_000_000_000),
        root_end_ns=ns(121_000_000_000),
        exit_code=0,
        resource=ResourceAttributes(),
    )

    chunks = list(spans.spans("ya.chunk"))
    tests = {span.name: span for span in spans.spans("ya.test")}
    assert len(chunks) == 2
    assert attributes(spans[0])["ya.chunk.count"] == 2
    assert tests["Suite::first"].start_time_unix_nano == 102_000_000_000
    assert span_duration_ns(tests["Suite::first"]) == 3_000_000_000
    assert tests["Suite::second"].start_time_unix_nano == 108_000_000_000
    assert span_duration_ns(tests["Suite::second"]) == 1_000_000_000
    assert (
        attributes(tests["Suite::first"])["test.timing.source"]
        == "chunk-delay-and-test-duration"
    )
    assert {test.parent_span_id for test in tests.values()} == {
        chunk.span_id for chunk in chunks
    }


def test_ya_evlog_adds_phases_and_build_nodes(
    tmp_path: Path,
) -> None:
    trace_path = (
        tmp_path / "out" / "suite" / "test-results" / "tests" / "ytest.report.trace"
    )
    trace_path.parent.mkdir(parents=True)
    trace_path.write_text(
        "".join(
            json.dumps(event) + "\n"
            for event in (
                {
                    "name": "subtest-finished",
                    "timestamp": 19.5,
                    "value": {
                        "class": "Suite",
                        "subtest": "critical_test",
                        "status": "good",
                        "time": 1,
                    },
                },
                {
                    "name": "chunk-event",
                    "timestamp": 20,
                    "value": {
                        "chunk_index": 0,
                        "nchunks": 1,
                        "metrics": {
                            "suite_start_timestamp": 18,
                            "suite_finish_timestamp": 20,
                        },
                    },
                },
            )
        )
    )
    evlog_path = tmp_path / "ya_evlog.jsonl"
    evlog_events = [
        {
            "namespace": "stages",
            "event": "stage-finished",
            "value": {
                "name": "build_graph_and_tests",
                "tag": "build_graph_and_tests",
                "time": [10, 12],
            },
        },
        {
            "namespace": "stages",
            "event": "stage-finished",
            "value": {
                "name": "dispatch_build",
                "tag": "dispatch_build",
                "time": [12, 30],
            },
        },
        {
            "namespace": "worker_threads",
            "event": "node-finished",
            "thread_name": "Worker-001",
            "value": {
                "name": ("FromCache(cacheuid$(BUILD_ROOT)/library/cached.a)"),
                "tag": "restore[AR]",
                "time": [12.5, 13],
            },
        },
        {
            "namespace": "worker_threads",
            "event": "node-finished",
            "thread_name": "Worker-002",
            "value": {
                "name": ("FromCache(loweruid$(BUILD_ROOT)/library/lower.a)"),
                "tag": "restore[ar]",
                "time": [12.6, 12.9],
            },
        },
        {
            "namespace": "worker_threads",
            "event": "node-finished",
            "thread_name": "Worker-003",
            "value": {
                "name": "Run(compileuid$(BUILD_ROOT)/suite/main.cpp.o)",
                "tag": "CC",
                "time": [13, 17],
            },
        },
        {
            "namespace": "worker_threads",
            "event": "node-finished",
            "value": {
                "name": "PutInCache(uid)",
                "tag": "put_in_cache[CC]",
                "time": [16.9, 17],
            },
        },
        {
            "namespace": "worker_threads",
            "event": "node-finished",
            "value": {
                "name": ("Run(uid$(BUILD_ROOT)/suite/test-results/tests/meta.json)"),
                "tag": "TM",
                "time": [18, 20],
            },
        },
        {
            "namespace": "dump_debug",
            "event": "log",
            "value": {
                "key": "stats",
                "value_type": "dict",
                "value": {
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
                    "execution_stages_msec": {
                        "build_only": 3_500,
                        "tests_only": 2_000,
                    },
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
                },
            },
        },
    ]
    evlog_path.write_text("".join(json.dumps(event) + "\n" for event in evlog_events))

    spans = build_ya_trace(
        load_ya_traces(tmp_path / "out"),
        root_start_ns=ns(9_000_000_000),
        root_end_ns=ns(31_000_000_000),
        exit_code=0,
        resource=ResourceAttributes(),
        evlog=load_ya_evlog(evlog_path),
    )

    root = spans[0]
    phases = {
        attributes(span)["ya.stage.name"]: span for span in spans.spans("ya.phase")
    }
    build = next(spans.spans("ya.build"))
    nodes = list(spans.spans("ya.build.node"))
    chunk = next(spans.spans("ya.chunk"))
    test = next(spans.spans("ya.test"))
    build_attributes = attributes(build)
    root_attributes = attributes(root)
    chunk_attributes = attributes(chunk)
    test_attributes = attributes(test)

    assert set(phases) == {"build_graph_and_tests", "dispatch_build"}
    assert build.parent_span_id == phases["dispatch_build"].span_id
    assert chunk.parent_span_id == phases["dispatch_build"].span_id
    assert span_duration_ns(build) == 4_500_000_000
    assert build_attributes["ya.build.node.count"] == 4
    assert build_attributes["ya.build.node.cache_store.count"] == 1
    assert build_attributes["ya.build.first_test_node_offset_seconds"] == 5.5
    assert build_attributes["ya.build.cache.considered_task.hit.ratio"] == 0.75
    assert build_attributes["ya.build.cache.considered_task.hit.count"] == 3
    assert build_attributes["ya.build.cache.considered_task.miss.count"] == 1
    assert build_attributes["ya.build.task.avoided.ratio"] == 0.96
    assert build_attributes["ya.build.task.reused_or_avoided.ratio"] == 0.99
    assert build_attributes["ya.build.cache.worker_node.hit.ratio"] == 2 / 3
    assert build_attributes["ya.build.task.avoided.count"] == 96
    assert build_attributes["ya.build.dist_cache.get.bytes"] == 1_024
    assert build_attributes["ya.build.execution.stage.build_only.seconds"] == 3.5
    assert build_attributes["ya.build.execution.total.seconds"] == 5.5
    assert build_attributes["ya.build.cache.tool.ar.hit.ratio"] == 1
    assert build_attributes["ya.build.cache.tool.ar.hit.count"] == 2
    assert build_attributes["ya.build.cache.tool.cc.hit.ratio"] == 0
    assert build_attributes["ya.build.critical_path.node.count"] == 1
    assert build_attributes["ya.build.critical_path.work.seconds"] == 4
    assert len(nodes) == 3
    assert {attributes(span)["ya.build.kind"] for span in nodes} == {
        "cache_restore",
        "execute",
    }
    cached = next(
        span for span in nodes if attributes(span)["ya.build.kind"] == "cache_restore"
    )
    assert attributes(cached)["ya.build.cache.hit"] is True
    assert attributes(cached)["ya.build.outputs"] == ["library/cached.a"]
    compiled = next(
        span for span in nodes if attributes(span)["ya.build.kind"] == "execute"
    )
    assert attributes(compiled)["ya.build.critical_path"] is True
    assert attributes(compiled)["ya.build.critical_path.index"] == 0
    assert attributes(compiled)["ya.build.critical_path.reported_seconds"] == 4
    assert root_attributes["ya.build.node.count"] == 4
    assert root_attributes["ya.build.node.span_count"] == 3
    assert root_attributes["ya.test.critical_path.entry.count"] == 1
    assert root_attributes["ya.test.critical_path.chunk.count"] == 1
    assert root_attributes["ya.test.critical_path.span.count"] == 1
    assert chunk_attributes["ya.test.critical_path"] is True
    assert chunk_attributes["ya.test.critical_path.granularity"] == "test-chunk"
    assert test_attributes["ya.test.critical_path"] is True
    assert test_attributes["ya.test.critical_path.inferred"] is True
    assert test_attributes["ya.test.critical_path.reported_seconds"] == 2

    build_only = build_ya_trace(
        [],
        root_start_ns=ns(9_000_000_000),
        root_end_ns=ns(31_000_000_000),
        exit_code=0,
        resource=ResourceAttributes(),
        evlog=load_ya_evlog(evlog_path),
        operation="build",
    )
    assert build_only[0].name == "ya make build"
    assert not any(build_only.spans("ya.chunk"))
    assert any(build_only.spans("ya.build"))


def test_workflow_trace_adds_queue_job_step_and_imported_ya_spans() -> None:
    workflow_run = {
        "id": 123,
        "run_number": 8,
        "run_attempt": 2,
        "name": "PR-check",
        "display_title": "A useful change",
        "path": ".github/workflows/pr.yaml",
        "event": "pull_request_target",
        "status": "completed",
        "conclusion": "success",
        "created_at": "2026-01-01T00:00:00Z",
        "run_started_at": "2026-01-01T00:00:01Z",
        "updated_at": "2026-01-01T00:00:10Z",
        "head_sha": "a" * 40,
        "head_branch": "users/test/change",
        "html_url": "https://github.example/runs/123",
        "repository": {"full_name": "example/repo"},
        "actor": {"login": "user"},
        "pull_requests": [{"number": 42}],
    }
    jobs = [
        WorkflowJob(
            SimpleNamespace(is_not_lazy=False),
            attributes={
                "id": 99,
                "name": "tests",
                "status": "completed",
                "conclusion": "success",
                "created_at": "2026-01-01T00:00:02Z",
                "started_at": "2026-01-01T00:00:03Z",
                "completed_at": "2026-01-01T00:00:09Z",
                "labels": ["self-hosted", "runner_light"],
                "runner_name": "runner-1",
                "runner_group_name": "light",
                "html_url": "https://github.example/runs/123/jobs/99",
                "steps": [
                    {
                        "number": 1,
                        "name": "Run tests",
                        "status": "completed",
                        "conclusion": "success",
                        "started_at": "2026-01-01T00:00:03Z",
                        "completed_at": "2026-01-01T00:00:09Z",
                    }
                ],
            },
            completed=True,
        )
    ]
    imported = make_trace(
        make_span(
            trace_id=bytes.fromhex("3" * 32),
            span_id=bytes.fromhex("4" * 16),
            name="ya make tests",
            start_ns=1_767_225_604_000_000_000,
            end_ns=1_767_225_608_000_000_000,
        ),
        resource=ResourceAttributes({"service.name": "nbs-ya-tests"}),
    )

    trace, metadata = build_workflow_trace(workflow_run, jobs, imported)

    by_name = {span.name: span for span in trace}
    assert span_duration_ns(by_name["workflow queue"]) == 1_000_000_000
    assert span_duration_ns(by_name["queued for runner"]) == 1_000_000_000
    assert by_name["step: Run tests"].parent_span_id == by_name["job: tests"].span_id
    assert by_name["ya make tests"].parent_span_id == by_name["job: tests"].span_id
    assert by_name["ya make tests"].trace_id == by_name["workflow: PR-check"].trace_id
    assert attributes(by_name["ya make tests"])["ci.trace.source"] == "ya-otlp"
    assert metadata["github.pull_request.number"] == [42]


def test_s3_trace_discovery_accepts_an_empty_prefix(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(stdout="null\n")

    monkeypatch.setattr("scripts.workflow_trace_report.subprocess.run", fake_run)
    assert (
        download_s3_trace_inputs(
            bucket="reports",
            prefix="reports/example/repo/pr.yaml/1/1",
            output_dir=tmp_path,
        )
        == []
    )
    assert calls[0][0][:3] == ["aws", "s3api", "list-objects-v2"]
