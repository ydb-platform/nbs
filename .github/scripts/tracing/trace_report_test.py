from __future__ import annotations

import base64
import gzip
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from github.WorkflowJob import WorkflowJob
from jinja2 import UndefinedError
from opentelemetry.proto_json.trace.v1.trace import TracesData

from scripts.tracing.ya_trace_report import (
    build_resource_attributes,
    build_ya_trace,
)
from scripts.tracing.otlp import (
    ResourceAttributes,
    Span,
    Trace,
    decode_attributes,
    make_event,
    make_span as new_span,
    Ns,
    span_duration_ns,
    stable_span_id,
    stable_trace_id,
)
from scripts.tracing.trace_report import (
    TRACE_HTML_TEMPLATE,
    TRACE_SCRIPT_TEMPLATE,
    TRACE_TEMPLATE_ENV,
    _trace_model,
    read_otlp_jsonl,
    render_html,
    write_trace_bundle,
)
from scripts.tracing.yatrace import YaTraceCollection, load_ya_evlog
from scripts.tracing.workflow_trace_report import (
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
        metadata={
            "github.sha": "<sha>",
            "github.run.url": "https://github.example/run?name=<trace>",
        },
    )

    restored = read_otlp_jsonl([tmp_path / manifest["otlp_file"]])
    assert restored.to_dict() == trace.to_dict()
    assert isinstance(restored[0].start_time_unix_nano, Ns)
    assert isinstance(restored[0].events[0].time_unix_nano, Ns)

    report = (tmp_path / manifest["html_file"]).read_text()
    assert "<script>alert" not in report
    assert "&lt;sha&gt;" in report
    assert (
        'href="https://github.example/run?name=&lt;trace&gt;"'
        ">https://github.example/run?name=&lt;trace&gt;</a>"
    ) in report
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
    report = render_html(trace)
    assert 'id="trace-data"' in report
    assert "Rows per load" in report
    assert '<select id="row-load-size" disabled>' in report
    assert '<option value="200" selected>200</option>' in report
    assert '<option value="1000">1,000</option>' in report
    assert '<option value="5000">5,000</option>' in report
    assert 'id="load-rows"' in report
    assert "Load next 200 rows" in report
    assert 'id="row-status"' in report
    assert 'role="status" aria-live="polite"' in report
    assert "All (may be slow)" not in report
    assert "No spans found" in render_html(Trace())


def test_trace_template_rejects_missing_context() -> None:
    template = TRACE_TEMPLATE_ENV.get_template(TRACE_HTML_TEMPLATE.name)
    with pytest.raises(UndefinedError):
        template.render()


def test_trace_script_is_safe_to_embed() -> None:
    assert "</script" not in TRACE_SCRIPT_TEMPLATE.read_text().lower()


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


def test_renderer_collapses_generated_ya_groups_by_default(tmp_path: Path) -> None:
    trace_path = (
        tmp_path
        / "out"
        / "cloud/tasks/storage/tests"
        / "test-results"
        / "unittest"
        / "ytest.report.trace"
    )
    trace_path.parent.mkdir(parents=True)
    trace_path.write_text(
        "".join(
            json.dumps(event) + "\n"
            for event in (
                {
                    "name": "subtest-finished",
                    "timestamp": 6,
                    "value": {
                        "class": "TestSuite",
                        "subtest": "test_case",
                        "status": "good",
                        "time": 1,
                    },
                },
                {
                    "name": "subtest-started",
                    "timestamp": 6.5,
                    "value": {
                        "class": "TestSuite",
                        "subtest": "unfinished",
                    },
                },
                {
                    "name": "chunk-event",
                    "timestamp": 7,
                    "value": {
                        "chunk_index": 0,
                        "nchunks": 1,
                        "metrics": {
                            "suite_start_timestamp": 4,
                            "suite_finish_timestamp": 7,
                            "suite_prepare_recipes_(seconds)": 0.25,
                        },
                    },
                },
            )
        )
    )
    evlog_path = tmp_path / "ya_evlog.jsonl"
    evlog_path.write_text(
        "".join(
            json.dumps(event) + "\n"
            for event in (
                {
                    "namespace": "stages",
                    "event": "stage-finished",
                    "value": {
                        "name": "dispatch_build",
                        "tag": "dispatch_build",
                        "time": [1, 8],
                    },
                },
                {
                    "namespace": "worker_threads",
                    "event": "node-finished",
                    "value": {
                        "name": "Run(uid$(BUILD_ROOT)/output.o)",
                        "tag": "CC",
                        "time": [2, 3],
                    },
                },
            )
        )
    )
    trace = build_ya_trace(
        YaTraceCollection.load(tmp_path / "out").traces,
        root_start_ns=Ns(0),
        root_end_ns=Ns.from_s(9),
        exit_code=0,
        resource=ResourceAttributes(),
        evlog=load_ya_evlog(evlog_path),
    )

    model = _trace_model(trace)
    encoded = {span[2]: span for span in model["s"]}
    indexes = {span[2]: index for index, span in enumerate(model["s"])}
    report = render_html(trace)
    chunk_name = "cloud/tasks/storage/tests [unittest chunk 1/1]"
    phase_name = next(trace.spans("ya.phase")).name
    assert encoded["build operations"][1] == indexes[phase_name]
    assert encoded["CC: output.o"][1] == indexes["build operations"]
    assert encoded[chunk_name][1] == indexes[phase_name]
    assert encoded["TestSuite::test_case"][1] == indexes[chunk_name]
    assert encoded["TestSuite::unfinished"][1] == indexes[chunk_name]
    assert encoded["TestSuite::test_case"][5]["test.timing.inferred"] is True
    assert encoded["TestSuite::unfinished"][5]["test.incomplete"] is True
    assert (
        encoded[chunk_name][5]["ya.chunk.metric.suite_prepare_recipes_seconds"] == 0.25
    )
    assert {"ya.build", "ya.build.node", "ya.chunk", "ya.test"} <= set(model["c"])
    template = TRACE_HTML_TEMPLATE.read_text()
    script = TRACE_SCRIPT_TEMPLATE.read_text()
    assert '{% include "trace_report.js" %}' in template
    assert script in report
    assert '{% include "trace_report.js" %}' not in report
    assert "CC: output.o" not in report


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

    monkeypatch.setattr(
        "scripts.tracing.workflow_trace_report.subprocess.run",
        fake_run,
    )
    assert (
        download_s3_trace_inputs(
            bucket="reports",
            prefix="reports/example/repo/pr.yaml/1/1",
            output_dir=tmp_path,
        )
        == []
    )
    assert calls[0][0][:3] == ["aws", "s3api", "list-objects-v2"]
