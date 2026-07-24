from __future__ import annotations

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.tests.ya_trace_report import build_ya_spans, load_ya_traces
from scripts.trace_report import (
    Span,
    SpanEvent,
    read_otlp_jsonl,
    render_html,
    stable_hex_id,
    write_trace_bundle,
)
from scripts.workflow_trace_report import (
    build_workflow_spans,
    download_s3_trace_inputs,
)


def make_span(**kwargs) -> Span:
    values = {
        "trace_id": "1" * 32,
        "span_id": "2" * 16,
        "name": "root",
        "start_ns": 1_000,
        "end_ns": 2_000,
    }
    values.update(kwargs)
    return Span(**values)


def test_otlp_round_trip_and_static_html_escaping(tmp_path: Path) -> None:
    spans = [
        make_span(
            attributes={
                "string": '<script>alert("attribute")</script>',
                "integer": 42,
                "boolean": True,
                "array": ["one", "two"],
            },
            events=[
                SpanEvent(
                    name="<event>",
                    time_ns=1_500,
                    attributes={"detail": "<b>unsafe</b>"},
                )
            ],
            resource_attributes={"service.name": "test"},
            status_code=2,
            status_message="<failed>",
        )
    ]

    manifest = write_trace_bundle(
        tmp_path,
        spans,
        title="<trace>",
        metadata={"github.sha": "<sha>"},
    )

    restored = read_otlp_jsonl([tmp_path / manifest["otlp_file"]])
    assert restored == spans

    report = (tmp_path / manifest["html_file"]).read_text()
    assert "<script>alert" not in report
    assert "&lt;script&gt;alert" in report
    assert "<h1>&lt;trace&gt;</h1>" in report
    assert "default-src &#x27;none&#x27;" not in report
    assert manifest["span_count"] == 1


def test_renderer_marks_error_spans_and_supports_empty_input() -> None:
    assert 'class="span error"' in render_html([make_span(status_code=2)])
    assert "No spans found" in render_html([])


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
    with pytest.raises(ValueError, match="Invalid OTLP span IDs"):
        read_otlp_jsonl([path])


def test_reader_limits_decompressed_input(tmp_path: Path) -> None:
    path = tmp_path / "compressed.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        stream.write(" " * 1_000 + "{}\n")
    assert path.stat().st_size < 200
    with pytest.raises(ValueError, match="Decoded OTLP input exceeds"):
        read_otlp_jsonl([path], max_input_bytes=200)


def test_stable_hex_id_is_deterministic() -> None:
    assert stable_hex_id("run", 1, length=16) == stable_hex_id(
        "run", 1, length=16
    )
    assert len(stable_hex_id("run", length=32)) == 32


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
    spans = build_ya_spans(
        traces,
        root_start_ns=99_000_000_000,
        root_end_ns=111_000_000_000,
        exit_code=1,
        resource={"service.name": "test"},
    )

    root, chunk = spans[:2]
    tests = {span.name: span for span in spans[2:]}
    observed = tests["Suite::test_ok"]
    inferred = tests["Suite::test_failed"]
    assert root.name == "ya make tests"
    assert root.status_code == 2
    assert chunk.start_ns == 100_000_000_000
    assert chunk.end_ns == 110_000_000_000
    assert (
        chunk.attributes[
            "ya.chunk.metric.suite_prepare_recipes_seconds"
        ]
        == 2.25
    )
    assert observed.duration_ns == 1_500_000_000
    assert "test.timing.inferred" not in observed.attributes
    assert inferred.duration_ns == 500_000_000
    assert inferred.attributes["test.timing.inferred"] is True
    assert inferred.status_code == 2


def test_ya_trace_keeps_started_but_unfinished_test(
    tmp_path: Path,
) -> None:
    trace_path = (
        tmp_path
        / "suite"
        / "test-results"
        / "runner"
        / "ytest.report.trace"
    )
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
    spans = build_ya_spans(
        load_ya_traces(tmp_path),
        root_start_ns=9_000_000_000,
        root_end_ns=12_000_000_000,
        exit_code=137,
        resource={},
    )
    test_span = spans[-1]
    assert test_span.end_ns == 12_000_000_000
    assert test_span.status_code == 2
    assert test_span.attributes["test.incomplete"] is True


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
        {
            "id": 99,
            "name": "tests",
            "status": "completed",
            "conclusion": "success",
            "created_at": "2026-01-01T00:00:02Z",
            "started_at": "2026-01-01T00:00:03Z",
            "completed_at": "2026-01-01T00:00:09Z",
            "labels": ["self-hosted", "runner_light"],
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
        }
    ]
    imported = [
        make_span(
            trace_id="3" * 32,
            span_id="4" * 16,
            name="ya make tests",
            start_ns=1_767_225_604_000_000_000,
            end_ns=1_767_225_608_000_000_000,
            resource_attributes={"service.name": "nbs-ya-tests"},
        )
    ]

    spans, metadata = build_workflow_spans(workflow_run, jobs, imported)

    by_name = {span.name: span for span in spans}
    assert by_name["workflow queue"].duration_ns == 1_000_000_000
    assert by_name["queued for runner"].duration_ns == 1_000_000_000
    assert by_name["step: Run tests"].parent_span_id == by_name["job: tests"].span_id
    assert by_name["ya make tests"].parent_span_id == by_name["job: tests"].span_id
    assert by_name["ya make tests"].trace_id == by_name["workflow: PR-check"].trace_id
    assert by_name["ya make tests"].attributes["ci.trace.source"] == "ya-otlp"
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
