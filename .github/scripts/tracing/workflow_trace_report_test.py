from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from github.WorkflowJob import WorkflowJob

from scripts.tracing.otlp import (
    Ns,
    ResourceAttributes,
    Span,
    Trace,
    decode_attributes,
    make_span,
    span_duration_ns,
)
from scripts.tracing.workflow_trace import (
    _bounded_times,
    _parent_job,
    build_workflow_trace,
    timestamp_ns,
)
from scripts.tracing.workflow_trace_report import download_s3_trace_inputs


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


@pytest.mark.parametrize(
    "value",
    [
        "2026-01-01T00:00:00.000001Z",
        "2026-01-01T01:00:00.000001+01:00",
    ],
)
def test_github_timestamp_conversion_preserves_microseconds(value: str) -> None:
    assert timestamp_ns(value) == 1_767_225_600_000_001_000


def test_bounded_times_preserve_zero_timestamps() -> None:
    assert _bounded_times(Ns(0), Ns(0), Ns(10), Ns(20)) == (Ns(0), Ns(0))


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


def test_imported_span_touching_job_endpoints_has_no_job_parent() -> None:
    imported = make_span(
        trace_id=bytes.fromhex("1" * 32),
        span_id=bytes.fromhex("1" * 16),
        name="imported",
        start_ns=10,
        end_ns=20,
    )
    jobs = [
        make_span(
            trace_id=bytes.fromhex("2" * 32),
            span_id=bytes.fromhex(f"{index}" * 16),
            name=f"job {index}",
            start_ns=start,
            end_ns=end,
        )
        for index, (start, end) in enumerate(((0, 10), (20, 30)), start=2)
    ]

    assert _parent_job(imported, jobs) is None


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
