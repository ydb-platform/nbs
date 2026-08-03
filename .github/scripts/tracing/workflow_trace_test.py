from __future__ import annotations

from types import SimpleNamespace

from github.WorkflowJob import WorkflowJob

from scripts.tracing.otlp import (
    ResourceAttributes,
    Trace,
    decode_attributes,
    make_span,
    span_duration_ns,
)
from scripts.tracing.workflow_trace import build_workflow_trace
from scripts.tracing.workflow_trace_report import download_s3_trace_inputs


def _workflow_run() -> dict:
    return {
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


def _jobs() -> list[WorkflowJob]:
    return [
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


def _imported_trace() -> Trace:
    trace = Trace()
    trace.add_span(
        make_span(
            trace_id=bytes.fromhex("3" * 32),
            span_id=bytes.fromhex("4" * 16),
            name="ya make tests",
            start_ns=1_767_225_604_000_000_000,
            end_ns=1_767_225_608_000_000_000,
        ),
        resource=ResourceAttributes({"service.name": "nbs-ya-tests"}),
        scope_name="nbs.ci",
    )
    return trace


def test_workflow_projection_from_github_jobs_and_imported_trace() -> None:
    trace, metadata = build_workflow_trace(
        _workflow_run(),
        _jobs(),
        _imported_trace(),
    )

    by_name = {span.name: span for span in trace}
    assert len(trace) == 6
    assert span_duration_ns(by_name["workflow queue"]) == 1_000_000_000
    assert span_duration_ns(by_name["queued for runner"]) == 1_000_000_000
    assert by_name["step: Run tests"].parent_span_id == by_name["job: tests"].span_id
    assert by_name["ya make tests"].parent_span_id == by_name["job: tests"].span_id
    assert by_name["ya make tests"].trace_id == by_name["workflow: PR-check"].trace_id
    assert (
        decode_attributes(by_name["ya make tests"].attributes)["ci.trace.source"]
        == "ya-otlp"
    )
    assert metadata["github.pull_request.number"] == [42]


def test_s3_trace_discovery_accepts_an_empty_inventory(
    monkeypatch,
    tmp_path,
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
            prefix="reports/example/repo/nightly.yaml/1/1",
            output_dir=tmp_path,
        )
        == []
    )
    assert calls[0][0][:3] == ["aws", "s3api", "list-objects-v2"]
