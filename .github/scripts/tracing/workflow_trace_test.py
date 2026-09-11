from __future__ import annotations

from types import SimpleNamespace

from github.WorkflowJob import WorkflowJob

from scripts.tracing.otlp import (
    Ns,
    ResourceAttributes,
    Trace,
    decode_attributes,
    make_span,
    span_duration_ns,
    span_status_code,
    span_status_message,
)
from scripts.tracing.workflow_trace import build_workflow_trace
from scripts.tracing.workflow_trace_report import download_s3_trace_inputs


def _workflow_run() -> dict:
    return {
        "id": 123,
        "run_number": 8,
        "run_attempt": 1,
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


def _jobs(
    *, conclusion: str = "success", step_conclusion: str = "success"
) -> list[WorkflowJob]:
    return [
        WorkflowJob(
            SimpleNamespace(is_not_lazy=False),
            attributes={
                "id": 99,
                "name": "tests",
                "status": "completed",
                "conclusion": conclusion,
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
                        "conclusion": step_conclusion,
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
            start_ns=Ns(1_767_225_604_000_000_000),
            end_ns=Ns(1_767_225_608_000_000_000),
        ),
        resource=ResourceAttributes(
            {
                "service.name": "nbs-ya-tests",
                "ci.artifact.test_log.url_prefix": "https://reports.example/logs/a/",
                "ci.artifact.test_data.url_prefix": "https://reports.example/data/a/",
            }
        ),
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
    assert span_duration_ns(by_name["workflow queue"]) == Ns(1_000_000_000)
    assert span_duration_ns(by_name["queued for runner"]) == Ns(1_000_000_000)
    assert by_name["step: Run tests"].parent_span_id == by_name["job: tests"].span_id
    assert by_name["ya make tests"].parent_span_id == by_name["job: tests"].span_id
    assert by_name["ya make tests"].trace_id == by_name["workflow: PR-check"].trace_id
    assert (
        decode_attributes(by_name["ya make tests"].attributes)["ci.trace.source"]
        == "ya-otlp"
    )
    resources = {
        span.name: decode_attributes(resource.attributes)
        for resource, _, span in trace.walk()
    }
    assert (
        resources["ya make tests"]["ci.artifact.test_log.url_prefix"]
        == "https://reports.example/logs/a/"
    )
    assert metadata["github.pull_request.number"] == [42]


def test_rerun_timeline_ignores_original_creation_time() -> None:
    workflow_run = _workflow_run()
    workflow_run.update(
        {
            "run_attempt": 2,
            "created_at": "2025-12-30T00:00:00Z",
        }
    )

    trace, _ = build_workflow_trace(workflow_run, _jobs(), _imported_trace())

    by_name = {span.name: span for span in trace}
    assert "workflow queue" not in by_name
    assert span_duration_ns(by_name["workflow: PR-check"]) == Ns(9_000_000_000)


def test_rerun_uses_current_job_when_run_start_is_missing() -> None:
    workflow_run = _workflow_run()
    workflow_run.update(
        {
            "run_attempt": 2,
            "created_at": "2025-12-30T00:00:00Z",
            "run_started_at": None,
        }
    )

    trace, _ = build_workflow_trace(workflow_run, _jobs(), Trace())

    root = next(span for span in trace if span.name == "workflow: PR-check")
    assert span_duration_ns(root) == Ns(8_000_000_000)


def test_rerun_uses_imported_span_when_run_and_job_timing_are_missing() -> None:
    workflow_run = _workflow_run()
    workflow_run.update(
        {
            "run_attempt": 2,
            "created_at": "2025-12-30T00:00:00Z",
            "run_started_at": None,
        }
    )

    trace, _ = build_workflow_trace(workflow_run, [], _imported_trace())

    root = next(span for span in trace if span.name == "workflow: PR-check")
    assert span_duration_ns(root) == Ns(6_000_000_000)
    assert (
        decode_attributes(root.attributes)["ci.workflow.timing_source"]
        == "imported_span.start_time_unix_nano"
    )


def test_rerun_without_current_timing_evidence_falls_back_to_updated_at() -> None:
    workflow_run = _workflow_run()
    workflow_run.update(
        {
            "run_attempt": 2,
            "created_at": "2025-12-30T00:00:00Z",
            "run_started_at": None,
        }
    )

    trace, _ = build_workflow_trace(workflow_run, [], Trace())

    root = next(span for span in trace if span.name == "workflow: PR-check")
    assert root.start_time_unix_nano == root.end_time_unix_nano
    assert root.end_time_unix_nano == 1_767_225_610_000_000_000
    assert (
        decode_attributes(root.attributes)["ci.workflow.timing_source"]
        == "workflow_run.updated_at"
    )


def test_stale_workflow_job_and_step_have_error_status() -> None:
    workflow_run = _workflow_run()
    workflow_run["conclusion"] = "stale"

    trace, _ = build_workflow_trace(
        workflow_run,
        _jobs(conclusion="stale", step_conclusion="stale"),
        Trace(),
    )
    by_name = {span.name: span for span in trace}

    for name in ("workflow: PR-check", "job: tests", "step: Run tests"):
        assert span_status_code(by_name[name]) == 2
        assert span_status_message(by_name[name]) == "stale"


def test_neutral_and_skipped_conclusions_remain_non_errors() -> None:
    workflow_run = _workflow_run()
    workflow_run["conclusion"] = "neutral"

    trace, _ = build_workflow_trace(
        workflow_run,
        _jobs(conclusion="skipped", step_conclusion="neutral"),
        Trace(),
    )
    by_name = {span.name: span for span in trace}

    for name in ("workflow: PR-check", "job: tests", "step: Run tests"):
        assert span_status_code(by_name[name]) == 0
        assert span_status_message(by_name[name]) == ""


def test_s3_trace_discovery_accepts_an_empty_inventory(
    tmp_path,
) -> None:
    calls = []
    operations = []

    paginator = SimpleNamespace(
        paginate=lambda **kwargs: calls.append(kwargs) or [{}],
    )

    def get_paginator(operation):
        operations.append(operation)
        return paginator

    s3 = SimpleNamespace(
        get_paginator=get_paginator,
    )

    assert (
        download_s3_trace_inputs(
            bucket="reports",
            prefix="reports/example/repo/nightly.yaml/1/1",
            output_dir=tmp_path,
            s3_client=s3,
        )
        == []
    )
    assert calls == [
        {
            "Bucket": "reports",
            "Prefix": "reports/example/repo/nightly.yaml/1/1/",
        }
    ]
    assert operations == ["list_objects_v2"]


def test_s3_trace_discovery_downloads_matching_objects_from_all_pages(
    tmp_path,
) -> None:
    prefix = "reports/example/repo/nightly.yaml/1/1/"
    pages = [
        {
            "Contents": [
                {"Key": f"{prefix}job-a/trace.otlp.jsonl.gz", "Size": 3},
                {"Key": f"{prefix}job-a/test.html", "Size": 100},
            ]
        },
        {
            "Contents": [
                {"Key": f"{prefix}job-b/trace.otlp.jsonl.gz", "Size": 4},
            ]
        },
    ]
    downloads = []
    list_requests = []

    def download_file(bucket, key, destination):
        downloads.append((bucket, key, destination))

    def paginate(**kwargs):
        list_requests.append(kwargs)
        return pages

    def get_paginator(operation):
        assert operation == "list_objects_v2"
        return SimpleNamespace(paginate=paginate)

    s3 = SimpleNamespace(
        get_paginator=get_paginator,
        download_file=download_file,
    )

    inputs = download_s3_trace_inputs(
        bucket="reports",
        prefix=prefix,
        output_dir=tmp_path,
        max_input_bytes=7,
        s3_client=s3,
    )

    assert inputs == [
        tmp_path / "00000.otlp.jsonl.gz",
        tmp_path / "00001.otlp.jsonl.gz",
    ]
    assert list_requests == [{"Bucket": "reports", "Prefix": prefix}]
    assert [(bucket, key) for bucket, key, _ in downloads] == [
        ("reports", f"{prefix}job-a/trace.otlp.jsonl.gz"),
        ("reports", f"{prefix}job-b/trace.otlp.jsonl.gz"),
    ]
