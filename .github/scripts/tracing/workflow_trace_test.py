from __future__ import annotations

import json
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from github.WorkflowJob import WorkflowJob

from scripts.tracing.otlp import (
    Ns,
    decode_attributes,
    span_duration_ns,
    span_status_code,
    span_status_message,
)
from scripts.tracing.workflow_trace import build_workflow_trace
from scripts.tracing.trace_io import read_otlp_jsonl
from scripts.tracing import workflow_trace_report as cli
from scripts.tracing import workflow_trace_links as discovery


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


def test_workflow_projection_contains_only_github_spans_and_report_links() -> None:
    trace, metadata = build_workflow_trace(
        _workflow_run(),
        _jobs(),
        report_links={
            99: [("Build trace", "https://reports.example/build/trace.html")]
        },
    )

    by_name = {span.name: span for span in trace}
    assert set(by_name) == {
        "workflow: PR-check",
        "workflow queue",
        "job: tests",
        "queued for runner",
        "step: Run tests",
    }
    assert span_duration_ns(by_name["workflow queue"]) == Ns(1_000_000_000)
    assert span_duration_ns(by_name["queued for runner"]) == Ns(1_000_000_000)
    assert by_name["step: Run tests"].parent_span_id == by_name["job: tests"].span_id
    attributes = decode_attributes(by_name["job: tests"].attributes)
    assert attributes["ci.trace.report.0.name"] == "Build trace"
    assert (
        attributes["ci.trace.report.0.url"]
        == "https://reports.example/build/trace.html"
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

    trace, _ = build_workflow_trace(workflow_run, _jobs())

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

    trace, _ = build_workflow_trace(workflow_run, _jobs())

    root = next(span for span in trace if span.name == "workflow: PR-check")
    assert span_duration_ns(root) == Ns(8_000_000_000)


@pytest.mark.parametrize("run_attempt", [1, 2])
def test_missing_start_timing_falls_back_to_updated_at(run_attempt) -> None:
    workflow_run = _workflow_run()
    workflow_run.update(
        {
            "run_attempt": run_attempt,
            "created_at": None,
            "run_started_at": None,
        }
    )

    trace, _ = build_workflow_trace(workflow_run, [])

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
    )
    by_name = {span.name: span for span in trace}

    for name in ("workflow: PR-check", "job: tests", "step: Run tests"):
        assert span_status_code(by_name[name]) == 0
        assert span_status_message(by_name[name]) == ""


def _manifest(**metadata) -> bytes:
    return json.dumps(
        {
            "schema": "nbs-otlp-trace-bundle",
            "html_file": "trace.html",
            "metadata": {
                "github.repository": "example/repo",
                "github.run.id": 123,
                "github.run.attempt": 1,
                "github.runner.name": "runner-1",
                "ci.ya.operation": "tests",
                "ci.ya.retry": 1,
                **metadata,
            },
        }
    ).encode()


PREFIX = "reports/example/repo/pr.yaml/123/1/"


def _s3(objects):
    requests = []

    def paginate(**kwargs):
        assert kwargs == {"Bucket": "reports", "Prefix": PREFIX}
        for key, payload in objects.items():
            yield {"Contents": [{"Key": PREFIX + key, "Size": len(payload)}]}

    def get_object(*, Bucket, Key):
        assert Bucket == "reports"
        requests.append(Key)
        return {"Body": BytesIO(objects[Key.removeprefix(PREFIX)])}

    def get_paginator(operation):
        assert operation == "list_objects_v2"
        return SimpleNamespace(paginate=paginate)

    return SimpleNamespace(get_paginator=get_paginator, get_object=get_object), requests


def _load_links(s3, jobs=None):
    return discovery.load_ya_report_links(
        s3_client=s3,
        bucket="reports",
        prefix=PREFIX,
        website_suffix="example.test",
        workflow_run=_workflow_run(),
        jobs=_jobs() if jobs is None else jobs,
    )


def test_parallel_jobs_link_by_runner_and_discovery_never_reads_otlp() -> None:
    first = _jobs()[0]
    second = WorkflowJob(
        SimpleNamespace(is_not_lazy=False),
        attributes={
            **first.raw_data,
            "id": 100,
            "name": "other parallel job",
            "runner_name": "runner-2",
            "created_at": "2026-01-01T00:00:03Z",
            "completed_at": "2026-01-01T00:00:08Z",
        },
        completed=True,
    )
    s3, requests = _s3(
        {
            "variants/x86/1/trace.manifest.json": _manifest(),
            "variants/x86/1/trace.otlp.jsonl.gz": b"invalid OTLP",
            "variants/arm/build/trace.manifest.json": _manifest(
                **{
                    "github.runner.name": "runner-2",
                    "ci.ya.operation": "build",
                }
            ),
        }
    )
    jobs = [first, second]
    links = _load_links(s3, jobs)
    trace, _ = build_workflow_trace(_workflow_run(), jobs, report_links=links)
    attributes = {span.name: decode_attributes(span.attributes) for span in trace}
    assert attributes["job: tests"]["ci.trace.report.0.url"].endswith(
        "/variants/x86/1/trace.html"
    )
    assert attributes["job: other parallel job"]["ci.trace.report.0.url"].endswith(
        "/variants/arm/build/trace.html"
    )
    assert links[99][0][0] == "Tests — attempt 1"
    assert links[100][0][0] == "Build trace"
    assert len(requests) == 2
    assert all(key.endswith("trace.manifest.json") for key in requests)


@pytest.mark.parametrize(
    "metadata",
    [
        {"github.runner.name": ""},
        {"github.runner.name": "unknown-runner"},
        {"github.repository": "other/repo"},
        {"github.run.id": 456},
        {"github.run.attempt": 2},
    ],
)
def test_unmatched_manifests_are_not_attached(metadata) -> None:
    s3, _ = _s3({"1/trace.manifest.json": _manifest(**metadata)})
    assert _load_links(s3) == {}


def test_reused_runner_is_ambiguous_without_guessing_from_timestamps() -> None:
    first = _jobs()[0]
    second = WorkflowJob(
        SimpleNamespace(is_not_lazy=False),
        attributes={**first.raw_data, "id": 100, "name": "another job"},
        completed=True,
    )
    s3, _ = _s3({"1/trace.manifest.json": _manifest()})
    assert _load_links(s3, [first, second]) == {}


def test_bad_and_oversized_manifests_do_not_hide_valid_reports() -> None:
    s3, requests = _s3(
        {
            "bad/trace.manifest.json": b"{invalid json}",
            "deep/trace.manifest.json": b"[" * 2000 + b"]" * 2000,
            "large/trace.manifest.json": b" " * (discovery.MAX_MANIFEST_BYTES + 1),
            "good/trace.manifest.json": _manifest(),
        }
    )
    links = _load_links(s3)
    assert links[99] == [
        ("Tests — attempt 1", f"https://reports.example.test/{PREFIX}good/trace.html")
    ]
    assert not any("/large/" in key for key in requests)


def test_manifest_object_limit_preserves_discovered_links(monkeypatch) -> None:
    monkeypatch.setattr(discovery, "MAX_MANIFESTS", 1)
    s3, requests = _s3(
        {
            "1/trace.manifest.json": _manifest(),
            "2/trace.manifest.json": _manifest(**{"ci.ya.retry": 2}),
        }
    )
    assert len(_load_links(s3)[99]) == 1
    assert len(requests) == 1


@pytest.mark.parametrize("failure", ["client", "listing", "download", "manifest"])
def test_cli_writes_workflow_bundle_when_optional_s3_discovery_fails(
    tmp_path, monkeypatch, failure
) -> None:
    event = tmp_path / "event.json"
    event.write_text(
        json.dumps(
            {
                "workflow_run": _workflow_run(),
                "repository": {"full_name": "example/repo"},
            }
        )
    )
    output = tmp_path / "report"
    monkeypatch.setattr(
        "sys.argv",
        [
            "workflow-trace",
            "--event",
            str(event),
            "--output-dir",
            str(output),
            "--github-token",
            "test-token",
            "--s3-bucket",
            "reports",
            "--s3-prefix",
            PREFIX,
            "--s3-website-suffix",
            "example.test",
        ],
    )
    monkeypatch.setattr(cli, "github_client", Mock(return_value=None))
    monkeypatch.setattr(cli, "get_jobs", Mock(return_value=_jobs()))
    s3, _ = _s3({"bad/trace.manifest.json": b"invalid"})

    fail = Mock(
        side_effect=ClientError({"Error": {"Code": "AccessDenied"}}, "ListObjectsV2")
    )

    monkeypatch.setattr(
        cli.boto3,
        "client",
        fail if failure == "client" else Mock(return_value=s3),
    )
    if failure == "listing":
        s3.get_paginator = fail
    if failure == "download":
        s3.get_object = fail
    cli.main()

    trace = read_otlp_jsonl([output / "workflow-trace.otlp.jsonl.gz"])
    assert len(trace) == 5
    assert (output / "workflow-trace.html").is_file()
    assert (output / "workflow-trace.manifest.json").is_file()
