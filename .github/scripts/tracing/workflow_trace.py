"""Project GitHub workflow timing with links to separate execution reports."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from github.WorkflowJob import WorkflowJob

from .otlp import (
    Interval,
    Ns,
    ResourceAttributes,
    SECOND,
    Trace,
    stable_trace_id,
)

from .projector import SpanProjector

ERROR_CONCLUSIONS = frozenset(
    {
        "action_required",
        "cancelled",
        "failure",
        "stale",
        "startup_failure",
        "timed_out",
    }
)
UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def timestamp_ns(value: Any) -> Ns | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        delta = parsed.astimezone(timezone.utc) - UNIX_EPOCH
        return Ns(
            (delta.days * 86_400 + delta.seconds) * SECOND.value
            + delta.microseconds * 1_000
        )
    except ValueError:
        return None


def _status(conclusion: Any) -> tuple[int, str]:
    value = str(conclusion or "")
    normalized = value.lower()
    code = 1 if normalized == "success" else 2 if normalized in ERROR_CONCLUSIONS else 0
    return code, value if code == 2 else ""


def _interval(
    start: Ns | None,
    end: Ns | None,
    default: Interval,
) -> Interval:
    start_ns = default.start if start is None else start
    end_ns = default.end if end is None else end
    return Interval(start_ns, max(start_ns, end_ns))


def _workflow_start(
    workflow_run: Mapping[str, Any],
    jobs: Sequence[WorkflowJob],
    *,
    created_ns: Ns | None,
    started_ns: Ns | None,
    updated_ns: Ns,
) -> tuple[Ns, str]:
    try:
        run_attempt = max(1, int(workflow_run.get("run_attempt", 1)))
    except (TypeError, ValueError):
        run_attempt = 1
    if run_attempt == 1:
        if created_ns is not None:
            return created_ns, "workflow_run.created_at"
        if started_ns is not None:
            return started_ns, "workflow_run.run_started_at"

    candidates: list[tuple[Ns, str]] = []

    def add_candidate(value: Ns | None, source: str) -> None:
        if value is not None and Ns(0) < value <= updated_ns:
            candidates.append((value, source))

    add_candidate(started_ns, "workflow_run.run_started_at")
    for job in jobs:
        add_candidate(timestamp_ns(job.created_at), "job.created_at")
        add_candidate(timestamp_ns(job.started_at), "job.started_at")
    return min(
        candidates,
        key=lambda item: item[0],
        default=(updated_ns, "workflow_run.updated_at"),
    )


def _job_spans(
    jobs: Sequence[WorkflowJob],
    workflow: SpanProjector,
    workflow_interval: Interval,
    report_links: Mapping[int, Sequence[tuple[str, str]]],
) -> None:
    for job_index, job in enumerate(jobs):
        created_ns = timestamp_ns(job.created_at)
        started_ns = timestamp_ns(job.started_at)
        job_interval = _interval(
            created_ns if created_ns is not None else started_ns,
            timestamp_ns(job.completed_at),
            workflow_interval,
        )
        job_id = job.id if job.id is not None else job_index
        conclusion = str(job.conclusion or "")
        status_code, status_message = _status(conclusion)
        link_attributes = {}
        for index, (label, url) in enumerate(report_links.get(job_id, ())):
            link_attributes[f"ci.trace.report.{index}.name"] = label
            link_attributes[f"ci.trace.report.{index}.url"] = url
        span = workflow.emit(
            "job",
            job_id,
            name=f"job: {job.name or job_id}",
            interval=job_interval,
            attributes={
                "github.job.id": job_id,
                "github.job.name": job.name or "",
                "github.job.status": job.status or "",
                "github.job.conclusion": conclusion,
                "github.job.runner.name": job.runner_name or "",
                "github.job.runner.group": job.runner_group_name or "",
                "github.job.labels": job.labels or [],
                "github.job.url": job.html_url or "",
                **link_attributes,
            },
            status_code=status_code,
            status_message=status_message,
        )
        job_context = workflow.under(span)

        if started_ns is not None and job_interval.start < started_ns:
            job_context.emit(
                "job-queue",
                job_id,
                name="queued for runner",
                interval=Interval(
                    job_interval.start, min(started_ns, job_interval.end)
                ),
                attributes={
                    "ci.queue.scope": "job",
                    "ci.queue.timing_source": "job.created_at",
                },
            )

        for step_index, step in enumerate(job.steps or []):
            step_interval = _interval(
                timestamp_ns(step.started_at),
                timestamp_ns(step.completed_at),
                Interval(
                    started_ns if started_ns is not None else job_interval.start,
                    job_interval.end,
                ),
            )
            step_conclusion = str(step.conclusion or "")
            step_status_code, step_status_message = _status(step_conclusion)
            step_number = step.number if step.number is not None else step_index
            job_context.emit(
                "step",
                job_id,
                step_number,
                name=f"step: {step.name or step_index}",
                interval=step_interval,
                attributes={
                    "github.job.id": job_id,
                    "github.step.number": step_number,
                    "github.step.name": step.name or "",
                    "github.step.status": step.status or "",
                    "github.step.conclusion": step_conclusion,
                },
                status_code=step_status_code,
                status_message=step_status_message,
            )


def build_workflow_trace(
    workflow_run: Mapping[str, Any],
    jobs: Sequence[WorkflowJob],
    *,
    report_links: Mapping[int, Sequence[tuple[str, str]]] | None = None,
) -> tuple[Trace, ResourceAttributes]:
    metadata = ResourceAttributes.from_github(
        environment=os.environ,
        workflow_run=workflow_run,
    )
    created_ns = timestamp_ns(workflow_run.get("created_at"))
    started_ns = timestamp_ns(workflow_run.get("run_started_at"))
    updated_ns = timestamp_ns(workflow_run.get("updated_at"))
    if updated_ns is None:
        raise ValueError("workflow_run is missing usable timestamps")
    start_ns, start_source = _workflow_start(
        workflow_run,
        jobs,
        created_ns=created_ns,
        started_ns=started_ns,
        updated_ns=updated_ns,
    )
    interval = _interval(
        start_ns,
        updated_ns,
        Interval(Ns(0), Ns(0)),
    )

    trace = Trace()
    projector = SpanProjector(
        trace=trace,
        trace_id=stable_trace_id(
            metadata.get("github.repository", ""),
            metadata.get("github.run.id", ""),
            metadata.get("github.run.attempt", ""),
        ),
        resource=metadata.with_attributes({"service.name": "github-actions"}),
        scope_name="github.actions",
    )
    conclusion = str(workflow_run.get("conclusion") or "")
    status_code, status_message = _status(conclusion)
    root = projector.emit(
        "workflow",
        name=f"workflow: {workflow_run.get('name', 'unknown')}",
        interval=interval,
        attributes={
            "github.run.status": workflow_run.get("status", ""),
            "github.run.conclusion": conclusion,
            "ci.workflow.timing_source": start_source,
        },
        status_code=status_code,
        status_message=status_message,
    )
    workflow = projector.under(root)

    if started_ns is not None and interval.start < started_ns:
        workflow.emit(
            "workflow-queue",
            name="workflow queue",
            interval=Interval(interval.start, min(started_ns, interval.end)),
            attributes={
                "ci.queue.scope": "workflow",
                "ci.queue.timing_source": start_source,
            },
        )

    _job_spans(jobs, workflow, interval, report_links or {})
    return trace, metadata
