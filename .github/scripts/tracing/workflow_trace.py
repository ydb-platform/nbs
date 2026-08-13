"""Project GitHub workflow timing and imported traces into one OTLP trace."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from github.WorkflowJob import WorkflowJob

from .otlp import (
    Interval,
    Ns,
    ResourceAttributes,
    SECOND,
    Span,
    Trace,
    decode_attributes,
    encode_attributes,
    span_duration_ns,
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
            (delta.days * 86_400 + delta.seconds) * SECOND + delta.microseconds * 1_000
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
    return Interval(start_ns, Ns(max(start_ns, end_ns)))


def _workflow_start(
    workflow_run: Mapping[str, Any],
    jobs: Sequence[WorkflowJob],
    imported: Trace,
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
        return Ns(0), "unix_epoch"

    candidates: list[tuple[Ns, str]] = []

    def add_candidate(value: Ns | None, source: str) -> None:
        if value is not None and Ns(0) < value <= updated_ns:
            candidates.append((value, source))

    add_candidate(started_ns, "workflow_run.run_started_at")
    for job in jobs:
        add_candidate(timestamp_ns(job.created_at), "job.created_at")
        add_candidate(timestamp_ns(job.started_at), "job.started_at")
    if candidates:
        return min(candidates, key=lambda item: item[0])

    for span in imported:
        start_ns = Ns(span.start_time_unix_nano or 0)
        add_candidate(start_ns, "imported_span.start_time_unix_nano")
    return min(
        candidates,
        key=lambda item: item[0],
        default=(updated_ns, "workflow_run.updated_at"),
    )


def _job_spans(
    jobs: Sequence[WorkflowJob],
    workflow: SpanProjector,
    workflow_interval: Interval,
) -> list[Span]:
    spans = []
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
            },
            status_code=status_code,
            status_message=status_message,
        )
        spans.append(span)
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
                    "ci.queue.timing_source": (
                        "job.created_at"
                        if created_ns is not None
                        else "workflow.run_started_at"
                    ),
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
    return spans


def _parent_job(imported: Span, jobs: Sequence[Span]) -> Span | None:
    imported_interval = Interval(
        Ns(imported.start_time_unix_nano or 0),
        Ns(imported.end_time_unix_nano or 0),
    )
    candidates = [
        (
            job,
            Interval(
                Ns(job.start_time_unix_nano or 0),
                Ns(job.end_time_unix_nano or 0),
            ),
        )
        for job in jobs
    ]
    matches = [
        job
        for job, interval in candidates
        if interval.start <= imported_interval.start
        and imported_interval.end <= interval.end
    ] or [job for job, interval in candidates if interval.overlap(imported_interval)]
    return min(matches, key=span_duration_ns, default=None)


def _merge_imported(
    imported: Trace,
    workflow: SpanProjector,
    job_spans: Sequence[Span],
) -> None:
    imported_ids = {(span.trace_id, span.span_id) for span in imported}
    if len(imported_ids) != len(imported):
        raise ValueError("Imported OTLP contains duplicate span IDs")

    roots = [
        span
        for span in imported
        if not span.parent_span_id
        or (span.trace_id, span.parent_span_id) not in imported_ids
    ]
    root_parents = {
        (span.trace_id, span.span_id): (
            parent.span_id
            if (parent := _parent_job(span, job_spans))
            else workflow.parent_span_id
        )
        for span in roots
    }
    id_map = {
        (span.trace_id, span.span_id): workflow.span_id(
            "import", span.trace_id, span.span_id
        )
        for span in imported
    }

    for resource_spans, scope_spans, span in imported.walk_groups():
        key = span.trace_id, span.span_id
        parent_key = span.trace_id, span.parent_span_id
        attributes = decode_attributes(span.attributes)
        attributes["ci.trace.source"] = "ya-otlp"
        workflow.trace.add_span(
            replace(
                span,
                trace_id=workflow.trace_id,
                span_id=id_map[key],
                parent_span_id=id_map.get(
                    parent_key,
                    root_parents.get(key, workflow.parent_span_id),
                ),
                attributes=encode_attributes(attributes),
            ),
            resource=resource_spans.resource or ResourceAttributes(),
            scope=scope_spans.scope,
            resource_schema_url=str(resource_spans.schema_url or ""),
            scope_schema_url=str(scope_spans.schema_url or ""),
        )


def build_workflow_trace(
    workflow_run: Mapping[str, Any],
    jobs: Sequence[WorkflowJob],
    imported: Trace,
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
        imported,
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
            "ci.imported_ya_span_count": len(imported),
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

    job_spans = _job_spans(jobs, workflow, interval)
    _merge_imported(imported, workflow, job_spans)
    return trace, metadata
