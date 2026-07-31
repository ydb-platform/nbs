"""Build a trace from GitHub workflow timing and imported OTLP spans."""

from __future__ import annotations

import os
from dataclasses import replace
from datetime import datetime
from typing import Any, Mapping, Sequence

from github.WorkflowJob import WorkflowJob

from .otlp import (
    Ns,
    ResourceAttributes,
    Span,
    SpanWriter,
    Trace,
    decode_attributes,
    encode_attributes,
    make_span,
    span_duration_ns,
    stable_span_id,
    stable_trace_id,
)

ERROR_CONCLUSIONS = frozenset(
    {"action_required", "cancelled", "failure", "startup_failure", "timed_out"}
)


def timestamp_ns(value: Any) -> Ns | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        return Ns(datetime.fromisoformat(text).timestamp() * 1_000_000_000)
    except ValueError:
        return None


def _status_code(conclusion: Any) -> int:
    value = str(conclusion or "").lower()
    if value == "success":
        return 1
    return 2 if value in ERROR_CONCLUSIONS else 0


def _bounded_times(
    start: Ns | None,
    end: Ns | None,
    default_start: Ns,
    default_end: Ns,
) -> tuple[Ns, Ns]:
    start_ns = start or Ns(default_start)
    end_ns = end or Ns(default_end)
    return start_ns, Ns(max(start_ns, end_ns))


def _job_and_step_spans(
    jobs: Sequence[WorkflowJob],
    *,
    writer: SpanWriter,
    trace_id: bytes,
    root_span_id: bytes,
    workflow_start_ns: Ns,
    workflow_end_ns: Ns,
) -> list[Span]:
    job_spans: list[Span] = []
    for job_index, job in enumerate(jobs):
        created_ns = timestamp_ns(job.created_at)
        started_ns = timestamp_ns(job.started_at)
        completed_ns = timestamp_ns(job.completed_at)
        job_start_ns, job_end_ns = _bounded_times(
            created_ns or started_ns,
            completed_ns,
            workflow_start_ns,
            workflow_end_ns,
        )
        job_id = job.id if job.id is not None else job_index
        job_span_id = stable_span_id(trace_id, "job", job_id)
        conclusion = str(job.conclusion or "")
        status_code = _status_code(conclusion)
        job_span = make_span(
            trace_id=trace_id,
            span_id=job_span_id,
            parent_span_id=root_span_id,
            name=f"job: {job.name or job_id}",
            start_ns=job_start_ns,
            end_ns=job_end_ns,
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
            status_message=conclusion if status_code == 2 else "",
        )
        writer.add(job_span, scope_name="github.actions")
        job_spans.append(job_span)

        if started_ns and job_start_ns < started_ns:
            writer.add(
                make_span(
                    trace_id=trace_id,
                    span_id=stable_span_id(trace_id, "job-queue", job_id),
                    parent_span_id=job_span_id,
                    name="queued for runner",
                    start_ns=job_start_ns,
                    end_ns=min(started_ns, job_end_ns),
                    attributes={
                        "ci.queue.scope": "job",
                        "ci.queue.timing_source": (
                            "job.created_at"
                            if created_ns is not None
                            else "workflow.run_started_at"
                        ),
                    },
                ),
                scope_name="github.actions",
            )

        for step_index, step in enumerate(job.steps or []):
            step_start_ns, step_end_ns = _bounded_times(
                timestamp_ns(step.started_at),
                timestamp_ns(step.completed_at),
                started_ns or job_start_ns,
                job_end_ns,
            )
            step_conclusion = str(step.conclusion or "")
            step_status_code = _status_code(step_conclusion)
            step_number = step.number if step.number is not None else step_index
            writer.add(
                make_span(
                    trace_id=trace_id,
                    span_id=stable_span_id(
                        trace_id,
                        "step",
                        job_id,
                        step_number,
                    ),
                    parent_span_id=job_span_id,
                    name=f"step: {step.name or step_index}",
                    start_ns=step_start_ns,
                    end_ns=step_end_ns,
                    attributes={
                        "github.job.id": job_id,
                        "github.step.number": step_number,
                        "github.step.name": step.name or "",
                        "github.step.status": step.status or "",
                        "github.step.conclusion": step_conclusion,
                    },
                    status_code=step_status_code,
                    status_message=(step_conclusion if step_status_code == 2 else ""),
                ),
                scope_name="github.actions",
            )
    return job_spans


def _parent_job(imported_root: Span, job_spans: Sequence[Span]) -> Span | None:
    containing = [
        job
        for job in job_spans
        if (job.start_time_unix_nano or 0) <= (imported_root.start_time_unix_nano or 0)
        and (imported_root.end_time_unix_nano or 0) <= (job.end_time_unix_nano or 0)
    ]
    if not containing:
        overlapping = [
            job
            for job in job_spans
            if (job.start_time_unix_nano or 0)
            <= (imported_root.end_time_unix_nano or 0)
            and (imported_root.start_time_unix_nano or 0)
            <= (job.end_time_unix_nano or 0)
        ]
        containing = overlapping
    return min(containing, key=span_duration_ns, default=None)


def _merge_imported_spans(
    imported: Trace,
    *,
    result: Trace,
    trace_id: bytes,
    root_span_id: bytes,
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
    root_parents = {}
    for span in roots:
        parent_job = _parent_job(span, job_spans)
        root_parents[(span.trace_id, span.span_id)] = (
            parent_job.span_id if parent_job is not None else root_span_id
        )
    id_map = {
        (span.trace_id, span.span_id): stable_span_id(
            trace_id, "import", span.trace_id, span.span_id
        )
        for span in imported
    }

    for resource, scope, span in imported.walk():
        old_key = (span.trace_id, span.span_id)
        old_parent_key = (span.trace_id, span.parent_span_id)
        parent_span_id = id_map.get(
            old_parent_key,
            root_parents.get(old_key, root_span_id),
        )
        attributes = decode_attributes(span.attributes)
        attributes["ci.trace.source"] = "ya-otlp"
        result.add_span(
            replace(
                span,
                trace_id=trace_id,
                span_id=id_map[old_key],
                parent_span_id=parent_span_id,
                attributes=encode_attributes(attributes),
            ),
            resource=resource,
            scope_name=str(scope.name or ""),
            scope_version=str(scope.version or ""),
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
    start_ns, end_ns = _bounded_times(
        timestamp_ns(workflow_run.get("created_at"))
        or timestamp_ns(workflow_run.get("run_started_at")),
        timestamp_ns(workflow_run.get("updated_at")),
        Ns(0),
        Ns(0),
    )
    if end_ns == 0:
        raise ValueError("workflow_run is missing usable timestamps")

    trace_id = stable_trace_id(
        metadata.get("github.repository", ""),
        metadata.get("github.run.id", ""),
        metadata.get("github.run.attempt", ""),
    )
    root_span_id = stable_span_id(trace_id, "workflow")
    conclusion = str(workflow_run.get("conclusion") or "")
    status_code = _status_code(conclusion)
    resource = metadata.with_attributes({"service.name": "github-actions"})
    root = make_span(
        trace_id=trace_id,
        span_id=root_span_id,
        name=f"workflow: {workflow_run.get('name', 'unknown')}",
        start_ns=start_ns,
        end_ns=end_ns,
        attributes={
            "github.run.status": workflow_run.get("status", ""),
            "github.run.conclusion": conclusion,
            "ci.imported_ya_span_count": len(imported),
        },
        status_code=status_code,
        status_message=conclusion if status_code == 2 else "",
    )
    trace = Trace()
    writer = trace.writer(resource)
    writer.add(root, scope_name="github.actions")

    run_started_ns = timestamp_ns(workflow_run.get("run_started_at"))
    if run_started_ns and start_ns < run_started_ns:
        writer.add(
            make_span(
                trace_id=trace_id,
                span_id=stable_span_id(trace_id, "workflow-queue"),
                parent_span_id=root_span_id,
                name="workflow queue",
                start_ns=start_ns,
                end_ns=min(run_started_ns, end_ns),
                attributes={
                    "ci.queue.scope": "workflow",
                    "ci.queue.timing_source": "workflow_run.created_at",
                },
            ),
            scope_name="github.actions",
        )

    job_spans = _job_and_step_spans(
        jobs,
        writer=writer,
        trace_id=trace_id,
        root_span_id=root_span_id,
        workflow_start_ns=start_ns,
        workflow_end_ns=end_ns,
    )
    _merge_imported_spans(
        imported,
        result=trace,
        trace_id=trace_id,
        root_span_id=root_span_id,
        job_spans=job_spans,
    )
    return trace, metadata
