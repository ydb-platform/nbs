#!/usr/bin/env python3
"""Build one static trace from GitHub workflow timing and ya OTLP bundles."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from github.WorkflowJob import WorkflowJob

from .helpers import get_workflow_jobs, github_client
from .otlp import (
    ResourceAttributes,
    Span,
    Trace,
    decode_attributes,
    encode_attributes,
    make_span,
    ns,
    span_duration_ns,
    stable_span_id,
    stable_trace_id,
)
from .trace_report import (
    MAX_INPUT_BYTES,
    MAX_SPANS,
    read_otlp_jsonl,
    write_trace_bundle,
)

MAX_TRACE_INPUTS = 10_000


def timestamp_ns(value: Any) -> ns | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        return ns(datetime.fromisoformat(text).timestamp() * 1_000_000_000)
    except ValueError:
        return None


def _status_code(conclusion: Any) -> int:
    value = str(conclusion or "").lower()
    if value in {"success", "neutral", "skipped"}:
        return 1 if value == "success" else 0
    if value in {
        "action_required",
        "cancelled",
        "failure",
        "startup_failure",
        "timed_out",
    }:
        return 2
    return 0


def _bounded_times(
    start: ns | None,
    end: ns | None,
    default_start: ns,
    default_end: ns,
) -> tuple[ns, ns]:
    start_ns = start or ns(default_start)
    end_ns = end or ns(default_end)
    return start_ns, ns(max(start_ns, end_ns))


def _job_and_step_spans(
    jobs: Sequence[WorkflowJob],
    *,
    trace: Trace,
    trace_id: bytes,
    root_span_id: bytes,
    workflow_start_ns: ns,
    workflow_end_ns: ns,
    resource: ResourceAttributes,
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
            status_code=_status_code(conclusion),
            status_message=conclusion if _status_code(conclusion) == 2 else "",
        )
        trace.add_span(
            job_span,
            resource=resource,
            scope_name="github.actions",
        )
        job_spans.append(job_span)

        if started_ns and job_start_ns < started_ns:
            trace.add_span(
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
                resource=resource,
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
            step_number = step.number if step.number is not None else step_index
            trace.add_span(
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
                    status_code=_status_code(step_conclusion),
                    status_message=(
                        step_conclusion if _status_code(step_conclusion) == 2 else ""
                    ),
                ),
                resource=resource,
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
        ns(0),
        ns(0),
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
        status_code=_status_code(conclusion),
        status_message=conclusion if _status_code(conclusion) == 2 else "",
    )
    trace = Trace()
    trace.add_span(root, resource=resource, scope_name="github.actions")

    run_started_ns = timestamp_ns(workflow_run.get("run_started_at"))
    if run_started_ns and start_ns < run_started_ns:
        trace.add_span(
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
            resource=resource,
            scope_name="github.actions",
        )

    job_spans = _job_and_step_spans(
        jobs,
        trace=trace,
        trace_id=trace_id,
        root_span_id=root_span_id,
        workflow_start_ns=start_ns,
        workflow_end_ns=end_ns,
        resource=resource,
    )
    _merge_imported_spans(
        imported,
        result=trace,
        trace_id=trace_id,
        root_span_id=root_span_id,
        job_spans=job_spans,
    )
    return trace, metadata


def _find_inputs(paths: Iterable[Path], output_dir: Path) -> list[Path]:
    inputs = []
    output_dir = output_dir.resolve()
    for path in paths:
        if path.is_dir():
            candidates = path.rglob("trace.otlp.jsonl.gz")
        else:
            candidates = [path]
        for candidate in candidates:
            resolved = candidate.resolve()
            if resolved.is_file() and output_dir not in resolved.parents:
                inputs.append(resolved)
    return sorted(set(inputs))


def download_s3_trace_inputs(
    *,
    bucket: str,
    prefix: str,
    output_dir: Path,
    max_input_bytes: int = MAX_INPUT_BYTES,
) -> list[Path]:
    if not bucket or not prefix or prefix.startswith("/"):
        raise ValueError("A bucket and relative S3 prefix are required")
    query = "Contents[?ends_with(Key, '/trace.otlp.jsonl.gz')].[Key,Size]"
    result = subprocess.run(
        [
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket",
            bucket,
            "--prefix",
            prefix.rstrip("/") + "/",
            "--query",
            query,
            "--output",
            "json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    inventory = json.loads(result.stdout or "[]")
    if inventory is None:
        inventory = []
    if not isinstance(inventory, list) or len(inventory) > MAX_TRACE_INPUTS:
        raise ValueError(f"S3 trace inventory exceeds {MAX_TRACE_INPUTS} objects")
    total_bytes = 0
    inputs: list[Path] = []
    output_dir.mkdir(parents=True, exist_ok=True)
    for index, item in enumerate(inventory):
        if not isinstance(item, list) or len(item) != 2:
            raise ValueError("Unexpected S3 trace inventory response")
        key, raw_size = item
        key = str(key)
        size = int(raw_size)
        if (
            not key.startswith(prefix.rstrip("/") + "/")
            or not key.endswith("/trace.otlp.jsonl.gz")
            or size < 0
        ):
            raise ValueError(f"Invalid S3 trace object: {key!r}")
        total_bytes += size
        if total_bytes > max_input_bytes:
            raise ValueError(f"S3 traces exceed {max_input_bytes} bytes")
        destination = output_dir / f"{index:05d}.otlp.jsonl.gz"
        subprocess.run(
            [
                "aws",
                "s3api",
                "get-object",
                "--bucket",
                bucket,
                "--key",
                key,
                str(destination),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        inputs.append(destination)
    return inputs


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", required=True, type=Path)
    parser.add_argument("--input", action="append", type=Path, default=[])
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--s3-bucket", default="")
    parser.add_argument("--s3-prefix", default="")
    parser.add_argument("--s3-input-dir", type=Path)
    parser.add_argument("--github-token", default=os.environ.get("GITHUB_TOKEN", ""))
    parser.add_argument(
        "--github-api-url",
        default=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    event = json.loads(args.event.read_text(encoding="utf-8"))
    workflow_run = event.get("workflow_run")
    if not isinstance(workflow_run, dict):
        raise ValueError("Event does not contain workflow_run")
    repository = event.get("repository", {}).get("full_name") or os.environ.get(
        "GITHUB_REPOSITORY", ""
    )
    if not repository or not args.github_token:
        raise ValueError("GitHub repository and token are required")
    workflow_run.setdefault("repository", {"full_name": repository})

    github = github_client(args.github_token, base_url=args.github_api_url)
    repo = github.get_repo(repository)
    run = repo.get_workflow_run(int(workflow_run["id"]))
    jobs = get_workflow_jobs(
        run,
        run_attempt=int(workflow_run.get("run_attempt", 1)),
    )
    if len(jobs) > MAX_SPANS:
        raise ValueError(f"Workflow has more than {MAX_SPANS} jobs")
    input_paths = _find_inputs(args.input, args.output_dir)
    if args.s3_bucket or args.s3_prefix:
        if not args.s3_bucket or not args.s3_prefix:
            raise ValueError("--s3-bucket and --s3-prefix must be used together")
        s3_input_dir = args.s3_input_dir or args.output_dir.parent / "trace-input"
        input_paths.extend(
            download_s3_trace_inputs(
                bucket=args.s3_bucket,
                prefix=args.s3_prefix,
                output_dir=s3_input_dir,
            )
        )
    imported = read_otlp_jsonl(
        input_paths,
        max_input_bytes=MAX_INPUT_BYTES,
        max_spans=MAX_SPANS,
    )
    trace, metadata = build_workflow_trace(workflow_run, jobs, imported)
    manifest = write_trace_bundle(
        args.output_dir,
        trace,
        title=f"Workflow trace · {workflow_run.get('name', 'unknown')}",
        metadata=metadata,
        file_prefix="workflow-trace",
    )
    print(
        f"Wrote {manifest['span_count']} spans, including {len(imported)} "
        f"imported ya spans, to {args.output_dir}"
    )


if __name__ == "__main__":
    main()
