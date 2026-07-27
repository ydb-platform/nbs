#!/usr/bin/env python3
"""Convert ya test trace events into a portable OTLP trace bundle."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Sequence

from ..otlp import (
    ResourceAttributes,
    Trace,
    make_span,
    ns,
    stable_span_id,
    stable_trace_id,
    update_span_attributes,
)
from ..trace_report import write_trace_bundle
from .ya_trace import YaEvlog, YaTraceFile, load_ya_evlog, load_ya_traces


def build_resource_attributes(args: argparse.Namespace) -> ResourceAttributes:
    return ResourceAttributes.from_github(environment=os.environ).with_attributes(
        {
            "service.name": f"nbs-ya-{args.operation}",
            "ci.component": args.component or "all",
            "ci.build.preset": args.build_preset,
            "ci.build.target": args.build_target,
            "ci.test.target": args.test_target,
            "ci.test.type": args.test_type,
            "ci.test.size": args.test_size,
            "ci.ya.retry": args.retry,
            "ci.ya.operation": args.operation,
        }
    )


def build_ya_trace(
    traces: Sequence[YaTraceFile],
    *,
    root_start_ns: ns,
    root_end_ns: ns,
    exit_code: int,
    result_code: int | None = None,
    resource: ResourceAttributes,
    evlog: YaEvlog | None = None,
    operation: str = "tests",
) -> Trace:
    root_start_ns = ns(root_start_ns)
    root_end_ns = ns(root_end_ns)
    if root_end_ns < root_start_ns:
        raise ValueError("root span end precedes start")

    effective_result_code = exit_code if result_code is None else result_code
    trace_id = stable_trace_id(
        resource.get("github.repository", ""),
        resource.get("github.run.id", ""),
        resource.get("github.run.attempt", ""),
        resource.get("github.job", ""),
        resource.get("ci.component", ""),
        resource.get("ci.ya.retry", ""),
        operation,
        root_start_ns,
    )
    root_span_id = stable_span_id(trace_id, "ya make")
    root = make_span(
        trace_id=trace_id,
        span_id=root_span_id,
        name=f"ya make {operation}",
        start_ns=root_start_ns,
        end_ns=root_end_ns,
        attributes={
            "process.exit.code": exit_code,
            "ci.result.code": effective_result_code,
            "ya.trace.file_count": len(traces),
            "ya.chunk.count": sum(trace.chunk_count for trace in traces),
        },
        status_code=1 if effective_result_code == 0 else 2,
        status_message=(
            ""
            if effective_result_code == 0
            else f"ya test result code {effective_result_code}"
        ),
    )
    result = Trace()
    result.add_span(root, resource=resource, scope_name="ya")

    for trace_index, ya_trace in enumerate(traces):
        ya_trace.build_spans(
            trace=result,
            trace_id=trace_id,
            root_span_id=root_span_id,
            root_start_ns=root_start_ns,
            root_end_ns=root_end_ns,
            resource=resource,
            trace_index=trace_index,
        )
    if evlog is not None:
        dispatch_span_id, metadata = evlog.build_spans(
            trace=result,
            trace_id=trace_id,
            root_span_id=root_span_id,
            root_start_ns=root_start_ns,
            root_end_ns=root_end_ns,
            resource=resource,
        )
        metadata.update(evlog.mark_critical_test_spans(result))
        update_span_attributes(root, metadata)
        if dispatch_span_id:
            for span in result.spans("ya.chunk"):
                if span.parent_span_id == root_span_id:
                    span.parent_span_id = dispatch_span_id
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ya-out", type=Path, required=True)
    parser.add_argument("--evlog", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--attempt-start-ns", type=ns, required=True)
    parser.add_argument("--attempt-end-ns", type=ns, required=True)
    parser.add_argument("--exit-code", type=int, required=True)
    parser.add_argument("--result-code", type=int)
    parser.add_argument("--component", default="")
    parser.add_argument("--build-preset", default="")
    parser.add_argument("--build-target", default="")
    parser.add_argument("--test-target", default="")
    parser.add_argument("--test-type", default="")
    parser.add_argument("--test-size", default="")
    parser.add_argument("--retry", type=int, default=1)
    parser.add_argument(
        "--operation",
        choices=("build", "tests"),
        default="tests",
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    logging.basicConfig(level=logging.INFO)
    if args.attempt_end_ns < args.attempt_start_ns:
        raise ValueError("attempt end precedes attempt start")
    resource = build_resource_attributes(args)
    traces = load_ya_traces(
        args.ya_out,
        modified_since=args.attempt_start_ns / 1_000_000_000 - 5,
    )
    trace = build_ya_trace(
        traces,
        root_start_ns=args.attempt_start_ns,
        root_end_ns=args.attempt_end_ns,
        exit_code=args.exit_code,
        result_code=args.result_code,
        resource=resource,
        evlog=load_ya_evlog(args.evlog),
        operation=args.operation,
    )
    title_component = args.component or "all"
    manifest = write_trace_bundle(
        args.output_dir,
        trace,
        title=f"ya make trace · {title_component} · attempt {args.retry}",
        metadata=resource,
    )
    print(
        f"Wrote {manifest['span_count']} spans from {len(traces)} ya trace files "
        f"to {args.output_dir}"
    )


if __name__ == "__main__":
    main()
