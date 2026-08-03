"""Build ya OTLP traces through the experimental bound projector."""

from __future__ import annotations

from collections.abc import Sequence

from ..otlp import (
    Interval,
    Ns,
    ResourceAttributes,
    Trace,
    decode_attributes,
    span_duration_ns,
    stable_trace_id,
    update_span_attributes,
)
from ..projector import SpanProjector
from .evlog import project_evlog
from .evlog_data import YaEvlog
from .trace_model import SuiteTrace
from .trace_spans import YaTraceSpanProjector


def _mark_longest_tests(trace: Trace, limit: int = 10) -> int:
    candidates = []
    for span in trace.spans("ya.test"):
        attributes = decode_attributes(span.attributes)
        if attributes.get("test.status") in {"deselected", "not_launched"}:
            continue
        if attributes.get("test.incomplete") is True or not span_duration_ns(span):
            continue
        candidates.append(span)
    candidates.sort(
        key=lambda span: (
            -span_duration_ns(span),
            span.start_time_unix_nano or 0,
            span.name or "",
            span.span_id,
        )
    )
    for rank, span in enumerate(candidates[:limit], start=1):
        update_span_attributes(span, {"ya.test.duration.rank": rank})
    return min(limit, len(candidates))


def build_ya_trace(
    traces: Sequence[SuiteTrace],
    *,
    root_start_ns: Ns,
    root_end_ns: Ns,
    exit_code: int,
    result_code: int | None = None,
    resource: ResourceAttributes,
    evlog: YaEvlog | None = None,
    operation: str = "tests",
) -> Trace:
    root = Interval(root_start_ns, root_end_ns)
    result_code = exit_code if result_code is None else result_code
    trace = Trace()
    projector = SpanProjector(
        trace=trace,
        trace_id=stable_trace_id(
            resource.get("github.repository", ""),
            resource.get("github.run.id", ""),
            resource.get("github.run.attempt", ""),
            resource.get("github.job", ""),
            resource.get("ci.component", ""),
            resource.get("ci.ya.retry", ""),
            operation,
            root.start,
        ),
        resource=resource,
        scope_name="ya",
    )
    malformed_count = sum(item.malformed_json_record_count for item in traces)
    root_span = projector.emit(
        "ya make",
        name=f"ya make {operation}",
        interval=root,
        attributes={
            "process.exit.code": exit_code,
            "ci.result.code": result_code,
            "ya.trace.file_count": len(traces),
            "ya.trace.malformed_json_record.count": malformed_count,
            "ya.trace.input.incomplete": malformed_count > 0,
            "ya.chunk.count": 0,
        },
        status_code=1 if result_code == 0 else 2,
        status_message=(
            "" if result_code == 0 else f"ya make {operation} result code {result_code}"
        ),
    )
    root_context = projector.under(root_span)

    chunk_count = sum(
        YaTraceSpanProjector(source).project(root_context, root, trace_index)
        for trace_index, source in enumerate(traces)
    )
    update_span_attributes(root_span, {"ya.chunk.count": chunk_count})
    if evlog is not None:
        dispatch_span_id, metadata = project_evlog(evlog, root_context, root)
        metadata.update(evlog.critical_path.mark_test_spans(trace))
        update_span_attributes(root_span, metadata)
        if dispatch_span_id:
            for scope_name in ("ya.chunk", "ya.suite"):
                for span in trace.spans(scope_name):
                    if span.parent_span_id == root_span.span_id:
                        span.parent_span_id = dispatch_span_id

    ranked = _mark_longest_tests(trace)
    if ranked:
        update_span_attributes(root_span, {"ya.test.duration.ranked.count": ranked})
    return trace
