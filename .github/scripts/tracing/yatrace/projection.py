"""Build ya OTLP traces through the experimental bound projector."""

from __future__ import annotations

from collections.abc import Sequence

from ..otlp import (
    Interval,
    Ns,
    ResourceAttributes,
    Trace,
    decode_attributes,
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
        duration = attributes.get("test.duration.reported_seconds")
        if (
            attributes.get("test.status") in {"deselected", "not_launched"}
            or attributes.get("test.incomplete") is True
            or isinstance(duration, bool)
            or not isinstance(duration, (int, float))
            or duration <= 0
        ):
            continue
        candidates.append((span, duration))
    candidates.sort(
        key=lambda candidate: (
            -candidate[1],
            candidate[0].start_time_unix_nano or 0,
            candidate[0].name or "",
            candidate[0].span_id,
        )
    )
    for rank, (span, _duration) in enumerate(candidates[:limit], start=1):
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
            root.start.value,
        ),
        resource=resource,
        scope_name="ya",
    )
    malformed_trace_count = sum(item.malformed_json_record_count for item in traces)
    malformed_evlog_count = evlog.malformed_json_record_count if evlog else 0
    input_incomplete = malformed_trace_count > 0 or malformed_evlog_count > 0
    root_span = projector.emit(
        "ya make",
        name=f"ya make {operation}",
        interval=root,
        attributes={
            "process.exit.code": exit_code,
            "ci.result.code": result_code,
            "ya.trace.file_count": len(traces),
            "ya.trace.malformed_json_record.count": malformed_trace_count,
            "ya.evlog.malformed_json_record.count": malformed_evlog_count,
            "ya.trace.input.incomplete": input_incomplete,
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
