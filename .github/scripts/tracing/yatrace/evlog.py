from __future__ import annotations

from typing import Any

from ..otlp import Interval, Ns, Span, update_span_attributes
from ..projector import SpanProjector
from .build_operations import YaBuildOperations
from .evlog_data import YaEvlog
from .test_operations import YaTestOperations

RENDERED_STAGE_NAMES = {
    "get-tools": "prepare tools",
    "build_graph_and_tests": "build graph",
    "dispatch_build": "execute graph",
    "cache_test_statuses": "cache test statuses",
    "statistics": "collect statistics",
    "finalize-reports": "finalize reports",
    "dump_results": "dump results",
}


def _bound_reported_tests(parent: SpanProjector, bounds: Interval) -> None:
    for scope_name in ("ya.suite", "ya.chunk", "ya.test.stage"):
        for span in parent.trace.spans(scope_name):
            span.start_time_unix_nano = bounds.clamp(
                Ns(span.start_time_unix_nano or 0)
            ).value
            span.end_time_unix_nano = bounds.clamp(
                Ns(span.end_time_unix_nano or 0)
            ).value


def project_evlog(
    evlog: YaEvlog,
    parent: SpanProjector,
    root: Interval,
) -> tuple[bytes | None, dict[str, Any]]:
    dispatch_span: Span | None = None
    dispatch_interval: Interval | None = None
    stages = [
        clipped
        for record in evlog.stages
        if record.name in RENDERED_STAGE_NAMES
        if (clipped := record.clipped(root)) is not None
    ]
    stages.sort(key=lambda record: (record.start_ns, record.end_ns, record.name))
    phases = parent.scoped("ya.phase")
    for index, record in enumerate(stages):
        span = phases.emit(
            "ya.stage",
            record.name,
            record.start_ns,
            index,
            name=f"ya phase: {RENDERED_STAGE_NAMES[record.name]}",
            interval=record.interval,
            attributes={"ya.stage.name": record.name, "ya.stage.tag": record.tag},
        )
        if record.name == "dispatch_build":
            dispatch_span = span
            dispatch_interval = record.interval

    if dispatch_interval is not None:
        _bound_reported_tests(parent, dispatch_interval)
    node_bounds = dispatch_interval if dispatch_interval is not None else root
    nodes = [
        clipped
        for record in evlog.nodes
        if (clipped := record.clipped(node_bounds)) is not None
    ]
    build_nodes = [node for node in nodes if not node.is_test]
    test_starts = [node.start_ns for node in nodes if node.kind == "test_execute"]
    metadata: dict[str, Any] = {"ya.phase.count": len(stages)}
    graph_attributes = evlog.statistics.graph_attributes()
    if dispatch_span is not None:
        update_span_attributes(dispatch_span, graph_attributes)
    else:
        metadata.update(graph_attributes)

    operations = parent.under(
        dispatch_span if dispatch_span is not None else parent.parent_span_id
    )
    metadata.update(YaTestOperations(nodes, evlog.failures).project(operations))
    metadata.update(
        YaBuildOperations(
            build_nodes,
            evlog.failures,
            evlog.statistics,
            evlog.critical_path,
            test_starts,
        ).project(operations)
    )
    return dispatch_span.span_id if dispatch_span is not None else None, metadata
