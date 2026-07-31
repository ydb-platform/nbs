from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..otlp import (
    Interval,
    Ns,
    ResourceAttributes,
    Span,
    Trace,
    make_span,
    stable_span_id,
    update_span_attributes,
)
from .build_operations import YaBuildOperations
from .critical_path import YaCriticalPath
from .evlog_record import YaEvlogRecord
from .statistics import YaBuildStatistics
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


@dataclass
class YaEvlog:
    stages: list[YaEvlogRecord]
    nodes: list[YaEvlogRecord]
    statistics: dict[str, Any] = field(default_factory=dict)
    failures: dict[str, int | None] = field(default_factory=dict)

    @property
    def critical_path(self) -> YaCriticalPath:
        return YaCriticalPath.from_evlog(self.statistics, self.nodes)

    def build_spans(
        self,
        *,
        trace: Trace,
        trace_id: bytes,
        root_span_id: bytes,
        root_start_ns: Ns,
        root_end_ns: Ns,
        resource: ResourceAttributes,
    ) -> tuple[bytes | None, dict[str, Any]]:
        root = Interval(root_start_ns, root_end_ns)
        writer = trace.writer(resource)
        dispatch_span_id = None
        dispatch_span: Span | None = None
        dispatch_interval: Interval | None = None

        stages = [
            clipped
            for record in self.stages
            if record.name in RENDERED_STAGE_NAMES
            if (clipped := record.clipped(root)) is not None
        ]
        stages.sort(key=lambda record: (record.start_ns, record.end_ns, record.name))
        for index, record in enumerate(stages):
            span_id = stable_span_id(
                trace_id,
                "ya.stage",
                record.name,
                record.start_ns,
                index,
            )
            stage_span = make_span(
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=root_span_id,
                name=f"ya phase: {RENDERED_STAGE_NAMES[record.name]}",
                start_ns=record.start_ns,
                end_ns=record.end_ns,
                attributes={
                    "ya.stage.name": record.name,
                    "ya.stage.tag": record.tag,
                },
            )
            if record.name == "dispatch_build":
                dispatch_span_id = span_id
                dispatch_span = stage_span
                dispatch_interval = record.interval
            writer.add(stage_span, scope_name="ya.phase")

        clipped_nodes = [
            clipped
            for record in self.nodes
            if (
                clipped := record.clipped(
                    dispatch_interval if dispatch_interval is not None else root
                )
            )
            is not None
        ]
        classified = [(record, *record.kind_and_tool) for record in clipped_nodes]
        build_records = [
            record for record in classified if not record[1].startswith("test_")
        ]
        test_starts = [
            record.start_ns for record, kind, _ in classified if kind == "test_execute"
        ]
        metadata: dict[str, Any] = {"ya.phase.count": len(stages)}
        critical_path = self.critical_path
        statistics = YaBuildStatistics(self.statistics)
        graph_attributes = statistics.graph_attributes()
        if dispatch_span is not None:
            update_span_attributes(dispatch_span, graph_attributes)
        else:
            metadata.update(graph_attributes)
        metadata.update(
            YaTestOperations(classified, self.failures).build(
                writer=writer,
                trace_id=trace_id,
                parent_span_id=dispatch_span_id or root_span_id,
            )
        )
        metadata.update(
            YaBuildOperations(
                build_records,
                self.failures,
                statistics,
                critical_path,
                test_starts,
            ).build(
                writer=writer,
                trace_id=trace_id,
                parent_span_id=dispatch_span_id or root_span_id,
            )
        )
        return dispatch_span_id, metadata
