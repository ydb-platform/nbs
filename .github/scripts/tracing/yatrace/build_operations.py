from __future__ import annotations

import math
from collections import Counter
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
from . import limits

RENDERED_STAGE_NAMES = {
    "get-tools": "prepare tools",
    "build_graph_and_tests": "build graph",
    "dispatch_build": "execute graph",
    "cache_test_statuses": "cache test statuses",
    "statistics": "collect statistics",
    "finalize-reports": "finalize reports",
    "dump_results": "dump results",
}


class _YaEvlogSpanBuilder:
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
        dispatch_span_id = None
        dispatch_span: Span | None = None
        dispatch_interval: Interval | None = None

        stages = []
        for record in self.stages:
            if record.name not in RENDERED_STAGE_NAMES:
                continue
            clipped = record.clipped(root)
            if clipped is not None:
                stages.append(clipped)
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
            trace.add_span(
                stage_span,
                resource=resource,
                scope_name="ya.phase",
            )

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
        unbounded_build_records = [
            (record, kind, tool)
            for record, kind, tool in classified
            if not kind.startswith("test_")
        ]
        test_starts = [
            record.start_ns for record, kind, _ in classified if kind == "test_execute"
        ]
        metadata: dict[str, Any] = {
            "ya.phase.count": len(stages),
            "ya.build.node.count": 0,
        }
        graph_attributes = self.graph_statistics_attributes()
        if dispatch_span is not None:
            update_span_attributes(dispatch_span, graph_attributes)
        else:
            metadata.update(graph_attributes)
        metadata.update(
            self._build_test_operations(
                trace=trace,
                trace_id=trace_id,
                parent_span_id=dispatch_span_id or root_span_id,
                classified=classified,
                resource=resource,
            )
        )
        timed_records = [
            (record, kind, tool)
            for record, kind, tool in unbounded_build_records
            if kind in {"cache_restore", "execute", "materialize"}
        ]
        if not timed_records:
            return dispatch_span_id, metadata

        build_start_ns = min(record.start_ns for record, _, _ in timed_records)
        build_end_ns = max(record.end_ns for record, _, _ in timed_records)
        build_interval = Interval(build_start_ns, build_end_ns)
        build_records = []
        for record, kind, tool in unbounded_build_records:
            clipped = record.clipped(build_interval)
            if clipped is not None:
                build_records.append((clipped, kind, tool))
        metadata["ya.build.node.count"] = len(build_records)
        counts = Counter(kind for _, kind, _ in build_records)
        cumulative_ns = Ns(sum(len(record.interval) for record, _, _ in build_records))
        build_span_id = stable_span_id(
            trace_id,
            "ya.build",
            build_start_ns,
            build_end_ns,
        )
        attributes: dict[str, Any] = {
            "ya.build.wall_time.kind": "worker-node-execution-envelope",
            "ya.build.node.count": len(build_records),
            "ya.build.cumulative_node_seconds": cumulative_ns.to_s(),
        }
        command_details = [
            detail
            for record, _, _ in build_records
            for detail in record.details
            if detail.tag == "exec_cmd"
        ]
        if command_details:
            attributes["ya.build.command.count"] = len(command_details)
            attributes["ya.build.cumulative_command_seconds"] = Ns(
                sum(len(detail.interval) for detail in command_details)
            ).to_s()
        for kind, count in sorted(counts.items()):
            attributes[f"ya.build.node.{kind}.count"] = count
        if test_starts:
            attributes["ya.build.first_test_node_offset_seconds"] = Ns(
                max(0, min(test_starts) - build_start_ns)
            ).to_s()
        statistics_attributes, critical_entries = self.build_statistics_attributes(
            build_records
        )
        attributes.update(statistics_attributes)
        critical_matches = self._match_build_critical_path(
            build_records,
            critical_entries,
        )
        failed_uids = {
            record.uid
            for record, _, _ in build_records
            if record.uid and record.uid in self.failures
        }
        if failed_uids:
            attributes["ya.build.failed_node.count"] = len(failed_uids)
        failed_representatives = {
            max(
                (
                    index
                    for index, (record, _, _) in enumerate(build_records)
                    if record.uid == uid
                ),
                key=lambda index: (
                    build_records[index][1] == "execute",
                    len(build_records[index][0].interval),
                    build_records[index][0].end_ns,
                ),
            )
            for uid in failed_uids
        }

        candidates = [
            (
                record_index,
                record,
                kind,
                tool,
                critical_matches.get(record_index),
                record_index in failed_representatives,
            )
            for record_index, (record, kind, tool) in enumerate(build_records)
            if kind != "cache_store" or record_index in failed_representatives
        ]
        dropped = max(0, len(candidates) - limits.MAX_BUILD_NODE_SPANS)
        if dropped:
            protected_candidates = [
                candidate
                for candidate in candidates
                if candidate[4] is not None or candidate[0] in failed_representatives
            ]
            other_candidates = [
                candidate
                for candidate in candidates
                if candidate[4] is None and candidate[0] not in failed_representatives
            ]
            protected_candidates.sort(
                key=lambda item: (
                    not item[5],
                    item[4].index if item[4] is not None else math.inf,
                    item[1].start_ns,
                    item[1].end_ns,
                )
            )
            selected_protected = protected_candidates[: limits.MAX_BUILD_NODE_SPANS]
            remaining = max(
                0,
                limits.MAX_BUILD_NODE_SPANS - len(selected_protected),
            )
            candidates = (
                selected_protected
                + sorted(
                    other_candidates,
                    key=lambda item: len(item[1].interval),
                    reverse=True,
                )[:remaining]
            )
            dropped = (
                len(protected_candidates) + len(other_candidates) - len(candidates)
            )
            attributes["ya.build.node_spans.dropped"] = dropped
            rendered_critical_count = sum(
                candidate[4] is not None for candidate in candidates
            )
            critical_dropped = (
                sum(candidate[4] is not None for candidate in protected_candidates)
                - rendered_critical_count
            )
            if critical_dropped:
                attributes["ya.build.critical_path.node_spans.dropped"] = (
                    critical_dropped
                )
            failed_representatives_dropped = len(
                failed_representatives - {candidate[0] for candidate in candidates}
            )
            if failed_representatives_dropped:
                attributes["ya.build.failed_node_spans.dropped"] = (
                    failed_representatives_dropped
                )
        candidates.sort(
            key=lambda item: (
                item[1].start_ns,
                item[1].end_ns,
                item[1].name,
                item[1].tag,
            )
        )
        attributes["ya.build.node_spans.rendered"] = len(candidates)

        node_spans = []
        command_candidates = []
        for index, candidate in enumerate(candidates):
            _, record, kind, tool, critical_entry, failed = candidate
            node_span = record.build_node_span(
                trace_id=trace_id,
                parent_span_id=build_span_id,
                kind=kind,
                tool=tool,
                index=index,
                critical_entry=critical_entry,
                failed=failed,
                exit_code=self.failures.get(record.uid) if failed else None,
            )
            node_spans.append(node_span)
            command_candidates.extend(
                (candidate, node_span, detail, detail_index)
                for detail_index, detail in enumerate(record.details)
                if detail.tag == "exec_cmd"
            )

        command_candidates.sort(
            key=lambda item: (
                not item[0][5],
                item[0][4] is None,
                -len(item[2].interval),
                item[2].start_ns,
            )
        )
        selected_commands = command_candidates[: limits.MAX_BUILD_COMMAND_SPANS]
        selected_commands.sort(
            key=lambda item: (
                item[2].start_ns,
                item[2].end_ns,
                item[0][1].name,
            )
        )
        command_dropped = len(command_details) - len(selected_commands)
        if command_details:
            attributes["ya.build.command_spans.rendered"] = len(selected_commands)
        if command_dropped:
            attributes["ya.build.command_spans.dropped"] = command_dropped

        trace.add_span(
            make_span(
                trace_id=trace_id,
                span_id=build_span_id,
                parent_span_id=dispatch_span_id or root_span_id,
                name="build operations",
                start_ns=build_start_ns,
                end_ns=build_end_ns,
                attributes=attributes,
                status_code=2 if failed_uids else 0,
                status_message=(
                    "one or more build nodes failed" if failed_uids else ""
                ),
            ),
            resource=resource,
            scope_name="ya.build",
        )

        for node_span in node_spans:
            trace.add_span(
                node_span,
                resource=resource,
                scope_name="ya.build.node",
            )
        for candidate, node_span, detail, detail_index in selected_commands:
            _, record, _, tool, _, failed = candidate
            trace.add_span(
                record.build_command_span(
                    detail,
                    trace_id=trace_id,
                    parent_span_id=node_span.span_id,
                    tool=tool,
                    index=detail_index,
                    failed=failed,
                ),
                resource=resource,
                scope_name="ya.build.command",
            )

        metadata["ya.build.node.span_count"] = len(candidates)
        metadata["ya.build.node.span_dropped_count"] = dropped
        metadata["ya.build.command.span_count"] = len(selected_commands)
        metadata["ya.build.command.span_dropped_count"] = command_dropped
        return dispatch_span_id, metadata
