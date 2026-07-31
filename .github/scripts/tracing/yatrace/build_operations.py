from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from ..otlp import Interval, Ns, Span, SpanWriter, make_span, stable_span_id
from . import limits
from .critical_path import YaCriticalPath, YaCriticalPathEntry
from .evlog_record import YaEvlogRecord
from .statistics import YaBuildStatistics

BuildRecord = tuple[YaEvlogRecord, str, str]


@dataclass(frozen=True, slots=True)
class BuildCandidate:
    source_index: int
    record: YaEvlogRecord
    kind: str
    tool: str
    critical_entry: YaCriticalPathEntry | None
    failed: bool


@dataclass(frozen=True, slots=True)
class BuildCommandCandidate:
    parent: BuildCandidate
    parent_span: Span
    detail: YaEvlogRecord
    detail_index: int


@dataclass(frozen=True, slots=True)
class YaBuildOperations:
    records: Sequence[BuildRecord]
    failures: Mapping[str, int | None]
    statistics: YaBuildStatistics
    critical_path: YaCriticalPath
    test_starts: Sequence[Ns]

    def build(
        self,
        *,
        writer: SpanWriter,
        trace_id: bytes,
        parent_span_id: bytes,
    ) -> dict[str, Any]:
        timed_records = [
            record
            for record in self.records
            if record[1] in {"cache_restore", "execute", "materialize"}
        ]
        metadata: dict[str, Any] = {"ya.build.node.count": 0}
        if not timed_records:
            return metadata

        build_start_ns = min(record.start_ns for record, _, _ in timed_records)
        build_end_ns = max(record.end_ns for record, _, _ in timed_records)
        build_interval = Interval(build_start_ns, build_end_ns)
        build_records = [
            (clipped, kind, tool)
            for record, kind, tool in self.records
            if (clipped := record.clipped(build_interval)) is not None
        ]
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
        if self.test_starts:
            attributes["ya.build.first_test_node_offset_seconds"] = Ns(
                max(0, min(self.test_starts) - build_start_ns)
            ).to_s()

        critical_entries = self.critical_path.build_entries
        statistics_attributes, _ = self.statistics.build_attributes(
            build_records,
            critical_entries,
        )
        attributes.update(statistics_attributes)
        critical_matches = self.critical_path.match_build(
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
            BuildCandidate(
                source_index=index,
                record=record,
                kind=kind,
                tool=tool,
                critical_entry=critical_matches.get(index),
                failed=index in failed_representatives,
            )
            for index, (record, kind, tool) in enumerate(build_records)
            if kind != "cache_store" or index in failed_representatives
        ]
        dropped = max(0, len(candidates) - limits.MAX_BUILD_NODE_SPANS)
        if dropped:
            protected = [
                candidate
                for candidate in candidates
                if candidate.critical_entry is not None or candidate.failed
            ]
            other = [
                candidate
                for candidate in candidates
                if candidate.critical_entry is None and not candidate.failed
            ]
            protected.sort(
                key=lambda candidate: (
                    not candidate.failed,
                    (
                        candidate.critical_entry.index
                        if candidate.critical_entry is not None
                        else math.inf
                    ),
                    candidate.record.start_ns,
                    candidate.record.end_ns,
                )
            )
            selected_protected = protected[: limits.MAX_BUILD_NODE_SPANS]
            remaining = max(
                0,
                limits.MAX_BUILD_NODE_SPANS - len(selected_protected),
            )
            candidates = (
                selected_protected
                + sorted(
                    other,
                    key=lambda candidate: len(candidate.record.interval),
                    reverse=True,
                )[:remaining]
            )
            dropped = len(protected) + len(other) - len(candidates)
            attributes["ya.build.node_spans.dropped"] = dropped
            critical_dropped = sum(
                candidate.critical_entry is not None for candidate in protected
            ) - sum(candidate.critical_entry is not None for candidate in candidates)
            if critical_dropped:
                attributes["ya.build.critical_path.node_spans.dropped"] = (
                    critical_dropped
                )
            failed_dropped = len(
                failed_representatives
                - {candidate.source_index for candidate in candidates}
            )
            if failed_dropped:
                attributes["ya.build.failed_node_spans.dropped"] = failed_dropped
        candidates.sort(
            key=lambda candidate: (
                candidate.record.start_ns,
                candidate.record.end_ns,
                candidate.record.name,
                candidate.record.tag,
            )
        )
        attributes["ya.build.node_spans.rendered"] = len(candidates)

        node_spans: list[Span] = []
        commands: list[BuildCommandCandidate] = []
        for index, candidate in enumerate(candidates):
            record = candidate.record
            node_span = record.build_node_span(
                trace_id=trace_id,
                parent_span_id=build_span_id,
                kind=candidate.kind,
                tool=candidate.tool,
                index=index,
                critical_entry=candidate.critical_entry,
                failed=candidate.failed,
                exit_code=(self.failures.get(record.uid) if candidate.failed else None),
            )
            node_spans.append(node_span)
            commands.extend(
                BuildCommandCandidate(candidate, node_span, detail, detail_index)
                for detail_index, detail in enumerate(record.details)
                if detail.tag == "exec_cmd"
            )

        commands.sort(
            key=lambda command: (
                not command.parent.failed,
                command.parent.critical_entry is None,
                -len(command.detail.interval),
                command.detail.start_ns,
            )
        )
        selected_commands = commands[: limits.MAX_BUILD_COMMAND_SPANS]
        selected_commands.sort(
            key=lambda command: (
                command.detail.start_ns,
                command.detail.end_ns,
                command.parent.record.name,
            )
        )
        command_dropped = len(command_details) - len(selected_commands)
        if command_details:
            attributes["ya.build.command_spans.rendered"] = len(selected_commands)
        if command_dropped:
            attributes["ya.build.command_spans.dropped"] = command_dropped

        writer.add(
            make_span(
                trace_id=trace_id,
                span_id=build_span_id,
                parent_span_id=parent_span_id,
                name="build operations",
                start_ns=build_start_ns,
                end_ns=build_end_ns,
                attributes=attributes,
                status_code=2 if failed_uids else 0,
                status_message=(
                    "one or more build nodes failed" if failed_uids else ""
                ),
            ),
            scope_name="ya.build",
        )

        for node_span in node_spans:
            writer.add(node_span, scope_name="ya.build.node")
        for command in selected_commands:
            candidate = command.parent
            writer.add(
                candidate.record.build_command_span(
                    command.detail,
                    trace_id=trace_id,
                    parent_span_id=command.parent_span.span_id,
                    tool=candidate.tool,
                    index=command.detail_index,
                    failed=candidate.failed,
                ),
                scope_name="ya.build.command",
            )

        metadata["ya.build.node.span_count"] = len(candidates)
        metadata["ya.build.node.span_dropped_count"] = dropped
        metadata["ya.build.command.span_count"] = len(selected_commands)
        metadata["ya.build.command.span_dropped_count"] = command_dropped
        return metadata
