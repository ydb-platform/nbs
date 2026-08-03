from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from ..otlp import Interval, Ns, Span, SpanWriter, make_span, stable_span_id
from . import limits
from .critical_path import YaCriticalPath, YaCriticalPathEntry
from .evlog_record import YaEvlogRecord
from .node import ClassifiedNode
from .statistics import YaBuildStatistics
from .worker_spans import WorkerSpanFactory


@dataclass(frozen=True, slots=True)
class BuildCandidate:
    source_index: int
    node: ClassifiedNode
    critical_entry: YaCriticalPathEntry | None
    failed: bool

    @property
    def record(self) -> YaEvlogRecord:
        return self.node.record


@dataclass(frozen=True, slots=True)
class BuildCommandCandidate:
    parent: BuildCandidate
    detail: YaEvlogRecord
    detail_index: int


@dataclass(frozen=True, slots=True)
class BuildEnvelope:
    start_ns: Ns
    end_ns: Ns
    span_id: bytes
    records: list[ClassifiedNode]


@dataclass(frozen=True, slots=True)
class BuildSelection:
    candidates: list[BuildCandidate]
    dropped: int
    critical_dropped: int
    failed_dropped: int


@dataclass(frozen=True, slots=True)
class BuildFailures:
    uids: set[str]
    representatives: set[int]


@dataclass(frozen=True, slots=True)
class CommandSelection:
    commands: list[BuildCommandCandidate]
    total: int

    @property
    def dropped(self) -> int:
        return self.total - len(self.commands)


@dataclass(frozen=True, slots=True)
class BuildPlan:
    envelope: BuildEnvelope
    attributes: dict[str, Any]
    selection: BuildSelection
    commands: CommandSelection
    failed_uids: set[str]


@dataclass(frozen=True, slots=True)
class YaBuildOperations:
    records: Sequence[ClassifiedNode]
    failures: Mapping[str, int | None]
    statistics: YaBuildStatistics
    critical_path: YaCriticalPath
    test_starts: Sequence[Ns]

    def _envelope(self, trace_id: bytes) -> BuildEnvelope | None:
        timed = [
            node
            for node in self.records
            if node.kind in {"cache_restore", "execute", "materialize"}
        ]
        if not timed:
            return None
        start_ns = min(node.start_ns for node in timed)
        end_ns = max(node.end_ns for node in timed)
        interval = Interval(start_ns, end_ns)
        return BuildEnvelope(
            start_ns=start_ns,
            end_ns=end_ns,
            span_id=stable_span_id(trace_id, "ya.build", start_ns, end_ns),
            records=[
                clipped
                for node in self.records
                if (clipped := node.clipped(interval)) is not None
            ],
        )

    def _base_attributes(self, envelope: BuildEnvelope) -> dict[str, Any]:
        records = envelope.records
        attributes: dict[str, Any] = {
            "ya.build.wall_time.kind": "worker-node-execution-envelope",
            "ya.build.node.count": len(records),
            "ya.build.cumulative_node_seconds": Ns(
                sum(len(node.interval) for node in records)
            ).to_s(),
        }
        commands = [
            detail
            for node in records
            for detail in node.record.details
            if detail.tag == "exec_cmd"
        ]
        if commands:
            attributes.update(
                {
                    "ya.build.command.count": len(commands),
                    "ya.build.cumulative_command_seconds": Ns(
                        sum(len(detail.interval) for detail in commands)
                    ).to_s(),
                }
            )
        for kind, count in sorted(Counter(node.kind for node in records).items()):
            attributes[f"ya.build.node.{kind}.count"] = count
        if self.test_starts:
            attributes["ya.build.first_test_node_offset_seconds"] = Ns(
                max(0, min(self.test_starts) - envelope.start_ns)
            ).to_s()
        attributes.update(
            self.statistics.build_attributes(
                records,
                self.critical_path.build_entries,
            )
        )
        return attributes

    def _failed_representatives(
        self,
        records: Sequence[ClassifiedNode],
    ) -> BuildFailures:
        failed_uids = {
            node.uid for node in records if node.uid and node.uid in self.failures
        }
        representatives = {
            max(
                (index for index, node in enumerate(records) if node.uid == uid),
                key=lambda index: (
                    records[index].kind == "execute",
                    len(records[index].interval),
                    records[index].end_ns,
                ),
            )
            for uid in failed_uids
        }
        return BuildFailures(failed_uids, representatives)

    @staticmethod
    def _trim_candidates(candidates: list[BuildCandidate]) -> BuildSelection:
        if len(candidates) <= limits.MAX_BUILD_NODE_SPANS:
            return BuildSelection(candidates, 0, 0, 0)
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
        selected = protected[: limits.MAX_BUILD_NODE_SPANS]
        selected += sorted(
            other,
            key=lambda candidate: len(candidate.record.interval),
            reverse=True,
        )[: max(0, limits.MAX_BUILD_NODE_SPANS - len(selected))]
        selected_indexes = {candidate.source_index for candidate in selected}
        return BuildSelection(
            candidates=selected,
            dropped=len(candidates) - len(selected),
            critical_dropped=sum(
                candidate.critical_entry is not None for candidate in protected
            )
            - sum(candidate.critical_entry is not None for candidate in selected),
            failed_dropped=sum(
                candidate.failed and candidate.source_index not in selected_indexes
                for candidate in candidates
            ),
        )

    def _select_candidates(
        self,
        records: list[ClassifiedNode],
        failed_representatives: set[int],
    ) -> BuildSelection:
        critical_matches = self.critical_path.match_build(
            records,
            self.critical_path.build_entries,
        )
        candidates = [
            BuildCandidate(
                source_index=index,
                node=node,
                critical_entry=critical_matches.get(index),
                failed=index in failed_representatives,
            )
            for index, node in enumerate(records)
            if node.kind != "cache_store" or index in failed_representatives
        ]
        selection = self._trim_candidates(candidates)
        selection.candidates.sort(
            key=lambda candidate: (
                candidate.record.start_ns,
                candidate.record.end_ns,
                candidate.record.name,
                candidate.record.tag,
            )
        )
        return selection

    @staticmethod
    def _select_commands(
        selection: BuildSelection,
        total: int,
    ) -> CommandSelection:
        commands = [
            BuildCommandCandidate(candidate, detail, detail_index)
            for candidate in selection.candidates
            for detail_index, detail in enumerate(candidate.record.details)
            if detail.tag == "exec_cmd"
        ]
        commands.sort(
            key=lambda command: (
                not command.parent.failed,
                command.parent.critical_entry is None,
                -len(command.detail.interval),
                command.detail.start_ns,
            )
        )
        selected = commands[: limits.MAX_BUILD_COMMAND_SPANS]
        selected.sort(
            key=lambda command: (
                command.detail.start_ns,
                command.detail.end_ns,
                command.parent.record.name,
            )
        )
        return CommandSelection(selected, total)

    def _plan(self, trace_id: bytes) -> BuildPlan | None:
        envelope = self._envelope(trace_id)
        if envelope is None:
            return None
        attributes = self._base_attributes(envelope)
        failures = self._failed_representatives(envelope.records)
        selection = self._select_candidates(
            envelope.records,
            failures.representatives,
        )
        commands = self._select_commands(
            selection,
            sum(
                detail.tag == "exec_cmd"
                for node in envelope.records
                for detail in node.record.details
            ),
        )
        attributes["ya.build.node_spans.rendered"] = len(selection.candidates)
        if failures.uids:
            attributes["ya.build.failed_node.count"] = len(failures.uids)
        for key, value in {
            "ya.build.node_spans.dropped": selection.dropped,
            "ya.build.critical_path.node_spans.dropped": selection.critical_dropped,
            "ya.build.failed_node_spans.dropped": selection.failed_dropped,
            "ya.build.command_spans.dropped": commands.dropped,
        }.items():
            if value:
                attributes[key] = value
        if commands.total:
            attributes["ya.build.command_spans.rendered"] = len(commands.commands)
        return BuildPlan(envelope, attributes, selection, commands, failures.uids)

    def _emit(
        self,
        plan: BuildPlan,
        *,
        writer: SpanWriter,
        trace_id: bytes,
        parent_span_id: bytes,
    ) -> None:
        envelope = plan.envelope
        writer.add(
            make_span(
                trace_id=trace_id,
                span_id=envelope.span_id,
                parent_span_id=parent_span_id,
                name="build operations",
                start_ns=envelope.start_ns,
                end_ns=envelope.end_ns,
                attributes=plan.attributes,
                status_code=2 if plan.failed_uids else 0,
                status_message=(
                    "one or more build nodes failed" if plan.failed_uids else ""
                ),
            ),
            scope_name="ya.build",
        )
        node_spans: dict[int, Span] = {}
        for index, candidate in enumerate(plan.selection.candidates):
            record = candidate.record
            span = WorkerSpanFactory(record).build_node_span(
                trace_id=trace_id,
                parent_span_id=envelope.span_id,
                kind=candidate.node.kind,
                tool=candidate.node.tool,
                index=index,
                critical_entry=candidate.critical_entry,
                failed=candidate.failed,
                exit_code=self.failures.get(record.uid) if candidate.failed else None,
            )
            node_spans[candidate.source_index] = span
            writer.add(span, scope_name="ya.build.node")
        for command in plan.commands.commands:
            candidate = command.parent
            writer.add(
                WorkerSpanFactory(candidate.record).build_command_span(
                    command.detail,
                    trace_id=trace_id,
                    parent_span_id=node_spans[candidate.source_index].span_id,
                    tool=candidate.node.tool,
                    index=command.detail_index,
                    failed=candidate.failed,
                ),
                scope_name="ya.build.command",
            )

    def build(
        self,
        *,
        writer: SpanWriter,
        trace_id: bytes,
        parent_span_id: bytes,
    ) -> dict[str, Any]:
        plan = self._plan(trace_id)
        if plan is None:
            return {"ya.build.node.count": 0}
        self._emit(
            plan,
            writer=writer,
            trace_id=trace_id,
            parent_span_id=parent_span_id,
        )
        return {
            "ya.build.node.count": len(plan.envelope.records),
            "ya.build.node.span_count": len(plan.selection.candidates),
            "ya.build.node.span_dropped_count": plan.selection.dropped,
            "ya.build.command.span_count": len(plan.commands.commands),
            "ya.build.command.span_dropped_count": plan.commands.dropped,
        }
