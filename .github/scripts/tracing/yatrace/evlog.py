from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from ..otlp import (
    Interval,
    Ns,
    ResourceAttributes,
    Span,
    Trace,
    decode_attributes,
    make_span,
    stable_span_id,
    update_span_attributes,
)
from . import limits
from .critical_path import YaCriticalPathEntry
from .evlog_record import YaEvlogRecord
from .metrics import _metric_name, _number

SAFE_TOOL_RE = re.compile(r"^[a-zA-Z0-9_.+-]{1,32}$")
RENDERED_STAGE_NAMES = {
    "get-tools": "prepare tools",
    "build_graph_and_tests": "build graph",
    "dispatch_build": "execute graph",
    "cache_test_statuses": "cache test statuses",
    "statistics": "collect statistics",
    "finalize-reports": "finalize reports",
    "dump_results": "dump results",
}
_ChunkSpan = tuple[Span, dict[str, Any], Interval]


@dataclass
class YaEvlog:
    stages: list[YaEvlogRecord]
    nodes: list[YaEvlogRecord]
    statistics: dict[str, Any] = field(default_factory=dict)
    failures: dict[str, int | None] = field(default_factory=dict)

    @property
    def critical_path_entries(self) -> tuple[YaCriticalPathEntry, ...]:
        return tuple(
            YaCriticalPathEntry.from_raw(index, entry)
            for index, entry in enumerate(self.statistics.get("critical_path", []))
            if isinstance(entry, Mapping)
        )

    def _critical_record_score(
        self,
        record: YaEvlogRecord,
        tool: str,
        entry: YaCriticalPathEntry,
    ) -> tuple[int, int, int, int] | None:
        if entry.interval is None:
            return None
        overlap_ns = entry.interval.overlap(record.interval)
        distance_ns = entry.interval.boundary_distance(record.interval)
        return (
            int(entry.base_type in {tool, record.tag}),
            overlap_ns,
            -distance_ns,
            len(record.interval),
        )

    def _match_build_critical_path(
        self,
        build_records: Sequence[tuple[YaEvlogRecord, str, str]],
        critical_entries: Sequence[YaCriticalPathEntry],
    ) -> dict[int, YaCriticalPathEntry]:
        available = {
            index
            for index, (_, kind, _) in enumerate(build_records)
            if kind != "cache_store"
        }
        records_by_uid: dict[str, list[int]] = defaultdict(list)
        for index in available:
            uid = build_records[index][0].uid
            if uid:
                records_by_uid[uid].append(index)

        matches: dict[int, YaCriticalPathEntry] = {}
        for entry in critical_entries:
            uid = entry.uid
            if uid:
                candidate_indices = [
                    index for index in records_by_uid.get(uid, ()) if index in available
                ]
            else:
                candidate_indices = list(available)
            scored = [
                (
                    self._critical_record_score(
                        build_records[index][0],
                        build_records[index][2],
                        entry,
                    ),
                    index,
                )
                for index in candidate_indices
            ]
            scored = [
                (score, index)
                for score, index in scored
                if score is not None and (uid or score[1] > 0)
            ]
            if not scored:
                continue
            _, record_index = max(scored)
            matches[record_index] = entry
            available.remove(record_index)
        return matches

    def _critical_test_node(
        self,
        entry: YaCriticalPathEntry,
        test_nodes: Sequence[YaEvlogRecord],
        test_nodes_by_uid: Mapping[str, Sequence[YaEvlogRecord]],
    ) -> YaEvlogRecord | None:
        interval = entry.interval
        if entry.uid:
            matching_uid = test_nodes_by_uid.get(entry.uid, ())
            if matching_uid:
                if interval is not None:
                    return max(
                        matching_uid,
                        key=lambda record: (
                            interval.overlap(record.interval),
                            -interval.boundary_distance(record.interval),
                        ),
                    )
                return max(
                    matching_uid,
                    key=lambda record: len(record.interval),
                )
        if interval is None:
            return None
        overlapping = [
            record for record in test_nodes if interval.overlap(record.interval)
        ]
        if not overlapping:
            return None
        return max(
            overlapping,
            key=lambda record: (
                bool(
                    record.test_result_identity
                    and record.test_result_identity[0] in entry.text
                ),
                interval.overlap(record.interval),
            ),
        )

    def mark_critical_test_spans(self, trace: Trace) -> dict[str, int]:
        chunks: list[_ChunkSpan] = []
        chunks_by_identity: dict[tuple[str, str], list[_ChunkSpan]] = defaultdict(list)
        chunks_by_suite: dict[str, list[_ChunkSpan]] = defaultdict(list)
        for span in trace.spans("ya.chunk"):
            attributes = decode_attributes(span.attributes)
            item = (
                span,
                attributes,
                Interval(
                    Ns(span.start_time_unix_nano or 0),
                    Ns(span.end_time_unix_nano or 0),
                ),
            )
            chunks.append(item)
            suite = str(attributes.get("test.suite", ""))
            result_folder = str(attributes.get("ya.test_results.folder", ""))
            if suite:
                chunks_by_suite[suite].append(item)
                if result_folder:
                    chunks_by_identity[(suite, result_folder)].append(item)
        test_nodes = [
            record for record in self.nodes if record.kind_and_tool[0] == "test"
        ]
        test_nodes_by_uid: dict[str, list[YaEvlogRecord]] = defaultdict(list)
        for record in test_nodes:
            if record.uid:
                test_nodes_by_uid[record.uid].append(record)
        tests_by_parent: dict[bytes, list[Span]] = defaultdict(list)
        for span in trace.spans("ya.test"):
            tests_by_parent[span.parent_span_id].append(span)

        marked_chunks: set[bytes] = set()
        marked_tests: set[bytes] = set()
        critical_entries = [
            entry for entry in self.critical_path_entries if entry.is_test
        ]
        for entry in critical_entries:
            node = self._critical_test_node(
                entry,
                test_nodes,
                test_nodes_by_uid,
            )
            interval = node.interval if node is not None else entry.interval
            if interval is None:
                continue

            identity = node.test_result_identity if node is not None else None
            candidate_pool = chunks
            if identity is not None:
                candidate_pool = (
                    chunks_by_identity.get(identity)
                    or chunks_by_suite.get(identity[0])
                    or chunks
                )
            candidates = [item for item in candidate_pool if interval.overlap(item[2])]
            if not candidates:
                continue

            chunk, _, _ = max(
                candidates,
                key=lambda item: (
                    bool(item[1].get("test.suite"))
                    and str(item[1]["test.suite"]) in entry.text,
                    interval.overlap(item[2]),
                ),
            )
            attributes = entry.span_attributes(test=True)
            update_span_attributes(chunk, attributes)
            marked_chunks.add(chunk.span_id)
            for test_span in tests_by_parent.get(chunk.span_id, []):
                update_span_attributes(test_span, attributes)
                marked_tests.add(test_span.span_id)

        return {
            "ya.test.critical_path.entry.count": len(critical_entries),
            "ya.test.critical_path.chunk.count": len(marked_chunks),
            "ya.test.critical_path.span.count": len(marked_tests),
        }

    def graph_statistics_attributes(self) -> dict[str, Any]:
        attributes: dict[str, Any] = {}
        cache = self.statistics.get("cache_hit")
        if isinstance(cache, Mapping):
            cache_hit_percent = _number(cache.get("cache_hit"))
            if cache_hit_percent is not None:
                attributes["ya.build.cache.considered_task.hit.ratio"] = (
                    cache_hit_percent / 100
                )
            cache_fields = {
                "run_tasks": "ya.build.task.total.count",
                "executed_tasks": "ya.build.task.considered.count",
                "cached_tasks": "ya.build.cache.considered_task.hit.count",
                "dyn_cached_tasks": (
                    "ya.build.cache.considered_task.dynamic_hit.count"
                ),
                "not_cached_tasks": "ya.build.cache.considered_task.miss.count",
                "tests_tasks": "ya.build.task.uncached_test.count",
                "failed_tasks": "ya.build.task.failed.count",
                "ok_tasks": "ya.build.task.uncached_non_test.count",
                "avoided_tasks": "ya.build.task.avoided.count",
            }
            for source, destination in cache_fields.items():
                value = _number(cache.get(source))
                if value is not None:
                    attributes[destination] = value
            total_tasks = _number(cache.get("run_tasks"))
            cached_tasks = _number(cache.get("cached_tasks"))
            avoided_tasks = _number(cache.get("avoided_tasks"))
            if (
                total_tasks is not None
                and total_tasks > 0
                and cached_tasks is not None
                and avoided_tasks is not None
            ):
                attributes["ya.build.task.avoided.ratio"] = avoided_tasks / total_tasks
                attributes["ya.build.task.reused_or_avoided.ratio"] = (
                    cached_tasks + avoided_tasks
                ) / total_tasks
            attributes["ya.build.cache.statistics.source"] = "ya"

        dist_cache = self.statistics.get("dist_cache_stat")
        if isinstance(dist_cache, Mapping):
            dist_cache_fields = {
                "get_count": "ya.build.dist_cache.get.count",
                "get_data_size": "ya.build.dist_cache.get.bytes",
                "put_count": "ya.build.dist_cache.put.count",
                "put_data_size": "ya.build.dist_cache.put.bytes",
            }
            for source, destination in dist_cache_fields.items():
                value = _number(dist_cache.get(source))
                if value is not None:
                    attributes[destination] = value

        execution_stages = self.statistics.get("execution_stages_msec")
        if isinstance(execution_stages, Mapping):
            for name, value in execution_stages.items():
                milliseconds = _number(value)
                normalized = _metric_name(name)
                if milliseconds is not None and normalized:
                    attributes[f"ya.build.execution.stage.{normalized}.seconds"] = (
                        milliseconds / 1_000
                    )
        task_execution_msec = _number(self.statistics.get("task_execution_msec"))
        if task_execution_msec is not None:
            attributes["ya.build.execution.total.seconds"] = task_execution_msec / 1_000

        languages = self.statistics.get("graph_lang_usage")
        if isinstance(languages, Mapping):
            attributes["ya.build.graph.languages"] = sorted(
                str(language) for language in languages
            )[:128]
        return attributes

    def build_statistics_attributes(
        self,
        build_records: Sequence[tuple[YaEvlogRecord, str, str]],
    ) -> tuple[
        dict[str, Any],
        list[YaCriticalPathEntry],
    ]:
        attributes: dict[str, Any] = {}
        tool_counts: dict[str, Counter[str]] = defaultdict(Counter)
        for _, kind, tool in build_records:
            if kind in {"cache_restore", "execute"} and SAFE_TOOL_RE.fullmatch(tool):
                tool_counts[_metric_name(tool)][kind] += 1
        ranked_tools = sorted(
            tool_counts,
            key=lambda tool: (
                -(tool_counts[tool]["cache_restore"] + tool_counts[tool]["execute"]),
                tool,
            ),
        )[: limits.MAX_WORKER_TOOL_STATS]
        for normalized in ranked_tools:
            for kind in ("cache_restore", "execute"):
                count = tool_counts[normalized][kind]
                if count:
                    attributes[f"ya.build.worker.tool.{normalized}.{kind}.count"] = (
                        count
                    )

        critical_entries = [
            entry for entry in self.critical_path_entries if not entry.is_test
        ]
        if critical_entries:
            work_msec = sum(entry.elapsed_ms or 0 for entry in critical_entries)
            start_times = [
                entry.start_ms
                for entry in critical_entries
                if entry.start_ms is not None
            ]
            end_times = [
                entry.end_ms for entry in critical_entries if entry.end_ms is not None
            ]
            attributes["ya.build.critical_path.node.count"] = len(critical_entries)
            attributes["ya.build.critical_path.work.seconds"] = work_msec / 1_000
            if start_times and end_times:
                attributes["ya.build.critical_path.elapsed.seconds"] = (
                    max(end_times) - min(start_times)
                ) / 1_000
            attributes["ya.build.critical_path.summary"] = [
                entry.summary for entry in critical_entries[:128]
            ]
        return attributes, critical_entries

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
            (record, kind, tool) for record, kind, tool in classified if kind != "test"
        ]
        test_starts = [
            record.start_ns for record, kind, _ in classified if kind == "test"
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
