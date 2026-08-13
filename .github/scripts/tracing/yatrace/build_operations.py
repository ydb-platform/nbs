from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from ..otlp import Interval, Ns, Span
from ..projector import SpanProjector
from . import limits
from .critical_path import YaCriticalPath, YaCriticalPathEntry
from .node import YaNode
from .statistics import YaBuildStatistics
from .worker_spans import WorkerSpanProjector


@dataclass(frozen=True, slots=True)
class YaBuildOperations:
    nodes: Sequence[YaNode]
    failures: Mapping[str, int | None]
    statistics: YaBuildStatistics
    critical_path: YaCriticalPath
    test_starts: Sequence[Ns]

    def _envelope(self) -> tuple[Interval, list[YaNode]] | None:
        timed = [
            node
            for node in self.nodes
            if node.kind in {"cache_restore", "execute", "materialize"}
        ]
        if not timed:
            return None
        interval = Interval(
            min(node.start_ns for node in timed),
            max(node.end_ns for node in timed),
        )
        return interval, [
            clipped
            for node in self.nodes
            if (clipped := node.clipped(interval)) is not None
        ]

    def _attributes(
        self,
        interval: Interval,
        nodes: Sequence[YaNode],
    ) -> dict[str, Any]:
        commands = [
            detail
            for node in nodes
            for detail in node.details
            if detail.tag == "exec_cmd"
        ]
        attributes: dict[str, Any] = {
            "ya.build.wall_time.kind": "worker-node-execution-envelope",
            "ya.build.node.count": len(nodes),
            "ya.build.cumulative_node_seconds": Ns(
                sum(len(node.interval) for node in nodes)
            ).to_s(),
        }
        if commands:
            attributes.update(
                {
                    "ya.build.command.count": len(commands),
                    "ya.build.cumulative_command_seconds": Ns(
                        sum(len(command.interval) for command in commands)
                    ).to_s(),
                }
            )
        for kind, count in sorted(Counter(node.kind for node in nodes).items()):
            attributes[f"ya.build.node.{kind}.count"] = count
        if self.test_starts:
            attributes["ya.build.first_test_node_offset_seconds"] = Ns(
                max(0, min(self.test_starts).value - interval.start.value)
            ).to_s()
        attributes.update(
            self.statistics.build_attributes(nodes, self.critical_path.build_entries)
        )
        return attributes

    def _failures(self, nodes: Sequence[YaNode]) -> tuple[set[str], set[int]]:
        uids = {node.uid for node in nodes if node.uid and node.uid in self.failures}
        representatives = {
            max(
                (index for index, node in enumerate(nodes) if node.uid == uid),
                key=lambda index: (
                    nodes[index].kind == "execute",
                    len(nodes[index].interval),
                    nodes[index].end_ns,
                ),
            )
            for uid in uids
        }
        return uids, representatives

    @staticmethod
    def _select_nodes(
        nodes: Sequence[YaNode],
        critical: Mapping[int, YaCriticalPathEntry],
        failed: set[int],
    ) -> tuple[list[int], int, int, int]:
        candidates = [
            index
            for index, node in enumerate(nodes)
            if node.kind != "cache_store" or index in failed
        ]
        limit = limits.MAX_BUILD_NODE_SPANS
        if len(candidates) <= limit:
            return candidates, 0, 0, 0
        protected = [
            index for index in candidates if index in critical or index in failed
        ]
        other = [
            index
            for index in candidates
            if index not in critical and index not in failed
        ]
        protected.sort(
            key=lambda index: (
                index not in failed,
                critical[index].index if index in critical else math.inf,
                nodes[index].start_ns,
                nodes[index].end_ns,
            )
        )
        selected = protected[:limit]
        selected.extend(
            sorted(other, key=lambda index: len(nodes[index].interval), reverse=True)[
                : max(0, limit - len(selected))
            ]
        )
        selected_set = set(selected)
        return (
            selected,
            len(candidates) - len(selected),
            sum(index in critical for index in protected)
            - sum(index in critical for index in selected),
            sum(index in failed and index not in selected_set for index in candidates),
        )

    @staticmethod
    def _select_commands(
        nodes: Sequence[YaNode],
        selected: Sequence[int],
        critical: Mapping[int, YaCriticalPathEntry],
        failed: set[int],
    ) -> tuple[list[tuple[int, int, YaNode]], int]:
        total = sum(
            detail.tag == "exec_cmd" for node in nodes for detail in node.details
        )
        commands = [
            (node_index, detail_index, detail)
            for node_index in selected
            for detail_index, detail in enumerate(nodes[node_index].details)
            if detail.tag == "exec_cmd"
        ]
        commands.sort(
            key=lambda item: (
                item[0] not in failed,
                item[0] not in critical,
                -len(item[2].interval),
                item[2].start_ns,
            )
        )
        commands = commands[: limits.MAX_BUILD_COMMAND_SPANS]
        commands.sort(
            key=lambda item: (
                item[2].start_ns,
                item[2].end_ns,
                nodes[item[0]].name,
            )
        )
        return commands, total

    def project(self, parent: SpanProjector) -> dict[str, Any]:
        envelope = self._envelope()
        if envelope is None:
            return {"ya.build.node.count": 0}
        interval, nodes = envelope
        failed_uids, failed = self._failures(nodes)
        critical = self.critical_path.match_build(
            nodes, self.critical_path.build_entries
        )
        selected, dropped, critical_dropped, failed_dropped = self._select_nodes(
            nodes, critical, failed
        )
        policy_omitted = len(nodes) - len(selected) - dropped
        selected.sort(
            key=lambda index: (
                nodes[index].start_ns,
                nodes[index].end_ns,
                nodes[index].name,
                nodes[index].tag,
            )
        )
        commands, command_total = self._select_commands(
            nodes, selected, critical, failed
        )
        attributes = self._attributes(interval, nodes)
        attributes["ya.build.node_spans.rendered"] = len(selected)
        attributes["ya.build.node_spans.policy_omitted"] = policy_omitted
        attributes["ya.build.node_spans.dropped"] = dropped
        optional = {
            "ya.build.failed_node.count": len(failed_uids),
            "ya.build.critical_path.node_spans.dropped": critical_dropped,
            "ya.build.failed_node_spans.dropped": failed_dropped,
            "ya.build.command_spans.dropped": command_total - len(commands),
        }
        attributes.update({key: value for key, value in optional.items() if value})
        if command_total:
            attributes["ya.build.command_spans.rendered"] = len(commands)

        operation = parent.scoped("ya.build").emit(
            "ya.build",
            interval.start,
            interval.end,
            name="build operations",
            interval=interval,
            attributes=attributes,
            status_code=2 if failed_uids else 0,
            status_message=("one or more build nodes failed" if failed_uids else ""),
        )
        context = parent.under(operation)
        spans: dict[int, Span] = {}
        for output_index, node_index in enumerate(selected):
            node = nodes[node_index]
            spans[node_index] = WorkerSpanProjector(node, context).build_node(
                kind=node.kind,
                tool=node.tool,
                index=output_index,
                critical_entry=critical.get(node_index),
                failed=node_index in failed,
                exit_code=self.failures.get(node.uid) if node_index in failed else None,
            )
        for node_index, detail_index, detail in commands:
            WorkerSpanProjector(
                nodes[node_index], parent.under(spans[node_index])
            ).build_command(
                detail,
                tool=nodes[node_index].tool,
                index=detail_index,
                failed=node_index in failed,
            )
        return {
            "ya.build.node.count": len(nodes),
            "ya.build.node.span_count": len(selected),
            "ya.build.node.span_policy_omitted_count": policy_omitted,
            "ya.build.node.span_dropped_count": dropped,
            "ya.build.command.span_count": len(commands),
            "ya.build.command.span_dropped_count": command_total - len(commands),
        }
