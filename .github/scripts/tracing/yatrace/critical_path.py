from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from ..otlp import Interval, Ns
from .metrics import finite_number
from .node import BUILD_ROOT_RE, TEST_NODE_MARKERS, YaNode, parse_test_identity

CRITICAL_TASK_SUFFIX_RE = re.compile(r"(?:(?:-CACHED|-DYN_UID_CACHE))+$")


@dataclass(frozen=True, slots=True)
class YaCriticalPathEntry:
    index: int
    base_type: str
    raw_type: str
    text: str
    uid: str
    elapsed_ms: int | float | None
    start_ms: int | float | None
    end_ms: int | float | None
    interval: Interval | None
    is_test: bool

    @classmethod
    def from_raw(
        cls,
        index: int,
        value: Mapping[str, Any],
    ) -> YaCriticalPathEntry:
        raw_type = str(value.get("type", ""))
        base_type = CRITICAL_TASK_SUFFIX_RE.sub("", raw_type)
        text = str(value.get("text", ""))
        start_ns = Ns.from_ms(value.get("start_ts"))
        end_ns = Ns.from_ms(value.get("end_ts"))
        interval = (
            Interval(start_ns, end_ns)
            if start_ns is not None and end_ns is not None and end_ns >= start_ns
            else None
        )
        return cls(
            index=index,
            base_type=base_type,
            raw_type=raw_type,
            text=text,
            uid=str(value.get("uid", "")),
            elapsed_ms=finite_number(value.get("elapsed")),
            start_ms=finite_number(value.get("start_ts")),
            end_ms=finite_number(value.get("end_ts")),
            interval=interval,
            is_test=base_type in TEST_NODE_MARKERS or "/test-results/" in text,
        )

    @property
    def reported_seconds(self) -> float | None:
        return self.elapsed_ms / 1_000 if self.elapsed_ms is not None else None

    @property
    def test_identity(self) -> tuple[str, str] | None:
        outputs = BUILD_ROOT_RE.findall(self.text)
        return parse_test_identity(outputs or self.text.split())

    def span_attributes(self, *, test: bool) -> dict[str, Any]:
        scope = "test" if test else "build"
        prefix = f"ya.{scope}.critical_path"
        attributes: dict[str, Any] = {
            prefix: True,
            f"{prefix}.index": self.index,
        }
        if test:
            attributes.update(
                {
                    f"{prefix}.inferred": True,
                    f"{prefix}.granularity": "test-chunk",
                }
            )
            if self.raw_type:
                attributes[f"{prefix}.type"] = self.raw_type
            if self.text:
                attributes[f"{prefix}.text"] = self.text
        if self.reported_seconds is not None:
            attributes[f"{prefix}.reported_seconds"] = self.reported_seconds
        return attributes

    @property
    def summary(self) -> str:
        return (
            f"{self.raw_type or 'unknown'}: {self.text or 'unknown'} "
            f"({self.reported_seconds or 0:.3f}s)"
        )


@dataclass(frozen=True, slots=True)
class YaCriticalPath:
    entries: tuple[YaCriticalPathEntry, ...]
    nodes: Sequence[YaNode]

    @classmethod
    def from_evlog(
        cls,
        statistics: Mapping[str, Any],
        nodes: Sequence[YaNode],
    ) -> YaCriticalPath:
        entries = tuple(
            YaCriticalPathEntry.from_raw(index, entry)
            for index, entry in enumerate(statistics.get("critical_path", []))
            if isinstance(entry, Mapping)
        )
        return cls(entries, nodes)

    @property
    def build_entries(self) -> tuple[YaCriticalPathEntry, ...]:
        return tuple(entry for entry in self.entries if not entry.is_test)

    @property
    def test_entries(self) -> tuple[YaCriticalPathEntry, ...]:
        return tuple(entry for entry in self.entries if entry.is_test)

    def _node_score(
        self,
        node: YaNode,
        entry: YaCriticalPathEntry,
    ) -> tuple[int, Ns, int, int] | None:
        if entry.interval is None:
            return None
        overlap_ns = entry.interval.overlap(node.interval)
        distance_ns = entry.interval.boundary_distance(node.interval)
        return (
            int(entry.base_type in {node.tool, node.tag}),
            overlap_ns,
            -distance_ns.value,
            len(node.interval),
        )

    def match_build(
        self,
        build_records: Sequence[YaNode],
        critical_entries: Sequence[YaCriticalPathEntry],
    ) -> dict[int, YaCriticalPathEntry]:
        available = {
            index
            for index, node in enumerate(build_records)
            if node.kind != "cache_store"
        }
        records_by_uid: dict[str, list[int]] = defaultdict(list)
        for index in available:
            uid = build_records[index].uid
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
            scored = (
                (score, index)
                for index in candidate_indices
                if (score := self._node_score(build_records[index], entry))
                if score is not None and (uid or score[1] > Ns(0))
            )
            match = max(scored, default=None)
            if match is None:
                continue
            _, record_index = match
            matches[record_index] = entry
            available.remove(record_index)
        return matches

    def match_tests(
        self, test_nodes: Sequence[YaNode]
    ) -> dict[int, YaCriticalPathEntry]:
        """Resolve critical entries to worker indexes without guessing their chunks."""
        nodes_by_uid: dict[str, list[int]] = defaultdict(list)
        for index, node in enumerate(test_nodes):
            if node.uid:
                nodes_by_uid[node.uid].append(index)

        matches: dict[int, YaCriticalPathEntry] = {}
        for entry in self.test_entries:
            candidates = (
                nodes_by_uid.get(entry.uid, [])
                if entry.uid
                else list(range(len(test_nodes)))
            )
            if (identity := entry.test_identity) is not None:
                candidates = [
                    index
                    for index in candidates
                    if test_nodes[index].test_identity == identity
                ]
            interval = entry.interval
            if interval is None:
                if not entry.uid:
                    continue
                index = max(
                    candidates,
                    key=lambda index: len(test_nodes[index].interval),
                    default=None,
                )
            else:
                if not entry.uid:
                    candidates = [
                        index
                        for index in candidates
                        if interval.overlap(test_nodes[index].interval) > Ns(0)
                    ]
                index = max(
                    candidates,
                    key=lambda index: (
                        interval.overlap(test_nodes[index].interval),
                        -interval.boundary_distance(test_nodes[index].interval).value,
                    ),
                    default=None,
                )
            if index is not None:
                matches[index] = entry
        return matches
