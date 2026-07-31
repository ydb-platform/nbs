from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from ..otlp import (
    Interval,
    Ns,
    Span,
    Trace,
    decode_attributes,
    update_span_attributes,
)
from .metrics import _number

if TYPE_CHECKING:
    from .evlog_record import YaEvlogRecord

CRITICAL_TASK_SUFFIX_RE = re.compile(r"(?:(?:-CACHED|-DYN_UID_CACHE))+$")
TEST_NODE_MARKERS = frozenset({"TA", "TL", "TM", "TS"})
_ChunkSpan = tuple[Span, dict[str, Any], Interval]


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
            elapsed_ms=_number(value.get("elapsed")),
            start_ms=_number(value.get("start_ts")),
            end_ms=_number(value.get("end_ts")),
            interval=interval,
            is_test=base_type in TEST_NODE_MARKERS or "/test-results/" in text,
        )

    @property
    def reported_seconds(self) -> float | None:
        return self.elapsed_ms / 1_000 if self.elapsed_ms is not None else None

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
    nodes: Sequence[YaEvlogRecord]

    @classmethod
    def from_evlog(
        cls,
        statistics: Mapping[str, Any],
        nodes: Sequence[YaEvlogRecord],
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

    def match_build(
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
            scored = (
                (score, index)
                for index in candidate_indices
                if (
                    score := self._critical_record_score(
                        build_records[index][0],
                        build_records[index][2],
                        entry,
                    )
                )
                if score is not None and (uid or score[1] > 0)
            )
            match = max(scored, default=None)
            if match is None:
                continue
            _, record_index = match
            matches[record_index] = entry
            available.remove(record_index)
        return matches

    def match_test_node(
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

    def mark_test_spans(self, trace: Trace) -> dict[str, int]:
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
            record for record in self.nodes if record.kind_and_tool[0] == "test_execute"
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
        critical_entries = self.test_entries
        for entry in critical_entries:
            node = self.match_test_node(
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
