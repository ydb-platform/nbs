"""Model and render one ``ytest.report.trace`` file."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..otlp import Interval, Ns, ResourceAttributes, SECOND, Trace
from .event import YaEvent
from .trace_spans import YaTraceSpanBuilder


@dataclass(slots=True)
class YaTraceFile:
    path: Path
    suite: str
    result_folder: str
    events: list[YaEvent]
    malformed_json_record_count: int = 0

    @staticmethod
    def identity(ya_out: Path, trace_path: Path) -> tuple[str, str]:
        relative = trace_path.relative_to(ya_out)
        parts = relative.parts
        try:
            marker = parts.index("test-results")
        except ValueError:
            return str(relative.parent), trace_path.parent.name
        suite = "/".join(parts[:marker]) or "unknown-suite"
        result_folder = parts[marker + 1] if len(parts) > marker + 1 else "unknown"
        return suite, result_folder

    def chunk_records(self) -> list[tuple[YaEvent | None, list[YaEvent]]]:
        chunk_events = [event for event in self.events if event.name == "chunk-event"]
        test_events = [
            event
            for event in self.events
            if event.name in {"subtest-started", "subtest-finished"}
        ]
        if not chunk_events:
            return [(None, test_events)] if test_events else []

        grouped_chunks: dict[
            tuple[int, int, str] | None,
            list[YaEvent],
        ] = {}
        for event in chunk_events:
            grouped_chunks.setdefault(event.chunk_identity, []).append(event)
        grouped_tests: dict[
            tuple[int, int, str] | None,
            list[YaEvent],
        ] = defaultdict(list)
        for event in test_events:
            grouped_tests[event.chunk_identity].append(event)

        assigned: dict[tuple[int, int, str] | None, list[YaEvent]] = defaultdict(list)
        unassigned: list[YaEvent] = []
        for test_identity, events in grouped_tests.items():
            if test_identity in grouped_chunks:
                assigned[test_identity].extend(events)
                continue

            candidates = (
                list(grouped_chunks)
                if test_identity is None
                else [
                    identity
                    for identity in grouped_chunks
                    if identity is not None and identity[:2] == test_identity[:2]
                ]
            )
            if len(candidates) == 1:
                assigned[candidates[0]].extend(events)
            else:
                unassigned.extend(events)

        records: list[tuple[YaEvent | None, list[YaEvent]]] = []
        for chunk_identity, snapshots in grouped_chunks.items():
            matching = assigned[chunk_identity]
            matching.sort(key=lambda event: event.order)
            records.append((YaEvent.merged(snapshots), matching))

        if unassigned:
            unassigned.sort(key=lambda event: event.order)
            records.append((None, unassigned))
        return records

    def suite_event(self) -> YaEvent | None:
        events = [event for event in self.events if event.name == "suite-event"]
        return YaEvent.merged(events) if events else None

    def finished_test(
        self,
        test_class: str,
        subtest: str,
    ) -> YaEvent | None:
        key = (test_class, subtest)
        candidates = [
            event for event in reversed(self.events) if event.name == "subtest-finished"
        ]
        exact = next(
            (event for event in candidates if event.test_key == key),
            None,
        )
        if exact is not None:
            return exact

        normalized_key = (test_class.replace("::", "."), subtest)
        fallback = [
            event for event in candidates if event.normalized_test_key == normalized_key
        ]
        if len({event.test_key for event in fallback}) == 1:
            return fallback[0]
        return None

    def chunk_events(self, chunk_index: int, chunks_total: int) -> list[YaEvent]:
        key = (chunk_index, chunks_total)
        matching = [
            event
            for event in self.events
            if event.name == "chunk-event" and event.chunk_key == key
        ]
        grouped: dict[str, list[YaEvent]] = {}
        for event in matching:
            grouped.setdefault(event.chunk_filename, []).append(event)
        return [YaEvent.merged(events) for events in grouped.values()]

    @property
    def chunk_count(self) -> int:
        return len(self.chunk_records())

    @staticmethod
    def _chunk_interval(
        chunk_event: YaEvent | None,
        events: Sequence[YaEvent],
        root: Interval,
    ) -> tuple[Interval, dict[str, Any]]:
        chunk_value = chunk_event.value if chunk_event is not None else {}
        metrics = chunk_value.get("metrics", {})
        start_ns = None
        end_ns = None
        wall_time_ns = None
        if isinstance(metrics, Mapping):
            start_ns = Ns.from_s(metrics.get("suite_start_timestamp"))
            end_ns = Ns.from_s(metrics.get("suite_finish_timestamp"))
            wall_time_ns = Ns.from_s(metrics.get("wall_time"))
            if (
                start_ns is not None
                and end_ns is not None
                and wall_time_ns is not None
                and start_ns.is_second_aligned()
                and end_ns.is_second_aligned()
                and chunk_event is not None
                and chunk_event.timestamp_ns is not None
            ):
                precise_end_ns = chunk_event.timestamp_ns
                earliest_end_ns = max(end_ns, start_ns + wall_time_ns)
                latest_end_ns = min(
                    end_ns + SECOND,
                    start_ns + SECOND + wall_time_ns,
                )
                if earliest_end_ns < latest_end_ns:
                    if earliest_end_ns <= precise_end_ns < latest_end_ns:
                        end_ns = precise_end_ns
                    else:
                        end_ns = Ns(earliest_end_ns)
                    start_ns = Ns(end_ns - wall_time_ns)
            elif end_ns is not None and start_ns is None and wall_time_ns is not None:
                start_ns = end_ns - wall_time_ns
            elif start_ns is not None and end_ns is None and wall_time_ns is not None:
                end_ns = start_ns + wall_time_ns

        timestamps = [
            event.timestamp_ns for event in events if event.timestamp_ns is not None
        ]
        if start_ns is None:
            start_ns = min(timestamps) if timestamps else root.start
        if end_ns is None:
            if chunk_event is not None and chunk_event.timestamp_ns is not None:
                end_ns = chunk_event.timestamp_ns
            elif chunk_event is not None and timestamps:
                end_ns = max(timestamps)
            else:
                end_ns = root.end
        start_ns = root.clamp(start_ns)
        end_ns = Ns(max(start_ns, root.clamp(end_ns)))
        return Interval(start_ns, end_ns), chunk_value

    def build_spans(
        self,
        *,
        trace: Trace,
        trace_id: bytes,
        root_span_id: bytes,
        root_start_ns: Ns,
        root_end_ns: Ns,
        resource: ResourceAttributes,
        trace_index: int,
    ) -> int:
        return YaTraceSpanBuilder(self).build(
            trace=trace,
            trace_id=trace_id,
            root_span_id=root_span_id,
            root_start_ns=root_start_ns,
            root_end_ns=root_end_ns,
            resource=resource,
            trace_index=trace_index,
        )
