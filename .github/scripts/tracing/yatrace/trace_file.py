"""Model and render one ``ytest.report.trace`` file."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from ..otlp import (
    Interval,
    ResourceAttributes,
    Span,
    Trace,
    make_span,
    Ns,
    SECOND,
    set_span_status,
    span_status_code,
    stable_span_id,
)
from .event import YaEvent
from .metrics import _metric_attributes
from .test_timing import YaTestTiming


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

        records: list[tuple[YaEvent | None, list[YaEvent]]] = []
        for chunk_identity, snapshots in grouped_chunks.items():
            chunk_event = YaEvent.merged(snapshots)
            matching = grouped_tests.pop(chunk_identity, [])
            if len(grouped_chunks) == 1 and chunk_identity is not None:
                matching.extend(grouped_tests.pop(None, []))
                matching.sort(key=lambda event: event.order)
            records.append((chunk_event, matching))

        unassigned = [event for events in grouped_tests.values() for event in events]
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
        wall_time_ns = Ns(0)
        if isinstance(metrics, Mapping):
            start_ns = Ns.from_s(metrics.get("suite_start_timestamp"))
            end_ns = Ns.from_s(metrics.get("suite_finish_timestamp"))
            wall_time_ns = Ns.from_s_or_zero(metrics.get("wall_time"))
            if (
                start_ns is not None
                and end_ns is not None
                and wall_time_ns
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
            elif end_ns is not None and start_ns is None and wall_time_ns:
                start_ns = end_ns - wall_time_ns
            elif start_ns is not None and end_ns is None and wall_time_ns:
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

    @staticmethod
    def _test_spans(
        trace_id: bytes,
        chunk_span_id: bytes,
        chunk: Interval,
        events: Iterable[YaEvent],
        identity: str,
        inferred_test_start_ns: Ns | None = None,
    ) -> list[Span]:
        grouped: dict[tuple[str, str, str, str], list[YaEvent]] = defaultdict(list)
        for event in events:
            if event.name in {"subtest-started", "subtest-finished"}:
                grouped[event.logical_test_key].append(event)

        result: list[Span] = []
        for (
            test_class,
            subtest,
            test_type,
            test_path,
        ), test_events in sorted(grouped.items()):
            starts = sorted(
                (event for event in test_events if event.name == "subtest-started"),
                key=lambda event: event.order,
            )
            finishes = [
                event for event in test_events if event.name == "subtest-finished"
            ]
            finishes.sort(key=lambda event: event.order)
            start = starts[0] if starts else None
            finish = None
            for candidate in finishes:
                if (
                    finish is None
                    or finish.status[0] in {"crashed", "deselected", "not_launched"}
                    or candidate.status[0] != "deselected"
                ):
                    finish = candidate
            if finish is not None and finish.status[0] in {
                "deselected",
                "not_launched",
            }:
                start = None

            timing = YaTestTiming.resolve(
                start,
                finish,
                chunk=chunk,
                inferred_start=inferred_test_start_ns,
                only_test=len(grouped) == 1,
            )
            if timing is None:
                continue
            status, status_code = timing.status

            result.append(
                make_span(
                    trace_id=trace_id,
                    span_id=stable_span_id(
                        trace_id,
                        identity,
                        test_class,
                        subtest,
                        test_type,
                        test_path,
                        timing.order,
                    ),
                    parent_span_id=chunk_span_id,
                    name=f"{test_class}::{subtest}",
                    start_ns=timing.interval.start,
                    end_ns=timing.interval.end,
                    attributes=timing.attributes(test_class, subtest),
                    status_code=status_code,
                    status_message=status if status_code == 2 else "",
                )
            )
        return result

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
        root = Interval(root_start_ns, root_end_ns)
        suite_event = self.suite_event()
        if suite_event is not None:
            timestamps = [
                event.timestamp_ns
                for event in self.events
                if event.name == "suite-event" and event.timestamp_ns is not None
            ]
            suite_start_ns = root.clamp(min(timestamps) if timestamps else root.start)
            suite_end_ns = Ns(
                max(
                    suite_start_ns,
                    root.clamp(
                        max(timestamps) if timestamps else suite_start_ns,
                    ),
                )
            )
            suite_status = ""
            suite_status_code = 0
            if suite_event.failing_errors:
                suite_status_code = 2
                suite_status = suite_event.failing_errors[0][0] or "error"
            suite_attributes: dict[str, Any] = {
                "test.suite": self.suite,
                "ya.test_results.folder": self.result_folder,
            }
            suite_attributes.update(suite_event.error_attributes("ya.suite"))
            suite_attributes.update(
                _metric_attributes(
                    suite_event.value.get("metrics"),
                    prefix="ya.suite.metric",
                )
            )
            trace.add_span(
                make_span(
                    trace_id=trace_id,
                    span_id=stable_span_id(
                        trace_id,
                        "ya.suite",
                        self.suite,
                        self.result_folder,
                        trace_index,
                    ),
                    parent_span_id=root_span_id,
                    name=f"{self.suite} [{self.result_folder} suite]",
                    start_ns=suite_start_ns,
                    end_ns=suite_end_ns,
                    attributes=suite_attributes,
                    status_code=suite_status_code,
                    status_message=suite_status if suite_status_code == 2 else "",
                ),
                resource=resource,
                scope_name="ya.suite",
            )

        chunk_records = self.chunk_records()
        for record_index, (chunk_event, chunk_events) in enumerate(chunk_records):
            chunk_interval, chunk_value = self._chunk_interval(
                chunk_event,
                chunk_events,
                root,
            )
            chunk_key = YaEvent.parse_chunk_key(chunk_value)
            identity = (
                f"{self.suite}:{self.result_folder}:{trace_index}:"
                f"{chunk_key or record_index}:{chunk_event.chunk_filename if chunk_event else ''}"
            )
            chunk_span_id = stable_span_id(trace_id, identity)
            attributes = {
                "test.suite": self.suite,
                "ya.test_results.folder": self.result_folder,
            }
            for field_name in ("chunk_index", "nchunks"):
                if field_name in chunk_value:
                    attributes[f"ya.chunk.{field_name}"] = chunk_value[field_name]
            if chunk_event is not None and chunk_event.chunk_filename:
                attributes["ya.chunk.filename"] = chunk_event.chunk_filename
            if chunk_event is not None:
                attributes.update(chunk_event.error_attributes("ya.chunk"))
            attributes.update(
                _metric_attributes(
                    chunk_value.get("metrics"),
                    prefix="ya.chunk.metric",
                )
            )
            chunk_status = ""
            chunk_status_code = 0
            if chunk_event is not None and chunk_event.failing_errors:
                chunk_status_code = 2
                chunk_status = chunk_event.failing_errors[0][0] or "error"
            if chunk_key is None:
                chunk_label = f"record {record_index + 1}"
            else:
                chunk_label = f"chunk {chunk_key[0] + 1}/{chunk_key[1]}"
            chunk = make_span(
                trace_id=trace_id,
                span_id=chunk_span_id,
                parent_span_id=root_span_id,
                name=(f"{self.suite} " f"[{self.result_folder} {chunk_label}]"),
                start_ns=chunk_interval.start,
                end_ns=chunk_interval.end,
                attributes=attributes,
                status_code=chunk_status_code,
                status_message=chunk_status if chunk_status_code == 2 else "",
            )
            metrics = chunk_value.get("metrics", {})
            test_start_ns = None
            if isinstance(metrics, Mapping):
                delay_ns = Ns.from_s_or_zero(
                    metrics.get("suite_delay_until_first_test_secs")
                )
                startup_ns = Ns.from_s_or_zero(metrics.get("suite_binary_startup_secs"))
                first_test_offset_ns = max(0, delay_ns - startup_ns)
                if delay_ns or startup_ns:
                    test_start_ns = Ns(chunk_interval.start + first_test_offset_ns)
            test_spans = self._test_spans(
                trace_id,
                chunk_span_id,
                chunk_interval,
                chunk_events,
                identity,
                inferred_test_start_ns=test_start_ns,
            )
            if any(span_status_code(span) == 2 for span in test_spans):
                set_span_status(chunk, 2, "one or more tests failed")
            trace.add_span(
                chunk,
                resource=resource,
                scope_name="ya.chunk",
            )
            for test_span in test_spans:
                trace.add_span(
                    test_span,
                    resource=resource,
                    scope_name="ya.test",
                )
        return len(chunk_records)
