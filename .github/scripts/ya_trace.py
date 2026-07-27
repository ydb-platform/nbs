"""Read, model, and render data from ya test traces and event logs."""

from __future__ import annotations

import json
import logging
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .trace_report import Span, stable_hex_id

__all__ = [
    "YaEvent",
    "YaEvlog",
    "YaEvlogRecord",
    "YaTraceCollection",
    "YaTraceFile",
    "load_ya_evlog",
    "load_ya_traces",
    "ns",
]

LOGGER = logging.getLogger(__name__)
MAX_YA_TRACE_BYTES = 512 * 1024 * 1024
MAX_YA_TRACE_FILES = 20_000
MAX_YA_EVENTS = 2_000_000
MAX_YA_EVLOG_BYTES = 512 * 1024 * 1024
MAX_YA_EVLOG_LINE_CHARACTERS = 16 * 1024 * 1024
MAX_YA_EVLOG_EVENTS = 2_000_000
MAX_BUILD_NODE_SPANS = 50_000
MAX_CACHE_TOOL_STATS = 24
SAFE_METRIC_RE = re.compile(r"[^a-zA-Z0-9_.-]+")
SAFE_TOOL_RE = re.compile(r"^[a-zA-Z0-9_.+-]{1,32}$")
BUILD_ROOT_RE = re.compile(r"\$\(BUILD_ROOT\)/([^\s)]+)")
NODE_UID_RE = re.compile(r"^[^(]+\(([^$()\s]+)\$\(BUILD_ROOT\)")
TAG_WRAPPER_RE = re.compile(
    r"^(restore|result|put_in_cache|write_through_caches)\[([^\]]+)\]$"
)
PASS_STATUSES = {"good", "pass", "passed", "success"}
ERROR_STATUSES = {
    "crashed",
    "error",
    "fail",
    "failed",
    "internal",
    "not_launched",
    "timeout",
}
RENDERED_STAGE_NAMES = {
    "get-tools": "prepare tools",
    "build_graph_and_tests": "build graph",
    "dispatch_build": "execute graph",
    "cache_test_statuses": "cache test statuses",
    "statistics": "collect statistics",
    "finalize-reports": "finalize reports",
    "dump_results": "dump results",
}


class ns(int):
    """A non-negative nanosecond value parsed from seconds."""

    def __new__(cls, value: int | float | str) -> ns:
        nanoseconds = int(value)
        if nanoseconds < 0:
            raise ValueError("nanoseconds must be non-negative")
        return int.__new__(cls, nanoseconds)

    @classmethod
    def from_seconds(cls, value: Any) -> ns | None:
        try:
            seconds = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(seconds) or seconds < 0:
            return None
        return cls(seconds * 1_000_000_000)

    @classmethod
    def from_seconds_or_zero(cls, value: Any) -> ns:
        return cls.from_seconds(value) or cls(0)

    @classmethod
    def from_milliseconds(cls, value: Any) -> ns | None:
        try:
            milliseconds = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(milliseconds) or milliseconds < 0:
            return None
        return cls(milliseconds * 1_000_000)


@dataclass
class YaEvent:
    name: str
    timestamp_ns: ns | None
    value: dict[str, Any]
    order: int

    @classmethod
    def from_raw(cls, raw: Any, order: int) -> YaEvent | None:
        if not isinstance(raw, Mapping):
            return None
        name = str(raw.get("name", ""))
        value = raw.get("value", {})
        if name not in {
            "chunk-event",
            "subtest-finished",
            "subtest-started",
        } or not isinstance(value, dict):
            return None
        return cls(
            name=name,
            timestamp_ns=ns.from_seconds(raw.get("timestamp")),
            value=value,
            order=order,
        )

    @staticmethod
    def parse_chunk_key(value: Mapping[str, Any]) -> tuple[int, int] | None:
        try:
            chunk_index = int(value["chunk_index"])
            chunks_total = int(value["nchunks"])
        except (KeyError, TypeError, ValueError):
            return None
        if chunk_index < 0 or chunks_total < 1 or chunk_index >= chunks_total:
            return None
        return chunk_index, chunks_total

    @property
    def chunk_key(self) -> tuple[int, int] | None:
        return self.parse_chunk_key(self.value)

    @property
    def test_key(self) -> tuple[str, str]:
        test_class = str(self.value.get("class", "unknown")).replace("::", ".")
        subtest = str(self.value.get("subtest", "unknown"))
        return test_class, subtest

    @property
    def status(self) -> tuple[str, int]:
        status = str(self.value.get("status", "unknown")).lower()
        if status in PASS_STATUSES:
            return status, 1
        if status in ERROR_STATUSES:
            return status, 2
        return status, 0

    def test_attributes(
        self,
        test_class: str,
        subtest: str,
        *,
        inferred: bool,
        timing_source: str = "",
        incomplete: bool = False,
    ) -> dict[str, Any]:
        status, _ = self.status
        attributes: dict[str, Any] = {
            "test.framework": "ya",
            "test.suite": test_class,
            "test.name": subtest,
            "test.status": status,
        }
        if inferred:
            attributes["test.timing.inferred"] = True
        if timing_source:
            attributes["test.timing.source"] = timing_source
        if incomplete:
            attributes["test.incomplete"] = True
        attributes.update(_metric_attributes(self.value.get("metrics")))
        return attributes

    @property
    def log_paths(self) -> dict[str, str]:
        logs = self.value.get("logs")
        if not isinstance(logs, Mapping):
            return {}
        return {str(name): path for name, path in logs.items() if isinstance(path, str)}

    def resolved_logs(self, build_root: Path) -> dict[str, str]:
        root = str(build_root)
        return {
            name: path.replace("$(BUILD_ROOT)", root)
            for name, path in self.log_paths.items()
            if name != "logsdir"
        }

    @property
    def logs_directory(self) -> str | None:
        logs_directory = self.log_paths.get("logsdir")
        if logs_directory is None:
            return None
        return logs_directory.replace("$(BUILD_ROOT)", "").lstrip("/")


@dataclass
class YaTraceFile:
    path: Path
    suite: str
    result_folder: str
    events: list[YaEvent]

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
            return [(None, test_events)]
        if len(chunk_events) == 1:
            return [(chunk_events[0], test_events)]

        records = []
        assigned_orders = set()
        for chunk_event in chunk_events:
            matching = [
                event
                for event in test_events
                if event.chunk_key == chunk_event.chunk_key
            ]
            assigned_orders.update(event.order for event in matching)
            records.append((chunk_event, matching))

        unassigned = [
            event for event in test_events if event.order not in assigned_orders
        ]
        if unassigned:
            records.append((None, unassigned))
        return records

    def finished_test(
        self,
        test_class: str,
        subtest: str,
    ) -> YaEvent | None:
        key = (test_class, subtest)
        return next(
            (
                event
                for event in reversed(self.events)
                if event.name == "subtest-finished" and event.test_key == key
            ),
            None,
        )

    def chunk_events(self, chunk_index: int, chunks_total: int) -> list[YaEvent]:
        key = (chunk_index, chunks_total)
        return [
            event
            for event in self.events
            if event.name == "chunk-event" and event.chunk_key == key
        ]

    @property
    def chunk_count(self) -> int:
        return len(self.chunk_records())

    @staticmethod
    def _chunk_bounds(
        chunk_event: YaEvent | None,
        events: Sequence[YaEvent],
        root_start_ns: ns,
        root_end_ns: ns,
    ) -> tuple[ns, ns, dict[str, Any]]:
        chunk_value = chunk_event.value if chunk_event is not None else {}
        metrics = chunk_value.get("metrics", {})
        start_ns = None
        end_ns = None
        wall_time_ns = ns(0)
        if isinstance(metrics, Mapping):
            start_ns = ns.from_seconds(metrics.get("suite_start_timestamp"))
            end_ns = ns.from_seconds(metrics.get("suite_finish_timestamp"))
            wall_time_ns = ns.from_seconds_or_zero(metrics.get("wall_time"))
            if end_ns is not None and wall_time_ns:
                start_ns = end_ns - wall_time_ns
            elif start_ns is not None and wall_time_ns:
                end_ns = start_ns + wall_time_ns

        timestamps = [
            event.timestamp_ns for event in events if event.timestamp_ns is not None
        ]
        start_ns = start_ns or (min(timestamps) if timestamps else root_start_ns)
        if end_ns is None:
            if chunk_event is not None and chunk_event.timestamp_ns is not None:
                end_ns = chunk_event.timestamp_ns
            elif chunk_event is not None and timestamps:
                end_ns = max(timestamps)
            else:
                end_ns = root_end_ns
        start_ns = ns(max(root_start_ns, min(start_ns, root_end_ns)))
        end_ns = ns(max(start_ns, min(end_ns, root_end_ns)))
        return start_ns, end_ns, chunk_value

    @staticmethod
    def _test_spans(
        trace_id: str,
        chunk_span_id: str,
        chunk_start_ns: ns,
        chunk_end_ns: ns,
        events: Iterable[YaEvent],
        identity: str,
        resource: Mapping[str, Any],
        inferred_test_start_ns: ns | None = None,
    ) -> list[Span]:
        grouped: dict[tuple[str, str], list[YaEvent]] = defaultdict(list)
        for event in events:
            if event.name in {"subtest-started", "subtest-finished"}:
                grouped[event.test_key].append(event)

        result: list[Span] = []
        for (test_class, subtest), test_events in sorted(grouped.items()):
            test_events.sort(
                key=lambda event: (
                    event.timestamp_ns is None,
                    event.timestamp_ns or 0,
                    event.order,
                )
            )
            starts = [event for event in test_events if event.name == "subtest-started"]
            finishes = [
                event for event in test_events if event.name == "subtest-finished"
            ]
            used_finishes: set[int] = set()

            for start_index, start in enumerate(starts):
                start_ns = start.timestamp_ns or chunk_start_ns
                next_start_ns = (
                    starts[start_index + 1].timestamp_ns
                    if start_index + 1 < len(starts)
                    else None
                )
                candidates = [
                    (finish_index, finish)
                    for finish_index, finish in enumerate(finishes)
                    if finish_index not in used_finishes
                    and (finish.timestamp_ns is None or finish.timestamp_ns >= start_ns)
                    and (
                        next_start_ns is None
                        or finish.timestamp_ns is None
                        or finish.timestamp_ns < next_start_ns
                    )
                ]
                if candidates:
                    for finish_index, _ in candidates:
                        used_finishes.add(finish_index)
                    _, finish = candidates[-1]
                    duration_ns = ns.from_seconds_or_zero(finish.value.get("time"))
                    end_ns = finish.timestamp_ns or min(
                        chunk_end_ns, start_ns + duration_ns
                    )
                    end_ns = max(start_ns, min(end_ns, chunk_end_ns))
                    status, status_code = finish.status
                    attributes = finish.test_attributes(
                        test_class,
                        subtest,
                        inferred=(
                            start.timestamp_ns is None or finish.timestamp_ns is None
                        ),
                        timing_source="subtest-events",
                    )
                    source_order = start.order
                else:
                    finish = start
                    end_ns = max(start_ns, chunk_end_ns)
                    status = "incomplete"
                    status_code = 2
                    attributes = start.test_attributes(
                        test_class,
                        subtest,
                        inferred=start.timestamp_ns is None,
                        timing_source="subtest-start-and-chunk-end",
                        incomplete=True,
                    )
                    source_order = start.order

                result.append(
                    Span(
                        trace_id=trace_id,
                        span_id=stable_hex_id(
                            trace_id,
                            identity,
                            test_class,
                            subtest,
                            source_order,
                            length=16,
                        ),
                        parent_span_id=chunk_span_id,
                        name=f"{test_class}::{subtest}",
                        start_ns=start_ns,
                        end_ns=end_ns,
                        attributes=attributes,
                        status_code=status_code,
                        status_message=status if status_code == 2 else "",
                        resource_attributes=dict(resource),
                        scope_name="ya.test",
                    )
                )

            for finish_index, finish in enumerate(finishes):
                if finish_index in used_finishes:
                    continue
                duration_ns = ns.from_seconds_or_zero(finish.value.get("time"))
                if inferred_test_start_ns is not None and len(grouped) == 1:
                    start_ns = max(
                        chunk_start_ns,
                        min(inferred_test_start_ns, chunk_end_ns),
                    )
                    end_ns = max(
                        start_ns,
                        min(start_ns + duration_ns, chunk_end_ns),
                    )
                    timing_source = "chunk-delay-and-test-duration"
                else:
                    end_ns = finish.timestamp_ns or chunk_end_ns
                    start_ns = max(chunk_start_ns, end_ns - duration_ns)
                    end_ns = max(start_ns, min(end_ns, chunk_end_ns))
                    timing_source = "finish-event-and-test-duration"
                status, status_code = finish.status
                result.append(
                    Span(
                        trace_id=trace_id,
                        span_id=stable_hex_id(
                            trace_id,
                            identity,
                            test_class,
                            subtest,
                            finish.order,
                            length=16,
                        ),
                        parent_span_id=chunk_span_id,
                        name=f"{test_class}::{subtest}",
                        start_ns=start_ns,
                        end_ns=end_ns,
                        attributes=finish.test_attributes(
                            test_class,
                            subtest,
                            inferred=True,
                            timing_source=timing_source,
                        ),
                        status_code=status_code,
                        status_message=status if status_code == 2 else "",
                        resource_attributes=dict(resource),
                        scope_name="ya.test",
                    )
                )
        return result

    def build_spans(
        self,
        *,
        trace_id: str,
        root_span_id: str,
        root_start_ns: ns,
        root_end_ns: ns,
        resource: Mapping[str, Any],
        trace_index: int,
    ) -> list[Span]:
        spans: list[Span] = []
        for record_index, (chunk_event, chunk_events) in enumerate(
            self.chunk_records()
        ):
            chunk_start_ns, chunk_end_ns, chunk_value = self._chunk_bounds(
                chunk_event,
                chunk_events,
                root_start_ns,
                root_end_ns,
            )
            chunk_key = YaEvent.parse_chunk_key(chunk_value)
            identity = (
                f"{self.suite}:{self.result_folder}:{trace_index}:"
                f"{chunk_key or record_index}"
            )
            chunk_span_id = stable_hex_id(trace_id, identity, length=16)
            attributes = {
                "test.suite": self.suite,
                "ya.test_results.folder": self.result_folder,
            }
            for field_name in ("chunk_index", "nchunks", "status"):
                if field_name in chunk_value:
                    attributes[f"ya.chunk.{field_name}"] = chunk_value[field_name]
            attributes.update(_metric_attributes(chunk_value.get("metrics")))
            chunk_status = str(chunk_value.get("status", "")).lower()
            chunk_status_code = 2 if chunk_status in ERROR_STATUSES else 0
            if chunk_key is None:
                chunk_label = f"record {record_index + 1}"
            else:
                chunk_label = f"chunk {chunk_key[0] + 1}/{chunk_key[1]}"
            chunk = Span(
                trace_id=trace_id,
                span_id=chunk_span_id,
                parent_span_id=root_span_id,
                name=(f"{self.suite} " f"[{self.result_folder} {chunk_label}]"),
                start_ns=chunk_start_ns,
                end_ns=chunk_end_ns,
                attributes=attributes,
                status_code=chunk_status_code,
                status_message=chunk_status if chunk_status_code == 2 else "",
                resource_attributes=dict(resource),
                scope_name="ya.chunk",
            )
            metrics = chunk_value.get("metrics", {})
            test_start_ns = None
            if isinstance(metrics, Mapping):
                delay_ns = ns.from_seconds_or_zero(
                    metrics.get("suite_delay_until_first_test_secs")
                )
                if delay_ns:
                    test_start_ns = ns(chunk_start_ns + delay_ns)
            test_spans = self._test_spans(
                trace_id,
                chunk_span_id,
                chunk_start_ns,
                chunk_end_ns,
                chunk_events,
                identity,
                resource,
                inferred_test_start_ns=test_start_ns,
            )
            if any(span.status_code == 2 for span in test_spans):
                chunk.status_code = 2
                if not chunk.status_message:
                    chunk.status_message = "one or more tests failed"
            spans.append(chunk)
            spans.extend(test_spans)
        return spans


@dataclass
class YaTraceCollection:
    root: Path
    traces: list[YaTraceFile]

    @classmethod
    def load(
        cls,
        root: Path,
        *,
        modified_since: float | None = None,
    ) -> YaTraceCollection:
        return cls(
            root=root,
            traces=load_ya_traces(root, modified_since=modified_since),
        )

    def finished_test(
        self,
        suite: str,
        test_class: str,
        subtest: str,
    ) -> YaEvent | None:
        for trace in reversed(self.traces):
            if trace.suite != suite:
                continue
            event = trace.finished_test(test_class, subtest)
            if event is not None:
                return event
        return None

    def chunk_event_candidates(
        self,
        suite: str,
        chunk_index: int,
        chunks_total: int,
    ) -> list[tuple[YaTraceFile, YaEvent]]:
        return [
            (trace, event)
            for trace in self.traces
            if trace.suite == suite
            for event in trace.chunk_events(chunk_index, chunks_total)
        ]

    def select_chunk_event(
        self,
        suite: str,
        chunk_index: int,
        chunks_total: int,
        failure_text: str | None = None,
    ) -> YaEvent | None:
        candidates = self.chunk_event_candidates(
            suite,
            chunk_index,
            chunks_total,
        )
        if len(candidates) == 1:
            return candidates[0][1]
        if not failure_text:
            return None

        for trace, event in candidates:
            if f"test-results/{trace.result_folder}" in failure_text:
                return event

        root = str(self.root)
        for _, event in candidates:
            for path in event.log_paths.values():
                if path.replace("$(BUILD_ROOT)", root) in failure_text:
                    return event
        return None


@dataclass
class YaEvlogRecord:
    name: str
    tag: str
    start_ns: ns
    end_ns: ns
    thread_name: str = ""

    @classmethod
    def from_raw(
        cls,
        value: Mapping[str, Any],
        *,
        thread_name: str = "",
    ) -> YaEvlogRecord | None:
        time_range = cls._time_range(value.get("time"))
        if time_range is None:
            return None
        return cls(
            name=str(value.get("name", "unknown")),
            tag=str(value.get("tag", "")),
            start_ns=time_range[0],
            end_ns=time_range[1],
            thread_name=thread_name,
        )

    @staticmethod
    def _time_range(value: Any) -> tuple[ns, ns] | None:
        if not isinstance(value, list) or len(value) != 2:
            return None
        start_ns = ns.from_seconds(value[0])
        end_ns = ns.from_seconds(value[1])
        if start_ns is None or end_ns is None or end_ns < start_ns:
            return None
        return start_ns, end_ns

    def clipped(self, start_ns: ns, end_ns: ns) -> YaEvlogRecord | None:
        clipped_start = max(start_ns, self.start_ns)
        clipped_end = min(end_ns, self.end_ns)
        if clipped_end <= clipped_start:
            return None
        return YaEvlogRecord(
            name=self.name,
            tag=self.tag,
            start_ns=ns(clipped_start),
            end_ns=ns(clipped_end),
            thread_name=self.thread_name,
        )

    @property
    def kind_and_tool(self) -> tuple[str, str]:
        if "/test-results/" in self.name:
            match = TAG_WRAPPER_RE.fullmatch(self.tag)
            return "test", match.group(2) if match else self.tag
        match = TAG_WRAPPER_RE.fullmatch(self.tag)
        if match:
            wrapper, tool = match.groups()
            if tool == "TM":
                return "test", tool
            return {
                "restore": "cache_restore",
                "result": "materialize",
                "put_in_cache": "cache_store",
                "write_through_caches": "cache_store",
            }[wrapper], tool
        if self.tag == "TM":
            return "test", self.tag
        if self.name.startswith("Run("):
            return "execute", self.tag
        return "orchestration", self.tag

    @property
    def outputs(self) -> list[str]:
        return list(dict.fromkeys(BUILD_ROOT_RE.findall(self.name)))[:16]

    @property
    def uid(self) -> str:
        match = NODE_UID_RE.match(self.name)
        return match.group(1) if match else ""

    @property
    def test_result_identity(self) -> tuple[str, str] | None:
        for output in self.outputs:
            suite, marker, relative = output.partition("/test-results/")
            if not marker or not suite or not relative:
                continue
            result_folder = relative.split("/", 1)[0]
            if result_folder:
                return suite, result_folder
        return None

    def span_name(self, kind: str, tool: str) -> str:
        output = next(iter(self.outputs), "")
        labels = {
            "cache_restore": "cache restore",
            "materialize": "materialize",
            "cache_store": "cache store",
            "execute": tool or "execute",
            "orchestration": tool or self.name,
        }
        label = labels[kind]
        if kind in {"cache_restore", "materialize", "cache_store"} and tool:
            label += f" [{tool}]"
        return f"{label}: {output}" if output else label

    def build_node_span(
        self,
        *,
        trace_id: str,
        parent_span_id: str,
        kind: str,
        tool: str,
        index: int,
        resource: Mapping[str, Any],
        critical_by_uid: Mapping[str, tuple[int, Mapping[str, Any]]],
    ) -> Span:
        attributes: dict[str, Any] = {
            "ya.build.kind": kind,
            "ya.build.tag": self.tag,
            "ya.build.node.name": self.name,
        }
        if tool:
            attributes["ya.build.tool"] = tool
        if self.outputs:
            attributes["ya.build.outputs"] = self.outputs
        if self.thread_name:
            attributes["ya.worker.thread"] = self.thread_name
        if kind == "cache_restore":
            attributes["ya.build.cache.hit"] = True
        if self.uid:
            attributes["ya.build.node.uid"] = self.uid
        if kind in {"cache_restore", "execute"} and self.uid in critical_by_uid:
            critical_index, critical_entry = critical_by_uid[self.uid]
            attributes["ya.build.critical_path"] = True
            attributes["ya.build.critical_path.index"] = critical_index
            elapsed = _number(critical_entry.get("elapsed"))
            if elapsed is not None:
                attributes["ya.build.critical_path.reported_seconds"] = elapsed / 1_000
        return Span(
            trace_id=trace_id,
            span_id=stable_hex_id(
                trace_id,
                "ya.build.node",
                self.name,
                self.tag,
                self.start_ns,
                index,
                length=16,
            ),
            parent_span_id=parent_span_id,
            name=self.span_name(kind, tool),
            start_ns=self.start_ns,
            end_ns=self.end_ns,
            attributes=attributes,
            resource_attributes=dict(resource),
            scope_name="ya.build.node",
        )


@dataclass
class YaEvlog:
    stages: list[YaEvlogRecord]
    nodes: list[YaEvlogRecord]
    statistics: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def _is_test_critical_path_entry(entry: Mapping[str, Any]) -> bool:
        task_type = str(entry.get("type", ""))
        text = str(entry.get("text", ""))
        return task_type in {"TA", "TM", "TS"} or "/test-results/" in text

    @staticmethod
    def _overlap_ns(
        left_start_ns: int,
        left_end_ns: int,
        right_start_ns: int,
        right_end_ns: int,
    ) -> int:
        return max(
            0,
            min(left_end_ns, right_end_ns) - max(left_start_ns, right_start_ns),
        )

    def _critical_test_node(
        self,
        entry: Mapping[str, Any],
        start_ns: ns | None,
        end_ns: ns | None,
        test_nodes: Sequence[YaEvlogRecord],
    ) -> YaEvlogRecord | None:
        uid = str(entry.get("uid", ""))
        if uid:
            matching_uid = [record for record in test_nodes if record.uid == uid]
            if matching_uid:
                return max(
                    matching_uid,
                    key=lambda record: record.end_ns - record.start_ns,
                )
        if start_ns is None or end_ns is None:
            return None
        overlapping = [
            record
            for record in test_nodes
            if self._overlap_ns(
                start_ns,
                end_ns,
                record.start_ns,
                record.end_ns,
            )
        ]
        if not overlapping:
            return None
        text = str(entry.get("text", ""))
        return max(
            overlapping,
            key=lambda record: (
                bool(
                    record.test_result_identity
                    and record.test_result_identity[0] in text
                ),
                self._overlap_ns(
                    start_ns,
                    end_ns,
                    record.start_ns,
                    record.end_ns,
                ),
            ),
        )

    @staticmethod
    def _critical_test_attributes(
        index: int,
        entry: Mapping[str, Any],
    ) -> dict[str, Any]:
        attributes: dict[str, Any] = {
            "ya.test.critical_path": True,
            "ya.test.critical_path.index": index,
            "ya.test.critical_path.inferred": True,
            "ya.test.critical_path.granularity": "test-chunk",
        }
        task_type = str(entry.get("type", ""))
        if task_type:
            attributes["ya.test.critical_path.type"] = task_type
        text = str(entry.get("text", ""))
        if text:
            attributes["ya.test.critical_path.text"] = text
        elapsed_msec = _number(entry.get("elapsed"))
        if elapsed_msec is not None:
            attributes["ya.test.critical_path.reported_seconds"] = elapsed_msec / 1_000
        return attributes

    def mark_critical_test_spans(self, spans: Sequence[Span]) -> dict[str, int]:
        chunks = [span for span in spans if span.scope_name == "ya.chunk"]
        test_nodes = [
            record for record in self.nodes if record.kind_and_tool[0] == "test"
        ]
        tests_by_parent: dict[str, list[Span]] = defaultdict(list)
        for span in spans:
            if span.scope_name == "ya.test":
                tests_by_parent[span.parent_span_id].append(span)

        marked_chunks: set[str] = set()
        marked_tests: set[str] = set()
        critical_entries = [
            (index, entry)
            for index, entry in enumerate(self.statistics.get("critical_path", []))
            if isinstance(entry, Mapping) and self._is_test_critical_path_entry(entry)
        ]
        for index, entry in critical_entries:
            start_ns = ns.from_milliseconds(entry.get("start_ts"))
            end_ns = ns.from_milliseconds(entry.get("end_ts"))
            if start_ns is not None and end_ns is not None and end_ns < start_ns:
                start_ns = None
                end_ns = None
            node = self._critical_test_node(
                entry,
                start_ns,
                end_ns,
                test_nodes,
            )
            if node is not None:
                start_ns = node.start_ns
                end_ns = node.end_ns
            if start_ns is None or end_ns is None:
                continue

            candidates = [
                chunk
                for chunk in chunks
                if self._overlap_ns(
                    start_ns,
                    end_ns,
                    chunk.start_ns,
                    chunk.end_ns,
                )
            ]
            if not candidates:
                continue
            identity = node.test_result_identity if node is not None else None
            if identity is not None:
                exact = [
                    chunk
                    for chunk in candidates
                    if (
                        chunk.attributes.get("test.suite"),
                        chunk.attributes.get("ya.test_results.folder"),
                    )
                    == identity
                ]
                if exact:
                    candidates = exact
                else:
                    matching_suite = [
                        chunk
                        for chunk in candidates
                        if chunk.attributes.get("test.suite") == identity[0]
                    ]
                    if matching_suite:
                        candidates = matching_suite

            text = str(entry.get("text", ""))
            chunk = max(
                candidates,
                key=lambda candidate: (
                    bool(candidate.attributes.get("test.suite"))
                    and str(candidate.attributes["test.suite"]) in text,
                    self._overlap_ns(
                        start_ns,
                        end_ns,
                        candidate.start_ns,
                        candidate.end_ns,
                    ),
                ),
            )
            attributes = self._critical_test_attributes(index, entry)
            chunk.attributes.update(attributes)
            marked_chunks.add(chunk.span_id)
            for test_span in tests_by_parent.get(chunk.span_id, []):
                test_span.attributes.update(attributes)
                marked_tests.add(test_span.span_id)

        return {
            "ya.test.critical_path.entry.count": len(critical_entries),
            "ya.test.critical_path.chunk.count": len(marked_chunks),
            "ya.test.critical_path.span.count": len(marked_tests),
        }

    def statistics_attributes(
        self,
        build_records: Sequence[tuple[YaEvlogRecord, str, str]],
    ) -> tuple[dict[str, Any], dict[str, tuple[int, Mapping[str, Any]]]]:
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
                "tests_tasks": "ya.build.task.test.count",
                "failed_tasks": "ya.build.task.failed.count",
                "ok_tasks": "ya.build.task.ok.count",
                "avoided_tasks": "ya.build.task.avoided.count",
            }
            for source, destination in cache_fields.items():
                number = _number(cache.get(source))
                if number is not None:
                    attributes[destination] = number
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
                number = _number(dist_cache.get(source))
                if number is not None:
                    attributes[destination] = number

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

        tool_counts: dict[str, Counter[str]] = defaultdict(Counter)
        for _, kind, tool in build_records:
            if kind in {"cache_restore", "execute"} and SAFE_TOOL_RE.fullmatch(tool):
                tool_counts[_metric_name(tool)][kind] += 1
        observed_hits = sum(counts["cache_restore"] for counts in tool_counts.values())
        observed_misses = sum(counts["execute"] for counts in tool_counts.values())
        observed_total = observed_hits + observed_misses
        attributes["ya.build.cache.worker_node.hit.count"] = observed_hits
        attributes["ya.build.cache.worker_node.miss.count"] = observed_misses
        if observed_total:
            attributes["ya.build.cache.worker_node.hit.ratio"] = (
                observed_hits / observed_total
            )
        ranked_tools = sorted(
            tool_counts,
            key=lambda tool: (
                -(tool_counts[tool]["cache_restore"] + tool_counts[tool]["execute"]),
                tool,
            ),
        )[:MAX_CACHE_TOOL_STATS]
        for normalized in ranked_tools:
            hits = tool_counts[normalized]["cache_restore"]
            misses = tool_counts[normalized]["execute"]
            total = hits + misses
            prefix = f"ya.build.cache.tool.{normalized}"
            attributes[f"{prefix}.hit.count"] = hits
            attributes[f"{prefix}.miss.count"] = misses
            attributes[f"{prefix}.hit.ratio"] = hits / total

        critical_entries = [
            entry
            for entry in self.statistics.get("critical_path", [])
            if isinstance(entry, Mapping)
            and not self._is_test_critical_path_entry(entry)
        ]
        critical_by_uid = {
            str(entry["uid"]): (index, entry)
            for index, entry in enumerate(critical_entries)
            if entry.get("uid")
        }
        if critical_entries:
            work_msec = sum(
                _number(entry.get("elapsed")) or 0 for entry in critical_entries
            )
            start_times = [
                number
                for entry in critical_entries
                if (number := _number(entry.get("start_ts"))) is not None
            ]
            end_times = [
                number
                for entry in critical_entries
                if (number := _number(entry.get("end_ts"))) is not None
            ]
            attributes["ya.build.critical_path.node.count"] = len(critical_entries)
            attributes["ya.build.critical_path.work.seconds"] = work_msec / 1_000
            if start_times and end_times:
                attributes["ya.build.critical_path.elapsed.seconds"] = (
                    max(end_times) - min(start_times)
                ) / 1_000
            attributes["ya.build.critical_path.summary"] = [
                (
                    f"{entry.get('type', 'unknown')}: "
                    f"{entry.get('text', 'unknown')} "
                    f"({(_number(entry.get('elapsed')) or 0) / 1_000:.3f}s)"
                )
                for entry in critical_entries[:128]
            ]
        return attributes, critical_by_uid

    def build_spans(
        self,
        *,
        trace_id: str,
        root_span_id: str,
        root_start_ns: ns,
        root_end_ns: ns,
        resource: Mapping[str, Any],
    ) -> tuple[list[Span], str, dict[str, Any]]:
        spans: list[Span] = []
        dispatch_span_id = ""
        dispatch_bounds: tuple[ns, ns] | None = None

        stages = []
        for record in self.stages:
            if record.name not in RENDERED_STAGE_NAMES:
                continue
            clipped = record.clipped(root_start_ns, root_end_ns)
            if clipped is not None:
                stages.append(clipped)
        stages.sort(key=lambda record: (record.start_ns, record.end_ns, record.name))

        for index, record in enumerate(stages):
            span_id = stable_hex_id(
                trace_id,
                "ya.stage",
                record.name,
                record.start_ns,
                index,
                length=16,
            )
            if record.name == "dispatch_build":
                dispatch_span_id = span_id
                dispatch_bounds = (record.start_ns, record.end_ns)
            spans.append(
                Span(
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
                    resource_attributes=dict(resource),
                    scope_name="ya.phase",
                )
            )

        clipped_nodes = [
            clipped
            for record in self.nodes
            if (
                clipped := record.clipped(
                    dispatch_bounds[0] if dispatch_bounds else root_start_ns,
                    dispatch_bounds[1] if dispatch_bounds else root_end_ns,
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
        timed_records = [
            (record, kind, tool)
            for record, kind, tool in unbounded_build_records
            if kind in {"cache_restore", "execute", "materialize"}
        ]
        if not timed_records:
            return spans, dispatch_span_id, metadata

        build_start_ns = min(record.start_ns for record, _, _ in timed_records)
        build_end_ns = max(record.end_ns for record, _, _ in timed_records)
        build_records = []
        for record, kind, tool in unbounded_build_records:
            clipped = record.clipped(build_start_ns, build_end_ns)
            if clipped is not None:
                build_records.append((clipped, kind, tool))
        metadata["ya.build.node.count"] = len(build_records)
        counts = Counter(kind for _, kind, _ in build_records)
        cumulative_ns = sum(
            record.end_ns - record.start_ns for record, _, _ in build_records
        )
        build_span_id = stable_hex_id(
            trace_id,
            "ya.build",
            build_start_ns,
            build_end_ns,
            length=16,
        )
        attributes: dict[str, Any] = {
            "ya.build.wall_time.kind": "worker-node-execution-envelope",
            "ya.build.node.count": len(build_records),
            "ya.build.cumulative_node_seconds": (cumulative_ns / 1_000_000_000),
        }
        for kind, count in sorted(counts.items()):
            attributes[f"ya.build.node.{kind}.count"] = count
        if test_starts:
            attributes["ya.build.first_test_node_offset_seconds"] = (
                max(0, min(test_starts) - build_start_ns) / 1_000_000_000
            )
        statistics_attributes, critical_by_uid = self.statistics_attributes(
            build_records
        )
        attributes.update(statistics_attributes)

        candidates = [
            (record, kind, tool)
            for record, kind, tool in build_records
            if kind != "cache_store"
        ]
        dropped = max(0, len(candidates) - MAX_BUILD_NODE_SPANS)
        if dropped:
            candidates = sorted(
                candidates,
                key=lambda item: item[0].end_ns - item[0].start_ns,
                reverse=True,
            )[:MAX_BUILD_NODE_SPANS]
            attributes["ya.build.node_spans.dropped"] = dropped
        candidates.sort(
            key=lambda item: (
                item[0].start_ns,
                item[0].end_ns,
                item[0].name,
                item[0].tag,
            )
        )
        attributes["ya.build.node_spans.rendered"] = len(candidates)
        spans.append(
            Span(
                trace_id=trace_id,
                span_id=build_span_id,
                parent_span_id=dispatch_span_id or root_span_id,
                name="build operations",
                start_ns=build_start_ns,
                end_ns=build_end_ns,
                attributes=attributes,
                resource_attributes=dict(resource),
                scope_name="ya.build",
            )
        )

        spans.extend(
            record.build_node_span(
                trace_id=trace_id,
                parent_span_id=build_span_id,
                kind=kind,
                tool=tool,
                index=index,
                resource=resource,
                critical_by_uid=critical_by_uid,
            )
            for index, (record, kind, tool) in enumerate(candidates)
        )

        metadata["ya.build.node.span_count"] = len(candidates)
        metadata["ya.build.node.span_dropped_count"] = dropped
        return spans, dispatch_span_id, metadata


def load_ya_traces(
    ya_out: Path, *, modified_since: float | None = None
) -> list[YaTraceFile]:
    traces: list[YaTraceFile] = []
    total_bytes = 0
    event_count = 0
    paths = sorted(ya_out.rglob("ytest.report.trace"))
    if len(paths) > MAX_YA_TRACE_FILES:
        raise ValueError(f"Found more than {MAX_YA_TRACE_FILES} ya trace files")

    for path in paths:
        if not path.is_file():
            continue
        stat = path.stat()
        if modified_since is not None and stat.st_mtime < modified_since:
            continue
        total_bytes += stat.st_size
        if total_bytes > MAX_YA_TRACE_BYTES:
            raise ValueError(f"Ya traces exceed {MAX_YA_TRACE_BYTES} bytes")
        suite, result_folder = YaTraceFile.identity(ya_out, path)
        events = []
        with path.open(encoding="utf-8", errors="replace") as stream:
            for line_number, line in enumerate(stream, 1):
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError as error:
                    LOGGER.warning("Skipping %s:%s: %s", path, line_number, error)
                    continue
                event = YaEvent.from_raw(raw, event_count)
                if event is None:
                    continue
                events.append(event)
                event_count += 1
                if event_count > MAX_YA_EVENTS:
                    raise ValueError(f"Ya traces exceed {MAX_YA_EVENTS} events")
        traces.append(
            YaTraceFile(
                path=path,
                suite=suite,
                result_folder=result_folder,
                events=events,
            )
        )
    return traces


def _safe_statistics_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    result = {}
    for key, item in list(value.items())[:256]:
        if isinstance(item, (bool, int)) or (
            isinstance(item, float) and math.isfinite(item)
        ):
            result[str(key)] = item
        elif isinstance(item, str):
            result[str(key)] = item[:8_192]
    return result


def _selected_evlog_statistics(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, Any] = {}
    for key in (
        "cache_hit",
        "dist_cache_stat",
        "execution_stages_msec",
        "graph_lang_usage",
    ):
        selected = _safe_statistics_mapping(value.get(key))
        if selected:
            result[key] = selected
    task_execution_msec = value.get("task_execution_msec")
    if (
        isinstance(task_execution_msec, int)
        and not isinstance(task_execution_msec, bool)
    ) or (
        isinstance(task_execution_msec, float) and math.isfinite(task_execution_msec)
    ):
        result["task_execution_msec"] = task_execution_msec

    critical_path = []
    if isinstance(value.get("critical_path"), list):
        for item in value["critical_path"][:128]:
            if not isinstance(item, Mapping):
                continue
            selected = _safe_statistics_mapping(
                {
                    key: item.get(key)
                    for key in (
                        "type",
                        "elapsed",
                        "start_ts",
                        "end_ts",
                        "text",
                        "host",
                        "uid",
                    )
                }
            )
            if selected:
                critical_path.append(selected)
    if critical_path:
        result["critical_path"] = critical_path
    return result


def load_ya_evlog(path: Path | None) -> YaEvlog:
    result = YaEvlog(stages=[], nodes=[])
    if path is None:
        return result
    if not path.is_file():
        LOGGER.warning("Ya event log does not exist: %s", path)
        return result
    if path.stat().st_size > MAX_YA_EVLOG_BYTES:
        raise ValueError(f"Ya event log exceeds {MAX_YA_EVLOG_BYTES} bytes")

    event_count = 0
    with path.open(encoding="utf-8", errors="replace") as stream:
        line_number = 0
        while True:
            line = stream.readline(MAX_YA_EVLOG_LINE_CHARACTERS + 1)
            if not line:
                break
            line_number += 1
            if len(line) > MAX_YA_EVLOG_LINE_CHARACTERS:
                raise ValueError(
                    "Ya event log line exceeds "
                    f"{MAX_YA_EVLOG_LINE_CHARACTERS} characters "
                    f"in {path}:{line_number}"
                )
            if not line.strip():
                continue
            event_count += 1
            if event_count > MAX_YA_EVLOG_EVENTS:
                raise ValueError(f"Ya event log exceeds {MAX_YA_EVLOG_EVENTS} events")
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as error:
                LOGGER.warning("Skipping %s:%s: %s", path, line_number, error)
                continue
            namespace = raw.get("namespace")
            event = raw.get("event")
            raw_value = raw.get("value")
            if (
                namespace == "dump_debug"
                and event == "log"
                and isinstance(raw_value, Mapping)
                and raw_value.get("key") == "stats"
            ):
                result.statistics = _selected_evlog_statistics(raw_value.get("value"))
                continue
            if (namespace, event) not in {
                ("stages", "stage-finished"),
                ("worker_threads", "node-finished"),
            } or not isinstance(raw_value, Mapping):
                continue
            record = YaEvlogRecord.from_raw(
                raw_value,
                thread_name=str(raw.get("thread_name", "")),
            )
            if record is None:
                continue
            if namespace == "stages":
                result.stages.append(record)
            else:
                result.nodes.append(record)
    return result


def _metric_name(name: object) -> str:
    normalized = SAFE_METRIC_RE.sub("_", str(name)).strip("_").lower()
    return re.sub(r"_+", "_", normalized)[:200]


def _metric_attributes(metrics: Any) -> dict[str, Any]:
    if not isinstance(metrics, Mapping):
        return {}
    attributes = {}
    for key, value in list(metrics.items())[:256]:
        if not isinstance(value, (str, bool, int, float)):
            continue
        if isinstance(value, float) and not math.isfinite(value):
            continue
        normalized = _metric_name(key)
        if normalized:
            attributes[f"ya.chunk.metric.{normalized}"] = value
    return attributes


def _number(value: Any) -> int | float | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    return None
