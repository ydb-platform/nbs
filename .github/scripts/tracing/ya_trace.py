"""Read, model, and render data from ya test traces and event logs."""

from __future__ import annotations

import json
import logging
import math
import os
import re
import stat
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .otlp import (
    Interval,
    ResourceAttributes,
    Span,
    Trace,
    decode_attributes,
    make_span,
    Ns,
    SECOND,
    set_span_status,
    span_status_code,
    stable_span_id,
    update_span_attributes,
)

__all__ = [
    "YaCriticalPathEntry",
    "YaEvent",
    "YaEvlog",
    "YaEvlogRecord",
    "YaTraceInputs",
    "YaTraceCollection",
    "YaTraceFile",
    "load_ya_evlog",
]

LOGGER = logging.getLogger(__name__)
MAX_YA_TRACE_BYTES = 512 * 1024 * 1024
MAX_YA_TRACE_FILES = 20_000
MAX_YA_EVENTS = 2_000_000
MAX_YA_EVLOG_BYTES = 512 * 1024 * 1024
MAX_YA_EVLOG_LINE_CHARACTERS = 16 * 1024 * 1024
MAX_YA_EVLOG_EVENTS = 2_000_000
MAX_BUILD_NODE_SPANS = 50_000
MAX_WORKER_TOOL_STATS = 24
SAFE_METRIC_RE = re.compile(r"[^a-zA-Z0-9_.-]+")
SAFE_TOOL_RE = re.compile(r"^[a-zA-Z0-9_.+-]{1,32}$")
BUILD_ROOT_RE = re.compile(r"\$\(BUILD_ROOT\)/([^\s)]+)")
NODE_UID_RE = re.compile(r"^[^(]+\(([^$()\s]+)\$\(BUILD_ROOT\)")
OUTPUTLESS_NODE_UID_RE = re.compile(r"^[^(]+\(([^$()\s]+)\)$")
TAG_WRAPPER_RE = re.compile(
    r"^(restore|restore_from_dist_cache|result|put_in_cache|"
    r"put_in_dist_cache|write_through_caches)\[([^\]]+)\]$"
)
CRITICAL_TASK_SUFFIX_RE = re.compile(r"(?:(?:-CACHED|-DYN_UID_CACHE))+$")
PASS_STATUSES = {"good", "pass", "passed", "success"}
ERROR_STATUSES = {
    "crashed",
    "diff",
    "error",
    "fail",
    "failed",
    "flaky",
    "internal",
    "missing",
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
_ChunkSpan = tuple[Span, dict[str, Any], Interval]


@dataclass(slots=True)
class YaEvent:
    name: str
    timestamp_ns: Ns | None
    value: dict[str, Any]
    order: int

    @classmethod
    def from_raw(cls, raw: Any, order: int) -> YaEvent | None:
        if not isinstance(raw, Mapping):
            return None
        name = str(raw.get("name", "")).replace("_", "-")
        value = raw.get("value", {})
        if name not in {
            "chunk-event",
            "suite-event",
            "subtest-finished",
            "subtest-started",
        } or not isinstance(value, dict):
            return None
        return cls(
            name=name,
            timestamp_ns=Ns.from_s(raw.get("timestamp")),
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
    def chunk_filename(self) -> str:
        value = self.value.get("chunk_filename")
        return str(value) if value is not None else ""

    @property
    def chunk_identity(self) -> tuple[int, int, str] | None:
        key = self.chunk_key
        if key is None:
            return None
        return key[0], key[1], self.chunk_filename

    @property
    def test_key(self) -> tuple[str, str]:
        test_class = str(self.value.get("class", "unknown")).replace("::", ".")
        subtest = str(self.value.get("subtest", "unknown"))
        return test_class, subtest

    @property
    def logical_test_key(self) -> tuple[str, str, str, str]:
        test_class, subtest = self.test_key
        return (
            test_class,
            subtest,
            str(self.value.get("type") or ""),
            str(self.value.get("path") or ""),
        )

    @property
    def status(self) -> tuple[str, int]:
        status = str(self.value.get("status", "unknown")).lower()
        if status in PASS_STATUSES:
            return status, 1
        if status in ERROR_STATUSES:
            return status, 2
        return status, 0

    @property
    def errors(self) -> list[tuple[str, str]]:
        raw_errors = self.value.get("errors")
        if not isinstance(raw_errors, list):
            return []
        result = []
        for raw_error in raw_errors:
            if not isinstance(raw_error, (list, tuple)) or not raw_error:
                continue
            status = str(raw_error[0]).lower()
            message = str(raw_error[1]) if len(raw_error) > 1 else ""
            result.append((status, message))
        return result

    @property
    def failing_errors(self) -> list[tuple[str, str]]:
        return [
            (status, message)
            for status, message in self.errors
            if status in ERROR_STATUSES
        ]

    def error_attributes(self, prefix: str) -> dict[str, Any]:
        errors = self.errors
        if not errors:
            return {}
        return {
            f"{prefix}.error.count": len(errors),
            f"{prefix}.error.statuses": [status for status, _ in errors],
            f"{prefix}.error.messages": [message for _, message in errors],
        }

    @classmethod
    def merged(cls, events: Sequence[YaEvent]) -> YaEvent:
        if not events:
            raise ValueError("cannot merge an empty event sequence")
        ordered = sorted(events, key=lambda event: event.order)
        value: dict[str, Any] = {}
        merged_mappings: dict[str, dict[str, Any]] = {
            "logs": {},
            "metrics": {},
        }
        errors: list[list[str]] = []
        seen_errors: set[tuple[str, str]] = set()
        for event in ordered:
            for key, item in event.value.items():
                if key in merged_mappings and isinstance(item, Mapping):
                    merged_mappings[key].update(item)
                elif key == "errors" and isinstance(item, list):
                    for status, message in event.errors:
                        error = (status, message)
                        if error not in seen_errors:
                            seen_errors.add(error)
                            errors.append([status, message])
                else:
                    value[key] = item
        for key, item in merged_mappings.items():
            if item:
                value[key] = item
        if errors:
            value["errors"] = errors
        timestamp_ns = next(
            (
                event.timestamp_ns
                for event in reversed(ordered)
                if event.timestamp_ns is not None
            ),
            None,
        )
        return cls(
            name=ordered[-1].name,
            timestamp_ns=timestamp_ns,
            value=value,
            order=ordered[-1].order,
        )

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
        test_type = str(self.value.get("type") or "")
        if test_type:
            attributes["test.type"] = test_type
        test_path = str(self.value.get("path") or "")
        if test_path:
            attributes["test.path"] = test_path
        if inferred:
            attributes["test.timing.inferred"] = True
        if timing_source:
            attributes["test.timing.source"] = timing_source
        if incomplete:
            attributes["test.incomplete"] = True
        attributes.update(
            _metric_attributes(
                self.value.get("metrics"),
                prefix="ya.test.metric",
            )
        )
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


@dataclass(frozen=True, slots=True)
class _YaTestTiming:
    interval: Interval
    event: YaEvent
    order: int
    source: str
    inferred: bool
    incomplete: bool = False

    @classmethod
    def resolve(
        cls,
        start: YaEvent | None,
        finish: YaEvent | None,
        *,
        chunk: Interval,
        inferred_start: Ns | None,
        only_test: bool,
    ) -> _YaTestTiming | None:
        if start is not None:
            start_ns = chunk.clamp(start.timestamp_ns or chunk.start)
            if finish is None:
                return cls(
                    interval=Interval(start_ns, chunk.end),
                    event=start,
                    order=start.order,
                    source="subtest-start-and-chunk-end",
                    inferred=start.timestamp_ns is None,
                    incomplete=True,
                )

            duration_ns = Ns.from_s_or_zero(finish.value.get("time"))
            end_ns = Ns(
                max(
                    start_ns,
                    chunk.clamp(
                        finish.timestamp_ns or Ns(start_ns + duration_ns),
                    ),
                )
            )
            return cls(
                interval=Interval(start_ns, end_ns),
                event=finish,
                order=start.order,
                source="subtest-events",
                inferred=start.timestamp_ns is None or finish.timestamp_ns is None,
            )

        if finish is None:
            return None

        duration_ns = Ns.from_s_or_zero(finish.value.get("time"))
        if (
            inferred_start is not None
            and only_test
            and finish.status[0] not in {"deselected", "not_launched"}
        ):
            start_ns = chunk.clamp(inferred_start)
            end_ns = Ns(
                max(
                    start_ns,
                    chunk.clamp(Ns(start_ns + duration_ns)),
                )
            )
            source = "chunk-delay-and-test-duration"
        else:
            end_ns = chunk.clamp(finish.timestamp_ns or chunk.end)
            start_ns = Ns(max(chunk.start, end_ns - duration_ns))
            source = "finish-event-and-test-duration"
        return cls(
            interval=Interval(start_ns, end_ns),
            event=finish,
            order=finish.order,
            source=source,
            inferred=True,
        )

    @property
    def status(self) -> tuple[str, int]:
        return ("incomplete", 2) if self.incomplete else self.event.status

    def attributes(self, test_class: str, subtest: str) -> dict[str, Any]:
        return self.event.test_attributes(
            test_class,
            subtest,
            inferred=self.inferred,
            timing_source=self.source,
            incomplete=self.incomplete,
        )


@dataclass(slots=True)
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

            timing = _YaTestTiming.resolve(
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


@dataclass
class YaTraceInputs:
    root: Path
    trace_paths: list[Path]
    evlog_path: Path | None = None

    @classmethod
    def discover(
        cls,
        root: Path,
        *,
        evlog_path: Path | None = None,
        modified_since: float | None = None,
    ) -> YaTraceInputs:
        root = root.resolve()
        trace_paths = []
        for path in sorted(root.rglob("ytest.report.trace")):
            try:
                relative = path.relative_to(root)
                path_stat = path.lstat()
                resolved = path.resolve(strict=True)
            except (OSError, ValueError) as error:
                LOGGER.warning("Skipping ya trace input %s: %s", path, error)
                continue
            if not stat.S_ISREG(path_stat.st_mode) or resolved != root / relative:
                LOGGER.warning("Skipping unsafe ya trace input: %s", path)
                continue
            if modified_since is not None and path_stat.st_mtime < modified_since:
                continue
            trace_paths.append(path)

        safe_evlog_path = None
        if evlog_path is not None:
            try:
                evlog_stat = evlog_path.lstat()
            except FileNotFoundError:
                LOGGER.warning("Ya event log does not exist: %s", evlog_path)
            except OSError as error:
                LOGGER.warning(
                    "Unable to inspect ya event log %s: %s",
                    evlog_path,
                    error,
                )
            else:
                if stat.S_ISREG(evlog_stat.st_mode):
                    safe_evlog_path = evlog_path
                else:
                    LOGGER.warning("Skipping unsafe ya event log: %s", evlog_path)

        return cls(
            root=root,
            trace_paths=trace_paths,
            evlog_path=safe_evlog_path,
        )

    def bundle_manifest(
        self,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        evlog_archive_name = (
            self.evlog_path.name if self.evlog_path is not None else None
        )
        if evlog_archive_name in {"trace-inputs.manifest.json", "ya-out"}:
            raise ValueError(
                "Ya event log name conflicts with a reserved bundle member"
            )
        return {
            "schema": "nbs-ya-trace-input-bundle",
            "schema_version": 1,
            "evlog_file": evlog_archive_name,
            "ya_out_dir": "ya-out",
            "ya_trace_file_count": len(self.trace_paths),
            "metadata": dict(metadata or {}),
        }

    def tar_file_list(self) -> bytes:
        """Return NUL-delimited trace paths relative to the ya output root."""
        return b"".join(
            b"./" + os.fsencode(trace_path.relative_to(self.root)) + b"\0"
            for trace_path in self.trace_paths
        )

    def parse(self) -> YaTraceCollection:
        return YaTraceCollection(
            root=self.root,
            traces=_load_ya_trace_files(self.root, self.trace_paths),
        )


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
        return YaTraceInputs.discover(
            root,
            modified_since=modified_since,
        ).parse()

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
            is_test=base_type in {"TA", "TL", "TM", "TS"} or "/test-results/" in text,
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


@dataclass(slots=True)
class YaEvlogRecord:
    name: str
    tag: str
    start_ns: Ns
    end_ns: Ns
    thread_name: str = ""
    node_uid: str = ""

    @classmethod
    def from_raw(
        cls,
        value: Mapping[str, Any],
        *,
        thread_name: str = "",
    ) -> YaEvlogRecord | None:
        interval = cls._time_range(value.get("time"))
        if interval is None:
            return None
        return cls(
            name=str(value.get("name", "unknown")),
            tag=str(value.get("tag", "")),
            start_ns=interval.start,
            end_ns=interval.end,
            thread_name=thread_name,
            node_uid=str(value.get("uid") or ""),
        )

    @staticmethod
    def _time_range(value: Any) -> Interval | None:
        if not isinstance(value, list) or len(value) != 2:
            return None
        start_ns = Ns.from_s(value[0])
        end_ns = Ns.from_s(value[1])
        if start_ns is None or end_ns is None or end_ns < start_ns:
            return None
        return Interval(start_ns, end_ns)

    @property
    def interval(self) -> Interval:
        return Interval(self.start_ns, self.end_ns)

    def clipped(self, bounds: Interval) -> YaEvlogRecord | None:
        clipped = self.interval.intersection(bounds)
        if clipped is None:
            return None
        return YaEvlogRecord(
            name=self.name,
            tag=self.tag,
            start_ns=clipped.start,
            end_ns=clipped.end,
            thread_name=self.thread_name,
            node_uid=self.node_uid,
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
                "restore_from_dist_cache": "cache_restore",
                "result": "materialize",
                "put_in_cache": "cache_store",
                "put_in_dist_cache": "cache_store",
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
        if self.node_uid:
            return self.node_uid
        match = NODE_UID_RE.match(self.name) or OUTPUTLESS_NODE_UID_RE.match(self.name)
        return match.group(1) if match else ""

    @property
    def cache_source(self) -> str:
        if self.tag.startswith(("restore_from_dist_cache[", "put_in_dist_cache[")):
            return "distributed"
        if self.tag.startswith(("restore[", "put_in_cache[")):
            return "local"
        return ""

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
        trace_id: bytes,
        parent_span_id: bytes,
        kind: str,
        tool: str,
        index: int,
        critical_entry: YaCriticalPathEntry | None,
        failed: bool,
        exit_code: int | None,
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
        if self.cache_source:
            attributes["ya.build.cache.source"] = self.cache_source
        if self.uid:
            attributes["ya.build.node.uid"] = self.uid
        if critical_entry is not None:
            attributes.update(critical_entry.span_attributes(test=False))
        if failed:
            attributes["ya.build.failed"] = True
            if exit_code is not None:
                attributes["process.exit.code"] = exit_code
        return make_span(
            trace_id=trace_id,
            span_id=stable_span_id(
                trace_id,
                "ya.build.node",
                self.name,
                self.tag,
                self.start_ns,
                index,
            ),
            parent_span_id=parent_span_id,
            name=self.span_name(kind, tool),
            start_ns=self.start_ns,
            end_ns=self.end_ns,
            attributes=attributes,
            status_code=2 if failed else 0,
            status_message="build node failed" if failed else "",
        )


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
        )[:MAX_WORKER_TOOL_STATS]
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
        dropped = max(0, len(candidates) - MAX_BUILD_NODE_SPANS)
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
            selected_protected = protected_candidates[:MAX_BUILD_NODE_SPANS]
            remaining = max(
                0,
                MAX_BUILD_NODE_SPANS - len(selected_protected),
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

        for index, (
            _,
            record,
            kind,
            tool,
            critical_entry,
            failed,
        ) in enumerate(candidates):
            trace.add_span(
                record.build_node_span(
                    trace_id=trace_id,
                    parent_span_id=build_span_id,
                    kind=kind,
                    tool=tool,
                    index=index,
                    critical_entry=critical_entry,
                    failed=failed,
                    exit_code=self.failures.get(record.uid) if failed else None,
                ),
                resource=resource,
                scope_name="ya.build.node",
            )

        metadata["ya.build.node.span_count"] = len(candidates)
        metadata["ya.build.node.span_dropped_count"] = dropped
        return dispatch_span_id, metadata


def _load_ya_trace_files(
    ya_out: Path,
    paths: Sequence[Path],
) -> list[YaTraceFile]:
    traces: list[YaTraceFile] = []
    total_bytes = 0
    event_count = 0
    if len(paths) > MAX_YA_TRACE_FILES:
        raise ValueError(f"Found more than {MAX_YA_TRACE_FILES} ya trace files")

    for path in paths:
        path_stat = path.stat()
        total_bytes += path_stat.st_size
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
    task_execution_msec = _number(value.get("task_execution_msec"))
    if task_execution_msec is not None:
        result["task_execution_msec"] = task_execution_msec

    critical_path = []
    if isinstance(value.get("critical_path"), list):
        for item in value["critical_path"]:
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
            if (
                namespace == "devtools.ya.build.reports.failed_node_info"
                and event == "node-failed"
                and isinstance(raw_value, Mapping)
            ):
                uid = str(raw_value.get("uid", ""))
                raw_exit_code = raw_value.get("exit_code")
                exit_code = (
                    raw_exit_code
                    if isinstance(raw_exit_code, int)
                    and not isinstance(raw_exit_code, bool)
                    else None
                )
                if uid:
                    result.failures[uid] = exit_code
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


def _metric_attributes(metrics: Any, *, prefix: str) -> dict[str, Any]:
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
            attributes[f"{prefix}.{normalized}"] = value
    return attributes


def _number(value: Any) -> int | float | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    return None
