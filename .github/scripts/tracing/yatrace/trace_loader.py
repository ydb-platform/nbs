from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..otlp import Ns
from . import limits
from .trace_model import (
    ERROR_STATUSES,
    PASS_STATUSES,
    Chunk,
    SuiteTrace,
    TestAttempt,
    TestEvent,
    TraceRecord,
)

LOGGER = logging.getLogger(__name__)
EVENT_NAMES = frozenset(
    {"chunk-event", "suite-event", "subtest-finished", "subtest-started"}
)


@dataclass(frozen=True, slots=True)
class _Event:
    name: str
    timestamp_ns: Ns | None
    value: Mapping[str, Any]
    order: int

    @property
    def chunk_identity(self) -> tuple[int, int, str] | None:
        try:
            index = int(self.value["chunk_index"])
            total = int(self.value["nchunks"])
        except (KeyError, TypeError, ValueError):
            return None
        if index < 0 or total < 1 or index >= total:
            return None
        filename = self.value.get("chunk_filename")
        return index, total, str(filename) if filename is not None else ""

    @property
    def test_key(self) -> tuple[str, str, str, str]:
        return (
            str(self.value.get("class", "unknown")),
            str(self.value.get("subtest", "unknown")),
            str(self.value.get("type") or ""),
            str(self.value.get("path") or ""),
        )


def _errors(value: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    result = []
    raw_errors = value.get("errors")
    if not isinstance(raw_errors, list):
        return ()
    for raw in raw_errors:
        if isinstance(raw, (list, tuple)) and raw:
            result.append((str(raw[0]).lower(), str(raw[1]) if len(raw) > 1 else ""))
    return tuple(result)


def _record(event: _Event) -> TraceRecord:
    metrics = event.value.get("metrics")
    logs = event.value.get("logs")
    return TraceRecord(
        timestamp_ns=event.timestamp_ns,
        order=event.order,
        metrics=dict(metrics) if isinstance(metrics, Mapping) else {},
        errors=_errors(event.value),
        logs=(
            {str(name): path for name, path in logs.items() if isinstance(path, str)}
            if isinstance(logs, Mapping)
            else {}
        ),
    )


def _test(event: _Event) -> TestEvent:
    status = str(event.value.get("status", "unknown")).lower()
    status_code = 1 if status in PASS_STATUSES else 2 if status in ERROR_STATUSES else 0
    test_class, name, test_type, path = event.test_key
    return TestEvent(
        record=_record(event),
        test_class=test_class,
        name=name,
        test_type=test_type,
        path=path,
        status=status,
        status_code=status_code,
        duration_ns=Ns.from_s_or_zero(event.value.get("time")),
    )


def _merge(events: Sequence[_Event]) -> _Event:
    ordered = sorted(events, key=lambda event: event.order)
    value: dict[str, Any] = {}
    merged = {"logs": {}, "metrics": {}}
    errors: list[list[str]] = []
    seen_errors: set[tuple[str, str]] = set()
    for event in ordered:
        for key, item in event.value.items():
            if key in merged and isinstance(item, Mapping):
                merged[key].update(item)
            elif key == "errors" and isinstance(item, list):
                for error in _errors(event.value):
                    if error not in seen_errors:
                        seen_errors.add(error)
                        errors.append(list(error))
            else:
                value[key] = item
    value.update({key: item for key, item in merged.items() if item})
    if errors:
        value["errors"] = errors
    timestamp = next(
        (
            event.timestamp_ns
            for event in reversed(ordered)
            if event.timestamp_ns is not None
        ),
        None,
    )
    return _Event(ordered[-1].name, timestamp, value, ordered[-1].order)


def _attempts(events: Sequence[_Event]) -> tuple[tuple[TestAttempt, ...], int]:
    grouped: dict[tuple[str, str, str, str], list[_Event]] = defaultdict(list)
    for event in events:
        grouped[event.test_key].append(event)
    attempts = []
    for key in sorted(grouped):
        current: list[_Event] = []
        groups: list[list[_Event]] = []
        for event in sorted(grouped[key], key=lambda item: item.order):
            if event.name == "subtest-started" and current:
                groups.append(current)
                current = []
            current.append(event)
        if current:
            groups.append(current)
        for group in groups:
            start = next(
                (event for event in group if event.name == "subtest-started"), None
            )
            finish = None
            for candidate in (
                event for event in group if event.name == "subtest-finished"
            ):
                candidate_status = _test(candidate).status
                finish_status = _test(finish).status if finish is not None else ""
                if (
                    finish is None
                    or finish_status in {"crashed", "deselected", "not_launched"}
                    or candidate_status != "deselected"
                ):
                    finish = candidate
            if finish is not None and _test(finish).status in {
                "deselected",
                "not_launched",
            }:
                start = None
            attempts.append(
                TestAttempt(
                    _test(start) if start is not None else None,
                    _test(finish) if finish is not None else None,
                )
            )
    return tuple(attempts), len(grouped)


def _chunks(events: Sequence[_Event]) -> tuple[Chunk, ...]:
    chunk_events = [event for event in events if event.name == "chunk-event"]
    tests = [event for event in events if event.name.startswith("subtest-")]
    if not chunk_events:
        attempts, count = _attempts(tests)
        timestamps = tuple(
            event.timestamp_ns for event in tests if event.timestamp_ns is not None
        )
        return (
            (Chunk(None, None, "", None, attempts, count, timestamps),) if tests else ()
        )

    chunks: dict[tuple[int, int, str] | None, list[_Event]] = {}
    for event in chunk_events:
        chunks.setdefault(event.chunk_identity, []).append(event)
    tests_by_chunk: dict[tuple[int, int, str] | None, list[_Event]] = defaultdict(list)
    for event in tests:
        tests_by_chunk[event.chunk_identity].append(event)
    assigned: dict[tuple[int, int, str] | None, list[_Event]] = defaultdict(list)
    unassigned = []
    for identity, matching in tests_by_chunk.items():
        if identity in chunks:
            assigned[identity].extend(matching)
            continue
        candidates = (
            list(chunks)
            if identity is None
            else [key for key in chunks if key is not None and key[:2] == identity[:2]]
        )
        (assigned[candidates[0]] if len(candidates) == 1 else unassigned).extend(
            matching
        )

    result = []
    for identity, snapshots in chunks.items():
        event = _merge(snapshots)
        attempts, count = _attempts(
            sorted(assigned[identity], key=lambda item: item.order)
        )
        timestamps = tuple(
            item.timestamp_ns
            for item in assigned[identity]
            if item.timestamp_ns is not None
        )
        key = event.chunk_identity
        result.append(
            Chunk(
                key[0] if key is not None else None,
                key[1] if key is not None else None,
                key[2] if key is not None else "",
                _record(event),
                attempts,
                count,
                timestamps,
            )
        )
    if unassigned:
        attempts, count = _attempts(sorted(unassigned, key=lambda item: item.order))
        timestamps = tuple(
            item.timestamp_ns for item in unassigned if item.timestamp_ns is not None
        )
        result.append(Chunk(None, None, "", None, attempts, count, timestamps))
    return tuple(result)


def _identity(root: Path, path: Path) -> tuple[str, str]:
    relative = path.relative_to(root)
    parts = relative.parts
    try:
        marker = parts.index("test-results")
    except ValueError:
        return str(relative.parent), path.parent.name
    suite = "/".join(parts[:marker]) or "unknown-suite"
    folder = parts[marker + 1] if len(parts) > marker + 1 else "unknown"
    return suite, folder


def load_trace_files(root: Path, paths: Sequence[Path]) -> list[SuiteTrace]:
    if len(paths) > limits.MAX_YA_TRACE_FILES:
        raise ValueError(f"Found more than {limits.MAX_YA_TRACE_FILES} ya trace files")
    path_stats = [(path, path.stat()) for path in paths]
    if sum(item.st_size for _, item in path_stats) > limits.MAX_YA_TRACE_BYTES:
        raise ValueError(f"Ya traces exceed {limits.MAX_YA_TRACE_BYTES} bytes")

    result = []
    event_count = 0
    for path, _ in path_stats:
        events = []
        malformed = 0
        first_malformed = None
        with path.open(encoding="utf-8", errors="replace") as stream:
            for line_number, line in enumerate(stream, 1):
                line = line.strip("\r\t\n\x00 ")
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError as error:
                    malformed += 1
                    first_malformed = first_malformed or (line_number, str(error))
                    continue
                if not isinstance(raw, Mapping):
                    continue
                name = str(raw.get("name", "")).replace("_", "-")
                value = raw.get("value", {})
                if name not in EVENT_NAMES or not isinstance(value, dict):
                    continue
                events.append(
                    _Event(name, Ns.from_s(raw.get("timestamp")), value, event_count)
                )
                event_count += 1
                if event_count > limits.MAX_YA_EVENTS:
                    raise ValueError(f"Ya traces exceed {limits.MAX_YA_EVENTS} events")
        if first_malformed is not None:
            LOGGER.warning(
                "Ya trace %s contains %s malformed JSON records; first at line %s: %s",
                path,
                malformed,
                *first_malformed,
            )
        suite, folder = _identity(root, path)
        suite_events = [event for event in events if event.name == "suite-event"]
        finished = tuple(
            _test(event) for event in events if event.name == "subtest-finished"
        )
        result.append(
            SuiteTrace(
                path,
                suite,
                folder,
                _record(_merge(suite_events)) if suite_events else None,
                tuple(
                    event.timestamp_ns
                    for event in suite_events
                    if event.timestamp_ns is not None
                ),
                _chunks(events),
                finished,
                malformed,
            )
        )
    return result
