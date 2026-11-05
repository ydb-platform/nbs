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

    @property
    def test_pair_key(self) -> tuple[str, str]:
        return self.test_key[:2]

    @property
    def status(self) -> str:
        return str(self.value.get("status", "unknown")).lower()


@dataclass(frozen=True, slots=True)
class _AttemptEvents:
    events: tuple[_Event, ...]
    start: _Event | None
    finish: _Event | None

    @property
    def test_key(self) -> tuple[str, str, str, str]:
        event = self.finish if self.finish is not None else self.start
        assert event is not None
        return event.test_key

    @property
    def chunk_identities(self) -> tuple[tuple[int, int, str], ...]:
        identities = {
            identity
            for event in (self.start, self.finish)
            if event is not None
            if (identity := event.chunk_identity) is not None
        }
        coordinates = {(index, total) for index, total, _ in identities}
        filenames = {filename for _, _, filename in identities if filename}
        if len(coordinates) > 1 or len(filenames) > 1:
            return tuple(sorted(identities))
        if not coordinates:
            return ()
        index, total = next(iter(coordinates))
        filename = next(iter(filenames), "")
        return ((index, total, filename),)

    @property
    def test_keys(self) -> tuple[tuple[str, str, str, str], ...]:
        if (
            self.start is not None
            and self.finish is not None
            and _identity_match_score(self.start, self.finish) is None
        ):
            return self.start.test_key, self.finish.test_key
        return (self.test_key,)

    def materialize(self) -> tuple[TestAttempt, ...]:
        start = _test(self.start) if self.start is not None else None
        finish = _test(self.finish) if self.finish is not None else None
        if (
            self.start is not None
            and self.finish is not None
            and _identity_match_score(self.start, self.finish) is None
        ):
            return TestAttempt(start, None), TestAttempt(None, finish)
        return (TestAttempt(start, finish),)


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
    status = event.status
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


def _identity_match_score(left: _Event, right: _Event) -> tuple[int, int] | None:
    left_chunk = left.chunk_identity
    right_chunk = right.chunk_identity
    chunk_match = 0
    if left_chunk is not None and right_chunk is not None:
        if left_chunk[:2] != right_chunk[:2]:
            return None
        if left_chunk[2] and right_chunk[2] and left_chunk[2] != right_chunk[2]:
            return None
        chunk_match = 2 if left_chunk == right_chunk else 1

    detail_matches = 0
    for left_value, right_value in zip(left.test_key[2:], right.test_key[2:]):
        if left_value and right_value:
            if left_value != right_value:
                return None
            detail_matches += 1
    return chunk_match, detail_matches


def _attempt_match_score(
    start: _Event,
    finish: _Event,
) -> tuple[int, int, int, int, int] | None:
    identity_score = _identity_match_score(start, finish)
    if identity_score is None:
        return None
    chunk_match, detail_matches = identity_score
    timing_missing = 1
    timing_distance = 0
    if start.timestamp_ns is not None and finish.timestamp_ns is not None:
        inferred_start = finish.timestamp_ns - Ns.from_s_or_zero(
            finish.value.get("time")
        )
        timing_missing = 0
        timing_distance = abs(start.timestamp_ns.value - inferred_start.value)
    return (
        -chunk_match,
        -detail_matches,
        timing_missing,
        timing_distance,
        start.order,
    )


def _is_finish_snapshot(group: Sequence[_Event], event: _Event) -> bool:
    previous = next(
        (item for item in reversed(group) if item.name == "subtest-finished"),
        None,
    )
    return previous is not None and _identity_match_score(previous, event) is not None


def _attempt_groups(events: Sequence[_Event]) -> tuple[_AttemptEvents, ...]:
    grouped: dict[tuple[str, str], list[_Event]] = defaultdict(list)
    for event in events:
        grouped[event.test_pair_key].append(event)
    attempts: list[_AttemptEvents] = []
    for key in sorted(grouped):
        groups: list[list[_Event]] = []
        open_groups: list[int] = []
        for event in sorted(grouped[key], key=lambda item: item.order):
            if event.name == "subtest-started":
                groups.append([event])
                open_groups.append(len(groups) - 1)
                continue
            matches = [
                (score, position, group_index)
                for position, group_index in enumerate(open_groups)
                if (score := _attempt_match_score(groups[group_index][0], event))
                is not None
            ]
            if matches:
                _, position, group_index = min(matches)
                open_groups.pop(position)
                groups[group_index].append(event)
                continue
            if event.status in {"deselected", "not_launched"}:
                groups.append([event])
                continue
            if open_groups:
                groups[open_groups.pop(0)].append(event)
                continue
            previous = groups[-1] if groups else []
            if _is_finish_snapshot(previous, event):
                previous.append(event)
            else:
                groups.append([event])
        for group in groups:
            start = next(
                (event for event in group if event.name == "subtest-started"), None
            )
            finish = None
            for candidate in (
                event for event in group if event.name == "subtest-finished"
            ):
                candidate_status = candidate.status
                finish_status = finish.status if finish is not None else ""
                if (
                    finish is None
                    or finish_status in {"crashed", "deselected", "not_launched"}
                    or candidate_status != "deselected"
                ):
                    finish = candidate
            if finish is not None and finish.status in {
                "deselected",
                "not_launched",
            }:
                start = None
            attempts.append(_AttemptEvents(tuple(group), start, finish))
    return tuple(attempts)


def _attempts(
    attempts: Sequence[_AttemptEvents],
) -> tuple[TestAttempt, ...]:
    return tuple(
        materialized for attempt in attempts for materialized in attempt.materialize()
    )


def _attempt_timestamps(attempts: Sequence[_AttemptEvents]) -> tuple[Ns, ...]:
    return tuple(
        event.timestamp_ns
        for attempt in attempts
        for event in attempt.events
        if event.timestamp_ns is not None
    )


def _chunks(events: Sequence[_Event]) -> tuple[Chunk, ...]:
    chunk_events = [event for event in events if event.name == "chunk-event"]
    tests = [event for event in events if event.name.startswith("subtest-")]
    test_attempts = _attempt_groups(tests)
    if not chunk_events:
        attempts = _attempts(test_attempts)
        timestamps = _attempt_timestamps(test_attempts)
        return (Chunk(None, None, "", None, attempts, timestamps),) if tests else ()

    filenames: dict[tuple[int, int], set[str]] = defaultdict(set)
    for event in chunk_events:
        identity = event.chunk_identity
        if identity is not None and identity[2]:
            filenames[identity[:2]].add(identity[2])

    chunks: dict[tuple[int, int, str] | None, list[_Event]] = {}
    for event in chunk_events:
        identity = event.chunk_identity
        if identity is not None and not identity[2]:
            known_filenames = filenames[identity[:2]]
            if len(known_filenames) == 1:
                identity = (*identity[:2], next(iter(known_filenames)))
        chunks.setdefault(identity, []).append(event)
    assigned: dict[tuple[int, int, str] | None, list[_AttemptEvents]] = defaultdict(
        list
    )
    unassigned: list[_AttemptEvents] = []
    for attempt in test_attempts:
        identities = attempt.chunk_identities
        if len(identities) > 1:
            unassigned.append(attempt)
            continue
        identity = identities[0] if identities else None
        if identity in chunks:
            assigned[identity].append(attempt)
            continue
        candidates = (
            list(chunks)
            if identity is None
            else [key for key in chunks if key is not None and key[:2] == identity[:2]]
        )
        (assigned[candidates[0]] if len(candidates) == 1 else unassigned).extend(
            [attempt]
        )

    result = []
    for identity, snapshots in chunks.items():
        event = _merge(snapshots)
        matching = assigned[identity]
        attempts = _attempts(matching)
        timestamps = _attempt_timestamps(matching)
        key = identity
        result.append(
            Chunk(
                key[0] if key is not None else None,
                key[1] if key is not None else None,
                key[2] if key is not None else "",
                _record(event),
                attempts,
                timestamps,
            )
        )
    if unassigned:
        attempts = _attempts(unassigned)
        timestamps = _attempt_timestamps(unassigned)
        result.append(Chunk(None, None, "", None, attempts, timestamps))
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
