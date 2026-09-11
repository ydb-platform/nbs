from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

from ..otlp import Interval, Ns, SECOND

PASS_STATUSES = frozenset({"good", "pass", "passed", "success", "xfail", "xpass"})
ERROR_STATUSES = frozenset(
    {
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
        "xfaildiff",
    }
)


def relative_build_path(path: str) -> str | None:
    prefix = "$(BUILD_ROOT)/"
    if "\x00" in path or not path.startswith(prefix):
        return None
    relative = path[len(prefix) :]
    parsed = PurePosixPath(relative)
    if (
        not relative
        or parsed.is_absolute()
        or any(part == ".." for part in parsed.parts)
    ):
        return None
    normalized = parsed.as_posix()
    return normalized if normalized not in {"", "."} else None


@dataclass(frozen=True, slots=True)
class TraceRecord:
    timestamp_ns: Ns | None
    order: int
    metrics: Mapping[str, Any]
    errors: tuple[tuple[str, str], ...]
    logs: Mapping[str, str]

    @property
    def failing_errors(self) -> tuple[tuple[str, str], ...]:
        return tuple(error for error in self.errors if error[0] in ERROR_STATUSES)

    def resolved_logs(self, build_root: Path) -> dict[str, str]:
        root = str(build_root)
        return {
            name: path.replace("$(BUILD_ROOT)", root)
            for name, path in self.logs.items()
            if name != "logsdir"
        }

    @property
    def logs_directory(self) -> str | None:
        path = self.logs.get("logsdir")
        return relative_build_path(path) if path is not None else None


@dataclass(frozen=True, slots=True)
class TestEvent:
    record: TraceRecord
    test_class: str
    name: str
    test_type: str
    path: str
    status: str
    status_code: int
    duration_ns: Ns

    @property
    def key(self) -> tuple[str, str, str, str]:
        return self.test_class, self.name, self.test_type, self.path

    @property
    def normalized_key(self) -> tuple[str, str]:
        return self.test_class.replace("::", "."), self.name

    def resolved_logs(self, build_root: Path) -> dict[str, str]:
        return self.record.resolved_logs(build_root)

    @property
    def logs_directory(self) -> str | None:
        return self.record.logs_directory


@dataclass(frozen=True, slots=True)
class TestAttempt:
    start: TestEvent | None
    finish: TestEvent | None


@dataclass(frozen=True, slots=True)
class Chunk:
    index: int | None
    total: int | None
    filename: str
    record: TraceRecord | None
    attempts: tuple[TestAttempt, ...]
    test_timestamps: tuple[Ns, ...]

    @property
    def key(self) -> tuple[int, int] | None:
        if self.index is None or self.total is None:
            return None
        return self.index, self.total

    def resolved_logs(self, build_root: Path) -> dict[str, str]:
        return self.record.resolved_logs(build_root) if self.record is not None else {}

    @property
    def logs_directory(self) -> str | None:
        return self.record.logs_directory if self.record is not None else None

    def interval(self, root: Interval) -> Interval:
        record = self.record
        metrics = record.metrics if record is not None else {}
        start = Ns.from_s(metrics.get("suite_start_timestamp"))
        end = Ns.from_s(metrics.get("suite_finish_timestamp"))
        wall = Ns.from_s(metrics.get("wall_time"))
        if (
            start is not None
            and end is not None
            and wall is not None
            and start.is_second_aligned()
            and end.is_second_aligned()
            and record is not None
            and record.timestamp_ns is not None
        ):
            earliest = max(end, start + wall)
            latest = min(end + SECOND, start + SECOND + wall)
            if earliest < latest:
                end = (
                    record.timestamp_ns
                    if earliest <= record.timestamp_ns < latest
                    else earliest
                )
                start = end - wall
        elif end is not None and start is None and wall is not None:
            start = end - wall
        elif start is not None and end is None and wall is not None:
            end = start + wall

        timestamps = self.test_timestamps
        if start is None:
            start = min(timestamps) if timestamps else root.start
        if end is None:
            if record is not None and record.timestamp_ns is not None:
                end = record.timestamp_ns
            elif record is not None and timestamps:
                end = max(timestamps)
            else:
                end = root.end
        start = root.clamp(start)
        return Interval(start, max(start, root.clamp(end)))


@dataclass(frozen=True, slots=True)
class SuiteTrace:
    path: Path
    suite: str
    result_folder: str
    suite_record: TraceRecord | None
    suite_timestamps: tuple[Ns, ...]
    chunks: tuple[Chunk, ...]
    finished: tuple[TestEvent, ...]
    malformed_json_record_count: int = 0

    def finished_test(self, test_class: str, name: str) -> TestEvent | None:
        exact = next(
            (
                event
                for event in reversed(self.finished)
                if (event.test_class, event.name) == (test_class, name)
            ),
            None,
        )
        if exact is not None:
            return exact
        key = test_class.replace("::", "."), name
        fallback = [
            event for event in reversed(self.finished) if event.normalized_key == key
        ]
        return (
            fallback[0]
            if len({(event.test_class, event.name) for event in fallback}) == 1
            else None
        )

    def matching_chunks(self, index: int, total: int) -> list[Chunk]:
        return [chunk for chunk in self.chunks if chunk.key == (index, total)]
