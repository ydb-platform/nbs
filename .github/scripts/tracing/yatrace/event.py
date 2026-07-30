"""Events read from a ``ytest.report.trace`` file."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..otlp import Ns
from .metrics import _metric_attributes

PASS_STATUSES = {"good", "pass", "passed", "success", "xfail", "xpass"}
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
    "xfaildiff",
}


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
        test_class = str(self.value.get("class", "unknown"))
        subtest = str(self.value.get("subtest", "unknown"))
        return test_class, subtest

    @property
    def normalized_test_key(self) -> tuple[str, str]:
        test_class, subtest = self.test_key
        return test_class.replace("::", "."), subtest

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
