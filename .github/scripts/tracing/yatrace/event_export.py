"""Map parsed ya events to OTLP span attributes."""

from __future__ import annotations

from typing import Any

from .event import YaEvent
from .metrics import _metric_attributes, _metric_name


def event_error_attributes(event: YaEvent, prefix: str) -> dict[str, Any]:
    errors = event.errors
    if not errors:
        return {}
    return {
        f"{prefix}.error.count": len(errors),
        f"{prefix}.error.statuses": [status for status, _ in errors],
        f"{prefix}.error.messages": [message for _, message in errors],
    }


def event_log_attributes(event: YaEvent, prefix: str) -> dict[str, str]:
    """Return portable build-root-relative paths, never runner-local URLs."""
    attributes: dict[str, str] = {}
    for name, path in list(event.log_paths.items())[:32]:
        relative = event.relative_build_path(path)
        if relative is None:
            continue
        if name == "logsdir":
            attributes[f"{prefix}.logs_directory.path"] = relative
            continue
        normalized = _metric_name(name)
        if normalized:
            attributes[f"{prefix}.log.{normalized}.path"] = relative
    return attributes


def test_event_attributes(
    event: YaEvent,
    test_class: str,
    subtest: str,
    *,
    inferred: bool,
    timing_source: str = "",
    incomplete: bool = False,
) -> dict[str, Any]:
    status, _ = event.status
    attributes: dict[str, Any] = {
        "test.framework": "ya",
        "test.suite": test_class,
        "test.name": subtest,
        "test.status": status,
    }
    if event.test_type:
        attributes["test.type"] = event.test_type
    if event.test_path:
        attributes["test.path"] = event.test_path
    if inferred:
        attributes["test.timing.inferred"] = True
    if timing_source:
        attributes["test.timing.source"] = timing_source
    if incomplete:
        attributes["test.incomplete"] = True
    attributes.update(_metric_attributes(event.metrics, prefix="ya.test.metric"))
    attributes.update(event_error_attributes(event, "ya.test"))
    attributes.update(event_log_attributes(event, "ya.test"))
    return attributes
