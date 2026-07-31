from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from typing import Any

from opentelemetry.proto_json.trace.v1.trace import Span, Status

from .attributes import (
    MAX_ATTRIBUTES,
    MAX_ATTRIBUTE_LENGTH,
    clean_attributes,
    decode_attributes,
    encode_attributes,
)
from .time import Ns

MAX_UINT64 = (1 << 64) - 1


def update_span_attributes(span: Span, values: Mapping[str, Any]) -> None:
    attributes = decode_attributes(span.attributes)
    updates = clean_attributes(values)
    for key in updates:
        attributes.pop(key, None)
    retained = dict(list(attributes.items())[: max(0, MAX_ATTRIBUTES - len(updates))])
    retained.update(updates)
    span.attributes = encode_attributes(retained)


def span_attribute(span: Span, key: str, default: Any = None) -> Any:
    return decode_attributes(span.attributes).get(key, default)


def _stable_id(*parts: object, size: int) -> bytes:
    text = "\0".join(
        part.hex() if isinstance(part, bytes) else str(part) for part in parts
    )
    return hashlib.sha256(text.encode()).digest()[:size]


def stable_trace_id(*parts: object) -> bytes:
    return _stable_id(*parts, size=16)


def stable_span_id(*parts: object) -> bytes:
    return _stable_id(*parts, size=8)


def span_duration_ns(span: Span) -> Ns:
    return Ns(max(0, (span.end_time_unix_nano or 0) - (span.start_time_unix_nano or 0)))


def span_status_code(span: Span) -> int:
    return int(span.status.code or 0) if span.status else 0


def span_status_message(span: Span) -> str:
    return str(span.status.message or "") if span.status else ""


def set_span_status(span: Span, code: int, message: str = "") -> None:
    span.status = Status(code=code, message=message[:MAX_ATTRIBUTE_LENGTH])


def make_span(
    *,
    trace_id: bytes,
    span_id: bytes,
    name: str,
    start_ns: Ns | int,
    end_ns: Ns | int,
    parent_span_id: bytes = b"",
    attributes: Mapping[str, Any] | None = None,
    events: Sequence[Span.Event] = (),
    status_code: int = 0,
    status_message: str = "",
) -> Span:
    return Span(
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=parent_span_id,
        name=name,
        kind=Span.SpanKind.SPAN_KIND_INTERNAL,
        start_time_unix_nano=Ns(start_ns),
        end_time_unix_nano=Ns(end_ns),
        attributes=encode_attributes(attributes or {}),
        events=list(events),
        status=Status(
            code=status_code,
            message=status_message[:MAX_ATTRIBUTE_LENGTH],
        ),
    )


def make_event(
    *,
    name: str,
    time_ns: Ns | int,
    attributes: Mapping[str, Any] | None = None,
) -> Span.Event:
    return Span.Event(
        name=name,
        time_unix_nano=Ns(time_ns),
        attributes=encode_attributes(attributes or {}),
    )


def validate_span(span: Span) -> None:
    if (
        len(span.trace_id or b"") != 16
        or span.trace_id == b"\0" * 16
        or len(span.span_id or b"") != 8
        or span.span_id == b"\0" * 8
    ):
        raise ValueError(f"Invalid OTLP span IDs for {span.name!r}")
    if span.parent_span_id and (
        len(span.parent_span_id) != 8 or span.parent_span_id == b"\0" * 8
    ):
        raise ValueError(f"Invalid parent span ID for {span.name!r}")
    start_ns = Ns(span.start_time_unix_nano or 0)
    end_ns = Ns(span.end_time_unix_nano or 0)
    if end_ns < start_ns or start_ns > MAX_UINT64 or end_ns > MAX_UINT64:
        raise ValueError(f"Invalid timestamps for {span.name!r}")
    if any(Ns(event.time_unix_nano or 0) > MAX_UINT64 for event in span.events):
        raise ValueError(f"Invalid event timestamp for {span.name!r}")
    if span_status_code(span) not in {0, 1, 2}:
        raise ValueError(f"Invalid status code for {span.name!r}")


def normalize_span_times(span: Span) -> None:
    span.start_time_unix_nano = Ns(span.start_time_unix_nano or 0)
    span.end_time_unix_nano = Ns(span.end_time_unix_nano or 0)
    for event in span.events:
        event.time_unix_nano = Ns(event.time_unix_nano or 0)
