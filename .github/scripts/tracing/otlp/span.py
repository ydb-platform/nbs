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


def _stable_id_part(part: object) -> tuple[bytes, bytes]:
    if isinstance(part, bytes):
        return b"bytes", part
    if isinstance(part, str):
        return b"str", part.encode()
    if isinstance(part, bool):
        return b"bool", str(part).encode()
    if isinstance(part, int):
        return b"int", str(part).encode()
    if isinstance(part, float):
        return b"float", repr(part).encode()
    if part is None:
        return b"none", b""
    kind = f"{type(part).__module__}.{type(part).__qualname__}".encode()
    return kind, str(part).encode()


def _stable_id(*parts: object, size: int) -> bytes:
    digest = hashlib.sha256()
    for part in parts:
        kind, value = _stable_id_part(part)
        digest.update(len(kind).to_bytes(4, "big"))
        digest.update(kind)
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)
    return digest.digest()[:size]


def stable_trace_id(*parts: object) -> bytes:
    return _stable_id(*parts, size=16)


def stable_span_id(*parts: object) -> bytes:
    return _stable_id(*parts, size=8)


def _ns_from_proto(value: object) -> Ns:
    return Ns(0 if value is None else value)  # type: ignore[arg-type]


def _ns_value(value: object, field: str) -> int:
    if not isinstance(value, Ns):
        raise TypeError(f"{field} must be Ns")
    return value.value


def span_duration_ns(span: Span) -> Ns:
    start_ns = _ns_from_proto(span.start_time_unix_nano)
    end_ns = _ns_from_proto(span.end_time_unix_nano)
    return Ns(max(0, end_ns.value - start_ns.value))


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
    start_ns: Ns,
    end_ns: Ns,
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
        start_time_unix_nano=_ns_value(start_ns, "start_ns"),
        end_time_unix_nano=_ns_value(end_ns, "end_ns"),
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
    time_ns: Ns,
    attributes: Mapping[str, Any] | None = None,
) -> Span.Event:
    return Span.Event(
        name=name,
        time_unix_nano=_ns_value(time_ns, "time_ns"),
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
    start_ns = _ns_from_proto(span.start_time_unix_nano)
    end_ns = _ns_from_proto(span.end_time_unix_nano)
    if end_ns < start_ns or start_ns.value > MAX_UINT64 or end_ns.value > MAX_UINT64:
        raise ValueError(f"Invalid timestamps for {span.name!r}")
    event_times_ns = (_ns_from_proto(event.time_unix_nano) for event in span.events)
    if any(time_ns.value > MAX_UINT64 for time_ns in event_times_ns):
        raise ValueError(f"Invalid event timestamp for {span.name!r}")
    if span_status_code(span) not in {0, 1, 2}:
        raise ValueError(f"Invalid status code for {span.name!r}")


def normalize_span_times(span: Span) -> None:
    span.start_time_unix_nano = _ns_from_proto(span.start_time_unix_nano).value
    span.end_time_unix_nano = _ns_from_proto(span.end_time_unix_nano).value
    for event in span.events:
        event.time_unix_nano = _ns_from_proto(event.time_unix_nano).value
