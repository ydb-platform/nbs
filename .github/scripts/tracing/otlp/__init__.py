"""OTLP trace model types shared by trace producers and renderers."""

from opentelemetry.proto_json.trace.v1.trace import Span

from .attributes import (
    MAX_ATTRIBUTES,
    MAX_ATTRIBUTE_LENGTH,
    ResourceAttributes,
    any_value,
    clean_attributes,
    decode_attributes,
    encode_attributes,
    value_from_any,
)
from .span import (
    MAX_UINT64,
    make_event,
    make_span,
    normalize_span_times,
    set_span_status,
    span_attribute,
    span_duration_ns,
    span_status_code,
    span_status_message,
    stable_span_id,
    stable_trace_id,
    update_span_attributes,
    validate_span,
)
from .time import MILLISECOND, SECOND, Interval, Ns
from .trace import Trace

__all__ = [
    "MAX_ATTRIBUTES",
    "MAX_ATTRIBUTE_LENGTH",
    "MAX_UINT64",
    "MILLISECOND",
    "SECOND",
    "Interval",
    "Ns",
    "ResourceAttributes",
    "Span",
    "Trace",
    "any_value",
    "clean_attributes",
    "decode_attributes",
    "encode_attributes",
    "make_event",
    "make_span",
    "normalize_span_times",
    "set_span_status",
    "span_attribute",
    "span_duration_ns",
    "span_status_code",
    "span_status_message",
    "stable_span_id",
    "stable_trace_id",
    "update_span_attributes",
    "validate_span",
    "value_from_any",
]
