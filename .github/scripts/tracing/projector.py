from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any

from opentelemetry.proto_json.resource.v1.resource import Resource
from opentelemetry.proto_json.trace.v1.trace import Span

from .otlp import (
    Interval,
    ResourceAttributes,
    Trace,
    make_span,
    stable_span_id,
)


@dataclass(frozen=True, slots=True)
class SpanProjector:
    """Project domain intervals into one bound OTLP trace context."""

    trace: Trace
    trace_id: bytes
    resource: ResourceAttributes | Resource
    scope_name: str
    parent_span_id: bytes = b""

    def span_id(self, *identity: object) -> bytes:
        return stable_span_id(self.trace_id, *identity)

    def under(self, parent: Span | bytes) -> SpanProjector:
        parent_span_id = parent.span_id if isinstance(parent, Span) else parent
        return replace(self, parent_span_id=parent_span_id)

    def scoped(self, name: str) -> SpanProjector:
        return replace(self, scope_name=name)

    def make(
        self,
        *identity: object,
        name: str,
        interval: Interval,
        attributes: Mapping[str, Any] | None = None,
        events: Sequence[Span.Event] = (),
        status_code: int = 0,
        status_message: str = "",
    ) -> Span:
        return make_span(
            trace_id=self.trace_id,
            span_id=self.span_id(*identity),
            parent_span_id=self.parent_span_id,
            name=name,
            start_ns=interval.start,
            end_ns=interval.end,
            attributes=attributes,
            events=events,
            status_code=status_code,
            status_message=status_message,
        )

    def add(self, span: Span) -> Span:
        self.trace.add_span(
            span,
            resource=self.resource,
            scope_name=self.scope_name,
        )
        return span

    def emit(
        self,
        *identity: object,
        name: str,
        interval: Interval,
        attributes: Mapping[str, Any] | None = None,
        events: Sequence[Span.Event] = (),
        status_code: int = 0,
        status_message: str = "",
    ) -> Span:
        return self.add(
            self.make(
                *identity,
                name=name,
                interval=interval,
                attributes=attributes,
                events=events,
                status_code=status_code,
                status_message=status_message,
            )
        )
