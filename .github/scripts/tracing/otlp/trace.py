from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from opentelemetry.proto_json.common.v1.common import InstrumentationScope
from opentelemetry.proto_json.resource.v1.resource import Resource
from opentelemetry.proto_json.trace.v1.trace import (
    ResourceSpans,
    ScopeSpans,
    Span,
    TracesData,
)

from .attributes import ResourceAttributes, encode_attributes
from .span import normalize_span_times, validate_span


class Trace:
    """Mutable convenience wrapper around the canonical OTLP TracesData tree."""

    def __init__(self, data: TracesData | None = None) -> None:
        self.data = data or TracesData()
        self._resources: dict[tuple[str, str], ResourceSpans] = {}
        self._scopes: dict[tuple[str, str, str, str], ScopeSpans] = {}
        self._index()

    @classmethod
    def from_json(cls, value: str | bytes) -> Trace:
        return cls(TracesData.from_json(value))

    def to_dict(self) -> dict[str, Any]:
        return self.data.to_dict()

    def _index(self) -> None:
        for resource_spans in self.data.resource_spans:
            resource = resource_spans.resource or Resource()
            resource_key = (
                self._message_key(resource),
                str(resource_spans.schema_url or ""),
            )
            self._resources[resource_key] = resource_spans
            for scope_spans in resource_spans.scope_spans:
                scope = scope_spans.scope or InstrumentationScope()
                scope_key = (
                    *resource_key,
                    self._message_key(scope),
                    str(scope_spans.schema_url or ""),
                )
                self._scopes[scope_key] = scope_spans
                for span in scope_spans.spans:
                    normalize_span_times(span)

    @staticmethod
    def _message_key(message: Resource | InstrumentationScope) -> str:
        value = message.to_dict()
        attributes = value.get("attributes")
        if isinstance(attributes, list):
            attributes.sort(
                key=lambda attribute: json.dumps(
                    attribute,
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _resource(value: ResourceAttributes | Resource) -> Resource:
        if isinstance(value, Resource):
            return value
        return Resource(attributes=encode_attributes(value))

    def add_span(
        self,
        span: Span,
        *,
        resource: ResourceAttributes | Resource,
        scope_name: str = "",
        scope_version: str = "",
        scope: InstrumentationScope | None = None,
        resource_schema_url: str = "",
        scope_schema_url: str = "",
    ) -> None:
        normalize_span_times(span)
        validate_span(span)
        otlp_resource = self._resource(resource)
        resource_key = (self._message_key(otlp_resource), resource_schema_url)
        resource_spans = self._resources.get(resource_key)
        if resource_spans is None:
            resource_spans = ResourceSpans(
                resource=otlp_resource,
                schema_url=resource_schema_url,
            )
            self.data.resource_spans.append(resource_spans)
            self._resources[resource_key] = resource_spans

        otlp_scope = scope or InstrumentationScope(
            name=scope_name,
            version=scope_version,
        )
        scope_key = (*resource_key, self._message_key(otlp_scope), scope_schema_url)
        scope_spans = self._scopes.get(scope_key)
        if scope_spans is None:
            scope_spans = ScopeSpans(
                scope=otlp_scope,
                schema_url=scope_schema_url,
            )
            resource_spans.scope_spans.append(scope_spans)
            self._scopes[scope_key] = scope_spans
        scope_spans.spans.append(span)

    def writer(self, resource: ResourceAttributes | Resource) -> SpanWriter:
        return SpanWriter(self, resource)

    def walk_groups(self) -> Iterator[tuple[ResourceSpans, ScopeSpans, Span]]:
        for resource_spans in self.data.resource_spans:
            for scope_spans in resource_spans.scope_spans:
                for span in scope_spans.spans:
                    yield resource_spans, scope_spans, span

    def _add_grouped_span(
        self,
        resource_spans: ResourceSpans,
        scope_spans: ScopeSpans,
        span: Span,
    ) -> None:
        self.add_span(
            span,
            resource=resource_spans.resource or Resource(),
            scope=scope_spans.scope or InstrumentationScope(),
            resource_schema_url=str(resource_spans.schema_url or ""),
            scope_schema_url=str(scope_spans.schema_url or ""),
        )

    def walk(self) -> Iterator[tuple[Resource, InstrumentationScope, Span]]:
        for resource_spans, scope_spans, span in self.walk_groups():
            resource = resource_spans.resource or Resource()
            scope = scope_spans.scope or InstrumentationScope()
            yield resource, scope, span

    def spans(self, scope_name: str | None = None) -> Iterator[Span]:
        for _, scope, span in self.walk():
            if scope_name is None or scope.name == scope_name:
                yield span

    def extend(self, other: Trace) -> None:
        for grouped_span in other.walk_groups():
            self._add_grouped_span(*grouped_span)

    def batches(self, size: int) -> Iterator[Trace]:
        if size < 1:
            raise ValueError("batch size must be positive")
        batch = Trace()
        for index, grouped_span in enumerate(self.walk_groups(), start=1):
            batch._add_grouped_span(*grouped_span)
            if index % size == 0:
                yield batch
                batch = Trace()
        if batch:
            yield batch

    def validate(self) -> None:
        seen: dict[tuple[bytes, bytes], str] = {}
        for span in self.spans():
            validate_span(span)
            identity = bytes(span.trace_id), bytes(span.span_id)
            if identity in seen:
                previous = seen[identity]
                raise ValueError(
                    f"Duplicate OTLP span ID {span.span_id.hex()} in trace "
                    f"{span.trace_id.hex()} for {previous!r} and {span.name!r}"
                )
            seen[identity] = str(span.name or "")

    def __iter__(self) -> Iterator[Span]:
        return self.spans()

    def __len__(self) -> int:
        return sum(1 for _ in self.spans())

    def __getitem__(self, index: int | slice) -> Span | list[Span]:
        return list(self.spans())[index]

    def __bool__(self) -> bool:
        return any(True for _ in self.spans())


@dataclass(frozen=True, slots=True)
class SpanWriter:
    trace: Trace
    resource: ResourceAttributes | Resource

    def add(
        self,
        span: Span,
        *,
        scope_name: str,
        scope_version: str = "",
    ) -> Span:
        self.trace.add_span(
            span,
            resource=self.resource,
            scope_name=scope_name,
            scope_version=scope_version,
        )
        return span
