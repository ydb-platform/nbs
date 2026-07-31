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
        self._resources: dict[str, ResourceSpans] = {}
        self._scopes: dict[tuple[str, str, str], ScopeSpans] = {}
        self._index()

    @classmethod
    def from_json(cls, value: str | bytes) -> Trace:
        return cls(TracesData.from_json(value))

    def to_dict(self) -> dict[str, Any]:
        return self.data.to_dict()

    def _index(self) -> None:
        for resource_spans in self.data.resource_spans:
            resource = resource_spans.resource or Resource()
            resource_key = self._resource_key(resource)
            self._resources[resource_key] = resource_spans
            for scope_spans in resource_spans.scope_spans:
                scope = scope_spans.scope or InstrumentationScope()
                self._scopes[
                    (resource_key, str(scope.name or ""), str(scope.version or ""))
                ] = scope_spans
                for span in scope_spans.spans:
                    normalize_span_times(span)

    @staticmethod
    def _resource_key(resource: Resource) -> str:
        return json.dumps(
            resource.to_dict(),
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
        scope_name: str,
        scope_version: str = "1",
    ) -> None:
        normalize_span_times(span)
        validate_span(span)
        otlp_resource = self._resource(resource)
        resource_key = self._resource_key(otlp_resource)
        resource_spans = self._resources.get(resource_key)
        if resource_spans is None:
            resource_spans = ResourceSpans(resource=otlp_resource)
            self.data.resource_spans.append(resource_spans)
            self._resources[resource_key] = resource_spans

        scope_key = (resource_key, scope_name, scope_version)
        scope_spans = self._scopes.get(scope_key)
        if scope_spans is None:
            scope_spans = ScopeSpans(
                scope=InstrumentationScope(name=scope_name, version=scope_version)
            )
            resource_spans.scope_spans.append(scope_spans)
            self._scopes[scope_key] = scope_spans
        scope_spans.spans.append(span)

    def writer(self, resource: ResourceAttributes | Resource) -> SpanWriter:
        return SpanWriter(self, resource)

    def walk(self) -> Iterator[tuple[Resource, InstrumentationScope, Span]]:
        for resource_spans in self.data.resource_spans:
            resource = resource_spans.resource or Resource()
            for scope_spans in resource_spans.scope_spans:
                scope = scope_spans.scope or InstrumentationScope()
                for span in scope_spans.spans:
                    yield resource, scope, span

    def spans(self, scope_name: str | None = None) -> Iterator[Span]:
        for _, scope, span in self.walk():
            if scope_name is None or scope.name == scope_name:
                yield span

    def extend(self, other: Trace) -> None:
        for resource, scope, span in other.walk():
            self.add_span(
                span,
                resource=resource,
                scope_name=str(scope.name or ""),
                scope_version=str(scope.version or ""),
            )

    def batches(self, size: int) -> Iterator[Trace]:
        if size < 1:
            raise ValueError("batch size must be positive")
        batch = Trace()
        for index, (resource, scope, span) in enumerate(self.walk(), start=1):
            batch.add_span(
                span,
                resource=resource,
                scope_name=str(scope.name or ""),
                scope_version=str(scope.version or ""),
            )
            if index % size == 0:
                yield batch
                batch = Trace()
        if batch:
            yield batch

    def validate(self) -> None:
        for span in self.spans():
            validate_span(span)

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
        scope_version: str = "1",
    ) -> Span:
        self.trace.add_span(
            span,
            resource=self.resource,
            scope_name=scope_name,
            scope_version=scope_version,
        )
        return span
