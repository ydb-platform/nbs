"""OTLP trace model types shared by trace producers and renderers."""

from __future__ import annotations

import hashlib
import json
import math
import urllib.parse
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any

from opentelemetry.proto_json.common.v1.common import (
    AnyValue,
    ArrayValue,
    InstrumentationScope,
    KeyValue,
)
from opentelemetry.proto_json.resource.v1.resource import Resource
from opentelemetry.proto_json.trace.v1.trace import (
    ResourceSpans,
    ScopeSpans,
    Span,
    Status,
    TracesData,
)

MAX_ATTRIBUTES = 256
MAX_ATTRIBUTE_LENGTH = 8_192
MAX_UINT64 = (1 << 64) - 1


class ns(int):
    """A non-negative nanosecond value."""

    def __new__(cls, value: int | float | str) -> ns:
        nanoseconds = int(value)
        if nanoseconds < 0:
            raise ValueError("nanoseconds must be non-negative")
        return int.__new__(cls, nanoseconds)

    @classmethod
    def from_seconds(cls, value: Any) -> ns | None:
        try:
            seconds = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(seconds) or seconds < 0:
            return None
        return cls(seconds * 1_000_000_000)

    @classmethod
    def from_seconds_or_zero(cls, value: Any) -> ns:
        return cls.from_seconds(value) or cls(0)

    @classmethod
    def from_milliseconds(cls, value: Any) -> ns | None:
        try:
            milliseconds = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(milliseconds) or milliseconds < 0:
            return None
        return cls(milliseconds * 1_000_000)


def _clean_scalar(value: Any) -> str | bool | int | float:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if value is None:
        return ""
    text = str(value)
    if len(text) > MAX_ATTRIBUTE_LENGTH:
        return f"{text[:MAX_ATTRIBUTE_LENGTH]}…"
    return text


def clean_attributes(values: Mapping[str, Any] | None) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for raw_key, raw_value in list((values or {}).items())[:MAX_ATTRIBUTES]:
        key = str(raw_key)[:256]
        if isinstance(raw_value, (list, tuple)):
            result[key] = [_clean_scalar(value) for value in raw_value[:128]]
        else:
            result[key] = _clean_scalar(raw_value)
    return result


class ResourceAttributes(dict[str, Any]):
    """Normalized OTLP resource attributes with common construction helpers."""

    def __init__(self, values: Mapping[str, Any] | None = None) -> None:
        super().__init__(clean_attributes(values))

    def with_attributes(
        self,
        values: Mapping[str, Any],
    ) -> ResourceAttributes:
        merged = dict(self)
        merged.update(
            {
                key: value
                for key, value in values.items()
                if value is not None and value != "" and value != []
            }
        )
        return type(self)(merged)

    @classmethod
    def from_github(
        cls,
        *,
        environment: Mapping[str, Any],
        workflow_run: Mapping[str, Any] | None = None,
    ) -> ResourceAttributes:
        server_url = str(
            environment.get("GITHUB_SERVER_URL") or "https://github.com"
        ).rstrip("/")
        if workflow_run is None:
            values: dict[str, Any] = {
                "github.repository": environment.get("GITHUB_REPOSITORY", ""),
                "github.workflow": environment.get("GITHUB_WORKFLOW", ""),
                "github.workflow.ref": environment.get("GITHUB_WORKFLOW_REF", ""),
                "github.job": environment.get("GITHUB_JOB", ""),
                "github.run.id": environment.get("GITHUB_RUN_ID", ""),
                "github.run.number": environment.get("GITHUB_RUN_NUMBER", ""),
                "github.run.attempt": environment.get("GITHUB_RUN_ATTEMPT", ""),
                "github.event.name": environment.get("GITHUB_EVENT_NAME", ""),
                "github.actor": environment.get("GITHUB_ACTOR", ""),
                "github.sha": environment.get("GITHUB_SHA", ""),
                "github.ref": environment.get("GITHUB_REF", ""),
            }
        else:
            repository = workflow_run.get("repository") or {}
            if not isinstance(repository, Mapping):
                repository = {}
            actor = workflow_run.get("actor") or {}
            if not isinstance(actor, Mapping):
                actor = {}
            triggering_actor = workflow_run.get("triggering_actor") or {}
            if not isinstance(triggering_actor, Mapping):
                triggering_actor = {}
            pull_requests = workflow_run.get("pull_requests") or []
            if not isinstance(pull_requests, list):
                pull_requests = []
            values = {
                "github.repository": repository.get("full_name")
                or environment.get("GITHUB_REPOSITORY", ""),
                "github.workflow": workflow_run.get("name", ""),
                "github.workflow.path": workflow_run.get("path", ""),
                "github.run.id": workflow_run.get("id", ""),
                "github.run.number": workflow_run.get("run_number", ""),
                "github.run.attempt": workflow_run.get("run_attempt", 1),
                "github.run.url": workflow_run.get("html_url", ""),
                "github.event.name": workflow_run.get("event", ""),
                "github.actor": actor.get("login", ""),
                "github.triggering_actor": triggering_actor.get("login", ""),
                "github.sha": workflow_run.get("head_sha", ""),
                "github.ref.name": workflow_run.get("head_branch", ""),
                "github.pull_request.number": [
                    item.get("number")
                    for item in pull_requests
                    if isinstance(item, Mapping) and item.get("number") is not None
                ],
                "ci.conclusion": workflow_run.get("conclusion", ""),
                "ci.display_title": workflow_run.get("display_title", ""),
            }

        repository_name = str(values.get("github.repository") or "")
        run_id = values.get("github.run.id")
        sha = str(values.get("github.sha") or "")
        if repository_name and run_id and not values.get("github.run.url"):
            values["github.run.url"] = (
                f"{server_url}/{repository_name}/actions/runs/"
                f"{urllib.parse.quote(str(run_id), safe='')}"
            )
        if repository_name and sha:
            values["github.commit.url"] = (
                f"{server_url}/{repository_name}/commit/"
                f"{urllib.parse.quote(sha, safe='')}"
            )

        return cls(
            {
                key: value
                for key, value in values.items()
                if value is not None and value != "" and value != []
            }
        )


def any_value(value: Any) -> AnyValue:
    if isinstance(value, bool):
        return AnyValue(bool_value=value)
    if isinstance(value, int):
        return AnyValue(int_value=value)
    if isinstance(value, float):
        return AnyValue(double_value=value)
    if isinstance(value, (list, tuple)):
        return AnyValue(array_value=ArrayValue(values=[any_value(v) for v in value]))
    return AnyValue(string_value=str(value))


def value_from_any(value: AnyValue) -> Any:
    if value.string_value is not None:
        return value.string_value
    if value.bool_value is not None:
        return value.bool_value
    if value.int_value is not None:
        return value.int_value
    if value.double_value is not None:
        return value.double_value
    if value.array_value is not None:
        return [value_from_any(item) for item in value.array_value.values]
    if value.kvlist_value is not None:
        return decode_attributes(value.kvlist_value.values)
    if value.bytes_value is not None:
        return value.bytes_value
    return ""


def encode_attributes(values: Mapping[str, Any]) -> list[KeyValue]:
    return [
        KeyValue(key=key, value=any_value(value))
        for key, value in clean_attributes(values).items()
    ]


def decode_attributes(values: Iterable[KeyValue]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for item in list(values)[:MAX_ATTRIBUTES]:
        key = str(item.key or "")[:256]
        if key and item.value is not None:
            result[key] = value_from_any(item.value)
    return clean_attributes(result)


def update_span_attributes(span: Span, values: Mapping[str, Any]) -> None:
    attributes = decode_attributes(span.attributes)
    attributes.update(values)
    span.attributes = encode_attributes(attributes)


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


def span_duration_ns(span: Span) -> ns:
    return ns(max(0, (span.end_time_unix_nano or 0) - (span.start_time_unix_nano or 0)))


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
    start_ns: ns | int,
    end_ns: ns | int,
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
        start_time_unix_nano=ns(start_ns),
        end_time_unix_nano=ns(end_ns),
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
    time_ns: ns | int,
    attributes: Mapping[str, Any] | None = None,
) -> Span.Event:
    return Span.Event(
        name=name,
        time_unix_nano=ns(time_ns),
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
    start_ns = ns(span.start_time_unix_nano or 0)
    end_ns = ns(span.end_time_unix_nano or 0)
    if end_ns < start_ns or start_ns > MAX_UINT64 or end_ns > MAX_UINT64:
        raise ValueError(f"Invalid timestamps for {span.name!r}")
    if any(ns(event.time_unix_nano or 0) > MAX_UINT64 for event in span.events):
        raise ValueError(f"Invalid event timestamp for {span.name!r}")
    if span_status_code(span) not in {0, 1, 2}:
        raise ValueError(f"Invalid status code for {span.name!r}")


def normalize_span_times(span: Span) -> None:
    span.start_time_unix_nano = ns(span.start_time_unix_nano or 0)
    span.end_time_unix_nano = ns(span.end_time_unix_nano or 0)
    for event in span.events:
        event.time_unix_nano = ns(event.time_unix_nano or 0)


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
