from __future__ import annotations

import pytest

from scripts.otlp import (
    ResourceAttributes,
    Span,
    Trace,
    make_event,
    make_span,
    ns,
    span_duration_ns,
    stable_span_id,
    stable_trace_id,
)


def test_ns_owns_unit_conversion_and_validation() -> None:
    value = ns.from_seconds(1.5)
    assert isinstance(value, ns)
    assert value == 1_500_000_000
    assert ns.from_seconds(None) is None
    assert ns.from_seconds(-1) is None
    assert ns.from_seconds(float("nan")) is None
    assert ns.from_seconds_or_zero("invalid") == 0
    assert ns.from_milliseconds(1.5) == 1_500_000
    assert ns.from_milliseconds(-1) is None
    with pytest.raises(ValueError, match="non-negative"):
        ns(-1)


def test_official_span_uses_byte_ids_and_nanosecond_values() -> None:
    event = make_event(name="started", time_ns=1_500)
    span = make_span(
        trace_id=stable_trace_id("trace"),
        span_id=stable_span_id("span"),
        name="operation",
        start_ns=1_000,
        end_ns=2_000,
        events=[event],
    )

    assert isinstance(span, Span)
    assert isinstance(event, Span.Event)
    assert stable_span_id("span") == stable_span_id("span")
    assert len(span.trace_id) == 16
    assert len(span.span_id) == 8
    assert isinstance(span.start_time_unix_nano, ns)
    assert isinstance(span.end_time_unix_nano, ns)
    assert isinstance(span_duration_ns(span), ns)
    assert isinstance(event.time_unix_nano, ns)


def test_trace_owns_resource_and_scope_hierarchy() -> None:
    trace = Trace()
    trace.add_span(
        make_span(
            trace_id=stable_trace_id("trace"),
            span_id=stable_span_id("span"),
            name="operation",
            start_ns=1_000,
            end_ns=2_000,
        ),
        resource=ResourceAttributes({"service.name": "test"}),
        scope_name="tests",
    )

    resource_spans = trace.data.resource_spans[0]
    assert resource_spans.scope_spans[0].scope.name == "tests"
    assert resource_spans.scope_spans[0].spans[0].name == "operation"


def test_resource_attributes_clean_and_extend_github_metadata() -> None:
    attributes = ResourceAttributes.from_github(
        environment={
            "GITHUB_SERVER_URL": "https://github.example",
            "GITHUB_REPOSITORY": "example/repo",
            "GITHUB_RUN_ID": "123",
            "GITHUB_SHA": "abc123",
        }
    ).with_attributes(
        {
            "service.name": "test",
            "empty": "",
        }
    )

    assert attributes["service.name"] == "test"
    assert "empty" not in attributes
    assert attributes["github.run.url"] == (
        "https://github.example/example/repo/actions/runs/123"
    )
    assert attributes["github.commit.url"] == (
        "https://github.example/example/repo/commit/abc123"
    )
