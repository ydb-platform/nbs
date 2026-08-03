from __future__ import annotations

import pytest
from opentelemetry.proto_json.common.v1.common import InstrumentationScope
from opentelemetry.proto_json.resource.v1.resource import Resource
from opentelemetry.proto_json.trace.v1.trace import (
    ResourceSpans,
    ScopeSpans,
    TracesData,
)

from scripts.tracing.otlp import (
    Interval,
    ResourceAttributes,
    Span,
    Trace,
    decode_attributes,
    encode_attributes,
    make_event,
    make_span,
    MILLISECOND,
    Ns,
    SECOND,
    span_duration_ns,
    stable_span_id,
    stable_trace_id,
    update_span_attributes,
)


def test_ns_owns_unit_conversion_and_validation() -> None:
    assert isinstance(MILLISECOND, Ns)
    assert isinstance(SECOND, Ns)
    assert MILLISECOND == 1_000_000
    assert SECOND == 1_000_000_000
    value = Ns.from_s(1.5)
    assert isinstance(value, Ns)
    assert value == 1_500_000_000
    assert Ns.from_s(None) is None
    assert Ns.from_s(-1) is None
    assert Ns.from_s(float("nan")) is None
    assert Ns.from_s_or_zero("invalid") == 0
    assert Ns.from_ms(1.5) == 1_500_000
    assert Ns.from_ms(-1) is None
    assert Ns.from_ms(10**10_000) is None
    assert Ns.from_ms(1e308) is None
    assert Ns(1_500_000_000).to_s() == 1.5
    assert Ns(1_500_000).to_ms() == 1.5
    assert Ns(2_000_000_000).is_second_aligned()
    assert not Ns(2_000_000_001).is_second_aligned()
    with pytest.raises(ValueError, match="non-negative"):
        Ns(-1)


def test_interval_owns_duration_overlap_and_intersection() -> None:
    interval = Interval(Ns(1_000), Ns(5_000))
    coerced = Interval(1_000, 5_000)
    overlapping = Interval(Ns(3_000), Ns(7_000))
    disjoint = Interval(Ns(8_000), Ns(9_000))

    assert isinstance(coerced.start, Ns)
    assert isinstance(coerced.end, Ns)
    assert len(interval) == 4_000
    assert interval.clamp(Ns(500)) == Ns(1_000)
    assert interval.clamp(Ns(3_000)) == Ns(3_000)
    assert interval.clamp(Ns(8_000)) == Ns(5_000)
    assert interval.overlap(overlapping) == Ns(2_000)
    assert interval.overlap(disjoint) == Ns(0)
    assert interval.boundary_distance(overlapping) == Ns(4_000)
    assert interval.intersection(overlapping) == Interval(Ns(3_000), Ns(5_000))
    assert interval.intersection(disjoint) is None
    with pytest.raises(ValueError, match="end precedes start"):
        Interval(Ns(2_000), Ns(1_000))
    with pytest.raises(ValueError, match="non-negative"):
        Interval(-2, -1)


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
    assert isinstance(span.start_time_unix_nano, Ns)
    assert isinstance(span.end_time_unix_nano, Ns)
    assert isinstance(span_duration_ns(span), Ns)
    assert isinstance(event.time_unix_nano, Ns)


def test_stable_ids_encode_part_boundaries_and_types() -> None:
    left = ("a\0b", "c")
    right = ("a", "b\0c")

    assert stable_span_id(*left) != stable_span_id(*right)
    assert stable_trace_id(*left) != stable_trace_id(*right)
    assert stable_span_id(1) != stable_span_id("1")
    assert stable_span_id(Ns(1)) == stable_span_id(1)


def test_trace_owns_resource_and_scope_hierarchy() -> None:
    trace = Trace()
    span = make_span(
        trace_id=stable_trace_id("trace"),
        span_id=stable_span_id("span"),
        name="operation",
        start_ns=1_000,
        end_ns=2_000,
    )
    writer = trace.writer(ResourceAttributes({"service.name": "test"}))

    assert writer.add(span, scope_name="tests") is span

    resource_spans = trace.data.resource_spans[0]
    assert resource_spans.scope_spans[0].scope.name == "tests"
    assert resource_spans.scope_spans[0].spans[0].name == "operation"


def test_trace_length_and_truthiness_do_not_walk_spans(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trace = Trace()
    trace.add_span(
        make_span(
            trace_id=stable_trace_id("trace"),
            span_id=stable_span_id("span"),
            name="operation",
            start_ns=1_000,
            end_ns=2_000,
        ),
        resource=ResourceAttributes(),
        scope_name="tests",
    )

    def fail_if_walked(*_args, **_kwargs):
        raise AssertionError("walked")

    monkeypatch.setattr(trace, "spans", fail_if_walked)

    assert len(trace) == 1
    assert trace


def test_trace_default_scope_matches_an_unversioned_imported_scope() -> None:
    trace = Trace()
    resource = ResourceAttributes({"service.name": "test"})
    for index, scope_version in enumerate(("", None), start=1):
        kwargs = {"scope_version": scope_version} if scope_version is not None else {}
        trace.add_span(
            make_span(
                trace_id=stable_trace_id("trace"),
                span_id=stable_span_id("span", index),
                name=f"operation {index}",
                start_ns=1_000,
                end_ns=2_000,
            ),
            resource=resource,
            scope_name="tests",
            **kwargs,
        )

    scope_spans = trace.data.resource_spans[0].scope_spans
    assert len(scope_spans) == 1
    assert scope_spans[0].scope.version == ""


def test_trace_groups_resources_independent_of_attribute_order() -> None:
    trace = Trace()
    for index, attributes in enumerate(({"a": 1, "b": 2}, {"b": 2, "a": 1})):
        trace.add_span(
            make_span(
                trace_id=stable_trace_id("trace"),
                span_id=stable_span_id("span", index),
                name=f"operation {index}",
                start_ns=1_000,
                end_ns=2_000,
            ),
            resource=ResourceAttributes(attributes),
            scope_name="tests",
        )

    assert len(trace.data.resource_spans) == 1
    assert len(trace.data.resource_spans[0].scope_spans[0].spans) == 2


def test_trace_batching_preserves_instrumentation_scope_metadata() -> None:
    scope = InstrumentationScope(
        name="tests",
        version="2",
        attributes=encode_attributes({"library": "ya"}),
        dropped_attributes_count=3,
    )
    span = make_span(
        trace_id=stable_trace_id("trace"),
        span_id=stable_span_id("span"),
        name="operation",
        start_ns=1_000,
        end_ns=2_000,
    )
    source = Trace(
        TracesData(
            resource_spans=[
                ResourceSpans(
                    resource=Resource(),
                    scope_spans=[ScopeSpans(scope=scope, spans=[span])],
                )
            ]
        )
    )

    scope = next(source.batches(1)).data.resource_spans[0].scope_spans[0].scope
    assert decode_attributes(scope.attributes) == {"library": "ya"}
    assert scope.dropped_attributes_count == 3


def test_trace_batching_preserves_resource_and_scope_schema_urls() -> None:
    span = make_span(
        trace_id=stable_trace_id("trace"),
        span_id=stable_span_id("span"),
        name="operation",
        start_ns=1_000,
        end_ns=2_000,
    )
    source = Trace(
        TracesData(
            resource_spans=[
                ResourceSpans(
                    schema_url="https://resource.schema/v1",
                    scope_spans=[
                        ScopeSpans(
                            schema_url="https://scope.schema/v1",
                            spans=[span],
                        )
                    ],
                )
            ]
        )
    )

    resource_spans = next(source.batches(1)).data.resource_spans[0]
    assert resource_spans.schema_url == "https://resource.schema/v1"
    assert resource_spans.scope_spans[0].schema_url == "https://scope.schema/v1"


def test_trace_rejects_duplicate_span_ids_within_one_trace() -> None:
    trace = Trace()
    writer = trace.writer(ResourceAttributes())
    trace_id = stable_trace_id("trace")
    span_id = stable_span_id("duplicate")
    for name in ("first", "second"):
        writer.add(
            make_span(
                trace_id=trace_id,
                span_id=span_id,
                name=name,
                start_ns=1_000,
                end_ns=2_000,
            ),
            scope_name="tests",
        )

    with pytest.raises(ValueError, match="Duplicate OTLP span ID"):
        trace.validate()


def test_trace_allows_same_span_id_in_different_traces() -> None:
    trace = Trace()
    writer = trace.writer(ResourceAttributes())
    span_id = stable_span_id("shared")
    for name in ("first", "second"):
        writer.add(
            make_span(
                trace_id=stable_trace_id(name),
                span_id=span_id,
                name=name,
                start_ns=1_000,
                end_ns=2_000,
            ),
            scope_name="tests",
        )

    trace.validate()


def test_trace_rejects_cyclic_parent_relationships() -> None:
    trace = Trace()
    writer = trace.writer(ResourceAttributes())
    trace_id = stable_trace_id("trace")
    span_ids = [stable_span_id("span", index) for index in range(2)]
    for index, span_id in enumerate(span_ids):
        writer.add(
            make_span(
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=span_ids[1 - index],
                name=f"operation {index}",
                start_ns=1_000,
                end_ns=2_000,
            ),
            scope_name="tests",
        )

    with pytest.raises(ValueError, match="Cyclic OTLP parent relationship"):
        trace.validate()


def test_resource_attributes_clean_and_extend_github_metadata() -> None:
    attributes = ResourceAttributes.from_github(
        environment={
            "GITHUB_SERVER_URL": "https://github.example",
            "GITHUB_REPOSITORY": "example/repo",
            "GITHUB_RUN_ID": "123",
            "GITHUB_RUN_NUMBER": "45",
            "GITHUB_RUN_ATTEMPT": "2",
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
    assert attributes["github.run.id"] == 123
    assert attributes["github.run.number"] == 45
    assert attributes["github.run.attempt"] == 2
    assert attributes["github.run.url"] == (
        "https://github.example/example/repo/actions/runs/123"
    )
    assert attributes["github.commit.url"] == (
        "https://github.example/example/repo/commit/abc123"
    )


def test_span_attribute_updates_take_priority_at_otlp_limit() -> None:
    span = make_span(
        trace_id=stable_trace_id("trace"),
        span_id=stable_span_id("span"),
        name="operation",
        start_ns=1_000,
        end_ns=2_000,
        attributes={f"metric.{index}": index for index in range(256)},
    )

    update_span_attributes(span, {"critical": True})

    attributes = decode_attributes(span.attributes)
    assert len(attributes) == 256
    assert attributes["critical"] is True
