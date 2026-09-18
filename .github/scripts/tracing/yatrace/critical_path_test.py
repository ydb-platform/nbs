from scripts.tracing.otlp import (
    Interval,
    Ns,
    ResourceAttributes,
    Trace,
    make_span,
)

from .critical_path import YaCriticalPath, YaCriticalPathEntry
from .node import YaNode


def test_build_matching_uses_scalar_ns_scores() -> None:
    entry = YaCriticalPathEntry.from_raw(
        0,
        {"type": "CC", "start_ts": 1, "end_ts": 2},
    )
    matching = YaNode(
        "Run(tool)",
        "CC",
        Interval(Ns(1_000_000), Ns(2_000_000)),
        "execute",
        "CC",
    )
    disjoint = YaNode(
        "Run(other)",
        "CC",
        Interval(Ns(3_000_000), Ns(4_000_000)),
        "execute",
        "CC",
    )
    critical_path = YaCriticalPath((entry,), (matching, disjoint))

    assert critical_path.match_build([matching], [entry]) == {0: entry}
    assert critical_path.match_build([disjoint], [entry]) == {}


def test_does_not_match_disjoint_test_node() -> None:
    entry = YaCriticalPathEntry.from_raw(
        0,
        {"type": "TM", "start_ts": 1, "end_ts": 2},
    )
    node = YaNode(
        "Run(test)",
        "TM",
        Interval(Ns(3_000_000), Ns(4_000_000)),
        "test_execute",
        "TM",
    )
    critical_path = YaCriticalPath((entry,), (node,))

    assert critical_path.match_test_node(entry, [node], {}) is None


def test_does_not_mark_disjoint_chunk() -> None:
    entry = YaCriticalPathEntry.from_raw(
        0,
        {"type": "TM", "start_ts": 1, "end_ts": 2},
    )
    chunk = make_span(
        trace_id=b"\x01" * 16,
        span_id=b"\x02" * 8,
        name="chunk",
        start_ns=Ns(3_000_000),
        end_ns=Ns(4_000_000),
    )
    trace = Trace()
    trace.add_span(chunk, resource=ResourceAttributes(), scope_name="ya.chunk")

    metrics = YaCriticalPath((entry,), ()).mark_test_spans(trace)

    assert metrics["ya.test.critical_path.chunk.count"] == 0
