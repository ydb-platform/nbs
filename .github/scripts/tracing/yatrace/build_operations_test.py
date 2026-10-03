import pytest

from scripts.tracing.otlp import (
    Interval,
    Ns,
    ResourceAttributes,
    decode_attributes,
    span_status_code,
    stable_span_id,
)

from . import limits
from .build_operations import BuildNodeSelection, YaBuildOperations
from .critical_path import YaCriticalPath, YaCriticalPathEntry
from .evlog_data import YaEvlog
from .node import YaNode
from .projection import build_ya_trace
from .statistics import YaBuildStatistics


def _node(
    uid: str,
    start: int = 0,
    end: int = 10,
    *,
    kind: str = "execute",
    details: tuple[YaNode, ...] = (),
) -> YaNode:
    return YaNode(
        name=f"Run({uid})",
        tag="CC",
        interval=Interval(Ns(start), Ns(end)),
        kind=kind,
        tool="CC",
        uid=uid,
        details=list(details),
    )


def _detail(start: int, end: int, tag: str = "exec_cmd") -> YaNode:
    return YaNode(tag, tag, Interval(Ns(start), Ns(end)), "execute", "")


def _operations(nodes, failures) -> YaBuildOperations:
    return YaBuildOperations(
        nodes,
        failures,
        YaBuildStatistics.from_raw(),
        YaCriticalPath.from_evlog({}, nodes),
        [],
    )


@pytest.mark.parametrize(
    ("candidates", "expected"),
    [
        ([("cache_restore", 0, 100), ("execute", 1, 2)], 1),
        ([("execute", 0, 10), ("execute", 19, 20)], 0),
        ([("execute", 1, 4), ("execute", 3, 6)], 1),
        ([("execute", 1, 4), ("execute", 1, 4)], 0),
        ([("cache_restore", 0, 10), ("materialize", 19, 20)], 0),
    ],
    ids=["execute", "duration", "latest-end", "first-tie", "without-execute"],
)
def test_failure_representative_priority(candidates, expected) -> None:
    nodes = [_node("failed", start, end, kind=kind) for kind, start, end in candidates]

    assert _operations(nodes, {"failed": 1})._failures(nodes) == (
        {"failed"},
        {expected},
    )


def test_failure_representatives_group_only_present_nonempty_failed_uids() -> None:
    nodes = [
        _node("first", kind="cache_restore"),
        _node("second"),
        _node("first", 1, 2),
        _node("healthy"),
        _node(""),
    ]
    failures = {"first": 1, "second": None, "missing": 2, "": 17}

    assert _operations(nodes, failures)._failures(nodes) == (
        {"first", "second"},
        {1, 2},
    )
    assert _operations(nodes, {})._failures(nodes) == (set(), set())
    assert _operations([], failures)._failures([]) == (set(), set())


def test_failure_representatives_scan_nodes_once() -> None:
    class CountingNodes(list):
        visits = 0

        def __iter__(self):
            for node in super().__iter__():
                self.visits += 1
                yield node

    nodes = CountingNodes(_node(f"uid{index // 2}") for index in range(128))
    failures = {f"uid{index}": 1 for index in range(64)}

    assert _operations(nodes, failures)._failures(nodes) == (
        set(failures),
        set(range(0, 128, 2)),
    )
    assert nodes.visits == len(nodes)


@pytest.mark.parametrize(
    ("limit", "expected"),
    [
        (10, BuildNodeSelection([0, 1, 2, 4, 5])),
        (4, BuildNodeSelection([4, 2, 5, 1], dropped=1)),
        (2, BuildNodeSelection([4, 2], dropped=3, critical_dropped=2)),
        (
            1,
            BuildNodeSelection([4], dropped=4, critical_dropped=2, failed_dropped=1),
        ),
        (
            0,
            BuildNodeSelection([], dropped=5, critical_dropped=3, failed_dropped=2),
        ),
    ],
)
def test_node_selection_preserves_priorities_and_drop_counts(
    monkeypatch, limit, expected
) -> None:
    monkeypatch.setattr(limits, "MAX_BUILD_NODE_SPANS", limit)
    nodes = [
        _node("ordinary", end=100),
        _node("critical-later"),
        _node("failed"),
        _node("cache", kind="cache_store"),
        _node("failed-critical", kind="cache_store"),
        _node("critical-earlier"),
    ]
    critical = {
        index: YaCriticalPathEntry.from_raw(order, {"type": "CC"})
        for order, index in enumerate((5, 4, 1))
    }

    assert YaBuildOperations._select_nodes(nodes, critical, {2, 4}) == expected


def test_command_selection_preserves_priority_order_indices_and_total(
    monkeypatch,
) -> None:
    monkeypatch.setattr(limits, "MAX_BUILD_COMMAND_SPANS", 3)
    nodes = [
        _node("ordinary", details=(_detail(0, 10),)),
        _node("critical", details=(_detail(2, 3),)),
        _node(
            "failed",
            details=(
                _detail(0, 1, "setup"),
                _detail(7, 9),
                _detail(4, 6),
                _detail(1, 2),
            ),
        ),
        _node("unselected", details=(_detail(0, 10),)),
    ]
    critical = {1: YaCriticalPathEntry.from_raw(0, {"type": "CC"})}

    commands, total = YaBuildOperations._select_commands(
        nodes, [0, 1, 2], critical, {2}
    )

    assert total == 6
    assert [(command.node_index, command.detail_index) for command in commands] == [
        (2, 3),
        (2, 2),
        (2, 1),
    ]
    assert all(
        command.detail is nodes[command.node_index].details[command.detail_index]
        for command in commands
    )

    commands, total = YaBuildOperations._select_commands(
        nodes, [0, 1, 2], critical, set()
    )

    assert total == 6
    assert [(command.node_index, command.detail_index) for command in commands] == [
        (0, 0),
        (1, 0),
        (2, 2),
    ]


def test_projected_commands_keep_parent_failure_marker_and_stable_id() -> None:
    nodes = [
        _node(
            "failed",
            details=(_detail(0, 1, "setup"), _detail(3, 5), _detail(1, 2)),
        )
    ]
    trace = build_ya_trace(
        [],
        root_start_ns=Ns(0),
        root_end_ns=Ns(10),
        exit_code=1,
        resource=ResourceAttributes(),
        evlog=YaEvlog.from_raw(nodes=nodes, failures={"failed": 17}),
    )

    parent = next(trace.spans("ya.build.node"))
    assert span_status_code(parent) == 2
    commands = list(trace.spans("ya.build.command"))
    assert len(commands) == 2
    assert [span.start_time_unix_nano for span in commands] == [1, 3]
    for span, detail_index in zip(commands, (2, 1)):
        detail = nodes[0].details[detail_index]
        assert span.parent_span_id == parent.span_id
        assert span_status_code(span) == 0
        attributes = decode_attributes(span.attributes)
        assert attributes["ya.build.node.uid"] == "failed"
        assert attributes["ya.build.node.failed"] is True
        assert span.span_id == stable_span_id(
            span.trace_id,
            "ya.build.command",
            parent.span_id,
            detail.start_ns,
            detail.end_ns,
            detail_index,
        )
