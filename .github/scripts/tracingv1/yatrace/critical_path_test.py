from pathlib import Path

import pytest

from scripts.tracingv1.yatrace_test_support import (
    ClassifiedNode,
    Interval,
    Ns,
    TestChunk,
    YaCriticalPathEntry,
    YaEvlog,
    YaEvlogRecord,
    _attributes,
    _render_ya_trace,
    _stage,
    _statistics,
    _worker,
    _write_jsonl,
    load_ya_evlog,
    make_span,
    yatrace_limits,
)


@pytest.mark.parametrize(
    ("task_type", "base_type"),
    [
        ("TA", "TA"),
        ("TL", "TL"),
        ("TL-CACHED", "TL"),
        ("TS-DYN_UID_CACHE", "TS"),
        ("TM-CACHED-DYN_UID_CACHE", "TM"),
    ],
)
def test_all_yatool_test_types_are_recognized_on_critical_path(
    task_type: str,
    base_type: str,
) -> None:
    entry = YaCriticalPathEntry.from_raw(3, {"type": task_type})

    assert entry.index == 3
    assert entry.raw_type == task_type
    assert entry.base_type == base_type
    assert entry.is_test


def test_critical_path_entry_parses_timing_and_attributes() -> None:
    entry = YaCriticalPathEntry.from_raw(
        7,
        {
            "type": "CC-CACHED",
            "text": "compile source.cpp",
            "uid": "compile",
            "elapsed": 1_500,
            "start_ts": 2_000,
            "end_ts": 3_500,
        },
    )

    assert entry.base_type == "CC"
    assert entry.text == "compile source.cpp"
    assert entry.uid == "compile"
    assert entry.elapsed_ms == 1_500
    assert entry.start_ms == 2_000
    assert entry.end_ms == 3_500
    assert entry.interval == Interval(
        Ns(2_000_000_000),
        Ns(3_500_000_000),
    )
    assert entry.span_attributes(test=False) == {
        "ya.build.critical_path": True,
        "ya.build.critical_path.index": 7,
        "ya.build.critical_path.reported_seconds": 1.5,
    }


def test_evlog_finalizes_statistics_and_critical_path() -> None:
    raw_statistics = {"critical_path": [{"type": "CC"}]}
    evlog = YaEvlog.from_raw(
        stages=[],
        nodes=[],
        statistics=raw_statistics,
    )

    raw_statistics["critical_path"] = [{"type": "LD"}]

    assert evlog.statistics.values["critical_path"][0]["type"] == "CC"
    assert evlog.critical_path.entries[0].raw_type == "CC"


def test_classified_node_owns_record_classification() -> None:
    record = YaEvlogRecord(
        name="Run(uid$(BUILD_ROOT)/obj.o)",
        tag="CC",
        start_ns=Ns(1),
        end_ns=Ns(4),
    )

    node = ClassifiedNode.from_record(record)
    clipped = node.clipped(Interval(Ns(2), Ns(3)))

    assert (node.kind, node.tool) == ("execute", "CC")
    assert node.uid == "uid"
    assert clipped is not None
    assert clipped.interval == Interval(Ns(2), Ns(3))
    assert (clipped.kind, clipped.tool) == (node.kind, node.tool)


def test_test_chunk_decodes_span_identity_and_timing() -> None:
    span = make_span(
        trace_id=b"t" * 16,
        span_id=b"s" * 8,
        parent_span_id=b"p" * 8,
        name="suite [chunk 2/3]",
        start_ns=Ns(10),
        end_ns=Ns(20),
        attributes={
            "test.suite": "suite",
            "ya.test_results.folder": "results",
            "ya.chunk.chunk_index": 1,
        },
    )

    chunk = TestChunk.from_span(span)

    assert chunk.identity == ("suite", "results")
    assert chunk.chunk_index == 1
    assert chunk.interval == Interval(Ns(10), Ns(20))
    assert chunk.overlap(Interval(Ns(15), Ns(25))) == 5


def test_test_critical_path_uid_matches_when_interval_is_malformed() -> None:
    short = YaEvlogRecord(
        name="Run(shared$(BUILD_ROOT)/suite/test-results/unit/short)",
        tag="TM",
        start_ns=Ns(1),
        end_ns=Ns(2),
    )
    long = YaEvlogRecord(
        name="Run(shared$(BUILD_ROOT)/suite/test-results/unit/long)",
        tag="TM",
        start_ns=Ns(3),
        end_ns=Ns(6),
    )
    entry = YaCriticalPathEntry.from_raw(
        0,
        {
            "type": "TM",
            "uid": "shared",
            "start_ts": 2,
            "end_ts": 1,
        },
    )
    evlog = YaEvlog.from_raw(nodes=[short, long])

    assert entry.interval is None
    assert (
        evlog.critical_path.match_test_node(
            entry,
            [short, long],
            {"shared": [short, long]},
        )
        is long
    )


def test_critical_path_keeps_all_entries_from_evlog(tmp_path: Path) -> None:
    evlog_path = tmp_path / "ya_evlog.jsonl"
    critical_path = [
        {
            "type": "CC",
            "uid": f"uid-{index}",
            "elapsed": 1,
            "start_ts": index,
            "end_ts": index + 1,
        }
        for index in range(129)
    ]
    critical_path.append(
        {
            "type": "TL",
            "uid": "terminal-test",
            "elapsed": 1,
            "start_ts": 129,
            "end_ts": 130,
        }
    )
    _write_jsonl(
        evlog_path,
        [_statistics({"critical_path": critical_path})],
    )

    assert len(load_ya_evlog(evlog_path).critical_path.entries) == 130


def test_critical_path_entry_after_128_is_rendered(tmp_path: Path) -> None:
    critical_path = [
        {
            "type": "CC",
            "uid": f"unmatched-{index}",
            "elapsed": 1,
            "start_ts": 0,
            "end_ts": 1,
        }
        for index in range(129)
    ]
    critical_path.append(
        {
            "type": "CC",
            "uid": "terminal-build",
            "elapsed": 1_000,
            "start_ts": 2_000,
            "end_ts": 3_000,
        }
    )
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 1, 5),
            _worker(
                "Run(terminal-build$(BUILD_ROOT)/terminal.o)",
                "CC",
                2,
                3,
            ),
            _statistics({"critical_path": critical_path}),
        ],
        root_end_s=6,
    )

    node_attributes = _attributes(next(trace.spans("ya.build.node")))
    assert node_attributes["ya.build.critical_path"] is True
    assert node_attributes["ya.build.critical_path.index"] == 129


def test_critical_path_preserves_indices_and_matches_duplicate_uids(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 0, 10),
            _worker("Run(shared$(BUILD_ROOT)/first.o)", "CC", 1, 2),
            _worker("Run(shared$(BUILD_ROOT)/second.o)", "LD", 3, 5),
            _statistics(
                {
                    "critical_path": [
                        {
                            "type": "TM",
                            "uid": "test",
                            "elapsed": 1_000,
                            "start_ts": 0,
                            "end_ts": 1_000,
                        },
                        {
                            "type": "CC",
                            "uid": "shared",
                            "elapsed": 1_000,
                            "start_ts": 1_000,
                            "end_ts": 2_000,
                        },
                        {
                            "type": "LD",
                            "uid": "shared",
                            "elapsed": 2_000,
                            "start_ts": 3_000,
                            "end_ts": 5_000,
                        },
                    ]
                }
            ),
        ],
        root_end_s=11,
    )

    nodes = {
        _attributes(span)["ya.build.outputs"][0]: _attributes(span)
        for span in trace.spans("ya.build.node")
    }
    assert nodes["first.o"]["ya.build.critical_path.index"] == 1
    assert nodes["first.o"]["ya.build.critical_path.reported_seconds"] == 1
    assert nodes["second.o"]["ya.build.critical_path.index"] == 2
    assert nodes["second.o"]["ya.build.critical_path.reported_seconds"] == 2


def test_critical_path_node_is_protected_from_span_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(yatrace_limits, "MAX_BUILD_NODE_SPANS", 1)
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 0, 10),
            _worker("Run(critical$(BUILD_ROOT)/critical.o)", "CC", 1, 1.1),
            _worker("Run(long$(BUILD_ROOT)/long.o)", "CC", 2, 8),
            _statistics(
                {
                    "critical_path": [
                        {
                            "type": "CC",
                            "uid": "critical",
                            "elapsed": 100,
                            "start_ts": 1_000,
                            "end_ts": 1_100,
                        }
                    ]
                }
            ),
        ],
        root_end_s=11,
    )

    node = next(trace.spans("ya.build.node"))
    assert _attributes(node)["ya.build.outputs"] == ["critical.o"]
    assert _attributes(node)["ya.build.critical_path"] is True


def test_one_critical_entry_marks_only_one_matching_worker_operation(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 0, 10),
            _worker(
                "FromCache(shared$(BUILD_ROOT)/cached.o)",
                "restore[CC]",
                1,
                2,
            ),
            _worker("Run(shared$(BUILD_ROOT)/built.o)", "CC", 3, 5),
            _statistics(
                {
                    "critical_path": [
                        {
                            "type": "CC",
                            "uid": "shared",
                            "elapsed": 2_000,
                            "start_ts": 3_000,
                            "end_ts": 5_000,
                        }
                    ]
                }
            ),
        ],
        root_end_s=11,
    )

    critical_nodes = [
        span
        for span in trace.spans("ya.build.node")
        if _attributes(span).get("ya.build.critical_path")
    ]
    assert len(critical_nodes) == 1
    assert _attributes(critical_nodes[0])["ya.build.outputs"] == ["built.o"]


def test_critical_entry_without_uid_matches_worker_by_time_and_type(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 0, 10),
            _worker("Run(worker$(BUILD_ROOT)/built.o)", "CC", 3, 5),
            _statistics(
                {
                    "critical_path": [
                        {
                            "type": "CC",
                            "elapsed": 2_000,
                            "start_ts": 3_000,
                            "end_ts": 5_000,
                        }
                    ]
                }
            ),
        ],
        root_end_s=11,
    )

    node = next(trace.spans("ya.build.node"))
    assert _attributes(node)["ya.build.critical_path"] is True


def test_build_node_span_limit_is_hard_for_critical_nodes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(yatrace_limits, "MAX_BUILD_NODE_SPANS", 1)
    evlog_events = [_stage("dispatch_build", 0, 10)]
    for index in range(2):
        evlog_events.append(
            _worker(
                f"Run(uid-{index}$(BUILD_ROOT)/{index}.o)",
                "CC",
                index + 1,
                index + 2,
            )
        )
    evlog_events.append(
        _statistics(
            {
                "critical_path": [
                    {
                        "type": "CC",
                        "uid": f"uid-{index}",
                        "elapsed": 1_000,
                        "start_ts": (index + 1) * 1_000,
                        "end_ts": (index + 2) * 1_000,
                    }
                    for index in range(2)
                ]
            }
        )
    )

    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=evlog_events,
        root_end_s=11,
    )

    assert len(list(trace.spans("ya.build.node"))) == 1
    build_attributes = _attributes(next(trace.spans("ya.build")))
    assert build_attributes["ya.build.critical_path.node_spans.dropped"] == 1
