from dataclasses import replace

import pytest

from scripts.tracing.otlp import Interval, Ns

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

    assert critical_path.match_tests([node]) == {}


def test_requires_a_worker_for_a_critical_test_entry() -> None:
    entry = YaCriticalPathEntry.from_raw(
        0,
        {"type": "TM", "start_ts": 1, "end_ts": 2},
    )
    assert YaCriticalPath((entry,), ()).match_tests([]) == {}


def test_missing_uid_does_not_fall_back_to_an_overlapping_worker() -> None:
    entry = YaCriticalPathEntry.from_raw(
        0,
        {"uid": "missing", "type": "TM", "start_ts": 1, "end_ts": 2},
    )
    node = YaNode("Run(other)", "TM", entry.interval, "test_execute", "TM", uid="other")

    assert YaCriticalPath((entry,), (node,)).match_tests([node]) == {}


@pytest.mark.parametrize("uid", ["", "uid"])
@pytest.mark.parametrize(
    "identity", [("other-suite", "unittest"), ("suite", "py3test"), None]
)
def test_explicit_identity_rejects_incompatible_workers(uid, identity) -> None:
    entry = YaCriticalPathEntry.from_raw(
        0,
        {
            "uid": uid,
            "type": "TM",
            "text": "suite/test-results/unittest/ytest.report.trace",
            "start_ts": 1,
            "end_ts": 2,
        },
    )
    node = YaNode(
        "Run(uid)",
        "TM",
        entry.interval,
        "test_execute",
        "TM",
        uid="uid",
        test_identity=identity,
    )

    assert YaCriticalPath((entry,), (node,)).match_tests([node]) == {}


@pytest.mark.parametrize(
    "text",
    [
        "suite/test-results/unittest/ytest.report.trace",
        "Run(uid$(BUILD_ROOT)/suite/test-results/unittest/ytest.report.trace)",
    ],
)
def test_identity_selects_the_compatible_overlapping_worker(text: str) -> None:
    entry = YaCriticalPathEntry.from_raw(
        0,
        {"type": "TM", "text": text, "start_ts": 1, "end_ts": 2},
    )
    other = YaNode(
        "Run(other)",
        "TM",
        entry.interval,
        "test_execute",
        "TM",
        test_identity=("other-suite", "unittest"),
    )
    matching = replace(other, test_identity=("suite", "unittest"))
    nodes = [other, matching]

    assert YaCriticalPath((entry,), nodes).match_tests(nodes) == {1: entry}


def test_same_uid_workers_are_ranked_by_timing() -> None:
    entry = YaCriticalPathEntry.from_raw(
        0,
        {"uid": "uid", "type": "TM", "start_ts": 1, "end_ts": 2},
    )
    matching = YaNode("Run(uid)", "TM", entry.interval, "test_execute", "TM", uid="uid")
    wider = replace(matching, interval=Interval(Ns(0), Ns(3_000_000)))
    nodes = [wider, matching]

    assert YaCriticalPath((entry,), nodes).match_tests(nodes) == {1: entry}


def test_uid_matches_without_critical_entry_timestamps() -> None:
    entry = YaCriticalPathEntry.from_raw(0, {"uid": "uid", "type": "TM"})
    node = YaNode(
        "Run(uid)",
        "TM",
        Interval(Ns(1_000_000), Ns(2_000_000)),
        "test_execute",
        "TM",
        uid="uid",
    )

    assert YaCriticalPath((entry,), (node,)).match_tests([node]) == {0: entry}
