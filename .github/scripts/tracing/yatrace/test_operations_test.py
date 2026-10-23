import json
from pathlib import Path

import pytest

from scripts.tracing.otlp import Ns, ResourceAttributes, Trace, decode_attributes

from .evlog_data import YaEvlog
from .node import YaNode
from .projection import build_ya_trace
from .trace_collection import YaTraceCollection


def _worker(
    uid: str,
    index: int = 0,
    *,
    suite: str = "suite",
    folder: str = "unittest",
) -> YaNode:
    node = YaNode.from_raw(
        {
            "name": (
                f"Run({uid}$(BUILD_ROOT)/{suite}/test-results/{folder}/"
                f"chunk{index}/ytest.report.trace)"
            ),
            "tag": "TM",
            "time": [1, 2],
        }
    )
    assert node is not None
    return node


def _project(
    tmp_path: Path,
    workers: list[YaNode],
    *,
    uid: str = "critical",
    text: str = "",
    indexes: tuple[int, ...] = (0,),
    chunk_start: int = 1,
    chunk_end: int = 2,
) -> Trace:
    trace_path = tmp_path / "suite/test-results/unittest/ytest.report.trace"
    trace_path.parent.mkdir(parents=True)
    events = []
    for index in indexes:
        chunk_key = {"chunk_index": index, "nchunks": 2}
        events.extend(
            [
                {
                    "name": "subtest-finished",
                    "timestamp": chunk_end,
                    "value": {
                        **chunk_key,
                        "class": "Suite",
                        "subtest": f"case{index}",
                        "status": "good",
                        "time": 0.1,
                    },
                },
                {
                    "name": "chunk-event",
                    "timestamp": chunk_end,
                    "value": {
                        **chunk_key,
                        "metrics": {
                            "suite_start_timestamp": chunk_start,
                            "suite_finish_timestamp": chunk_end,
                            "wall_time": chunk_end - chunk_start,
                        },
                    },
                },
            ]
        )
    trace_path.write_text("".join(json.dumps(event) + "\n" for event in events))
    evlog = YaEvlog.from_raw(
        nodes=workers,
        statistics={
            "critical_path": [
                {
                    "uid": uid,
                    "type": "TM",
                    "text": text,
                    "start_ts": 1_000,
                    "end_ts": 2_000,
                }
            ]
        },
    )
    return build_ya_trace(
        YaTraceCollection.load(tmp_path).traces,
        root_start_ns=Ns(0),
        root_end_ns=Ns(4_000_000_000),
        exit_code=0,
        resource=ResourceAttributes(),
        evlog=evlog,
    )


def _assert_no_critical_chunks(trace: Trace) -> None:
    root = decode_attributes(next(trace.spans("ya")).attributes)
    assert root["ya.test.critical_path.entry.count"] == 1
    assert root["ya.test.critical_path.chunk.count"] == 0
    assert root["ya.test.critical_path.span.count"] == 0
    for scope in ("ya.chunk", "ya.test"):
        assert all(
            "ya.test.critical_path" not in decode_attributes(span.attributes)
            for span in trace.spans(scope)
        )


@pytest.mark.parametrize("worker_order", [(0, 1), (1, 0)])
@pytest.mark.parametrize("chunk_order", [(0, 1), (1, 0)])
def test_critical_path_reuses_worker_chunk_associations(
    tmp_path: Path, worker_order, chunk_order
) -> None:
    trace = _project(
        tmp_path,
        [_worker(f"uid{index}", index) for index in worker_order],
        uid="uid1",
        indexes=chunk_order,
    )
    critical_chunk_ids = set()
    for chunk in trace.spans("ya.chunk"):
        attributes = decode_attributes(chunk.attributes)
        index = attributes["ya.chunk.chunk_index"]
        assert attributes["ya.test.worker.uid"] == f"uid{index}"
        assert attributes.get("ya.test.critical_path", False) == (index == 1)
        if index == 1:
            critical_chunk_ids.add(chunk.span_id)
    assert len(critical_chunk_ids) == 1
    for test in trace.spans("ya.test"):
        assert decode_attributes(test.attributes).get(
            "ya.test.critical_path", False
        ) == (test.parent_span_id in critical_chunk_ids)
    root = decode_attributes(next(trace.spans("ya")).attributes)
    assert root["ya.test.critical_path.entry.count"] == 1
    assert root["ya.test.critical_path.chunk.count"] == 1
    assert root["ya.test.critical_path.span.count"] == 1


@pytest.mark.parametrize(
    ("suite", "folder"),
    [("missing-suite", "unittest"), ("suite", "missing-folder")],
)
def test_unmatched_worker_does_not_mark_another_suite_or_folder(
    tmp_path: Path, suite: str, folder: str
) -> None:
    trace = _project(tmp_path, [_worker("critical", suite=suite, folder=folder)])

    _assert_no_critical_chunks(trace)


def test_missing_uid_does_not_mark_another_matched_worker(tmp_path: Path) -> None:
    trace = _project(tmp_path, [_worker("other")])

    _assert_no_critical_chunks(trace)


def test_missing_identity_does_not_mark_another_matched_worker(tmp_path: Path) -> None:
    trace = _project(
        tmp_path,
        [_worker("other")],
        uid="",
        text="missing-suite/test-results/unittest/ytest.report.trace",
    )

    _assert_no_critical_chunks(trace)


def test_no_worker_does_not_infer_a_critical_chunk_from_overlap(tmp_path: Path) -> None:
    trace = _project(tmp_path, [], uid="")

    _assert_no_critical_chunks(trace)


def test_exact_association_survives_nonoverlapping_reported_chunk_timing(
    tmp_path: Path,
) -> None:
    trace = _project(tmp_path, [_worker("critical")], chunk_start=2, chunk_end=3)

    chunk = next(trace.spans("ya.chunk"))
    attributes = decode_attributes(chunk.attributes)
    assert attributes["ya.test.worker.uid"] == "critical"
    assert attributes["ya.test.critical_path"] is True


def test_no_workers_or_chunks_preserves_critical_entry_count(tmp_path: Path) -> None:
    trace = _project(tmp_path, [], indexes=())

    _assert_no_critical_chunks(trace)
