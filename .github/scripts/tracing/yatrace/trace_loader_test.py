import json
from pathlib import Path

from scripts.tracing.otlp import Ns
from scripts.tracing.yatrace.trace_loader import load_trace_files
from scripts.tracing.yatrace.trace_model import Chunk, SuiteTrace


def _load_trace(tmp_path: Path, events: list[dict]) -> SuiteTrace:
    path = tmp_path / "suite" / "test-results" / "unittest" / "ytest.report.trace"
    path.parent.mkdir(parents=True)
    path.write_text("".join(f"{json.dumps(event)}\n" for event in events))
    return load_trace_files(tmp_path, [path])[0]


def _chunk(
    index: int,
    total: int,
    timestamp: int,
    *,
    chunk_filename: str | None = None,
    **attributes: object,
) -> dict:
    return {
        "name": "chunk-event",
        "timestamp": timestamp,
        "value": {
            "chunk_index": index,
            "nchunks": total,
            **attributes,
            **(
                {"chunk_filename": chunk_filename} if chunk_filename is not None else {}
            ),
        },
    }


def _start(name: str, timestamp: int, **attributes: object) -> dict:
    return {
        "name": "subtest-started",
        "timestamp": timestamp,
        "value": {"class": "Case", "subtest": name, **attributes},
    }


def _finish(
    name: str,
    timestamp: int,
    *,
    status: str,
    chunk_index: int,
    nchunks: int,
    test_type: str = "unittest",
    path: str = "target/client_ut",
    duration: float = 1,
    chunk_filename: str | None = None,
) -> dict:
    return {
        "name": "subtest-finished",
        "timestamp": timestamp,
        "value": {
            "class": "Case",
            "subtest": name,
            "type": test_type,
            "path": path,
            "status": status,
            "time": duration,
            "chunk_index": chunk_index,
            "nchunks": nchunks,
            **(
                {"chunk_filename": chunk_filename} if chunk_filename is not None else {}
            ),
        },
    }


def _by_key(chunks: tuple[Chunk, ...]) -> dict[tuple[int, int] | None, Chunk]:
    return {chunk.key: chunk for chunk in chunks}


def test_identityless_starts_follow_finishes_to_their_chunks(
    tmp_path: Path,
) -> None:
    source = _load_trace(
        tmp_path,
        [
            _start("one", 1),
            _finish("one", 2, status="good", chunk_index=0, nchunks=2),
            _start("two", 3),
            _finish("two", 4, status="good", chunk_index=1, nchunks=2),
            _chunk(0, 2, 5),
            _chunk(1, 2, 6),
        ],
    )

    chunks = _by_key(source.chunks)
    assert set(chunks) == {(0, 2), (1, 2)}
    assert [attempt.start.name for attempt in chunks[(0, 2)].attempts] == ["one"]
    assert [attempt.finish.name for attempt in chunks[(0, 2)].attempts] == ["one"]
    assert [attempt.start.name for attempt in chunks[(1, 2)].attempts] == ["two"]
    assert [attempt.finish.name for attempt in chunks[(1, 2)].attempts] == ["two"]


def test_repeated_attempts_remain_distinct(tmp_path: Path) -> None:
    source = _load_trace(
        tmp_path,
        [
            _start("retry", 1),
            _finish("retry", 2, status="fail", chunk_index=0, nchunks=1),
            _start("retry", 3),
            _finish("retry", 4, status="good", chunk_index=0, nchunks=1),
            _chunk(0, 1, 5),
        ],
    )

    chunk = source.chunks[0]
    assert chunk.key == (0, 1)
    assert [attempt.finish.status for attempt in chunk.attempts] == ["fail", "good"]
    assert all(attempt.start is not None for attempt in chunk.attempts)


def test_out_of_order_finishes_match_known_chunk_identities(tmp_path: Path) -> None:
    source = _load_trace(
        tmp_path,
        [
            _start("same", 1, chunk_index=1, nchunks=2),
            _start("same", 2, chunk_index=0, nchunks=2),
            _finish("same", 3, status="good", chunk_index=0, nchunks=2),
            _finish("same", 4, status="good", chunk_index=1, nchunks=2),
            _chunk(0, 2, 5),
            _chunk(1, 2, 6),
        ],
    )

    chunks = _by_key(source.chunks)
    assert set(chunks) == {(0, 2), (1, 2)}
    assert chunks[(0, 2)].attempts[0].start.record.timestamp_ns == Ns.from_s(2)
    assert chunks[(0, 2)].attempts[0].finish.record.timestamp_ns == Ns.from_s(3)
    assert chunks[(1, 2)].attempts[0].start.record.timestamp_ns == Ns.from_s(1)
    assert chunks[(1, 2)].attempts[0].finish.record.timestamp_ns == Ns.from_s(4)


def test_missing_chunk_filename_is_a_wildcard_when_routing(tmp_path: Path) -> None:
    source = _load_trace(
        tmp_path,
        [
            _start("same", 1, chunk_index=0, nchunks=2),
            _finish(
                "same",
                2,
                status="good",
                chunk_index=0,
                nchunks=2,
                chunk_filename="chunk-0",
            ),
            _chunk(0, 2, 3, chunk_filename="chunk-0"),
            _chunk(1, 2, 4, chunk_filename="chunk-1"),
        ],
    )

    chunks = _by_key(source.chunks)
    assert None not in chunks
    assert len(chunks[(0, 2)].attempts) == 1
    assert chunks[(0, 2)].attempts[0].start.name == "same"
    assert chunks[(0, 2)].attempts[0].finish.name == "same"


def test_missing_chunk_filename_merges_chunk_snapshots(tmp_path: Path) -> None:
    source = _load_trace(
        tmp_path,
        [
            _chunk(0, 1, 1, metrics={"suite_start_timestamp": 1}),
            _chunk(
                0,
                1,
                2,
                chunk_filename="chunk-0",
                metrics={"suite_finish_timestamp": 2},
            ),
        ],
    )

    assert len(source.chunks) == 1
    chunk = source.chunks[0]
    assert chunk.key == (0, 1)
    assert chunk.filename == "chunk-0"
    assert chunk.record is not None
    assert chunk.record.metrics == {
        "suite_start_timestamp": 1,
        "suite_finish_timestamp": 2,
    }
    assert source.matching_chunks(0, 1) == [chunk]


def test_missing_chunk_filename_does_not_bridge_conflicting_filenames(
    tmp_path: Path,
) -> None:
    source = _load_trace(
        tmp_path,
        [
            _chunk(0, 1, 1),
            _chunk(0, 1, 2, chunk_filename="first"),
            _chunk(0, 1, 3, chunk_filename="second"),
        ],
    )

    assert sorted(chunk.filename for chunk in source.chunks) == [
        "",
        "first",
        "second",
    ]


def test_out_of_order_finishes_match_known_type_and_path(tmp_path: Path) -> None:
    source = _load_trace(
        tmp_path,
        [
            _start("same", 1, type="pytest", path="target/py_test"),
            _start("same", 2, type="unittest", path="target/client_ut"),
            _finish("same", 3, status="good", chunk_index=0, nchunks=1),
            _finish(
                "same",
                4,
                status="good",
                chunk_index=0,
                nchunks=1,
                test_type="pytest",
                path="target/py_test",
            ),
            _chunk(0, 1, 5),
        ],
    )

    attempts = source.chunks[0].attempts
    assert len(attempts) == 2
    assert all(attempt.start.key == attempt.finish.key for attempt in attempts)


def test_incompatible_type_and_path_are_not_materialized_as_one_attempt(
    tmp_path: Path,
) -> None:
    source = _load_trace(
        tmp_path,
        [
            _start(
                "same",
                1,
                chunk_index=0,
                nchunks=1,
                type="pytest",
                path="target/py_test",
            ),
            _finish("same", 2, status="good", chunk_index=0, nchunks=1),
            _chunk(0, 1, 3),
        ],
    )

    chunk = source.chunks[0]
    assert chunk.key == (0, 1)
    assert len(chunk.attempts) == 2
    assert sum(attempt.start is not None for attempt in chunk.attempts) == 1
    assert sum(attempt.finish is not None for attempt in chunk.attempts) == 1


def test_identityless_concurrent_repeats_use_finish_duration(tmp_path: Path) -> None:
    source = _load_trace(
        tmp_path,
        [
            _start("same", 1),
            _start("same", 8),
            _finish(
                "same",
                9,
                status="good",
                chunk_index=0,
                nchunks=1,
                duration=1,
            ),
            _finish(
                "same",
                11,
                status="good",
                chunk_index=0,
                nchunks=1,
                duration=10,
            ),
            _chunk(0, 1, 12),
        ],
    )

    attempts = source.chunks[0].attempts
    pairs = {
        (attempt.start.record.timestamp_ns, attempt.finish.record.timestamp_ns)
        for attempt in attempts
    }
    assert pairs == {
        (Ns.from_s(8), Ns.from_s(9)),
        (Ns.from_s(1), Ns.from_s(11)),
    }


def test_deselected_finish_uses_its_own_chunk_identity(tmp_path: Path) -> None:
    source = _load_trace(
        tmp_path,
        [
            _start("same", 1, chunk_index=0, nchunks=2),
            _finish("same", 2, status="DESELECTED", chunk_index=1, nchunks=2),
            _chunk(0, 2, 3),
            _chunk(1, 2, 4),
        ],
    )

    chunks = _by_key(source.chunks)
    assert None not in chunks
    assert len(chunks[(0, 2)].attempts) == 1
    assert chunks[(0, 2)].attempts[0].start.name == "same"
    assert chunks[(0, 2)].attempts[0].finish is None
    assert len(chunks[(1, 2)].attempts) == 1
    assert chunks[(1, 2)].attempts[0].start is None
    assert chunks[(1, 2)].attempts[0].finish.status == "deselected"


def test_finish_only_status_snapshots_remain_one_attempt(tmp_path: Path) -> None:
    source = _load_trace(
        tmp_path,
        [
            _finish("same", 1, status="deselected", chunk_index=0, nchunks=1),
            _finish("same", 2, status="good", chunk_index=0, nchunks=1),
            _chunk(0, 1, 3),
        ],
    )

    attempts = source.chunks[0].attempts
    assert len(attempts) == 1
    assert attempts[0].start is None
    assert attempts[0].finish.status == "good"


def test_finish_only_deselected_and_ambiguous_events_are_preserved(
    tmp_path: Path,
) -> None:
    source = _load_trace(
        tmp_path,
        [
            _finish("shared", 1, status="good", chunk_index=0, nchunks=2),
            _finish("shared", 2, status="good", chunk_index=1, nchunks=2),
            _finish("finish-only", 3, status="good", chunk_index=0, nchunks=2),
            _start("deselected", 4),
            _finish("deselected", 5, status="deselected", chunk_index=1, nchunks=2),
            _start("start-only", 6),
            _start("conflicting", 7, chunk_index=0, nchunks=2),
            _finish("conflicting", 8, status="good", chunk_index=1, nchunks=2),
            _chunk(0, 2, 9),
            _chunk(1, 2, 10),
        ],
    )

    chunks = _by_key(source.chunks)
    chunk_zero = {attempt.finish.name: attempt for attempt in chunks[(0, 2)].attempts}
    assert set(chunk_zero) == {"finish-only", "shared"}
    assert all(attempt.start is None for attempt in chunk_zero.values())

    chunk_one = {attempt.finish.name: attempt for attempt in chunks[(1, 2)].attempts}
    assert set(chunk_one) == {"deselected", "shared"}
    assert all(attempt.start is None for attempt in chunk_one.values())
    assert chunk_one["deselected"].finish.status == "deselected"

    unassigned = chunks[None].attempts
    assert {
        attempt.start.name for attempt in unassigned if attempt.start is not None
    } == {
        "conflicting",
        "start-only",
    }
    assert {
        attempt.finish.name for attempt in unassigned if attempt.finish is not None
    } == {"conflicting"}
