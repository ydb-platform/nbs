from scripts.tracing.yatrace.test_timing import YaTestTiming
from scripts.tracing.yatrace_test_support import (
    Interval,
    Ns,
    Path,
    YaEvent,
    _attributes,
    _chunk,
    _render_ya_trace,
    _subtest,
    pytest,
    span_duration_ns,
    span_status_code,
)


def test_test_timing_preserves_explicit_zero_event_timestamp() -> None:
    start = YaEvent("subtest-started", Ns(0), {}, 0)
    finish = YaEvent("subtest-finished", Ns(0), {"time": 5}, 1)

    timing = YaTestTiming.resolve(
        start,
        finish,
        chunk=Interval(Ns(0), Ns.from_s_or_zero(10)),
        inferred_start=None,
        only_test=False,
    )

    assert timing is not None
    assert timing.interval == Interval(Ns(0), Ns(0))
    assert timing.inferred is False


def test_finish_only_test_clamps_end_before_applying_duration(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "chunk-event",
                "timestamp": 110.9,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "metrics": {
                        "suite_start_timestamp": 100,
                        "suite_finish_timestamp": 110,
                    },
                },
            },
            {
                "name": "subtest-finished",
                "timestamp": 110.9,
                "value": {
                    "class": "Suite",
                    "subtest": "finish_only",
                    "status": "good",
                    "time": 0.5,
                    "chunk_index": 0,
                    "nchunks": 1,
                },
            },
        ],
        root_start_s=99,
        root_end_s=112,
    )

    test = next(trace.spans("ya.test"))
    assert test.start_time_unix_nano == 109_500_000_000
    assert test.end_time_unix_nano == 110_000_000_000
    assert span_duration_ns(test) == 500_000_000


def test_test_timing_preserves_observed_inferred_and_incomplete_chunks(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            _subtest("observed", timestamp=101, chunk=(0, 4), started=True),
            _subtest("observed", timestamp=102.5, chunk=(0, 4), duration=1.5),
            _chunk(
                0,
                4,
                timestamp=105,
                metrics={
                    "suite_start_timestamp": 100,
                    "suite_finish_timestamp": 105,
                },
            ),
            _subtest("inferred", timestamp=120, chunk=(1, 4), duration=1),
            _chunk(
                1,
                4,
                timestamp=120,
                metrics={
                    "suite_finish_timestamp": 112,
                    "wall_time": 5,
                    "suite_delay_until_first_test_secs": 1,
                },
            ),
            _subtest("inferred_other", timestamp=120, chunk=(2, 4), duration=3),
            _chunk(
                2,
                4,
                timestamp=120,
                metrics={
                    "suite_finish_timestamp": 110,
                    "wall_time": 10,
                    "suite_delay_until_first_test_secs": 2,
                },
            ),
            _subtest("incomplete", timestamp=115, chunk=(3, 4), started=True),
        ],
        root_start_s=99,
        root_end_s=121,
        exit_code=1,
    )

    root = next(trace.spans("ya"))
    chunk_spans = list(trace.spans("ya.chunk"))
    chunks = {
        attributes["ya.chunk.chunk_index"]: span
        for span in chunk_spans
        if "ya.chunk.chunk_index" in (attributes := _attributes(span))
    }
    tests = {span.name: span for span in trace.spans("ya.test")}
    observed = tests["Suite::observed"]
    inferred = tests["Suite::inferred"]
    inferred_other = tests["Suite::inferred_other"]
    incomplete = tests["Suite::incomplete"]

    assert root.name == "ya make tests"
    assert span_status_code(root) == 2
    assert _attributes(root)["ya.chunk.count"] == 4
    assert span_duration_ns(observed) == Ns.from_s(1.5)
    assert "test.timing.inferred" not in _attributes(observed)
    assert inferred.start_time_unix_nano == Ns.from_s(108)
    assert span_duration_ns(inferred) == Ns.from_s(1)
    assert (
        _attributes(inferred)["test.timing.source"] == "chunk-delay-and-test-duration"
    )
    assert inferred_other.start_time_unix_nano == Ns.from_s(102)
    assert span_duration_ns(inferred_other) == Ns.from_s(3)
    assert incomplete.end_time_unix_nano == Ns.from_s(121)
    assert span_status_code(incomplete) == 2
    assert _attributes(incomplete)["test.incomplete"] is True
    assert observed.parent_span_id == chunks[0].span_id
    assert inferred.parent_span_id == chunks[1].span_id
    assert inferred_other.parent_span_id == chunks[2].span_id
    assert incomplete.parent_span_id in {span.span_id for span in chunk_spans}


def test_chunk_event_alias_and_real_error_schema_mark_chunk_failed(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "chunk_event",
                "timestamp": 5,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "errors": [["timeout", "test wrapper timed out"]],
                },
            }
        ],
        root_end_s=10,
        exit_code=1,
    )

    chunk = next(trace.spans("ya.chunk"))
    assert span_status_code(chunk) == 2
    assert _attributes(chunk)["ya.chunk.error.count"] == 1
    assert _attributes(chunk)["ya.chunk.error.statuses"] == ["timeout"]
    assert _attributes(chunk)["ya.chunk.error.messages"] == ["test wrapper timed out"]


def test_suite_error_has_localized_span_without_inventing_chunk(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "suite-event",
                "timestamp": 5,
                "value": {
                    "errors": [["fail", "recipe setup failed"]],
                },
            }
        ],
        root_end_s=10,
        exit_code=1,
    )

    assert not list(trace.spans("ya.chunk"))
    suite = next(trace.spans("ya.suite"))
    assert suite.start_time_unix_nano == 5_000_000_000
    assert suite.end_time_unix_nano == 5_000_000_000
    assert span_status_code(suite) == 2
    assert _attributes(suite)["ya.suite.error.count"] == 1
    assert _attributes(suite)["ya.suite.error.messages"] == ["recipe setup failed"]


@pytest.mark.parametrize(
    "status",
    ["good", "skipped", "xfail", "xpass", "deselected"],
)
def test_container_info_is_preserved_without_marking_span_failed(
    tmp_path: Path,
    status: str,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "chunk-event",
                "timestamp": 5,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "errors": [[status, "diagnostic information"]],
                },
            }
        ],
        root_end_s=10,
    )

    chunk = next(trace.spans("ya.chunk"))
    assert span_status_code(chunk) == 0
    assert _attributes(chunk)["ya.chunk.error.messages"] == ["diagnostic information"]


def test_invented_chunk_status_field_is_ignored(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "chunk-event",
                "timestamp": 5,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "status": "fail",
                },
            }
        ],
        root_end_s=10,
    )

    chunk = next(trace.spans("ya.chunk"))
    assert span_status_code(chunk) == 0
    assert "ya.chunk.status" not in _attributes(chunk)


def test_chunk_errors_take_priority_over_large_metric_sets(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "chunk-event",
                "timestamp": 5,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "errors": [["timeout", "wrapper timed out"]],
                    "metrics": {f"metric_{index}": index for index in range(256)},
                },
            }
        ],
        root_end_s=10,
        exit_code=1,
    )

    chunk = next(trace.spans("ya.chunk"))
    assert span_status_code(chunk) == 2
    assert _attributes(chunk)["ya.chunk.error.messages"] == ["wrapper timed out"]


@pytest.mark.parametrize("status", ["missing", "flaky", "diff"])
def test_yatool_failure_status_marks_test_and_chunk_failed(
    tmp_path: Path,
    status: str,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "subtest-finished",
                "timestamp": 5,
                "value": {
                    "class": "Suite",
                    "subtest": status,
                    "status": status,
                    "time": 1,
                },
            },
            {
                "name": "chunk-event",
                "timestamp": 6,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                },
            },
        ],
        root_end_s=10,
        exit_code=1,
    )

    assert span_status_code(next(trace.spans("ya.test"))) == 2
    assert span_status_code(next(trace.spans("ya.chunk"))) == 2


@pytest.mark.parametrize("status", ["xfail", "xpass"])
def test_yatool_expected_status_marks_test_ok(
    tmp_path: Path,
    status: str,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "subtest-finished",
                "timestamp": 5,
                "value": {
                    "class": "Suite",
                    "subtest": status,
                    "status": status,
                    "time": 1,
                },
            }
        ],
        root_end_s=10,
    )

    assert span_status_code(next(trace.spans("ya.test"))) == 1


def test_yatool_internal_xfaildiff_status_marks_test_failed(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "subtest-finished",
                "timestamp": 5,
                "value": {
                    "class": "Suite",
                    "subtest": "xfaildiff",
                    "status": "xfaildiff",
                    "time": 1,
                },
            }
        ],
        root_end_s=10,
        exit_code=1,
    )

    assert span_status_code(next(trace.spans("ya.test"))) == 2
    assert span_status_code(next(trace.spans("ya.chunk"))) == 2
