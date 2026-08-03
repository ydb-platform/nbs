from scripts.tracing.yatrace_test_support import (
    Ns,
    Path,
    ResourceAttributes,
    YaTraceCollection,
    _attributes,
    _render_ya_trace,
    build_ya_trace,
    json,
    span_status_code,
)


def test_yatool_nul_padding_is_accepted(tmp_path: Path) -> None:
    ya_out = tmp_path / "out"
    trace_path = ya_out / "suite" / "test-results" / "unittest" / "ytest.report.trace"
    trace_path.parent.mkdir(parents=True)
    event = {
        "name": "subtest-finished",
        "timestamp": 5,
        "value": {
            "class": "Suite",
            "subtest": "nul-padded",
            "status": "good",
            "time": 1,
        },
    }
    trace_path.write_bytes(b"\0 " + json.dumps(event).encode() + b"\0\r\n")

    trace = build_ya_trace(
        YaTraceCollection.load(ya_out).traces,
        root_start_ns=Ns(0),
        root_end_ns=Ns.from_s(10),
        exit_code=0,
        resource=ResourceAttributes(),
    )

    assert [span.name for span in trace.spans("ya.test")] == ["Suite::nul-padded"]


def test_malformed_json_is_counted_and_marks_input_incomplete(
    tmp_path: Path,
) -> None:
    ya_out = tmp_path / "out"
    trace_path = ya_out / "suite" / "test-results" / "unittest" / "ytest.report.trace"
    trace_path.parent.mkdir(parents=True)
    trace_path.write_text(
        '{"name":"subtest-finished","timestamp":5,'
        '"value":{"class":"Suite","subtest":"valid","status":"good","time":1}}\n'
        '{"name":"subtest-finished",broken-json}\n'
    )

    trace = build_ya_trace(
        YaTraceCollection.load(ya_out).traces,
        root_start_ns=Ns(0),
        root_end_ns=Ns.from_s(10),
        exit_code=0,
        resource=ResourceAttributes(),
    )

    assert [span.name for span in trace.spans("ya.test")] == ["Suite::valid"]
    attributes = _attributes(next(trace.spans("ya")))
    assert attributes["ya.trace.malformed_json_record.count"] == 1
    assert attributes["ya.trace.input.incomplete"] is True


def test_incremental_snapshots_merge_chunks_and_tests(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "subtest-started",
                "timestamp": 2,
                "value": {
                    "class": "Suite",
                    "subtest": "test",
                    "chunk_index": 0,
                    "nchunks": 1,
                    "chunk_filename": "chunk-a",
                },
            },
            {
                "name": "subtest-finished",
                "timestamp": 3,
                "value": {
                    "class": "Suite",
                    "subtest": "test",
                    "status": "crashed",
                    "time": 1,
                    "chunk_index": 0,
                    "nchunks": 1,
                    "chunk_filename": "chunk-a",
                },
            },
            {
                "name": "chunk-event",
                "timestamp": 3,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "chunk_filename": "chunk-a",
                    "metrics": {"snapshot": 1},
                    "errors": [["timeout", "intermediate timeout"]],
                },
            },
            {
                "name": "subtest-finished",
                "timestamp": 4,
                "value": {
                    "class": "Suite",
                    "subtest": "test",
                    "status": "good",
                    "time": 2,
                    "chunk_index": 0,
                    "nchunks": 1,
                    "chunk_filename": "chunk-a",
                },
            },
            {
                "name": "subtest-finished",
                "timestamp": 5,
                "value": {
                    "class": "Suite",
                    "subtest": "test",
                    "status": "deselected",
                    "time": 0,
                    "chunk_index": 0,
                    "nchunks": 1,
                    "chunk_filename": "chunk-a",
                },
            },
            {
                "name": "chunk-event",
                "timestamp": 6,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "chunk_filename": "chunk-a",
                    "metrics": {"snapshot": 2},
                },
            },
        ],
        root_end_s=10,
    )

    chunks = list(trace.spans("ya.chunk"))
    tests = list(trace.spans("ya.test"))
    assert len(chunks) == 1
    assert len(tests) == 1
    assert _attributes(chunks[0])["ya.chunk.metric.snapshot"] == 2
    assert _attributes(chunks[0])["ya.chunk.error.messages"] == ["intermediate timeout"]
    assert span_status_code(chunks[0]) == 2
    assert tests[0].start_time_unix_nano == 2_000_000_000
    assert tests[0].end_time_unix_nano == 4_000_000_000
    assert span_status_code(tests[0]) == 1


def test_repeated_test_executions_create_separate_spans(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "subtest-started",
                "timestamp": 2,
                "value": {"class": "Suite", "subtest": "test"},
            },
            {
                "name": "subtest-finished",
                "timestamp": 3,
                "value": {
                    "class": "Suite",
                    "subtest": "test",
                    "status": "failed",
                    "time": 1,
                },
            },
            {
                "name": "subtest-started",
                "timestamp": 8,
                "value": {"class": "Suite", "subtest": "test"},
            },
            {
                "name": "subtest-finished",
                "timestamp": 9,
                "value": {
                    "class": "Suite",
                    "subtest": "test",
                    "status": "good",
                    "time": 1,
                },
            },
            {
                "name": "chunk-event",
                "timestamp": 10,
                "value": {"chunk_index": 0, "nchunks": 1},
            },
        ],
        root_end_s=10,
    )

    tests = list(trace.spans("ya.test"))
    assert [(test.start_time_unix_nano, test.end_time_unix_nano) for test in tests] == [
        (2_000_000_000, 3_000_000_000),
        (8_000_000_000, 9_000_000_000),
    ]
    assert [span_status_code(test) for test in tests] == [2, 1]


def test_chunk_filename_is_part_of_chunk_identity(tmp_path: Path) -> None:
    events = []
    for index, filename in enumerate(("first.cpp", "second.cpp"), 1):
        events.extend(
            [
                {
                    "name": "subtest-finished",
                    "timestamp": index,
                    "value": {
                        "class": "Suite",
                        "subtest": filename,
                        "status": "good",
                        "time": 0.1,
                        "chunk_index": 0,
                        "nchunks": 1,
                        "chunk_filename": filename,
                    },
                },
                {
                    "name": "chunk-event",
                    "timestamp": index,
                    "value": {
                        "chunk_index": 0,
                        "nchunks": 1,
                        "chunk_filename": filename,
                    },
                },
            ]
        )

    trace = _render_ya_trace(tmp_path, events, root_end_s=10)

    chunks = list(trace.spans("ya.chunk"))
    assert len(chunks) == 2
    assert {_attributes(chunk)["ya.chunk.filename"] for chunk in chunks} == {
        "first.cpp",
        "second.cpp",
    }
    tests = list(trace.spans("ya.test"))
    chunk_ids = {
        _attributes(chunk)["ya.chunk.filename"]: chunk.span_id for chunk in chunks
    }
    assert {
        _attributes(test)["test.name"]: test.parent_span_id for test in tests
    } == chunk_ids


def test_test_without_filename_matches_unique_chunk_by_index(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "subtest-finished",
                "timestamp": 3,
                "value": {
                    "class": "Suite",
                    "subtest": "test",
                    "status": "good",
                    "time": 1,
                    "chunk_index": 0,
                    "nchunks": 1,
                },
            },
            {
                "name": "chunk-event",
                "timestamp": 4,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "chunk_filename": "chunk.cpp",
                },
            },
        ],
        root_end_s=10,
    )

    chunks = list(trace.spans("ya.chunk"))
    tests = list(trace.spans("ya.test"))
    assert len(chunks) == 1
    assert len(tests) == 1
    assert _attributes(chunks[0])["ya.chunk.filename"] == "chunk.cpp"
    assert tests[0].parent_span_id == chunks[0].span_id


def test_test_identity_includes_type_and_path(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "subtest-finished",
                "timestamp": index + 1,
                "value": {
                    "class": "Suite",
                    "subtest": "same_name",
                    "status": "good",
                    "time": 0.1,
                    "type": test_type,
                    "path": path,
                },
            }
            for index, (test_type, path) in enumerate(
                (("unit", "first.cpp"), ("integration", "second.cpp"))
            )
        ]
        + [
            {
                "name": "chunk-event",
                "timestamp": 3,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                },
            }
        ],
        root_end_s=10,
    )

    tests = list(trace.spans("ya.test"))
    assert len(tests) == 2
    assert {
        (_attributes(test)["test.type"], _attributes(test)["test.path"])
        for test in tests
    } == {
        ("unit", "first.cpp"),
        ("integration", "second.cpp"),
    }


def test_final_deselected_result_does_not_reuse_started_timestamp(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "subtest-started",
                "timestamp": 2,
                "value": {
                    "class": "Suite",
                    "subtest": "deselected",
                },
            },
            {
                "name": "subtest-finished",
                "timestamp": 5,
                "value": {
                    "class": "Suite",
                    "subtest": "deselected",
                    "status": "deselected",
                    "time": 0,
                },
            },
            {
                "name": "chunk-event",
                "timestamp": 6,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "metrics": {
                        "suite_start_timestamp": 1,
                        "suite_finish_timestamp": 6,
                        "suite_delay_until_first_test_secs": 2,
                    },
                },
            },
        ],
        root_end_s=10,
    )

    test = next(trace.spans("ya.test"))
    assert test.start_time_unix_nano == 5_000_000_000
    assert test.end_time_unix_nano == 5_000_000_000


def test_first_test_delay_excludes_binary_startup_and_uses_test_metric_prefix(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "chunk-event",
                "timestamp": 110,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "metrics": {
                        "suite_start_timestamp": 100,
                        "suite_finish_timestamp": 110,
                        "suite_delay_until_first_test_secs": 2,
                        "suite_binary_startup_secs": 1.5,
                    },
                },
            },
            {
                "name": "subtest-finished",
                "timestamp": 110,
                "value": {
                    "class": "Suite",
                    "subtest": "test",
                    "status": "good",
                    "time": 1,
                    "metrics": {"elapsed_time": 1},
                },
            },
        ],
        root_start_s=99,
        root_end_s=111,
    )

    test = next(trace.spans("ya.test"))
    assert test.start_time_unix_nano == 100_500_000_000
    test_attributes = _attributes(test)
    assert test_attributes["ya.test.metric.elapsed_time"] == 1
    assert "ya.chunk.metric.elapsed_time" not in test_attributes


def test_chunk_uses_precise_event_timestamp_when_consistent_with_metrics(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "chunk-event",
                "timestamp": 110.75,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "metrics": {
                        "suite_start_timestamp": 100,
                        "suite_finish_timestamp": 110,
                        "wall_time": 10.25,
                    },
                },
            }
        ],
        root_start_s=99,
        root_end_s=112,
    )

    chunk = next(trace.spans("ya.chunk"))
    assert chunk.start_time_unix_nano == 100_500_000_000
    assert chunk.end_time_unix_nano == 110_750_000_000


def test_chunk_uses_feasible_precise_wall_time_when_event_is_late(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "chunk-event",
                "timestamp": 120,
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "metrics": {
                        "suite_start_timestamp": 100,
                        "suite_finish_timestamp": 110,
                        "wall_time": 10.25,
                    },
                },
            }
        ],
        root_start_s=99,
        root_end_s=121,
    )

    chunk = next(trace.spans("ya.chunk"))
    assert chunk.start_time_unix_nano == 100_000_000_000
    assert chunk.end_time_unix_nano == 110_250_000_000


def test_suite_span_is_parented_to_dispatch_phase(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "suite-event",
                "timestamp": 5,
                "value": {
                    "errors": [["fail", "setup failed"]],
                },
            }
        ],
        evlog_events=[
            {
                "namespace": "stages",
                "event": "stage-finished",
                "value": {
                    "name": "dispatch_build",
                    "tag": "dispatch_build",
                    "time": [1, 10],
                },
            }
        ],
        root_end_s=11,
        exit_code=1,
    )

    dispatch = next(
        span
        for span in trace.spans("ya.phase")
        if _attributes(span)["ya.stage.name"] == "dispatch_build"
    )
    assert next(trace.spans("ya.suite")).parent_span_id == dispatch.span_id
