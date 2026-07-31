from scripts.tracing.yatrace_test_support import (
    Ns,
    Path,
    YaEvent,
    YaTraceCollection,
    _attributes,
    _chunk,
    _failed_node,
    _render_ya_trace,
    _stage,
    _subtest,
    _worker,
    _worker_detail,
    _write_jsonl,
    json,
    span_duration_ns,
    span_status_code,
)


def test_trace_collection_indexes_tests_and_disambiguates_chunks(
    tmp_path: Path,
) -> None:
    suite = "cloud/example/tests"
    for result_folder in ("py3test", "flake8"):
        trace_path = (
            tmp_path / suite / "test-results" / result_folder / "ytest.report.trace"
        )
        trace_path.parent.mkdir(parents=True)
        events = [
            {
                "name": "subtest-finished",
                "value": {
                    "class": "pkg::Suite",
                    "subtest": "test",
                    "logs": {
                        "stdout": (
                            f"$(BUILD_ROOT)/{suite}/test-results/"
                            f"{result_folder}/stdout.log"
                        ),
                        "logsdir": (
                            f"$(BUILD_ROOT)/{suite}/test-results/"
                            f"{result_folder}/testing_out_stuff"
                        ),
                    },
                },
            },
            {
                "name": "chunk-event",
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "logs": {
                        "log": (
                            f"$(BUILD_ROOT)/{suite}/test-results/"
                            f"{result_folder}/run_test.log"
                        )
                    },
                },
            },
        ]
        trace_path.write_text("".join(json.dumps(event) + "\n" for event in events))

    traces = YaTraceCollection.load(tmp_path)
    test_event = traces.finished_test(suite, "pkg.Suite", "test")

    assert test_event is not None
    assert test_event.logs_directory == (
        f"{suite}/test-results/py3test/testing_out_stuff"
    )
    assert test_event.resolved_logs(tmp_path)["stdout"] == str(
        tmp_path / suite / "test-results" / "py3test" / "stdout.log"
    )
    assert traces.select_chunk_event(suite, 0, 1) is None

    chunk_event = traces.select_chunk_event(
        suite,
        0,
        1,
        f"failure in {tmp_path}/{suite}/test-results/flake8/run_test.log",
    )
    assert chunk_event is not None
    assert "flake8" in chunk_event.log_paths["log"]


def test_log_attributes_keep_only_safe_build_root_relative_paths() -> None:
    event = YaEvent(
        name="subtest-finished",
        timestamp_ns=Ns(1),
        order=0,
        value={
            "logs": {
                "stdout": "$(BUILD_ROOT)/suite/output.log",
                "stderr": "$(BUILD_ROOT)/suite/../secret.log",
                "absolute": "$(BUILD_ROOT)//etc/passwd",
                "nul": "$(BUILD_ROOT)/suite/bad\x00name",
                "runner": "/home/runner/output.log",
            }
        },
    )

    assert event.log_attributes("ya.test") == {
        "ya.test.log.stdout.path": "suite/output.log"
    }


def test_raw_test_class_names_do_not_collapse_into_one_test(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            {
                "name": "subtest-finished",
                "timestamp": 5,
                "value": {
                    "class": test_class,
                    "subtest": "same-name",
                    "status": "good",
                    "time": 1,
                },
            }
            for test_class in ("A::B", "A.B")
        ],
        root_end_s=10,
    )

    tests = list(trace.spans("ya.test"))
    assert len(tests) == 2
    assert {_attributes(test)["test.suite"] for test in tests} == {"A::B", "A.B"}


def test_test_operations_group_enriches_chunks_and_renders_inferred_stages(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            _subtest(
                "case",
                timestamp=8,
                duration=4,
                logs={
                    "stdout": (
                        "$(BUILD_ROOT)/suite/test-results/unittest/"
                        "testing_out_stuff/case.out"
                    ),
                    "logsdir": (
                        "$(BUILD_ROOT)/suite/test-results/unittest/" "testing_out_stuff"
                    ),
                },
            ),
            _chunk(
                0,
                1,
                timestamp=10,
                metrics={
                    "suite_start_timestamp": 2,
                    "suite_finish_timestamp": 10,
                    "suite_initial_(seconds)": 1,
                    "suite_prepare_recipes_(seconds)": 2,
                    "suite_wrapper_execution_(seconds)": 4,
                    "suite_stop_recipes_(seconds)": 1,
                },
                logs={
                    "log": "$(BUILD_ROOT)/suite/test-results/unittest/run_test.log",
                    "logsdir": (
                        "$(BUILD_ROOT)/suite/test-results/unittest/" "testing_out_stuff"
                    ),
                },
            ),
        ],
        evlog_events=[
            _stage("dispatch_build", 1, 11),
            _worker(
                "Run(testuid$(BUILD_ROOT)/suite/test-results/unittest/meta.json)",
                "TM",
                2,
                10,
                thread_name="Worker-1",
            ),
        ],
        root_end_s=12,
    )

    dispatch = next(trace.spans("ya.phase"))
    operations = next(trace.spans("ya.test.operations"))
    worker = next(trace.spans("ya.test.worker"))
    chunk = next(trace.spans("ya.chunk"))
    test = next(trace.spans("ya.test"))
    stages = list(trace.spans("ya.test.stage"))

    assert operations.parent_span_id == dispatch.span_id
    assert worker.parent_span_id == operations.span_id
    assert chunk.parent_span_id == worker.span_id
    assert span_duration_ns(operations) == Ns.from_s(8)
    assert _attributes(operations) == {
        "ya.test.wall_time.kind": "worker-node-execution-envelope",
        "ya.test.worker.execute.count": 1,
        "ya.test.worker.unmatched.count": 0,
        "ya.test.worker.cumulative_seconds": 8,
        "ya.test.chunk.count": 1,
    }
    assert _attributes(chunk)["test.size"] == "medium"
    assert _attributes(chunk)["ya.test.worker.uid"] == "testuid"
    assert _attributes(chunk)["ya.test.worker.thread"] == "Worker-1"
    assert _attributes(test)["test.size"] == "medium"
    assert _attributes(test)["ya.test.log.stdout.path"] == (
        "suite/test-results/unittest/testing_out_stuff/case.out"
    )
    assert _attributes(test)["ya.test.logs_directory.path"] == (
        "suite/test-results/unittest/testing_out_stuff"
    )
    assert _attributes(chunk)["ya.chunk.log.log.path"] == (
        "suite/test-results/unittest/run_test.log"
    )
    assert _attributes(chunk)["ya.chunk.logs_directory.path"] == (
        "suite/test-results/unittest/testing_out_stuff"
    )

    assert [stage.name for stage in stages] == [
        "test stage: initial",
        "test stage: prepare recipes",
        "test stage: wrapper execution",
        "test stage: stop recipes",
    ]
    assert [span_duration_ns(stage) for stage in stages] == [
        Ns.from_s(1),
        Ns.from_s(2),
        Ns.from_s(4),
        Ns.from_s(1),
    ]
    assert all(stage.parent_span_id == chunk.span_id for stage in stages)
    assert all(_attributes(stage)["test.timing.inferred"] is True for stage in stages)


def test_unmatched_failed_test_worker_is_preserved(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 1, 8),
            _worker(
                "Run(missing$(BUILD_ROOT)/suite/test-results/unittest/meta.json)",
                "TS",
                2,
                7,
                thread_name="Worker-1",
            ),
            _worker_detail("setup", 2, 3, "Worker-1"),
            _worker_detail("exec_cmd", 3, 7, "Worker-1"),
            _failed_node("missing", 17),
        ],
        root_end_s=9,
        exit_code=1,
    )

    operations = next(trace.spans("ya.test.operations"))
    node = next(trace.spans("ya.test.node"))
    assert node.parent_span_id == operations.span_id
    assert span_status_code(node) == 2
    assert _attributes(node)["test.size"] == "small"
    assert _attributes(node)["ya.test.worker.uid"] == "missing"
    assert _attributes(node)["process.exit.code"] == 17
    assert _attributes(operations)["ya.test.worker.unmatched.count"] == 1
    phases = list(trace.spans("ya.test.worker.phase"))
    assert [phase.name for phase in phases] == [
        "worker phase: setup",
        "worker phase: exec command",
    ]
    assert all(phase.parent_span_id == node.span_id for phase in phases)


def test_exact_test_worker_phases_are_children_of_the_matching_chunk(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            _chunk(
                0,
                1,
                timestamp=8.5,
                metrics={
                    "suite_start_timestamp": 1.5,
                    "suite_finish_timestamp": 8.5,
                },
            )
        ],
        evlog_events=[
            _stage("dispatch_build", 1, 9),
            _worker(
                "Run(testuid$(BUILD_ROOT)/suite/test-results/unittest/meta.json)",
                "TS",
                2,
                8,
                thread_name="Worker-1",
            ),
            _worker_detail("setup", 2, 2.5, "Worker-1"),
            _worker_detail("exec_cmd", 2.5, 7, "Worker-1"),
            _worker_detail("post_cmd", 7, 7.2, "Worker-1"),
            _worker_detail("node_result", 7.2, 7.5, "Worker-1"),
            _worker_detail("finalize", 7.5, 8, "Worker-1"),
        ],
        root_end_s=10,
    )

    chunk = next(trace.spans("ya.chunk"))
    worker = next(trace.spans("ya.test.worker"))
    phases = list(trace.spans("ya.test.worker.phase"))
    assert [phase.name for phase in phases] == [
        "worker phase: setup",
        "worker phase: exec command",
        "worker phase: post command",
        "worker phase: node result",
        "worker phase: finalize",
    ]
    assert chunk.parent_span_id == worker.span_id
    assert worker.start_time_unix_nano == Ns.from_s(1.5)
    assert worker.end_time_unix_nano == Ns.from_s(8.5)
    assert _attributes(worker)["ya.test.worker.reported_seconds"] == 6
    assert _attributes(worker)["ya.test.worker.timeline.adjusted"] is True
    assert all(phase.parent_span_id == worker.span_id for phase in phases)
    assert [_attributes(phase)["ya.test.worker.phase"] for phase in phases] == [
        "setup",
        "exec_cmd",
        "post_cmd",
        "node_result",
        "finalize",
    ]
    assert all(
        _attributes(phase)["ya.test.worker.timing.source"] == "ya-evlog"
        for phase in phases
    )


def test_test_result_processing_nodes_are_preserved_as_test_operations(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 1, 9),
            _worker(
                "Run(aggregate$(BUILD_ROOT)/suite/test-results/unittest/meta.json)",
                "TA",
                2,
                4,
            ),
            _worker(
                "Run(merge$(BUILD_ROOT)/suite/test-results/unittest/meta.json)",
                "TR",
                4,
                6,
            ),
            _worker(
                "Result(materialize$(BUILD_ROOT)/suite/test-results/unittest/meta.json)",
                "result[TM]",
                6,
                7,
            ),
            _worker(
                "Put(store$(BUILD_ROOT)/suite/test-results/unittest/meta.json)",
                "put_in_cache[TM]",
                7,
                8,
            ),
        ],
        root_end_s=10,
    )

    operations = next(trace.spans("ya.test.operations"))
    nodes = list(trace.spans("ya.test.node"))
    assert span_duration_ns(operations) == Ns.from_s(6)
    assert [_attributes(node)["ya.test.graph_node.kind"] for node in nodes] == [
        "test_aggregate",
        "test_merge",
        "test_materialize",
        "test_cache_store",
    ]
    assert [node.name for node in nodes] == [
        "test results aggregation: suite [unittest]",
        "test results merge: suite [unittest]",
        "test result materialization: suite [unittest]",
        "test result cache store: suite [unittest]",
    ]
    assert all(_attributes(node)["test.suite"] == "suite" for node in nodes)
    assert all(
        _attributes(node)["ya.test_results.folder"] == "unittest" for node in nodes
    )
    assert all(node.parent_span_id == operations.span_id for node in nodes)
    assert not any(trace.spans("ya.build"))
    assert _attributes(operations)["ya.test.graph_node.aggregate.count"] == 1
    assert _attributes(operations)["ya.test.graph_node.merge.count"] == 1
    assert _attributes(operations)["ya.test.graph_node.materialize.count"] == 1
    assert _attributes(operations)["ya.test.graph_node.cache_store.count"] == 1


def test_ten_longest_completed_tests_are_ranked_per_ya_trace(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [
            *[
                _subtest(
                    f"case-{duration}",
                    timestamp=20,
                    duration=duration,
                )
                for duration in range(1, 13)
            ],
            _subtest(
                "not-launched",
                timestamp=20,
                duration=100,
                status="not_launched",
            ),
            _chunk(
                0,
                1,
                timestamp=21,
                metrics={
                    "suite_start_timestamp": 0,
                    "suite_finish_timestamp": 21,
                },
            ),
        ],
        root_end_s=22,
        exit_code=1,
    )

    tests = {span.name: _attributes(span) for span in trace.spans("ya.test")}
    assert tests["Suite::case-12"]["ya.test.duration.rank"] == 1
    assert tests["Suite::case-3"]["ya.test.duration.rank"] == 10
    assert "ya.test.duration.rank" not in tests["Suite::case-2"]
    assert "ya.test.duration.rank" not in tests["Suite::case-1"]
    assert "ya.test.duration.rank" not in tests["Suite::not-launched"]


def test_finished_test_prefers_raw_class_then_uses_unique_normalized_fallback(
    tmp_path: Path,
) -> None:
    ya_out = tmp_path / "out"
    suite = "cloud/example/tests"
    _write_jsonl(
        ya_out / suite / "test-results" / "unittest" / "ytest.report.trace",
        [
            {
                "name": "subtest-finished",
                "value": {
                    "class": test_class,
                    "subtest": "same-name",
                    "marker": marker,
                },
            }
            for test_class, marker in (
                ("A.B", "dot"),
                ("A::B", "colon"),
                ("C::D", "fallback"),
                ("E::F.G", "ambiguous-one"),
                ("E.F::G", "ambiguous-two"),
            )
        ],
    )
    traces = YaTraceCollection.load(ya_out)

    exact = traces.finished_test(suite, "A.B", "same-name")
    fallback = traces.finished_test(suite, "C.D", "same-name")
    ambiguous = traces.finished_test(suite, "E.F.G", "same-name")
    assert exact is not None and exact.value["marker"] == "dot"
    assert fallback is not None and fallback.value["marker"] == "fallback"
    assert ambiguous is None
