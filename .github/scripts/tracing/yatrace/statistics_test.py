from scripts.tracing.yatrace.statistics import YaBuildStatistics
from scripts.tracing.yatrace_test_support import (
    Ns,
    Path,
    ResourceAttributes,
    _attributes,
    _chunk,
    _render_ya_trace,
    _stage,
    _statistics,
    _subtest,
    _worker,
    build_ya_trace,
    load_ya_evlog,
    span_duration_ns,
)


def test_build_statistics_returns_attributes_dictionary() -> None:
    assert YaBuildStatistics.from_raw().build_attributes([], []) == {}


def test_build_statistics_and_test_critical_path_are_preserved(
    tmp_path: Path,
) -> None:
    statistics = {
        "cache_hit": {
            "cache_hit": 75,
            "run_tasks": 100,
            "executed_tasks": 4,
            "cached_tasks": 3,
            "dyn_cached_tasks": 1,
            "not_cached_tasks": 1,
            "tests_tasks": 1,
            "failed_tasks": 0,
            "ok_tasks": 1,
            "avoided_tasks": 96,
        },
        "dist_cache_stat": {
            "get_count": 2,
            "get_data_size": 1_024,
            "put_count": 1,
            "put_data_size": 512,
        },
        "execution_stages_msec": {"build_only": 3_500, "tests_only": 2_000},
        "task_execution_msec": 5_500,
        "graph_lang_usage": {"cpp": 1},
        "critical_path": [
            {
                "type": "CC",
                "elapsed": 4_000,
                "start_ts": 13_000,
                "end_ts": 17_000,
                "text": "$(SOURCE_ROOT)/suite/main.cpp",
                "uid": "compileuid",
            },
            {
                "type": "TM",
                "elapsed": 2_000,
                "start_ts": 18_000,
                "end_ts": 20_000,
                "text": "suite/tests",
                "uid": "testuid",
            },
        ],
    }
    evlog_events = [
        _stage("build_graph_and_tests", 10, 12),
        _stage("dispatch_build", 12, 30),
        _worker(
            "FromCache(cacheuid$(BUILD_ROOT)/library/cached.a)",
            "restore[AR]",
            12.5,
            13,
        ),
        _worker(
            "FromCache(loweruid$(BUILD_ROOT)/library/lower.a)",
            "restore[ar]",
            12.6,
            12.9,
        ),
        _worker(
            "Run(compileuid$(BUILD_ROOT)/suite/main.cpp.o)",
            "CC",
            13,
            17,
        ),
        _worker("PutInCache(uid)", "put_in_cache[CC]", 16.9, 17),
        _worker(
            "Run(actual-test$(BUILD_ROOT)/suite/test-results/unittest/meta.json)",
            "TM",
            18,
            20,
        ),
        _statistics(statistics),
    ]
    trace = _render_ya_trace(
        tmp_path,
        [
            _subtest("critical", timestamp=19.5, duration=1),
            _chunk(
                0,
                1,
                timestamp=20,
                metrics={
                    "suite_start_timestamp": 18,
                    "suite_finish_timestamp": 20,
                },
            ),
        ],
        evlog_events=evlog_events,
        root_start_s=9,
        root_end_s=31,
    )

    root = next(trace.spans("ya"))
    phases = {
        _attributes(span)["ya.stage.name"]: span for span in trace.spans("ya.phase")
    }
    build = next(trace.spans("ya.build"))
    nodes = list(trace.spans("ya.build.node"))
    operations = next(trace.spans("ya.test.operations"))
    worker = next(trace.spans("ya.test.worker"))
    chunk = next(trace.spans("ya.chunk"))
    test = next(trace.spans("ya.test"))
    dispatch_attributes = _attributes(phases["dispatch_build"])
    build_attributes = _attributes(build)
    root_attributes = _attributes(root)

    assert set(phases) == {"build_graph_and_tests", "dispatch_build"}
    assert build.parent_span_id == phases["dispatch_build"].span_id
    assert operations.parent_span_id == phases["dispatch_build"].span_id
    assert worker.parent_span_id == operations.span_id
    assert chunk.parent_span_id == worker.span_id
    assert span_duration_ns(build) == Ns.from_s(4.5)
    assert build_attributes["ya.build.node.count"] == 4
    assert build_attributes["ya.build.node.cache_store.count"] == 1
    assert build_attributes["ya.build.first_test_node_offset_seconds"] == 5.5
    assert build_attributes["ya.build.worker.tool.ar.cache_restore.count"] == 2
    assert build_attributes["ya.build.worker.tool.cc.execute.count"] == 1
    assert build_attributes["ya.build.critical_path.node.count"] == 1
    assert build_attributes["ya.build.critical_path.work.seconds"] == 4
    assert len(nodes) == 3

    expected_dispatch = {
        "ya.build.cache.considered_task.hit.ratio": 0.75,
        "ya.build.cache.considered_task.hit.count": 3,
        "ya.build.cache.considered_task.miss.count": 1,
        "ya.build.task.avoided.ratio": 0.96,
        "ya.build.task.reused_or_avoided.ratio": 0.99,
        "ya.build.task.avoided.count": 96,
        "ya.build.dist_cache.get.bytes": 1_024,
        "ya.build.execution.stage.build_only.seconds": 3.5,
        "ya.build.execution.total.seconds": 5.5,
    }
    assert {
        key: dispatch_attributes[key] for key in expected_dispatch
    } == expected_dispatch

    cached = next(
        node for node in nodes if _attributes(node)["ya.build.kind"] == "cache_restore"
    )
    compiled = next(
        node for node in nodes if _attributes(node)["ya.build.kind"] == "execute"
    )
    assert _attributes(cached)["ya.build.cache.source"] == "local"
    assert _attributes(cached)["ya.build.outputs"] == ["library/cached.a"]
    assert _attributes(compiled)["ya.build.critical_path"] is True
    assert _attributes(compiled)["ya.build.critical_path.index"] == 0
    assert _attributes(compiled)["ya.build.critical_path.reported_seconds"] == 4
    assert root_attributes["ya.build.node.count"] == 4
    assert root_attributes["ya.build.node.span_count"] == 3
    assert root_attributes["ya.test.critical_path.entry.count"] == 1
    assert root_attributes["ya.test.critical_path.chunk.count"] == 1
    assert root_attributes["ya.test.critical_path.span.count"] == 1
    assert _attributes(chunk)["ya.test.critical_path"] is True
    assert _attributes(chunk)["ya.test.critical_path.granularity"] == "test-chunk"
    assert _attributes(test)["ya.test.critical_path"] is True
    assert _attributes(test)["ya.test.critical_path.inferred"] is True
    assert _attributes(test)["ya.test.critical_path.reported_seconds"] == 2

    build_only = build_ya_trace(
        [],
        root_start_ns=Ns.from_s(9),
        root_end_ns=Ns.from_s(31),
        exit_code=0,
        resource=ResourceAttributes(),
        evlog=load_ya_evlog(tmp_path / "ya_evlog.jsonl"),
        operation="build",
    )
    assert build_only[0].name == "ya make build"
    assert not any(build_only.spans("ya.chunk"))
    assert any(build_only.spans("ya.build"))
