from scripts.tracing.yatrace_test_support import (
    Ns,
    Path,
    YaEvlogRecord,
    _attributes,
    _failed_node,
    _node_started,
    _render_ya_trace,
    _run_worker,
    _stage,
    _statistics,
    _worker,
    _worker_detail,
    json,
    load_ya_evlog,
    pytest,
    span_duration_ns,
    span_status_code,
    yatrace_limits,
)


def test_evlog_record_model_has_no_span_factories() -> None:
    assert {
        "build_command_span",
        "build_node_span",
        "matched_test_worker_span",
        "span_name",
        "test_node_span",
        "test_worker_attributes",
        "test_worker_phase_span",
    }.isdisjoint(vars(YaEvlogRecord))


def test_worker_operations_do_not_claim_cache_hit_or_miss(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 1, 10),
            _worker(
                "FromCache(uid$(BUILD_ROOT)/result.o)",
                "restore[CC]",
                2,
                3,
            ),
            _worker("Run(uid$(BUILD_ROOT)/result.o)", "CC", 3, 5),
            _statistics(
                {
                    "cache_hit": {
                        "cache_hit": 75,
                        "run_tasks": 4,
                        "executed_tasks": 4,
                        "cached_tasks": 3,
                        "not_cached_tasks": 1,
                    }
                }
            ),
        ],
        root_end_s=11,
    )

    build = next(trace.spans("ya.build"))
    build_attributes = _attributes(build)
    node_attributes = [_attributes(span) for span in trace.spans("ya.build.node")]
    assert all("ya.build.cache.hit" not in item for item in node_attributes)
    assert not any(
        key.startswith("ya.build.cache.worker_node.")
        or key.startswith("ya.build.cache.tool.")
        for key in build_attributes
    )
    assert build_attributes["ya.build.worker.tool.cc.cache_restore.count"] == 1
    assert build_attributes["ya.build.worker.tool.cc.execute.count"] == 1


def test_failed_node_record_marks_matching_build_span_failed(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 1, 10),
            _worker(
                "Run(failed-uid$(BUILD_ROOT)/result.o)",
                "CC",
                2,
                5,
            ),
            _failed_node("failed-uid", 42),
        ],
        root_end_s=11,
        exit_code=1,
    )

    node = next(trace.spans("ya.build.node"))
    build = next(trace.spans("ya.build"))
    assert span_status_code(node) == 2
    assert span_status_code(build) == 2
    assert _attributes(node)["process.exit.code"] == 42
    assert _attributes(node)["ya.build.failed"] is True


def test_outputless_failed_node_is_joined_by_uid(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 1, 10),
            _worker("Run(failed-uid)", "CC", 2, 5),
            _failed_node("failed-uid", 17),
        ],
        root_end_s=11,
        exit_code=1,
    )

    node = next(trace.spans("ya.build.node"))
    assert span_status_code(node) == 2
    assert _attributes(node)["process.exit.code"] == 17


def test_failed_node_is_protected_from_span_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(yatrace_limits, "MAX_BUILD_NODE_SPANS", 1)
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 1, 10),
            _worker("Run(failed$(BUILD_ROOT)/failed.o)", "CC", 2, 2.1),
            _worker("Run(long$(BUILD_ROOT)/long.o)", "CC", 3, 9),
            _failed_node("failed", 1),
        ],
        root_end_s=11,
        exit_code=1,
    )

    node = next(trace.spans("ya.build.node"))
    assert _attributes(node)["ya.build.outputs"] == ["failed.o"]
    assert span_status_code(node) == 2


def test_failed_node_count_uses_unique_uids(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 1, 10),
            _worker(
                "FromCache(failed$(BUILD_ROOT)/cached.o)",
                "restore[CC]",
                2,
                3,
            ),
            _worker("Run(failed$(BUILD_ROOT)/built.o)", "CC", 3, 5),
            _failed_node("failed", 1),
        ],
        root_end_s=11,
        exit_code=1,
    )

    build = next(trace.spans("ya.build"))
    assert _attributes(build)["ya.build.failed_node.count"] == 1
    assert (
        sum(span_status_code(node) == 2 for node in trace.spans("ya.build.node")) == 1
    )


def test_graph_statistics_are_attached_to_dispatch_not_build_envelope(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 1, 10),
            _worker("Run(build$(BUILD_ROOT)/build.o)", "CC", 2, 5),
            _statistics(
                {
                    "cache_hit": {
                        "tests_tasks": 2,
                        "ok_tasks": 3,
                    },
                    "execution_stages_msec": {
                        "build_only": 3_000,
                        "tests_only": 2_000,
                        "tests_with_other": 1_000,
                    },
                    "task_execution_msec": 6_000,
                }
            ),
        ],
        root_end_s=11,
    )

    dispatch = next(
        span
        for span in trace.spans("ya.phase")
        if _attributes(span)["ya.stage.name"] == "dispatch_build"
    )
    build = next(trace.spans("ya.build"))
    dispatch_attributes = _attributes(dispatch)
    build_attributes = _attributes(build)
    assert dispatch_attributes["ya.build.execution.total.seconds"] == 6
    assert dispatch_attributes["ya.build.execution.stage.tests_only.seconds"] == 2
    assert dispatch_attributes["ya.build.task.uncached_test.count"] == 2
    assert dispatch_attributes["ya.build.task.uncached_non_test.count"] == 3
    assert "ya.build.execution.total.seconds" not in build_attributes
    assert "ya.build.execution.stage.tests_only.seconds" not in build_attributes
    assert "ya.build.task.test.count" not in dispatch_attributes
    assert "ya.build.task.ok.count" not in dispatch_attributes
    assert "ya.build.task.test.count" not in build_attributes
    assert "ya.build.task.ok.count" not in build_attributes


@pytest.mark.parametrize(
    ("tag", "expected_kind"),
    [
        ("restore_from_dist_cache[CC]", "cache_restore"),
        ("put_in_dist_cache[CC]", "cache_store"),
    ],
)
def test_distributed_cache_wrapper_tags_are_classified(
    tag: str,
    expected_kind: str,
) -> None:
    record = YaEvlogRecord(
        name="Cache(uid$(BUILD_ROOT)/output.o)",
        tag=tag,
        start_ns=Ns(1),
        end_ns=Ns(2),
    )
    assert record.kind_and_tool == (expected_kind, "CC")
    assert record.cache_source == "distributed"


@pytest.mark.parametrize(
    ("tag", "expected_kind", "expected_tool"),
    [
        ("TS", "test_execute", "TS"),
        ("TM", "test_execute", "TM"),
        ("TL", "test_list", "TL"),
        ("YT", "test_execute", "YT"),
        ("TA", "test_aggregate", "TA"),
        ("TR", "test_merge", "TR"),
        ("restore[TS]", "test_cache_restore", "TS"),
        ("restore_from_dist_cache[TM]", "test_cache_restore", "TM"),
        ("result[TL]", "test_materialize", "TL"),
        ("put_in_cache[TS]", "test_cache_store", "TS"),
        ("put_in_dist_cache[TM]", "test_cache_store", "TM"),
    ],
)
def test_all_yatool_test_node_tags_are_classified(
    tag: str,
    expected_kind: str,
    expected_tool: str,
) -> None:
    record = YaEvlogRecord(
        name="Run(testuid)",
        tag=tag,
        start_ns=Ns(1),
        end_ns=Ns(2),
    )

    assert record.kind_and_tool == (expected_kind, expected_tool)


def test_large_test_runner_is_distinguished_from_test_list_node() -> None:
    record = YaEvlogRecord(
        name="Run(testuid$(BUILD_ROOT)/suite/test-results/unit/meta.json)",
        tag="TL",
        start_ns=Ns(1),
        end_ns=Ns(2),
    )

    assert record.kind_and_tool == ("test_execute", "TL")
    assert record.test_size == "large"


def test_test_result_path_does_not_override_a_worker_wrapper_kind() -> None:
    record = YaEvlogRecord(
        name="Result(uid$(BUILD_ROOT)/suite/test-results/unit/meta.json)",
        tag="result[CC]",
        start_ns=Ns(1),
        end_ns=Ns(2),
    )

    assert record.kind_and_tool == ("materialize", "CC")


def test_non_object_evlog_record_is_ignored(tmp_path: Path) -> None:
    evlog_path = tmp_path / "ya_evlog.jsonl"
    evlog_path.write_text(
        "[]\n" + json.dumps(_run_worker("uid", "output.o", 2, 3)) + "\n"
    )

    evlog = load_ya_evlog(evlog_path)
    assert [record.uid for record in evlog.nodes] == ["uid"]


def test_exec_cmd_interval_is_child_of_its_worker_node_with_interleaving(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _stage("dispatch_build", 1, 10),
            _run_worker("firstuid", "first.o", 2, 6, "Worker-1"),
            _run_worker("seconduid", "second.o", 3, 7, "Worker-2"),
            _worker_detail("exec_cmd", 4, 5, "Worker-1"),
            _worker_detail("post_cmd", 5, 5.5, "Worker-1"),
        ],
        root_end_s=11,
    )

    nodes = list(trace.spans("ya.build.node"))
    nodes_by_id = {node.span_id: _attributes(node) for node in nodes}
    commands = list(trace.spans("ya.build.command"))
    assert len(commands) == 1
    command = commands[0]
    assert nodes_by_id[command.parent_span_id]["ya.build.node.uid"] == "firstuid"
    assert span_duration_ns(
        next(
            node
            for node in nodes
            if _attributes(node)["ya.build.node.uid"] == "firstuid"
        )
    ) == Ns.from_s(4)
    assert span_duration_ns(command) == Ns.from_s(1)
    assert _attributes(command)["ya.build.timing.scope"] == "command"
    assert (
        _attributes(next(trace.spans("ya.build")))[
            "ya.build.cumulative_command_seconds"
        ]
        == 1
    )


def test_node_started_clears_stale_command_detail_parent(tmp_path: Path) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _run_worker("firstuid", "first.o", 2, 6, "Worker-1"),
            _node_started(
                "Worker-1",
                "Run(seconduid$(BUILD_ROOT)/second.o)",
            ),
            _worker_detail("exec_cmd", 4, 5, "Worker-1"),
        ],
        root_end_s=11,
    )

    assert not any(trace.spans("ya.build.command"))


def test_null_worker_thread_does_not_associate_command_detail(
    tmp_path: Path,
) -> None:
    worker = _run_worker("firstuid", "first.o", 2, 6)
    worker["thread_name"] = None
    detail = _worker_detail("exec_cmd", 3, 4, "unused")
    detail["thread_name"] = None

    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[worker, detail],
        root_end_s=11,
    )

    assert not any(trace.spans("ya.build.command"))


def test_failed_worker_does_not_imply_failed_command(
    tmp_path: Path,
) -> None:
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _run_worker("faileduid", "failed.o", 2, 6, "Worker-1"),
            _worker_detail("exec_cmd", 3, 4, "Worker-1"),
            _failed_node("faileduid", 1),
        ],
        root_end_s=11,
        exit_code=1,
    )

    assert span_status_code(next(trace.spans("ya.build.node"))) == 2
    command = next(trace.spans("ya.build.command"))
    assert span_status_code(command) == 0
    assert _attributes(command)["ya.build.node.failed"] is True


def test_build_command_span_limit_prefers_critical_parent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(yatrace_limits, "MAX_BUILD_COMMAND_SPANS", 1)
    trace = _render_ya_trace(
        tmp_path,
        [],
        evlog_events=[
            _run_worker("firstuid", "first.o", 2, 6, "Worker-1"),
            _worker_detail("exec_cmd", 3, 4, "Worker-1"),
            _run_worker("criticaluid", "critical.o", 4, 8, "Worker-2"),
            _worker_detail("exec_cmd", 5, 6, "Worker-2"),
            _statistics(
                {
                    "critical_path": [
                        {
                            "type": "CC",
                            "uid": "criticaluid",
                            "elapsed": 4_000,
                            "start_ts": 4_000,
                            "end_ts": 8_000,
                        }
                    ]
                }
            ),
        ],
        root_end_s=11,
    )

    nodes_by_id = {
        node.span_id: _attributes(node) for node in trace.spans("ya.build.node")
    }
    command = next(trace.spans("ya.build.command"))
    assert nodes_by_id[command.parent_span_id]["ya.build.node.uid"] == "criticaluid"
    build_attributes = _attributes(next(trace.spans("ya.build")))
    assert build_attributes["ya.build.command_spans.rendered"] == 1
    assert build_attributes["ya.build.command_spans.dropped"] == 1
