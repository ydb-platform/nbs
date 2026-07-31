from scripts.tracing.yatrace_test_support import (
    Path,
    YaEvlog,
    YaTraceInputs,
    json,
    os,
    pytest,
    sys,
    ya_trace_report_module,
    yatrace_limits,
)


def test_trace_inputs_produce_manifest_and_file_list(
    tmp_path: Path,
) -> None:
    ya_out = tmp_path / "out"
    trace_path = (
        ya_out
        / "cloud/example/tests"
        / "test-results"
        / "unittest"
        / "ytest.report.trace"
    )
    trace_path.parent.mkdir(parents=True)
    trace_path.write_text('{"name":"chunk-event","value":{}}\n')
    evlog_path = tmp_path / "ya_evlog.jsonl"
    evlog_path.write_text('{"namespace":"stages","event":"stage-finished"}\n')

    inputs = YaTraceInputs.discover(
        ya_out,
        evlog_path=evlog_path,
    )
    manifest = inputs.bundle_manifest({"github.run.id": "123"})

    assert manifest == {
        "schema": "nbs-ya-trace-input-bundle",
        "schema_version": 1,
        "evlog_file": "ya_evlog.jsonl",
        "ya_out_dir": "ya-out",
        "ya_trace_file_count": 1,
        "metadata": {"github.run.id": "123"},
    }
    assert inputs.tar_file_list() == (
        b"./cloud/example/tests/test-results/unittest/ytest.report.trace\0"
    )
    assert len(inputs.parse().traces) == 1


def test_trace_inputs_exclude_files_older_than_attempt(
    tmp_path: Path,
) -> None:
    ya_out = tmp_path / "out"
    old_trace = (
        ya_out / "old-suite" / "test-results" / "unittest" / "ytest.report.trace"
    )
    current_trace = (
        ya_out / "current-suite" / "test-results" / "unittest" / "ytest.report.trace"
    )
    for trace_path in (old_trace, current_trace):
        trace_path.parent.mkdir(parents=True)
        trace_path.write_text('{"name":"chunk-event","value":{}}\n')

    os.utime(old_trace, (100, 100))
    os.utime(current_trace, (200, 200))

    inputs = YaTraceInputs.discover(ya_out, modified_since=150)

    assert inputs.trace_paths == [current_trace]


def test_oversized_trace_collection_is_rejected_before_opening_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ya_out = tmp_path / "out"
    for suite in ("one", "two"):
        trace_path = ya_out / suite / "test-results" / "unittest" / "ytest.report.trace"
        trace_path.parent.mkdir(parents=True)
        trace_path.write_text("{}\n")

    inputs = YaTraceInputs.discover(ya_out)
    monkeypatch.setattr(yatrace_limits, "MAX_YA_TRACE_BYTES", 5)

    def reject_open(*args, **kwargs):
        raise AssertionError(
            f"oversized trace files must not be opened: {args!r}, {kwargs!r}"
        )

    monkeypatch.setattr(Path, "open", reject_open)
    with pytest.raises(ValueError, match="Ya traces exceed 5 bytes"):
        inputs.parse()


def test_trace_collection_above_old_512_mib_limit_is_parsed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ya_out = tmp_path / "out"
    trace_path = ya_out / "suite" / "test-results" / "unittest" / "ytest.report.trace"
    trace_path.parent.mkdir(parents=True)
    trace_path.write_text('{"name":"chunk-event","value":{}}\n')
    inputs = YaTraceInputs.discover(ya_out)
    actual_stat = Path.stat

    def inflated_stat(path: Path, *args, **kwargs):
        result = actual_stat(path, *args, **kwargs)
        if path == trace_path:
            return type(
                "InflatedStat",
                (),
                {"st_size": 512 * 1024**2 + 1},
            )()
        return result

    monkeypatch.setattr(Path, "stat", inflated_stat)

    assert len(inputs.parse().traces) == 1


def test_report_preserves_raw_input_list_when_trace_parsing_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ya_out = tmp_path / "out"
    trace_path = (
        ya_out
        / "cloud/example/tests"
        / "test-results"
        / "unittest"
        / "ytest.report.trace"
    )
    trace_path.parent.mkdir(parents=True)
    trace_path.write_text("{}\n")
    output_dir = tmp_path / "summary"

    monkeypatch.setattr(yatrace_limits, "MAX_YA_TRACE_BYTES", 1)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ya_trace_report",
            "--ya-out",
            str(ya_out),
            "--output-dir",
            str(output_dir),
            "--attempt-start-ns",
            "0",
            "--attempt-end-ns",
            "1",
            "--exit-code",
            "0",
        ],
    )

    with pytest.raises(ValueError, match="Ya traces exceed 1 bytes"):
        ya_trace_report_module.main()

    manifest = json.loads((output_dir / "trace-inputs.manifest.json").read_text())
    assert manifest["ya_trace_file_count"] == 1
    assert (output_dir / "trace-inputs.files").read_bytes() == (
        b"./cloud/example/tests/test-results/unittest/ytest.report.trace\0"
    )


def test_trace_inputs_skip_symlinks(tmp_path: Path) -> None:
    ya_out = tmp_path / "out"
    trace_path = (
        ya_out
        / "cloud/example/tests"
        / "test-results"
        / "unittest"
        / "ytest.report.trace"
    )
    trace_path.parent.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.write_text("must not be archived")
    trace_path.symlink_to(outside)
    evlog_path = tmp_path / "ya_evlog.jsonl"
    evlog_path.symlink_to(outside)

    inputs = YaTraceInputs.discover(ya_out, evlog_path=evlog_path)
    manifest = inputs.bundle_manifest()

    assert manifest["evlog_file"] is None
    assert manifest["ya_trace_file_count"] == 0
    assert inputs.tar_file_list() == b""


def test_report_loads_only_discovered_evlog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ya_out = tmp_path / "out"
    ya_out.mkdir()
    evlog_target = tmp_path / "outside.jsonl"
    evlog_target.write_text("{}\n")
    evlog_path = tmp_path / "ya_evlog.jsonl"
    evlog_path.symlink_to(evlog_target)
    loaded_paths = []

    def load_evlog(path):
        loaded_paths.append(path)
        return YaEvlog.from_raw()

    def write_bundle(output_dir, trace, **kwargs):
        assert output_dir == tmp_path / "summary"
        assert kwargs["title"]
        assert kwargs["test_log_url_prefix"] == "https://example.test/logs/1/"
        assert kwargs["test_data_url_prefix"] == (
            "https://example.test/test_data/1/workspace/"
        )
        return {"span_count": len(trace)}

    monkeypatch.setattr(ya_trace_report_module, "load_ya_evlog", load_evlog)
    monkeypatch.setattr(
        ya_trace_report_module,
        "write_trace_bundle",
        write_bundle,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ya_trace_report",
            "--ya-out",
            str(ya_out),
            "--evlog",
            str(evlog_path),
            "--output-dir",
            str(tmp_path / "summary"),
            "--attempt-start-ns",
            "0",
            "--attempt-end-ns",
            "1",
            "--exit-code",
            "0",
            "--test-log-url-prefix",
            "https://example.test/logs/1/",
            "--test-data-url-prefix",
            "https://example.test/test_data/1/workspace/",
        ],
    )

    ya_trace_report_module.main()

    assert loaded_paths == [None]


def test_trace_inputs_tar_file_list_handles_unusual_names(
    tmp_path: Path,
) -> None:
    ya_out = tmp_path / "out"
    trace_path = (
        ya_out
        / "-suite\nwith-newline"
        / "test-results"
        / "unittest"
        / "ytest.report.trace"
    )
    trace_path.parent.mkdir(parents=True)
    trace_path.write_text('{"name":"chunk-event","value":{}}\n')

    inputs = YaTraceInputs.discover(ya_out)

    assert inputs.tar_file_list() == (
        b"./-suite\nwith-newline/test-results/unittest/ytest.report.trace\0"
    )
