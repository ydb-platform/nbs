from __future__ import annotations

import json
from pathlib import Path

from scripts.tests.ya_trace import YaTraceCollection


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
