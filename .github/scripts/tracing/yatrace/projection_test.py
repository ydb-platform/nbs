import json
from pathlib import Path

from scripts.tracing.yatrace import load_ya_evlog


def test_malformed_evlog_marks_trace_incomplete_without_losing_valid_records(
    tmp_path: Path,
) -> None:
    evlog_path = tmp_path / "ya_evlog.jsonl"
    valid_records = [
        {
            "namespace": "stages",
            "event": "stage-finished",
            "value": {"name": "configure", "tag": "configure", "time": [0, 1]},
        },
        {
            "namespace": "worker_threads",
            "event": "node-finished",
            "value": {"name": "Run(tool)", "tag": "CC", "time": [1, 2]},
        },
    ]
    evlog_path.write_text(
        f"{json.dumps(valid_records[0])}\n{{malformed\n"
        f"{json.dumps(valid_records[1])}\n"
    )

    evlog = load_ya_evlog(evlog_path)
    assert len(evlog.stages) == 1
    assert len(evlog.nodes) == 1
    assert evlog.malformed_json_record_count == 1
