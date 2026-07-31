from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

import scripts.tracing.trace_io as trace_io_module
from scripts.tracing.otlp import (
    ResourceAttributes,
    Span,
    Trace,
    make_span as new_span,
    stable_span_id,
    stable_trace_id,
)
from scripts.tracing.trace_io import (
    MAX_INPUT_BYTES,
    MAX_SPANS,
    read_otlp_jsonl,
)


def make_span(**kwargs) -> Span:
    values = {
        "trace_id": bytes.fromhex("1" * 32),
        "span_id": bytes.fromhex("2" * 16),
        "name": "root",
        "start_ns": 1_000,
        "end_ns": 2_000,
    }
    values.update(kwargs)
    return new_span(**values)


def test_renderer_limits_allow_merging_detailed_nightly_traces() -> None:
    # One observed test artifact is already ~87k spans / ~90M decoded bytes;
    # the workflow report must still be able to merge build and job traces.
    assert MAX_SPANS >= 500_000
    assert MAX_INPUT_BYTES >= 512 * 1024 * 1024


def test_otlp_writer_splits_batches_to_respect_reader_line_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(trace_io_module, "MAX_JSON_LINE_CHARACTERS", 1_200)
    trace = Trace()
    for index in range(12):
        trace.add_span(
            make_span(
                span_id=(index + 1).to_bytes(8, "big"),
                name=f"span-{index}",
                attributes={"payload": "x" * 300},
            ),
            resource=ResourceAttributes(),
            scope_name="test",
        )
    path = tmp_path / "trace.otlp.jsonl.gz"

    trace_io_module.write_otlp_jsonl(path, trace)

    with gzip.open(path, "rt", encoding="utf-8") as stream:
        lines = stream.readlines()
    assert len(lines) > 1
    assert max(map(len, lines)) <= 1_200
    assert len(read_otlp_jsonl([path])) == len(trace)


def test_reader_rejects_invalid_ids(tmp_path: Path) -> None:
    path = tmp_path / "bad.jsonl"
    path.write_text(
        json.dumps(
            {
                "resourceSpans": [
                    {
                        "scopeSpans": [
                            {
                                "spans": [
                                    {
                                        "traceId": "not-a-trace-id",
                                        "spanId": "2" * 16,
                                        "name": "bad",
                                        "startTimeUnixNano": "1",
                                        "endTimeUnixNano": "2",
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        )
        + "\n"
    )
    with pytest.raises(ValueError, match="Invalid OTLP JSON.*Invalid hex string"):
        read_otlp_jsonl([path])


def test_reader_limits_decompressed_input(tmp_path: Path) -> None:
    path = tmp_path / "compressed.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        stream.write(" " * 1_000 + "{}\n")
    assert path.stat().st_size < 200
    with pytest.raises(ValueError, match="Decoded OTLP input exceeds"):
        read_otlp_jsonl([path], max_input_bytes=200)


def test_span_ids_are_stable_and_have_otlp_lengths() -> None:
    assert stable_span_id("run", 1) == stable_span_id("run", 1)
    assert len(stable_span_id("run")) == 8
    assert len(stable_trace_id("run")) == 16
