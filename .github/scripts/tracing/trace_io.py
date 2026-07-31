from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Iterable

from .otlp import Trace

MAX_INPUT_BYTES = 512 * 1024 * 1024
MAX_SPANS = 500_000
MAX_JSON_LINE_CHARACTERS = 16 * 1024 * 1024
OTLP_SPANS_PER_LINE = 5_000


def _open_text(path: Path, mode: str):
    if path.suffix == ".gz":
        return gzip.open(path, mode, encoding="utf-8")
    return path.open(mode, encoding="utf-8")


def _write_otlp_batch(stream, trace: Trace) -> None:
    encoded = json.dumps(
        trace.to_dict(),
        separators=(",", ":"),
        allow_nan=False,
    )
    if len(encoded) + 1 <= MAX_JSON_LINE_CHARACTERS:
        stream.write(encoded)
        stream.write("\n")
        return

    span_count = len(trace)
    if span_count <= 1:
        raise ValueError(
            "A single OTLP span exceeds "
            f"{MAX_JSON_LINE_CHARACTERS} JSON line characters"
        )
    for batch in trace.batches((span_count + 1) // 2):
        _write_otlp_batch(stream, batch)


def write_otlp_jsonl(path: Path, trace: Trace) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _open_text(path, "wt") as stream:
        wrote_batch = False
        for batch in trace.batches(OTLP_SPANS_PER_LINE):
            _write_otlp_batch(stream, batch)
            wrote_batch = True
        if not wrote_batch:
            stream.write('{"resourceSpans":[]}\n')


def read_otlp_jsonl(
    paths: Iterable[Path],
    *,
    max_input_bytes: int = MAX_INPUT_BYTES,
    max_spans: int = MAX_SPANS,
) -> Trace:
    trace = Trace()
    input_bytes = 0
    decoded_characters = 0
    for path in paths:
        input_bytes += path.stat().st_size
        if input_bytes > max_input_bytes:
            raise ValueError(f"OTLP input exceeds {max_input_bytes} bytes")
        with _open_text(path, "rt") as stream:
            line_number = 0
            while True:
                line = stream.readline(MAX_JSON_LINE_CHARACTERS + 1)
                if not line:
                    break
                line_number += 1
                if len(line) > MAX_JSON_LINE_CHARACTERS:
                    raise ValueError(
                        f"OTLP JSON line exceeds {MAX_JSON_LINE_CHARACTERS} "
                        f"characters in {path}:{line_number}"
                    )
                decoded_characters += len(line)
                if decoded_characters > max_input_bytes:
                    raise ValueError(
                        f"Decoded OTLP input exceeds {max_input_bytes} characters"
                    )
                if not line.strip():
                    continue
                try:
                    trace.extend(Trace.from_json(line))
                except (TypeError, ValueError, json.JSONDecodeError) as error:
                    raise ValueError(
                        f"Invalid OTLP JSON in {path}:{line_number}: {error}"
                    ) from error
                if len(trace) > max_spans:
                    raise ValueError(f"OTLP input exceeds {max_spans} spans")

    trace.validate()
    return trace
