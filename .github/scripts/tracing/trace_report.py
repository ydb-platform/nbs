#!/usr/bin/env python3
"""Small OTLP/JSONL trace renderer used by CI reports."""

from __future__ import annotations

import argparse
import base64
import gzip
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape

from .otlp import (
    Trace,
    clean_attributes,
    decode_attributes,
    Ns,
    span_duration_ns,
    span_status_code,
    span_status_message,
)

OTLP_FILE_NAME = "trace.otlp.jsonl.gz"
HTML_FILE_NAME = "trace.html"
MANIFEST_FILE_NAME = "trace.manifest.json"
MAX_INPUT_BYTES = 128 * 1024 * 1024
MAX_SPANS = 100_000
MAX_JSON_LINE_CHARACTERS = 16 * 1024 * 1024
OTLP_SPANS_PER_LINE = 5_000
TEMPLATE_DIR = Path(__file__).with_name("templates")
TRACE_HTML_TEMPLATE = TEMPLATE_DIR / "trace_report.html"
TRACE_SCRIPT_TEMPLATE = TEMPLATE_DIR / "trace_report.js"
TRACE_TEMPLATE_ENV = Environment(
    loader=FileSystemLoader(str(TEMPLATE_DIR)),
    autoescape=select_autoescape(["html"]),
    undefined=StrictUndefined,
)
TRACE_TEMPLATE_ENV.policies["json.dumps_kwargs"] = {
    "ensure_ascii": False,
    "sort_keys": True,
}


def _open_text(path: Path, mode: str):
    if path.suffix == ".gz":
        return gzip.open(path, mode, encoding="utf-8")
    return path.open(mode, encoding="utf-8")


def write_otlp_jsonl(path: Path, trace: Trace) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _open_text(path, "wt") as stream:
        wrote_batch = False
        for batch in trace.batches(OTLP_SPANS_PER_LINE):
            json.dump(
                batch.to_dict(),
                stream,
                separators=(",", ":"),
                allow_nan=False,
            )
            stream.write("\n")
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


def _format_duration(duration_ns: Ns) -> str:
    duration = duration_ns.to_s()
    if duration < 0.001:
        return f"{duration * 1_000_000:.0f} µs"
    if duration < 1:
        return f"{duration * 1_000:.1f} ms"
    if duration < 60:
        return f"{duration:.3f} s"
    minutes, seconds = divmod(duration, 60)
    if minutes < 60:
        return f"{int(minutes)}m {seconds:.1f}s"
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m {seconds:.0f}s"


def _trace_model(trace: Trace) -> dict[str, Any]:
    """Build the compact, browser-side model used by the static renderer."""
    ordered = sorted(
        trace.walk(),
        key=lambda item: (
            item[2].start_time_unix_nano or 0,
            -(item[2].end_time_unix_nano or 0),
            item[2].name or "",
        ),
    )
    if ordered:
        origin_ns = Ns(min(span.start_time_unix_nano or 0 for _, _, span in ordered))
        end_ns = Ns(max(span.end_time_unix_nano or 0 for _, _, span in ordered))
    else:
        origin_ns = Ns(0)
        end_ns = Ns(1)

    trace_ids = list(dict.fromkeys(span.trace_id.hex() for _, _, span in ordered))
    trace_indexes = {value: index for index, value in enumerate(trace_ids)}
    scopes = list(dict.fromkeys(str(scope.name or "") for _, scope, _ in ordered))
    scope_indexes = {value: index for index, value in enumerate(scopes)}

    resources: list[dict[str, Any]] = []
    resource_indexes: dict[str, int] = {}
    for resource, _, _ in ordered:
        attributes = decode_attributes(resource.attributes)
        key = json.dumps(attributes, sort_keys=True, separators=(",", ":"))
        if key not in resource_indexes:
            resource_indexes[key] = len(resources)
            resources.append(attributes)

    by_key = {
        (span.trace_id, span.span_id): index
        for index, (_, _, span) in enumerate(ordered)
    }
    encoded_spans = []
    for resource, scope, span in ordered:
        parent_index = by_key.get((span.trace_id, span.parent_span_id), -1)
        if parent_index == by_key[(span.trace_id, span.span_id)]:
            parent_index = -1
        resource_key = json.dumps(
            decode_attributes(resource.attributes),
            sort_keys=True,
            separators=(",", ":"),
        )
        start_ns = Ns(span.start_time_unix_nano or 0)
        encoded_spans.append(
            [
                span.span_id.hex(),
                parent_index,
                str(span.name or ""),
                start_ns - origin_ns,
                span_duration_ns(span),
                decode_attributes(span.attributes),
                [
                    [
                        str(event.name or ""),
                        Ns(event.time_unix_nano or 0) - start_ns,
                        decode_attributes(event.attributes),
                    ]
                    for event in span.events
                ],
                span_status_code(span),
                span_status_message(span),
                resource_indexes[resource_key],
                scope_indexes[str(scope.name or "")],
                trace_indexes[span.trace_id.hex()],
                span.parent_span_id.hex() if parent_index < 0 else "",
            ]
        )

    return {
        "v": 1,
        "o": str(origin_ns),
        "d": Ns(max(1, end_ns - origin_ns)),
        "r": resources,
        "t": trace_ids,
        "c": scopes,
        "s": encoded_spans,
    }


def _trace_payload(model: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        model,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return base64.b64encode(gzip.compress(encoded, compresslevel=9, mtime=0)).decode()


def render_html(
    trace: Trace,
    *,
    title: str = "CI execution trace",
    metadata: Mapping[str, Any] | None = None,
) -> str:
    model = _trace_model(trace)
    failures = sum(span_status_code(span) == 2 for span in trace)
    return TRACE_TEMPLATE_ENV.get_template(TRACE_HTML_TEMPLATE.name).render(
        title=title,
        summary=(
            f"{len(trace):,} spans · {failures:,} errors · "
            f"{_format_duration(model['d'])}"
        ),
        metadata=clean_attributes(metadata),
        payload=_trace_payload(model),
    )


def write_trace_bundle(
    output_dir: Path,
    trace: Trace,
    *,
    title: str,
    metadata: Mapping[str, Any] | None = None,
    file_prefix: str = "trace",
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    otlp_name = f"{file_prefix}.otlp.jsonl.gz"
    html_name = f"{file_prefix}.html"
    manifest_name = f"{file_prefix}.manifest.json"
    write_otlp_jsonl(output_dir / otlp_name, trace)
    (output_dir / html_name).write_text(
        render_html(trace, title=title, metadata=metadata),
        encoding="utf-8",
    )

    manifest = {
        "schema": "nbs-otlp-trace-bundle",
        "schema_version": 1,
        "otlp_format": "application/x-ndjson+gzip; message=opentelemetry.proto.trace.v1.TracesData",
        "otlp_file": otlp_name,
        "html_file": html_name,
        "span_count": len(trace),
        "trace_count": len({span.trace_id for span in trace}),
        "start_time_unix_nano": str(
            min(
                (span.start_time_unix_nano or 0 for span in trace),
                default=Ns(0),
            )
        ),
        "end_time_unix_nano": str(
            max(
                (span.end_time_unix_nano or 0 for span in trace),
                default=Ns(0),
            )
        ),
        "metadata": clean_attributes(metadata),
    }
    (output_dir / manifest_name).write_text(
        json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return manifest


def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", nargs="+", type=Path, help="OTLP JSONL[.gz] file")
    parser.add_argument("-o", "--output", required=True, type=Path)
    parser.add_argument("--title", default="CI execution trace")
    args = parser.parse_args()
    trace = read_otlp_jsonl(args.input)
    args.output.write_text(
        render_html(trace, title=args.title, metadata={"source": args.input}),
        encoding="utf-8",
    )


if __name__ == "__main__":
    _main()
