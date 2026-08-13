#!/usr/bin/env python3
"""Small OTLP/JSONL trace renderer used by CI reports."""

from __future__ import annotations

import argparse
import base64
import gzip
import json
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit, urlunsplit

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
from .trace_io import read_otlp_jsonl, write_otlp_jsonl

OTLP_FILE_NAME = "trace.otlp.jsonl.gz"
HTML_FILE_NAME = "trace.html"
MANIFEST_FILE_NAME = "trace.manifest.json"
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
TEST_LOG_URL_PREFIX_ATTRIBUTE = "ci.artifact.test_log.url_prefix"
TEST_DATA_URL_PREFIX_ATTRIBUTE = "ci.artifact.test_data.url_prefix"


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


def _http_url_prefix(value: str) -> str:
    if not value or "\x00" in value:
        return ""
    try:
        parsed = urlsplit(value)
        if (
            parsed.scheme.lower() not in {"http", "https"}
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            return ""
        path = parsed.path
        if not path.endswith("/"):
            path += "/"
        return urlunsplit((parsed.scheme.lower(), parsed.netloc, path, "", ""))
    except ValueError:
        return ""


def artifact_url_resource_attributes(
    *,
    test_log_url_prefix: str = "",
    test_data_url_prefix: str = "",
) -> dict[str, str]:
    return {
        key: prefix
        for key, prefix in (
            (TEST_LOG_URL_PREFIX_ATTRIBUTE, _http_url_prefix(test_log_url_prefix)),
            (TEST_DATA_URL_PREFIX_ATTRIBUTE, _http_url_prefix(test_data_url_prefix)),
        )
        if prefix
    }


def _trace_model(
    trace: Trace,
    *,
    test_log_url_prefix: str = "",
    test_data_url_prefix: str = "",
) -> dict[str, Any]:
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
                (start_ns - origin_ns).value,
                span_duration_ns(span).value,
                decode_attributes(span.attributes),
                [
                    [
                        str(event.name or ""),
                        (Ns(event.time_unix_nano or 0) - start_ns).value,
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

    artifact_urls = {
        key: prefix
        for key, prefix in (
            ("testLog", _http_url_prefix(test_log_url_prefix)),
            ("testData", _http_url_prefix(test_data_url_prefix)),
        )
        if prefix
    }
    return {
        "v": 1,
        "o": str(origin_ns.value),
        "d": max(1, (end_ns - origin_ns).value),
        "r": resources,
        "t": trace_ids,
        "c": scopes,
        "s": encoded_spans,
        "u": artifact_urls,
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
    test_log_url_prefix: str = "",
    test_data_url_prefix: str = "",
) -> str:
    model = _trace_model(
        trace,
        test_log_url_prefix=test_log_url_prefix,
        test_data_url_prefix=test_data_url_prefix,
    )
    failures = sum(span_status_code(span) == 2 for span in trace)
    return TRACE_TEMPLATE_ENV.get_template(TRACE_HTML_TEMPLATE.name).render(
        title=title,
        summary=(
            f"{len(trace):,} spans · {failures:,} errors · "
            f"{_format_duration(Ns(model['d']))}"
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
    test_log_url_prefix: str = "",
    test_data_url_prefix: str = "",
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    otlp_name = f"{file_prefix}.otlp.jsonl.gz"
    html_name = f"{file_prefix}.html"
    manifest_name = f"{file_prefix}.manifest.json"
    write_otlp_jsonl(output_dir / otlp_name, trace)
    (output_dir / html_name).write_text(
        render_html(
            trace,
            title=title,
            metadata=metadata,
            test_log_url_prefix=test_log_url_prefix,
            test_data_url_prefix=test_data_url_prefix,
        ),
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
                default=0,
            )
        ),
        "end_time_unix_nano": str(
            max(
                (span.end_time_unix_nano or 0 for span in trace),
                default=0,
            )
        ),
        "metadata": clean_attributes(metadata),
    }
    (output_dir / manifest_name).write_text(
        json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return manifest


def add_artifact_url_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--test-log-url-prefix",
        default="",
        help="Base URL for failed ya test and chunk log files",
    )
    parser.add_argument(
        "--test-data-url-prefix",
        default="",
        help="Base URL for failed ya test and chunk data directories",
    )


def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", nargs="+", type=Path, help="OTLP JSONL[.gz] file")
    parser.add_argument("-o", "--output", required=True, type=Path)
    parser.add_argument("--title", default="CI execution trace")
    add_artifact_url_arguments(parser)
    args = parser.parse_args()
    trace = read_otlp_jsonl(args.input)
    args.output.write_text(
        render_html(
            trace,
            title=args.title,
            metadata={"source": args.input},
            test_log_url_prefix=args.test_log_url_prefix,
            test_data_url_prefix=args.test_data_url_prefix,
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    _main()
