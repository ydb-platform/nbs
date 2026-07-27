#!/usr/bin/env python3
"""Small, dependency-free OTLP/JSONL trace renderer used by CI reports."""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import html
import json
import math
import re
import urllib.parse
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import islice
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

OTLP_FILE_NAME = "trace.otlp.jsonl.gz"
HTML_FILE_NAME = "trace.html"
MANIFEST_FILE_NAME = "trace.manifest.json"
MAX_INPUT_BYTES = 128 * 1024 * 1024
MAX_SPANS = 100_000
MAX_ATTRIBUTES = 256
MAX_ATTRIBUTE_LENGTH = 8_192
MAX_JSON_LINE_CHARACTERS = 16 * 1024 * 1024
OTLP_SPANS_PER_LINE = 5_000
MAX_UINT64 = (1 << 64) - 1
ID_RE = re.compile(r"^[0-9a-f]+$")
TEMPLATE_DIR = Path(__file__).with_name("templates")
TRACE_HTML_TEMPLATE = TEMPLATE_DIR / "trace_report.html"
TRACE_SCRIPT_TEMPLATE = TEMPLATE_DIR / "trace_report.js"
TRACE_PLACEHOLDER_RE = re.compile(r"@@TRACE_[A-Z_]+@@")


@dataclass
class SpanEvent:
    name: str
    time_ns: int
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class Span:
    trace_id: str
    span_id: str
    name: str
    start_ns: int
    end_ns: int
    parent_span_id: str = ""
    attributes: dict[str, Any] = field(default_factory=dict)
    events: list[SpanEvent] = field(default_factory=list)
    status_code: int = 0
    status_message: str = ""
    resource_attributes: dict[str, Any] = field(default_factory=dict)
    scope_name: str = "nbs.ci"
    scope_version: str = "1"

    @property
    def duration_ns(self) -> int:
        return max(0, self.end_ns - self.start_ns)


def stable_hex_id(*parts: object, length: int) -> str:
    digest = hashlib.sha256("\0".join(str(part) for part in parts).encode()).hexdigest()
    return digest[:length]


def _clean_scalar(value: Any) -> str | bool | int | float:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if value is None:
        return ""
    text = str(value)
    if len(text) > MAX_ATTRIBUTE_LENGTH:
        return f"{text[:MAX_ATTRIBUTE_LENGTH]}…"
    return text


def clean_attributes(values: Mapping[str, Any] | None) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for raw_key, raw_value in list((values or {}).items())[:MAX_ATTRIBUTES]:
        key = str(raw_key)[:256]
        if isinstance(raw_value, (list, tuple)):
            result[key] = [_clean_scalar(value) for value in raw_value[:128]]
        else:
            result[key] = _clean_scalar(raw_value)
    return result


def github_trace_attributes(
    *,
    environment: Mapping[str, Any],
    workflow_run: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build common GitHub trace attributes from an environment or run event."""

    server_url = str(
        environment.get("GITHUB_SERVER_URL") or "https://github.com"
    ).rstrip("/")
    if workflow_run is None:
        values: dict[str, Any] = {
            "github.repository": environment.get("GITHUB_REPOSITORY", ""),
            "github.workflow": environment.get("GITHUB_WORKFLOW", ""),
            "github.workflow.ref": environment.get("GITHUB_WORKFLOW_REF", ""),
            "github.job": environment.get("GITHUB_JOB", ""),
            "github.run.id": environment.get("GITHUB_RUN_ID", ""),
            "github.run.number": environment.get("GITHUB_RUN_NUMBER", ""),
            "github.run.attempt": environment.get("GITHUB_RUN_ATTEMPT", ""),
            "github.event.name": environment.get("GITHUB_EVENT_NAME", ""),
            "github.actor": environment.get("GITHUB_ACTOR", ""),
            "github.sha": environment.get("GITHUB_SHA", ""),
            "github.ref": environment.get("GITHUB_REF", ""),
        }
    else:
        repository = workflow_run.get("repository") or {}
        if not isinstance(repository, Mapping):
            repository = {}
        actor = workflow_run.get("actor") or {}
        if not isinstance(actor, Mapping):
            actor = {}
        triggering_actor = workflow_run.get("triggering_actor") or {}
        if not isinstance(triggering_actor, Mapping):
            triggering_actor = {}
        pull_requests = workflow_run.get("pull_requests") or []
        if not isinstance(pull_requests, list):
            pull_requests = []
        values = {
            "github.repository": repository.get("full_name")
            or environment.get("GITHUB_REPOSITORY", ""),
            "github.workflow": workflow_run.get("name", ""),
            "github.workflow.path": workflow_run.get("path", ""),
            "github.run.id": workflow_run.get("id", ""),
            "github.run.number": workflow_run.get("run_number", ""),
            "github.run.attempt": workflow_run.get("run_attempt", 1),
            "github.run.url": workflow_run.get("html_url", ""),
            "github.event.name": workflow_run.get("event", ""),
            "github.actor": actor.get("login", ""),
            "github.triggering_actor": triggering_actor.get("login", ""),
            "github.sha": workflow_run.get("head_sha", ""),
            "github.ref.name": workflow_run.get("head_branch", ""),
            "github.pull_request.number": [
                item.get("number")
                for item in pull_requests
                if isinstance(item, Mapping) and item.get("number") is not None
            ],
            "ci.conclusion": workflow_run.get("conclusion", ""),
            "ci.display_title": workflow_run.get("display_title", ""),
        }

    repository_name = str(values.get("github.repository") or "")
    run_id = values.get("github.run.id")
    sha = str(values.get("github.sha") or "")
    if repository_name and run_id and not values.get("github.run.url"):
        values["github.run.url"] = (
            f"{server_url}/{repository_name}/actions/runs/"
            f"{urllib.parse.quote(str(run_id), safe='')}"
        )
    if repository_name and sha:
        values["github.commit.url"] = (
            f"{server_url}/{repository_name}/commit/"
            f"{urllib.parse.quote(sha, safe='')}"
        )

    return clean_attributes(
        {
            key: value
            for key, value in values.items()
            if value is not None and value != "" and value != []
        }
    )


def _any_value(value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, (list, tuple)):
        return {"arrayValue": {"values": [_any_value(item) for item in value]}}
    return {"stringValue": str(value)}


def _from_any_value(value: Mapping[str, Any]) -> Any:
    if "stringValue" in value:
        return str(value["stringValue"])
    if "boolValue" in value:
        return bool(value["boolValue"])
    if "intValue" in value:
        return int(value["intValue"])
    if "doubleValue" in value:
        return float(value["doubleValue"])
    if "arrayValue" in value:
        return [
            _from_any_value(item)
            for item in value["arrayValue"].get("values", [])
            if isinstance(item, Mapping)
        ]
    if "kvlistValue" in value:
        return _decode_attributes(value["kvlistValue"].get("values", []))
    return ""


def _encode_attributes(values: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {"key": key, "value": _any_value(value)}
        for key, value in clean_attributes(values).items()
    ]


def _decode_attributes(values: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for item in list(values)[:MAX_ATTRIBUTES]:
        key = str(item.get("key", ""))[:256]
        value = item.get("value")
        if key and isinstance(value, Mapping):
            result[key] = _from_any_value(value)
    return clean_attributes(result)


def _validate_span(span: Span) -> None:
    if (
        len(span.trace_id) != 32
        or not ID_RE.fullmatch(span.trace_id)
        or span.trace_id == "0" * 32
        or len(span.span_id) != 16
        or not ID_RE.fullmatch(span.span_id)
        or span.span_id == "0" * 16
    ):
        raise ValueError(f"Invalid OTLP span IDs for {span.name!r}")
    if span.parent_span_id and (
        len(span.parent_span_id) != 16
        or not ID_RE.fullmatch(span.parent_span_id)
        or span.parent_span_id == "0" * 16
    ):
        raise ValueError(f"Invalid parent span ID for {span.name!r}")
    if (
        span.start_ns < 0
        or span.end_ns < span.start_ns
        or span.start_ns > MAX_UINT64
        or span.end_ns > MAX_UINT64
    ):
        raise ValueError(f"Invalid timestamps for {span.name!r}")
    if any(event.time_ns < 0 or event.time_ns > MAX_UINT64 for event in span.events):
        raise ValueError(f"Invalid event timestamp for {span.name!r}")
    if span.status_code not in {0, 1, 2}:
        raise ValueError(f"Invalid status code for {span.name!r}")


def spans_to_otlp(spans: Sequence[Span]) -> dict[str, Any]:
    """Encode spans as one OTLP TracesData JSON object."""
    resource_groups: dict[str, tuple[dict[str, Any], list[Span]]] = {}
    for span in spans:
        _validate_span(span)
        resource = clean_attributes(span.resource_attributes)
        key = json.dumps(resource, sort_keys=True, separators=(",", ":"))
        resource_groups.setdefault(key, (resource, []))[1].append(span)

    resource_spans: list[dict[str, Any]] = []
    for resource, resource_group in resource_groups.values():
        scope_groups: dict[tuple[str, str], list[Span]] = defaultdict(list)
        for span in resource_group:
            scope_groups[(span.scope_name, span.scope_version)].append(span)

        scope_spans = []
        for (scope_name, scope_version), scope_group in scope_groups.items():
            encoded_spans = []
            for span in scope_group:
                encoded_span: dict[str, Any] = {
                    "traceId": span.trace_id,
                    "spanId": span.span_id,
                    "name": span.name,
                    "kind": 1,
                    "startTimeUnixNano": str(span.start_ns),
                    "endTimeUnixNano": str(span.end_ns),
                    "attributes": _encode_attributes(span.attributes),
                    "events": [
                        {
                            "name": event.name,
                            "timeUnixNano": str(event.time_ns),
                            "attributes": _encode_attributes(event.attributes),
                        }
                        for event in span.events
                    ],
                    "status": {
                        "code": span.status_code,
                        "message": span.status_message[:MAX_ATTRIBUTE_LENGTH],
                    },
                }
                if span.parent_span_id:
                    encoded_span["parentSpanId"] = span.parent_span_id
                encoded_spans.append(encoded_span)

            scope_spans.append(
                {
                    "scope": {"name": scope_name, "version": scope_version},
                    "spans": encoded_spans,
                }
            )

        resource_spans.append(
            {
                "resource": {"attributes": _encode_attributes(resource)},
                "scopeSpans": scope_spans,
            }
        )

    return {"resourceSpans": resource_spans}


def _open_text(path: Path, mode: str):
    if path.suffix == ".gz":
        return gzip.open(path, mode, encoding="utf-8")
    return path.open(mode, encoding="utf-8")


def write_otlp_jsonl(path: Path, spans: Sequence[Span]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _open_text(path, "wt") as stream:
        iterator = iter(spans)
        wrote_batch = False
        while batch := list(islice(iterator, OTLP_SPANS_PER_LINE)):
            json.dump(
                spans_to_otlp(batch),
                stream,
                separators=(",", ":"),
                allow_nan=False,
            )
            stream.write("\n")
            wrote_batch = True
        if not wrote_batch:
            stream.write('{"resourceSpans":[]}\n')


def _decode_otlp_object(payload: Mapping[str, Any]) -> Iterator[Span]:
    for resource_span in payload.get("resourceSpans", []):
        resource = _decode_attributes(
            resource_span.get("resource", {}).get("attributes", [])
        )
        for scope_span in resource_span.get("scopeSpans", []):
            scope = scope_span.get("scope", {})
            scope_name = str(scope.get("name", ""))
            scope_version = str(scope.get("version", ""))
            for raw_span in scope_span.get("spans", []):
                events = [
                    SpanEvent(
                        name=str(event.get("name", "")),
                        time_ns=int(event.get("timeUnixNano", 0)),
                        attributes=_decode_attributes(event.get("attributes", [])),
                    )
                    for event in raw_span.get("events", [])
                ]
                status = raw_span.get("status", {})
                yield Span(
                    trace_id=str(raw_span.get("traceId", "")),
                    span_id=str(raw_span.get("spanId", "")),
                    parent_span_id=str(raw_span.get("parentSpanId", "")),
                    name=str(raw_span.get("name", ""))[:MAX_ATTRIBUTE_LENGTH],
                    start_ns=int(raw_span.get("startTimeUnixNano", 0)),
                    end_ns=int(raw_span.get("endTimeUnixNano", 0)),
                    attributes=_decode_attributes(raw_span.get("attributes", [])),
                    events=events,
                    status_code=int(status.get("code", 0)),
                    status_message=str(status.get("message", ""))[
                        :MAX_ATTRIBUTE_LENGTH
                    ],
                    resource_attributes=resource,
                    scope_name=scope_name,
                    scope_version=scope_version,
                )


def read_otlp_jsonl(
    paths: Iterable[Path],
    *,
    max_input_bytes: int = MAX_INPUT_BYTES,
    max_spans: int = MAX_SPANS,
) -> list[Span]:
    spans: list[Span] = []
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
                    payload = json.loads(line)
                    decoded = list(_decode_otlp_object(payload))
                except (TypeError, ValueError, json.JSONDecodeError) as error:
                    raise ValueError(
                        f"Invalid OTLP JSON in {path}:{line_number}: {error}"
                    ) from error
                spans.extend(decoded)
                if len(spans) > max_spans:
                    raise ValueError(f"OTLP input exceeds {max_spans} spans")

    for span in spans:
        _validate_span(span)
    return spans


def _format_duration(duration_ns: int) -> str:
    duration = duration_ns / 1_000_000_000
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


def _attribute_rows(values: Mapping[str, Any]) -> str:
    if not values:
        return '<p class="muted">No attributes</p>'
    rows = []
    for key, value in sorted(values.items()):
        rendered = (
            json.dumps(value, ensure_ascii=False)
            if not isinstance(value, str)
            else value
        )
        parsed = urllib.parse.urlparse(rendered)
        if (
            isinstance(value, str)
            and parsed.scheme in {"http", "https"}
            and parsed.netloc
        ):
            rendered_value = (
                '<a rel="noopener noreferrer" href="'
                + html.escape(rendered, quote=True)
                + '">'
                + html.escape(rendered)
                + "</a>"
            )
        else:
            rendered_value = html.escape(rendered)
        rows.append(
            "<tr><th>" + html.escape(key) + "</th><td>" + rendered_value + "</td></tr>"
        )
    return '<table class="attributes">' + "".join(rows) + "</table>"


def _trace_model(spans: Sequence[Span]) -> dict[str, Any]:
    """Build the compact, browser-side model used by the static renderer."""
    ordered = sorted(spans, key=lambda span: (span.start_ns, -span.end_ns, span.name))
    if ordered:
        origin_ns = min(span.start_ns for span in ordered)
        end_ns = max(span.end_ns for span in ordered)
    else:
        origin_ns = 0
        end_ns = 1

    trace_ids = list(dict.fromkeys(span.trace_id for span in ordered))
    trace_indexes = {value: index for index, value in enumerate(trace_ids)}
    scopes = list(dict.fromkeys(span.scope_name for span in ordered))
    scope_indexes = {value: index for index, value in enumerate(scopes)}

    resources: list[dict[str, Any]] = []
    resource_indexes: dict[str, int] = {}
    for span in ordered:
        resource = clean_attributes(span.resource_attributes)
        key = json.dumps(resource, sort_keys=True, separators=(",", ":"))
        if key not in resource_indexes:
            resource_indexes[key] = len(resources)
            resources.append(resource)

    by_key = {
        (span.trace_id, span.span_id): index for index, span in enumerate(ordered)
    }
    encoded_spans = []
    for span in ordered:
        parent_index = by_key.get((span.trace_id, span.parent_span_id), -1)
        if parent_index == by_key[(span.trace_id, span.span_id)]:
            parent_index = -1
        resource_key = json.dumps(
            clean_attributes(span.resource_attributes),
            sort_keys=True,
            separators=(",", ":"),
        )
        encoded_spans.append(
            [
                span.span_id,
                parent_index,
                span.name,
                span.start_ns - origin_ns,
                span.duration_ns,
                clean_attributes(span.attributes),
                [
                    [
                        event.name,
                        event.time_ns - span.start_ns,
                        clean_attributes(event.attributes),
                    ]
                    for event in span.events
                ],
                span.status_code,
                span.status_message,
                resource_indexes[resource_key],
                scope_indexes[span.scope_name],
                trace_indexes[span.trace_id],
                span.parent_span_id if parent_index < 0 else "",
            ]
        )

    return {
        "v": 1,
        "o": str(origin_ns),
        "d": max(1, end_ns - origin_ns),
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


def _render_trace_template(values: Mapping[str, str]) -> str:
    template = TRACE_HTML_TEMPLATE.read_text(encoding="utf-8")
    placeholders = set(TRACE_PLACEHOLDER_RE.findall(template))
    if placeholders != set(values):
        missing = sorted(placeholders - set(values))
        unused = sorted(set(values) - placeholders)
        raise ValueError(
            f"Trace template placeholder mismatch: missing={missing}, unused={unused}"
        )
    return TRACE_PLACEHOLDER_RE.sub(lambda match: values[match.group()], template)


def render_html(
    spans: Sequence[Span],
    *,
    title: str = "CI execution trace",
    metadata: Mapping[str, Any] | None = None,
) -> str:
    model = _trace_model(spans)
    script = TRACE_SCRIPT_TEMPLATE.read_text(encoding="utf-8")
    if "</script" in script.lower():
        raise ValueError(
            "Trace report JavaScript must not contain a closing script tag"
        )
    failures = sum(span.status_code == 2 for span in spans)
    return _render_trace_template(
        {
            "@@TRACE_TITLE@@": html.escape(title),
            "@@TRACE_SUMMARY@@": (
                f"{len(spans):,} spans · {failures:,} errors · "
                f"{_format_duration(model['d'])}"
            ),
            "@@TRACE_METADATA@@": _attribute_rows(clean_attributes(metadata)),
            "@@TRACE_PAYLOAD@@": _trace_payload(model),
            "@@TRACE_SCRIPT@@": script,
        }
    )


def write_trace_bundle(
    output_dir: Path,
    spans: Sequence[Span],
    *,
    title: str,
    metadata: Mapping[str, Any] | None = None,
    file_prefix: str = "trace",
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    otlp_name = f"{file_prefix}.otlp.jsonl.gz"
    html_name = f"{file_prefix}.html"
    manifest_name = f"{file_prefix}.manifest.json"
    write_otlp_jsonl(output_dir / otlp_name, spans)
    (output_dir / html_name).write_text(
        render_html(spans, title=title, metadata=metadata),
        encoding="utf-8",
    )

    manifest = {
        "schema": "nbs-otlp-trace-bundle",
        "schema_version": 1,
        "otlp_format": "application/x-ndjson+gzip; message=opentelemetry.proto.trace.v1.TracesData",
        "otlp_file": otlp_name,
        "html_file": html_name,
        "span_count": len(spans),
        "trace_count": len({span.trace_id for span in spans}),
        "start_time_unix_nano": str(min((span.start_ns for span in spans), default=0)),
        "end_time_unix_nano": str(max((span.end_ns for span in spans), default=0)),
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
    spans = read_otlp_jsonl(args.input)
    args.output.write_text(
        render_html(spans, title=args.title, metadata={"source": args.input}),
        encoding="utf-8",
    )


if __name__ == "__main__":
    _main()
