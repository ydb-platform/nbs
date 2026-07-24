#!/usr/bin/env python3
"""Small, dependency-free OTLP/JSONL trace renderer used by CI reports."""

from __future__ import annotations

import argparse
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
    if any(
        event.time_ns < 0 or event.time_ns > MAX_UINT64 for event in span.events
    ):
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
        rendered = json.dumps(value, ensure_ascii=False) if not isinstance(
            value, str
        ) else value
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
            "<tr><th>"
            + html.escape(key)
            + "</th><td>"
            + rendered_value
            + "</td></tr>"
        )
    return '<table class="attributes">' + "".join(rows) + "</table>"


def _span_row(span: Span, depth: int, origin_ns: int, total_ns: int) -> str:
    left = 100 * max(0, span.start_ns - origin_ns) / total_ns
    width = max(0.15, 100 * span.duration_ns / total_ns)
    status = "error" if span.status_code == 2 else "ok" if span.status_code == 1 else ""
    event_rows = "".join(
        "<li><code>"
        + html.escape(_format_duration(max(0, event.time_ns - span.start_ns)))
        + "</code> "
        + html.escape(event.name)
        + _attribute_rows(event.attributes)
        + "</li>"
        for event in span.events
    )
    details = _attribute_rows(span.attributes)
    if event_rows:
        details += "<h4>Events</h4><ul class=\"events\">" + event_rows + "</ul>"
    resource = _attribute_rows(span.resource_attributes)
    search = " ".join(
        [
            span.name,
            span.status_message,
            *(f"{key}={value}" for key, value in span.attributes.items()),
        ]
    ).lower()
    return f"""
<details class="span {status}" data-search="{html.escape(search, quote=True)}">
  <summary>
    <span class="name" style="padding-left:{depth * 1.1:.1f}rem">{html.escape(span.name)}</span>
    <span class="duration">{html.escape(_format_duration(span.duration_ns))}</span>
    <span class="track"><span class="bar" style="left:{left:.4f}%;width:{width:.4f}%"></span></span>
  </summary>
  <div class="detail">
    <p><b>Span ID:</b> <code>{span.span_id}</code>
       <b>Parent:</b> <code>{span.parent_span_id or "none"}</code>
       <b>Status:</b> {span.status_code} {html.escape(span.status_message)}</p>
    <h4>Attributes</h4>{details}
    <details><summary>Resource attributes</summary>{resource}</details>
  </div>
</details>"""


def render_html(
    spans: Sequence[Span],
    *,
    title: str = "CI execution trace",
    metadata: Mapping[str, Any] | None = None,
) -> str:
    ordered = sorted(spans, key=lambda span: (span.start_ns, -span.end_ns, span.name))
    if ordered:
        origin_ns = min(span.start_ns for span in ordered)
        end_ns = max(span.end_ns for span in ordered)
    else:
        origin_ns = 0
        end_ns = 1
    total_ns = max(1, end_ns - origin_ns)

    by_id = {span.span_id: span for span in ordered}
    depths: dict[str, int] = {}

    def depth(span: Span) -> int:
        if span.span_id in depths:
            return depths[span.span_id]
        seen = {span.span_id}
        value = 0
        current = span
        while value < 20:
            parent = by_id.get(current.parent_span_id)
            if parent is None or parent.span_id in seen:
                break
            seen.add(parent.span_id)
            value += 1
            current = parent
        depths[span.span_id] = value
        return value

    rows = "".join(
        _span_row(span, depth(span), origin_ns, total_ns) for span in ordered
    )
    metadata_table = _attribute_rows(clean_attributes(metadata))
    failures = sum(span.status_code == 2 for span in ordered)
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'">
<title>{html.escape(title)}</title>
<style>
:root {{ color-scheme:light dark; --bg:#fff; --fg:#172033; --muted:#667085;
  --line:#d8dee9; --panel:#f7f8fa; --bar:#6d5ce7; --bad:#d92d20; }}
@media (prefers-color-scheme:dark) {{ :root {{ --bg:#11151c; --fg:#e8edf5;
  --muted:#9ca9bd; --line:#344054; --panel:#1b2230; --bar:#9b8cff; --bad:#ff6b64; }} }}
* {{ box-sizing:border-box }} body {{ margin:0; background:var(--bg); color:var(--fg);
  font:14px/1.45 system-ui,sans-serif }} main {{ max-width:1800px; margin:auto; padding:1.5rem }}
h1 {{ margin:.2rem 0 }} .muted {{ color:var(--muted) }} .toolbar {{ position:sticky;
  top:0; z-index:2; display:flex; gap:.6rem; padding:.7rem 0; background:var(--bg) }}
input,button {{ color:inherit; background:var(--panel); border:1px solid var(--line);
  border-radius:6px; padding:.45rem .7rem }} input {{ min-width:20rem }}
.trace {{ border:1px solid var(--line); border-radius:8px; overflow:hidden }}
.span {{ border-bottom:1px solid var(--line) }} .span:last-child {{ border:0 }}
.span[hidden] {{ display:none }} summary {{ display:grid; grid-template-columns:minmax(20rem,38%)
  7rem 1fr; align-items:center; gap:.7rem; padding:.4rem .6rem; cursor:pointer }}
summary:hover {{ background:var(--panel) }} .name {{ overflow:hidden; text-overflow:ellipsis;
  white-space:nowrap }} .duration {{ text-align:right; font-variant-numeric:tabular-nums }}
.track {{ position:relative; height:.8rem; background:var(--panel); border-radius:4px }}
.bar {{ position:absolute; top:0; bottom:0; border-radius:4px; background:var(--bar);
  min-width:2px }} .error .bar {{ background:var(--bad) }} .error > summary .name {{ color:var(--bad) }}
.detail {{ padding:.5rem 1rem 1rem 2rem; overflow:auto }} table {{ border-collapse:collapse }}
th,td {{ border-bottom:1px solid var(--line); padding:.25rem .5rem; text-align:left;
  vertical-align:top }} th {{ color:var(--muted); white-space:nowrap }} td {{ word-break:break-word }}
.attributes {{ width:100%; max-width:100rem }} code {{ font-family:ui-monospace,monospace }}
.events {{ padding-left:1.5rem }} h4 {{ margin:.8rem 0 .3rem }}
@media (max-width:850px) {{ summary {{ grid-template-columns:1fr 6rem }} .track {{ display:none }}
  input {{ min-width:8rem; flex:1 }} }}
</style>
</head>
<body><main>
<h1>{html.escape(title)}</h1>
<p class="muted">{len(ordered):,} spans · {failures:,} errors · {_format_duration(total_ns)}</p>
<details><summary>Run metadata</summary>{metadata_table}</details>
<div class="toolbar">
  <input id="filter" type="search" placeholder="Filter spans or attributes">
  <button id="expand" type="button">Expand all</button>
  <button id="collapse" type="button">Collapse all</button>
</div>
<section class="trace">{rows or '<p class="muted">No spans found.</p>'}</section>
</main>
<script>
const spans=[...document.querySelectorAll('.span')];
document.getElementById('filter').addEventListener('input',event=>{{
  const query=event.target.value.trim().toLowerCase();
  spans.forEach(span=>span.hidden=query && !span.dataset.search.includes(query));
}});
document.getElementById('expand').addEventListener('click',()=>spans.forEach(span=>span.open=true));
document.getElementById('collapse').addEventListener('click',()=>spans.forEach(span=>span.open=false));
</script>
</body></html>
"""


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
