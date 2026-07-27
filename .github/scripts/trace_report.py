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


def render_html(
    spans: Sequence[Span],
    *,
    title: str = "CI execution trace",
    metadata: Mapping[str, Any] | None = None,
) -> str:
    model = _trace_model(spans)
    total_ns = model["d"]
    metadata_table = _attribute_rows(clean_attributes(metadata))
    failures = sum(span.status_code == 2 for span in spans)
    payload = _trace_payload(model)
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'">
<title>{html.escape(title)}</title>
<style>
:root {{ color-scheme:light dark; --bg:#fff; --fg:#172033; --muted:#667085;
  --line:#d8dee9; --panel:#f7f8fa; --bar:#6d5ce7; --bad:#d92d20;
  --selected:#eef2ff; }}
@media (prefers-color-scheme:dark) {{ :root {{ --bg:#11151c; --fg:#e8edf5;
  --muted:#9ca9bd; --line:#344054; --panel:#1b2230; --bar:#9b8cff;
  --bad:#ff6b64; --selected:#252d47; }} }}
* {{ box-sizing:border-box }} body {{ margin:0; background:var(--bg); color:var(--fg);
  font:14px/1.45 system-ui,sans-serif }} main {{ max-width:1800px; margin:auto; padding:1.5rem }}
h1 {{ margin:.2rem 0 }} .muted {{ color:var(--muted) }} .toolbar {{ position:sticky;
  top:0; z-index:2; display:flex; align-items:center; gap:.6rem; padding:.7rem 0;
  background:var(--bg) }}
input,button {{ color:inherit; background:var(--panel); border:1px solid var(--line);
  border-radius:6px; padding:.45rem .7rem }} input {{ min-width:20rem }}
.trace {{ border:1px solid var(--line); border-radius:8px; overflow:hidden }}
.trace-head,.span-row {{ display:grid; grid-template-columns:minmax(20rem,38%) 7rem 1fr;
  align-items:center; gap:.7rem; min-height:2.25rem; padding:.35rem .6rem }}
.trace-head {{ color:var(--muted); background:var(--panel); font-size:.8rem;
  font-weight:600; text-transform:uppercase }}
.span-row {{ border-top:1px solid var(--line) }} .span-row:hover {{ background:var(--panel) }}
.span-row.selected {{ background:var(--selected) }}
.name-cell {{ display:flex; align-items:center; min-width:0 }}
.toggle {{ flex:none; width:1.65rem; padding:.15rem; border:0; background:transparent }}
.toggle:disabled {{ opacity:0; cursor:default }}
.span-name {{ min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;
  padding:.15rem .25rem; border:0; background:transparent; text-align:left; cursor:pointer }}
.duration {{ text-align:right; font-variant-numeric:tabular-nums }}
.track {{ position:relative; height:.8rem; background:var(--panel); border-radius:4px }}
.bar {{ position:absolute; top:0; bottom:0; border-radius:4px; background:var(--bar);
  min-width:2px }} .error .bar {{ background:var(--bad) }}
.error .span-name {{ color:var(--bad) }}
.more-row {{ border-top:1px solid var(--line); padding:.35rem .6rem }}
.more-row button {{ margin-left:var(--indent) }}
.detail-panel {{ margin:.3rem 0 .8rem; padding:1rem; border:1px solid var(--line);
  border-radius:8px; background:var(--panel); overflow:auto }}
.detail-head {{ display:flex; align-items:start; justify-content:space-between; gap:1rem }}
.detail-head h2 {{ margin:0; font-size:1.1rem }} .detail-facts {{ margin:.4rem 0 }}
table {{ border-collapse:collapse }}
th,td {{ border-bottom:1px solid var(--line); padding:.25rem .5rem; text-align:left;
  vertical-align:top }} th {{ color:var(--muted); white-space:nowrap }} td {{ word-break:break-word }}
.attributes {{ width:100%; max-width:100rem }} code {{ font-family:ui-monospace,monospace }}
.events {{ padding-left:1.5rem }} h3,h4 {{ margin:.8rem 0 .3rem }}
#loading {{ padding:1rem }} [hidden] {{ display:none!important }}
@media (max-width:850px) {{ .trace-head,.span-row {{ grid-template-columns:1fr 6rem }}
  .track,.trace-head span:last-child {{ display:none }} input {{ min-width:8rem; flex:1 }}
  .toolbar {{ flex-wrap:wrap }} }}
</style>
</head>
<body><main>
<h1>{html.escape(title)}</h1>
<p class="muted">{len(spans):,} spans · {failures:,} errors · {_format_duration(total_ns)}</p>
<details><summary>Run metadata</summary>{metadata_table}</details>
<div class="toolbar">
  <input id="filter" type="search" placeholder="Filter spans or attributes" disabled>
  <button id="expand" type="button" disabled>Expand all</button>
  <button id="collapse" type="button" disabled>Collapse all</button>
  <span id="filter-status" class="muted"></span>
</div>
<section id="detail-panel" class="detail-panel" hidden>
  <div class="detail-head">
    <h2 id="detail-title"></h2>
    <button id="detail-close" type="button">Close</button>
  </div>
  <div id="detail-content"></div>
</section>
<section class="trace">
  <div class="trace-head"><span>Name</span><span>Duration</span><span>Waterfall</span></div>
  <div id="rows"><p id="loading" class="muted">Loading compressed trace data…</p></div>
</section>
</main>
<script id="trace-data" type="application/octet-stream">{payload}</script>
<script>
const ID=0,PARENT=1,NAME=2,START=3,DURATION=4,ATTRS=5,EVENTS=6,STATUS=7,
  STATUS_MESSAGE=8,RESOURCE=9,SCOPE=10,TRACE=11,ORPHAN_PARENT=12;
const PAGE_SIZE=200;
const INITIAL_RENDER_LIMIT=2000;
const COLLAPSED_SCOPES=new Set(['ya.build','ya.chunk']);
const rowsElement=document.getElementById('rows');
const filterElement=document.getElementById('filter');
const filterStatus=document.getElementById('filter-status');
const detailPanel=document.getElementById('detail-panel');
const detailTitle=document.getElementById('detail-title');
const detailContent=document.getElementById('detail-content');
let model,spans,children,roots,expanded,limits,visible,selected=null,searchCache=[];
let renderLimit=INITIAL_RENDER_LIMIT;

function formatDuration(durationNs){{
  const duration=durationNs/1e9;
  if(duration<.001)return `${{(duration*1e6).toFixed(0)}} µs`;
  if(duration<1)return `${{(duration*1e3).toFixed(1)}} ms`;
  if(duration<60)return `${{duration.toFixed(3)}} s`;
  const minutes=Math.floor(duration/60),seconds=duration-minutes*60;
  if(minutes<60)return `${{minutes}}m ${{seconds.toFixed(1)}}s`;
  const hours=Math.floor(minutes/60);
  return `${{hours}}h ${{minutes-hours*60}}m ${{seconds.toFixed(0)}}s`;
}}

async function decodeModel(){{
  if(!('DecompressionStream' in window)){{
    throw new Error('This report requires a browser with DecompressionStream support.');
  }}
  const encoded=document.getElementById('trace-data').textContent.trim();
  const binary=atob(encoded);
  const bytes=new Uint8Array(binary.length);
  for(let index=0;index<binary.length;index++)bytes[index]=binary.charCodeAt(index);
  const stream=new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'));
  return JSON.parse(await new Response(stream).text());
}}

function resetDefaults(){{
  expanded=new Set();
  limits=new Map();
  renderLimit=INITIAL_RENDER_LIMIT;
  spans.forEach((span,index)=>{{
    if(children[index].length&&!COLLAPSED_SCOPES.has(model.c[span[SCOPE]])){{
      expanded.add(index);
    }}
  }});
}}

function spanMatches(span,index,query){{
  if(!searchCache[index]){{
    const attributes=Object.entries(span[ATTRS])
      .map(([key,value])=>`${{key}}=${{typeof value==='string'?value:JSON.stringify(value)}}`);
    searchCache[index]=[span[NAME],span[STATUS_MESSAGE],...attributes].join(' ').toLowerCase();
  }}
  return searchCache[index].includes(query);
}}

function applyFilter(){{
  const query=filterElement.value.trim().toLowerCase();
  if(!query){{
    visible=null;
    filterStatus.textContent='';
    resetDefaults();
    renderRows();
    return;
  }}
  renderLimit=INITIAL_RENDER_LIMIT;
  visible=new Set();
  let matches=0;
  spans.forEach((span,index)=>{{
    if(!spanMatches(span,index,query))return;
    matches++;
    let current=index;
    while(current>=0&&!visible.has(current)){{
      visible.add(current);
      current=spans[current][PARENT];
    }}
  }});
  filterStatus.textContent=`${{matches.toLocaleString()}} matching spans`;
  renderRows();
}}

function flattenRows(){{
  const result=[],seen=new Set();
  let truncated=false;
  function addSpan(index,depth){{
    if(truncated||seen.has(index)||(visible&&!visible.has(index)))return;
    if(result.length>=renderLimit){{
      truncated=true;
      return;
    }}
    seen.add(index);
    result.push({{kind:'span',index,depth}});
    const candidates=visible
      ? children[index].filter(child=>visible.has(child))
      : children[index];
    if(!candidates.length||(!visible&&!expanded.has(index)))return;
    const limit=limits.get(index)||PAGE_SIZE;
    candidates.slice(0,limit).forEach(child=>addSpan(child,depth+1));
    if(candidates.length>limit){{
      result.push({{kind:'more',index,depth:depth+1,total:candidates.length,shown:limit}});
    }}
  }}
  roots.forEach(index=>addSpan(index,0));
  return {{items:result,truncated}};
}}

function spanRow(item){{
  const span=spans[item.index];
  const row=document.createElement('div');
  row.className=`span-row${{span[STATUS]===2?' error':''}}${{selected===item.index?' selected':''}}`;
  row.dataset.index=String(item.index);

  const nameCell=document.createElement('div');
  nameCell.className='name-cell';
  nameCell.style.paddingLeft=`${{Math.min(item.depth,20)*1.1}}rem`;
  const toggle=document.createElement('button');
  toggle.className='toggle';
  toggle.type='button';
  const hasChildren=children[item.index].length>0;
  const isOpen=visible?hasChildren:expanded.has(item.index);
  toggle.textContent=hasChildren?(isOpen?'▾':'▸'):'';
  toggle.disabled=!hasChildren||Boolean(visible);
  toggle.setAttribute('aria-label',isOpen?'Collapse span group':'Expand span group');
  toggle.addEventListener('click',()=>{{
    if(expanded.has(item.index))expanded.delete(item.index);
    else expanded.add(item.index);
    renderRows();
  }});
  const name=document.createElement('button');
  name.className='span-name';
  name.type='button';
  name.textContent=span[NAME];
  name.title=span[NAME];
  name.addEventListener('click',()=>showSpan(item.index));
  nameCell.append(toggle,name);

  const duration=document.createElement('span');
  duration.className='duration';
  duration.textContent=formatDuration(span[DURATION]);
  const track=document.createElement('span');
  track.className='track';
  const bar=document.createElement('span');
  bar.className='bar';
  bar.style.left=`${{100*Math.max(0,span[START])/model.d}}%`;
  bar.style.width=`${{Math.max(.15,100*span[DURATION]/model.d)}}%`;
  track.append(bar);
  row.append(nameCell,duration,track);
  return row;
}}

function moreRow(item){{
  const row=document.createElement('div');
  row.className='more-row';
  row.style.setProperty('--indent',`${{Math.min(item.depth,20)*1.1+1.65}}rem`);
  const button=document.createElement('button');
  const remaining=item.total-item.shown;
  button.type='button';
  button.textContent=`Load ${{Math.min(PAGE_SIZE,remaining).toLocaleString()}} more (${{remaining.toLocaleString()}} remaining)`;
  button.addEventListener('click',()=>{{
    limits.set(item.index,item.shown+PAGE_SIZE);
    renderRows();
  }});
  row.append(button);
  return row;
}}

function renderRows(){{
  const flattened=flattenRows();
  const items=flattened.items;
  const fragment=document.createDocumentFragment();
  if(!items.length){{
    const empty=document.createElement('p');
    empty.className='muted';
    empty.id='loading';
    empty.textContent=visible?'No matching spans.':'No spans found.';
    fragment.append(empty);
  }}else{{
    items.forEach(item=>fragment.append(item.kind==='span'?spanRow(item):moreRow(item)));
    if(flattened.truncated){{
      const limitRow=document.createElement('div');
      limitRow.className='more-row';
      const button=document.createElement('button');
      button.type='button';
      button.textContent=`Render up to ${{(renderLimit+INITIAL_RENDER_LIMIT).toLocaleString()}} visible rows`;
      button.addEventListener('click',()=>{{
        renderLimit+=INITIAL_RENDER_LIMIT;
        renderRows();
      }});
      limitRow.append(button);
      fragment.append(limitRow);
    }}
  }}
  rowsElement.replaceChildren(fragment);
}}

function valueText(value){{
  return typeof value==='string'?value:JSON.stringify(value);
}}

function attributeTable(values){{
  if(!Object.keys(values).length){{
    const empty=document.createElement('p');
    empty.className='muted';
    empty.textContent='No attributes';
    return empty;
  }}
  const table=document.createElement('table');
  table.className='attributes';
  Object.entries(values).sort(([left],[right])=>left.localeCompare(right))
    .forEach(([key,value])=>{{
      const row=document.createElement('tr');
      const heading=document.createElement('th');
      heading.textContent=key;
      const cell=document.createElement('td');
      const rendered=valueText(value);
      let linked=false;
      if(typeof value==='string'){{
        try{{
          const url=new URL(value);
          if(url.protocol==='http:'||url.protocol==='https:'){{
            const link=document.createElement('a');
            link.href=value;
            link.rel='noopener noreferrer';
            link.textContent=value;
            cell.append(link);
            linked=true;
          }}
        }}catch(error){{}}
      }}
      if(!linked)cell.textContent=rendered;
      row.append(heading,cell);
      table.append(row);
    }});
  return table;
}}

function heading(text,level=3){{
  const element=document.createElement(`h${{level}}`);
  element.textContent=text;
  return element;
}}

function showSpan(index){{
  selected=index;
  const span=spans[index];
  detailTitle.textContent=span[NAME];
  const facts=document.createElement('p');
  facts.className='detail-facts';
  const parent=span[PARENT]>=0?spans[span[PARENT]][ID]:(span[ORPHAN_PARENT]||'none');
  facts.append(
    `Duration: ${{formatDuration(span[DURATION])}} · Scope: ${{model.c[span[SCOPE]]}} · Status: ${{span[STATUS]}} ${{span[STATUS_MESSAGE]}}`,
    document.createElement('br'),
    `Trace ID: ${{model.t[span[TRACE]]}} · Span ID: ${{span[ID]}} · Parent: ${{parent}}`,
  );
  const content=document.createDocumentFragment();
  content.append(facts,heading('Attributes'),attributeTable(span[ATTRS]));
  if(span[EVENTS].length){{
    content.append(heading('Events'));
    const events=document.createElement('ul');
    events.className='events';
    span[EVENTS].forEach(event=>{{
      const item=document.createElement('li');
      item.append(`${{formatDuration(Math.max(0,event[1]))}} · ${{event[0]}}`);
      item.append(attributeTable(event[2]));
      events.append(item);
    }});
    content.append(events);
  }}
  const resources=document.createElement('details');
  const resourceSummary=document.createElement('summary');
  resourceSummary.textContent='Resource attributes';
  resources.append(resourceSummary,attributeTable(model.r[span[RESOURCE]]));
  content.append(resources);
  detailContent.replaceChildren(content);
  detailPanel.hidden=false;
  renderRows();
}}

function initialize(decoded){{
  model=decoded;
  spans=model.s;
  children=spans.map(()=>[]);
  roots=[];
  spans.forEach((span,index)=>{{
    if(span[PARENT]>=0&&span[PARENT]<spans.length)children[span[PARENT]].push(index);
    else roots.push(index);
  }});
  const reachable=new Set();
  function markReachable(start){{
    const pending=[start];
    while(pending.length){{
      const index=pending.pop();
      if(reachable.has(index))continue;
      reachable.add(index);
      pending.push(...children[index]);
    }}
  }}
  roots.forEach(markReachable);
  spans.forEach((span,index)=>{{
    if(!reachable.has(index)){{
      roots.push(index);
      markReachable(index);
    }}
  }});
  resetDefaults();
  filterElement.disabled=false;
  document.getElementById('expand').disabled=false;
  document.getElementById('collapse').disabled=false;
  renderRows();
}}

let filterTimer;
filterElement.addEventListener('input',()=>{{
  clearTimeout(filterTimer);
  filterTimer=setTimeout(applyFilter,120);
}});
document.getElementById('expand').addEventListener('click',()=>{{
  spans.forEach((span,index)=>{{if(children[index].length)expanded.add(index)}});
  renderRows();
}});
document.getElementById('collapse').addEventListener('click',()=>{{
  expanded.clear();
  renderRows();
}});
document.getElementById('detail-close').addEventListener('click',()=>{{
  selected=null;
  detailPanel.hidden=true;
  renderRows();
}});

decodeModel().then(initialize).catch(error=>{{
  const loading=document.getElementById('loading');
  loading.textContent=`Unable to load trace: ${{error.message}}`;
  loading.style.color='var(--bad)';
}});
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
