#!/usr/bin/env python3
"""Convert ya test trace events into a portable OTLP trace bundle."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from ..trace_report import Span, stable_hex_id, write_trace_bundle

LOGGER = logging.getLogger(__name__)
MAX_YA_TRACE_BYTES = 512 * 1024 * 1024
MAX_YA_TRACE_FILES = 20_000
MAX_YA_EVENTS = 2_000_000
SAFE_METRIC_RE = re.compile(r"[^a-zA-Z0-9_.-]+")
PASS_STATUSES = {"good", "pass", "passed", "success"}
ERROR_STATUSES = {
    "crashed",
    "error",
    "fail",
    "failed",
    "internal",
    "not_launched",
    "timeout",
}


@dataclass
class YaEvent:
    name: str
    timestamp_ns: int | None
    value: dict[str, Any]
    order: int


@dataclass
class YaTraceFile:
    path: Path
    suite: str
    result_folder: str
    events: list[YaEvent]


def _timestamp_ns(value: Any) -> int | None:
    try:
        timestamp = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(timestamp) or timestamp < 0:
        return None
    return int(timestamp * 1_000_000_000)


def _seconds_ns(value: Any) -> int:
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return 0
    if not math.isfinite(seconds) or seconds < 0:
        return 0
    return int(seconds * 1_000_000_000)


def _trace_identity(ya_out: Path, trace_path: Path) -> tuple[str, str]:
    relative = trace_path.relative_to(ya_out)
    parts = relative.parts
    try:
        marker = parts.index("test-results")
    except ValueError:
        return str(relative.parent), trace_path.parent.name
    suite = "/".join(parts[:marker]) or "unknown-suite"
    result_folder = parts[marker + 1] if len(parts) > marker + 1 else "unknown"
    return suite, result_folder


def load_ya_traces(
    ya_out: Path, *, modified_since: float | None = None
) -> list[YaTraceFile]:
    traces: list[YaTraceFile] = []
    total_bytes = 0
    event_count = 0
    paths = sorted(ya_out.rglob("ytest.report.trace"))
    if len(paths) > MAX_YA_TRACE_FILES:
        raise ValueError(f"Found more than {MAX_YA_TRACE_FILES} ya trace files")

    for path in paths:
        if not path.is_file():
            continue
        stat = path.stat()
        if modified_since is not None and stat.st_mtime < modified_since:
            continue
        total_bytes += stat.st_size
        if total_bytes > MAX_YA_TRACE_BYTES:
            raise ValueError(f"Ya traces exceed {MAX_YA_TRACE_BYTES} bytes")
        suite, result_folder = _trace_identity(ya_out, path)
        events = []
        with path.open(encoding="utf-8", errors="replace") as stream:
            for line_number, line in enumerate(stream, 1):
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError as error:
                    LOGGER.warning("Skipping %s:%s: %s", path, line_number, error)
                    continue
                name = str(raw.get("name", ""))
                value = raw.get("value", {})
                if name not in {
                    "chunk-event",
                    "subtest-finished",
                    "subtest-started",
                } or not isinstance(value, dict):
                    continue
                events.append(
                    YaEvent(
                        name=name,
                        timestamp_ns=_timestamp_ns(raw.get("timestamp")),
                        value=value,
                        order=event_count,
                    )
                )
                event_count += 1
                if event_count > MAX_YA_EVENTS:
                    raise ValueError(f"Ya traces exceed {MAX_YA_EVENTS} events")
        traces.append(
            YaTraceFile(
                path=path,
                suite=suite,
                result_folder=result_folder,
                events=events,
            )
        )
    return traces


def _metric_name(name: object) -> str:
    normalized = SAFE_METRIC_RE.sub("_", str(name)).strip("_").lower()
    return re.sub(r"_+", "_", normalized)[:200]


def _metric_attributes(metrics: Any) -> dict[str, Any]:
    if not isinstance(metrics, Mapping):
        return {}
    attributes = {}
    for key, value in list(metrics.items())[:256]:
        if not isinstance(value, (str, bool, int, float)):
            continue
        if isinstance(value, float) and not math.isfinite(value):
            continue
        normalized = _metric_name(key)
        if normalized:
            attributes[f"ya.chunk.metric.{normalized}"] = value
    return attributes


def _event_key(event: YaEvent) -> tuple[str, str]:
    test_class = str(event.value.get("class", "unknown")).replace("::", ".")
    subtest = str(event.value.get("subtest", "unknown"))
    return test_class, subtest


def _status(value: Mapping[str, Any]) -> tuple[str, int]:
    status = str(value.get("status", "unknown")).lower()
    if status in PASS_STATUSES:
        return status, 1
    if status in ERROR_STATUSES:
        return status, 2
    return status, 0


def _test_attributes(
    event: YaEvent,
    test_class: str,
    subtest: str,
    *,
    inferred: bool,
    timing_source: str = "",
    incomplete: bool = False,
) -> dict[str, Any]:
    status, _ = _status(event.value)
    attributes: dict[str, Any] = {
        "test.framework": "ya",
        "test.suite": test_class,
        "test.name": subtest,
        "test.status": status,
    }
    if inferred:
        attributes["test.timing.inferred"] = True
    if timing_source:
        attributes["test.timing.source"] = timing_source
    if incomplete:
        attributes["test.incomplete"] = True
    attributes.update(_metric_attributes(event.value.get("metrics")))
    return attributes


def _test_spans(
    trace_id: str,
    chunk_span_id: str,
    chunk_start_ns: int,
    chunk_end_ns: int,
    events: Iterable[YaEvent],
    identity: str,
    inferred_test_start_ns: int | None = None,
) -> list[Span]:
    grouped: dict[tuple[str, str], list[YaEvent]] = defaultdict(list)
    for event in events:
        if event.name in {"subtest-started", "subtest-finished"}:
            grouped[_event_key(event)].append(event)

    result: list[Span] = []
    for (test_class, subtest), test_events in sorted(grouped.items()):
        test_events.sort(
            key=lambda event: (
                event.timestamp_ns is None,
                event.timestamp_ns or 0,
                event.order,
            )
        )
        starts = [event for event in test_events if event.name == "subtest-started"]
        finishes = [
            event for event in test_events if event.name == "subtest-finished"
        ]
        used_finishes: set[int] = set()

        for start_index, start in enumerate(starts):
            start_ns = start.timestamp_ns or chunk_start_ns
            next_start_ns = (
                starts[start_index + 1].timestamp_ns
                if start_index + 1 < len(starts)
                else None
            )
            candidates = [
                (finish_index, finish)
                for finish_index, finish in enumerate(finishes)
                if finish_index not in used_finishes
                and (
                    finish.timestamp_ns is None
                    or finish.timestamp_ns >= start_ns
                )
                and (
                    next_start_ns is None
                    or finish.timestamp_ns is None
                    or finish.timestamp_ns < next_start_ns
                )
            ]
            if candidates:
                for finish_index, _ in candidates:
                    used_finishes.add(finish_index)
                _, finish = candidates[-1]
                duration_ns = _seconds_ns(finish.value.get("time"))
                end_ns = finish.timestamp_ns or min(
                    chunk_end_ns, start_ns + duration_ns
                )
                end_ns = max(start_ns, min(end_ns, chunk_end_ns))
                status, status_code = _status(finish.value)
                attributes = _test_attributes(
                    finish,
                    test_class,
                    subtest,
                    inferred=start.timestamp_ns is None
                    or finish.timestamp_ns is None,
                    timing_source="subtest-events",
                )
                source_order = start.order
            else:
                finish = start
                end_ns = max(start_ns, chunk_end_ns)
                status = "incomplete"
                status_code = 2
                attributes = _test_attributes(
                    start,
                    test_class,
                    subtest,
                    inferred=start.timestamp_ns is None,
                    timing_source="subtest-start-and-chunk-end",
                    incomplete=True,
                )
                source_order = start.order

            result.append(
                Span(
                    trace_id=trace_id,
                    span_id=stable_hex_id(
                        trace_id,
                        identity,
                        test_class,
                        subtest,
                        source_order,
                        length=16,
                    ),
                    parent_span_id=chunk_span_id,
                    name=f"{test_class}::{subtest}",
                    start_ns=start_ns,
                    end_ns=end_ns,
                    attributes=attributes,
                    status_code=status_code,
                    status_message=status if status_code == 2 else "",
                    scope_name="ya.test",
                )
            )

        for finish_index, finish in enumerate(finishes):
            if finish_index in used_finishes:
                continue
            duration_ns = _seconds_ns(finish.value.get("time"))
            if inferred_test_start_ns is not None and len(grouped) == 1:
                start_ns = max(
                    chunk_start_ns,
                    min(inferred_test_start_ns, chunk_end_ns),
                )
                end_ns = max(
                    start_ns,
                    min(start_ns + duration_ns, chunk_end_ns),
                )
                timing_source = "chunk-delay-and-test-duration"
            else:
                end_ns = finish.timestamp_ns or chunk_end_ns
                start_ns = max(chunk_start_ns, end_ns - duration_ns)
                end_ns = max(start_ns, min(end_ns, chunk_end_ns))
                timing_source = "finish-event-and-test-duration"
            status, status_code = _status(finish.value)
            result.append(
                Span(
                    trace_id=trace_id,
                    span_id=stable_hex_id(
                        trace_id,
                        identity,
                        test_class,
                        subtest,
                        finish.order,
                        length=16,
                    ),
                    parent_span_id=chunk_span_id,
                    name=f"{test_class}::{subtest}",
                    start_ns=start_ns,
                    end_ns=end_ns,
                    attributes=_test_attributes(
                        finish,
                        test_class,
                        subtest,
                        inferred=True,
                        timing_source=timing_source,
                    ),
                    status_code=status_code,
                    status_message=status if status_code == 2 else "",
                    scope_name="ya.test",
                )
            )
    return result


def _chunk_key(value: Mapping[str, Any]) -> tuple[int, int] | None:
    try:
        chunk_index = int(value["chunk_index"])
        chunks_total = int(value["nchunks"])
    except (KeyError, TypeError, ValueError):
        return None
    if chunk_index < 0 or chunks_total < 1 or chunk_index >= chunks_total:
        return None
    return chunk_index, chunks_total


def _chunk_bounds(
    chunk_event: YaEvent | None,
    events: Sequence[YaEvent],
    root_start_ns: int,
    root_end_ns: int,
) -> tuple[int, int, dict[str, Any]]:
    chunk_value = chunk_event.value if chunk_event is not None else {}
    metrics = chunk_value.get("metrics", {})
    start_ns = None
    end_ns = None
    wall_time_ns = 0
    if isinstance(metrics, Mapping):
        start_ns = _timestamp_ns(metrics.get("suite_start_timestamp"))
        end_ns = _timestamp_ns(metrics.get("suite_finish_timestamp"))
        wall_time_ns = _seconds_ns(metrics.get("wall_time"))
        if end_ns is not None and wall_time_ns:
            start_ns = end_ns - wall_time_ns
        elif start_ns is not None and wall_time_ns:
            end_ns = start_ns + wall_time_ns

    timestamps = [
        event.timestamp_ns for event in events if event.timestamp_ns is not None
    ]
    start_ns = start_ns or (min(timestamps) if timestamps else root_start_ns)
    if end_ns is None:
        if chunk_event is not None and chunk_event.timestamp_ns is not None:
            end_ns = chunk_event.timestamp_ns
        elif chunk_event is not None and timestamps:
            end_ns = max(timestamps)
        else:
            end_ns = root_end_ns
    start_ns = max(root_start_ns, min(start_ns, root_end_ns))
    end_ns = max(start_ns, min(end_ns, root_end_ns))
    return start_ns, end_ns, chunk_value


def _chunk_records(
    trace: YaTraceFile,
) -> list[tuple[YaEvent | None, list[YaEvent]]]:
    chunk_events = [event for event in trace.events if event.name == "chunk-event"]
    test_events = [
        event
        for event in trace.events
        if event.name in {"subtest-started", "subtest-finished"}
    ]
    if not chunk_events:
        return [(None, test_events)]
    if len(chunk_events) == 1:
        return [(chunk_events[0], test_events)]

    records = []
    assigned_orders = set()
    for chunk_event in chunk_events:
        key = _chunk_key(chunk_event.value)
        matching = [
            event for event in test_events if _chunk_key(event.value) == key
        ]
        assigned_orders.update(event.order for event in matching)
        records.append((chunk_event, matching))

    unassigned = [
        event for event in test_events if event.order not in assigned_orders
    ]
    if unassigned:
        records.append((None, unassigned))
    return records


def resource_attributes(args: argparse.Namespace) -> dict[str, Any]:
    server_url = os.environ.get("GITHUB_SERVER_URL", "https://github.com").rstrip("/")
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    sha = os.environ.get("GITHUB_SHA", "")
    values = {
        "service.name": "nbs-ya-tests",
        "ci.component": args.component or "all",
        "ci.build.preset": args.build_preset,
        "ci.test.target": args.test_target,
        "ci.test.type": args.test_type,
        "ci.test.size": args.test_size,
        "ci.ya.retry": args.retry,
        "github.repository": repository,
        "github.workflow": os.environ.get("GITHUB_WORKFLOW", ""),
        "github.workflow.ref": os.environ.get("GITHUB_WORKFLOW_REF", ""),
        "github.job": os.environ.get("GITHUB_JOB", ""),
        "github.run.id": run_id,
        "github.run.number": os.environ.get("GITHUB_RUN_NUMBER", ""),
        "github.run.attempt": os.environ.get("GITHUB_RUN_ATTEMPT", ""),
        "github.event.name": os.environ.get("GITHUB_EVENT_NAME", ""),
        "github.actor": os.environ.get("GITHUB_ACTOR", ""),
        "github.sha": sha,
        "github.ref": os.environ.get("GITHUB_REF", ""),
    }
    if repository and run_id:
        values["github.run.url"] = (
            f"{server_url}/{repository}/actions/runs/{run_id}"
        )
    if repository and sha:
        values["github.commit.url"] = f"{server_url}/{repository}/commit/{sha}"
    return {key: value for key, value in values.items() if value not in {"", None}}


def build_ya_spans(
    traces: Sequence[YaTraceFile],
    *,
    root_start_ns: int,
    root_end_ns: int,
    exit_code: int,
    result_code: int | None = None,
    resource: Mapping[str, Any],
) -> list[Span]:
    effective_result_code = exit_code if result_code is None else result_code
    trace_id = stable_hex_id(
        resource.get("github.repository", ""),
        resource.get("github.run.id", ""),
        resource.get("github.run.attempt", ""),
        resource.get("github.job", ""),
        resource.get("ci.component", ""),
        resource.get("ci.ya.retry", ""),
        root_start_ns,
        length=32,
    )
    root_span_id = stable_hex_id(trace_id, "ya make", length=16)
    root = Span(
        trace_id=trace_id,
        span_id=root_span_id,
        name="ya make tests",
        start_ns=root_start_ns,
        end_ns=root_end_ns,
        attributes={
            "process.exit.code": exit_code,
            "ci.result.code": effective_result_code,
            "ya.trace.file_count": len(traces),
            "ya.chunk.count": sum(len(_chunk_records(trace)) for trace in traces),
        },
        status_code=1 if effective_result_code == 0 else 2,
        status_message=(
            ""
            if effective_result_code == 0
            else f"ya test result code {effective_result_code}"
        ),
        resource_attributes=dict(resource),
        scope_name="ya",
    )
    spans = [root]

    for trace_index, trace in enumerate(traces):
        for record_index, (chunk_event, chunk_events) in enumerate(
            _chunk_records(trace)
        ):
            chunk_start_ns, chunk_end_ns, chunk_value = _chunk_bounds(
                chunk_event,
                chunk_events,
                root_start_ns,
                root_end_ns,
            )
            chunk_key = _chunk_key(chunk_value)
            identity = (
                f"{trace.suite}:{trace.result_folder}:{trace_index}:"
                f"{chunk_key or record_index}"
            )
            chunk_span_id = stable_hex_id(trace_id, identity, length=16)
            attributes = {
                "test.suite": trace.suite,
                "ya.test_results.folder": trace.result_folder,
            }
            for field_name in ("chunk_index", "nchunks", "status"):
                if field_name in chunk_value:
                    attributes[f"ya.chunk.{field_name}"] = chunk_value[
                        field_name
                    ]
            attributes.update(_metric_attributes(chunk_value.get("metrics")))
            chunk_status = str(chunk_value.get("status", "")).lower()
            chunk_status_code = 2 if chunk_status in ERROR_STATUSES else 0
            if chunk_key is None:
                chunk_label = f"record {record_index + 1}"
            else:
                chunk_label = f"chunk {chunk_key[0] + 1}/{chunk_key[1]}"
            chunk = Span(
                trace_id=trace_id,
                span_id=chunk_span_id,
                parent_span_id=root_span_id,
                name=(
                    f"{trace.suite} "
                    f"[{trace.result_folder} {chunk_label}]"
                ),
                start_ns=chunk_start_ns,
                end_ns=chunk_end_ns,
                attributes=attributes,
                status_code=chunk_status_code,
                status_message=chunk_status if chunk_status_code == 2 else "",
                resource_attributes=dict(resource),
                scope_name="ya.chunk",
            )
            metrics = chunk_value.get("metrics", {})
            test_start_ns = None
            if isinstance(metrics, Mapping):
                delay_ns = _seconds_ns(
                    metrics.get("suite_delay_until_first_test_secs")
                )
                if delay_ns:
                    test_start_ns = chunk_start_ns + delay_ns
            test_spans = _test_spans(
                trace_id,
                chunk_span_id,
                chunk_start_ns,
                chunk_end_ns,
                chunk_events,
                identity,
                inferred_test_start_ns=test_start_ns,
            )
            if any(span.status_code == 2 for span in test_spans):
                chunk.status_code = 2
                if not chunk.status_message:
                    chunk.status_message = "one or more tests failed"
            spans.append(chunk)
            for span in test_spans:
                span.resource_attributes = dict(resource)
            spans.extend(test_spans)
    return spans


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ya-out", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--attempt-start-ns", type=int, required=True)
    parser.add_argument("--attempt-end-ns", type=int, required=True)
    parser.add_argument("--exit-code", type=int, required=True)
    parser.add_argument("--result-code", type=int)
    parser.add_argument("--component", default="")
    parser.add_argument("--build-preset", default="")
    parser.add_argument("--test-target", default="")
    parser.add_argument("--test-type", default="")
    parser.add_argument("--test-size", default="")
    parser.add_argument("--retry", type=int, default=1)
    return parser


def main() -> None:
    args = _parser().parse_args()
    logging.basicConfig(level=logging.INFO)
    if args.attempt_end_ns < args.attempt_start_ns:
        raise ValueError("attempt end precedes attempt start")
    resource = resource_attributes(args)
    traces = load_ya_traces(
        args.ya_out,
        modified_since=args.attempt_start_ns / 1_000_000_000 - 5,
    )
    spans = build_ya_spans(
        traces,
        root_start_ns=args.attempt_start_ns,
        root_end_ns=args.attempt_end_ns,
        exit_code=args.exit_code,
        result_code=args.result_code,
        resource=resource,
    )
    title_component = args.component or "all"
    manifest = write_trace_bundle(
        args.output_dir,
        spans,
        title=f"ya make trace · {title_component} · attempt {args.retry}",
        metadata=resource,
    )
    print(
        f"Wrote {manifest['span_count']} spans from {len(traces)} ya trace files "
        f"to {args.output_dir}"
    )


if __name__ == "__main__":
    main()
