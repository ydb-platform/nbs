from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

import scripts.tracing.ya_trace_report as ya_trace_report_module
import scripts.tracing.yatrace.limits as yatrace_limits
from scripts.tracing.otlp import (
    Interval,
    ResourceAttributes,
    Ns,
    decode_attributes,
    make_span,
    span_duration_ns,
    span_status_code,
)
from scripts.tracing.yatrace import (
    ClassifiedNode,
    TestChunk,
    YaCriticalPathEntry,
    YaEvent,
    YaEvlog,
    YaEvlogRecord,
    YaTraceCollection,
    YaTraceInputs,
    load_ya_evlog,
)
from scripts.tracing.ya_trace_report import build_ya_trace


def _attributes(span) -> dict:
    return decode_attributes(span.attributes)


def _write_jsonl(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(event) + "\n" for event in events))


def _subtest(
    name: str,
    *,
    timestamp: float,
    chunk: tuple[int, int] | None = None,
    started: bool = False,
    status: str = "good",
    duration: float | None = None,
    logs: dict[str, str] | None = None,
) -> dict:
    value = {"class": "Suite", "subtest": name}
    if chunk is not None:
        value.update({"chunk_index": chunk[0], "nchunks": chunk[1]})
    if not started:
        value["status"] = status
    if duration is not None:
        value["time"] = duration
    if logs is not None:
        value["logs"] = logs
    return {
        "name": "subtest-started" if started else "subtest-finished",
        "timestamp": timestamp,
        "value": value,
    }


def _chunk(
    index: int,
    total: int,
    *,
    timestamp: float,
    metrics: dict[str, int | float] | None = None,
    logs: dict[str, str] | None = None,
) -> dict:
    return {
        "name": "chunk-event",
        "timestamp": timestamp,
        "value": {
            "chunk_index": index,
            "nchunks": total,
            "metrics": metrics or {},
            **({"logs": logs} if logs is not None else {}),
        },
    }


def _stage(name: str, start: float, end: float) -> dict:
    return {
        "namespace": "stages",
        "event": "stage-finished",
        "value": {"name": name, "tag": name, "time": [start, end]},
    }


def _worker(
    name: str,
    tag: str,
    start: float,
    end: float,
    *,
    thread_name: str = "",
) -> dict:
    event = {
        "namespace": "worker_threads",
        "event": "node-finished",
        "value": {"name": name, "tag": tag, "time": [start, end]},
    }
    if thread_name:
        event["thread_name"] = thread_name
    return event


def _worker_detail(
    tag: str,
    start: float,
    end: float,
    thread_name: str,
) -> dict:
    return {
        "thread_name": thread_name,
        "namespace": "worker_threads",
        "event": "node-detailed",
        "value": {"name": tag, "tag": tag, "time": [start, end]},
    }


def _run_worker(
    uid: str,
    output: str,
    start: float,
    end: float,
    thread_name: str = "",
) -> dict:
    return _worker(
        f"Run({uid}$(BUILD_ROOT)/{output})",
        "CC",
        start,
        end,
        thread_name=thread_name,
    )


def _node_started(thread_name: str, name: str) -> dict:
    return {
        "thread_name": thread_name,
        "namespace": "worker_threads",
        "event": "node-started",
        "value": {"name": name},
    }


def _statistics(value: dict) -> dict:
    return {
        "namespace": "dump_debug",
        "event": "log",
        "value": {"key": "stats", "value": value},
    }


def _failed_node(uid: str, exit_code: int) -> dict:
    return {
        "namespace": "devtools.ya.build.reports.failed_node_info",
        "event": "node-failed",
        "value": {"uid": uid, "exit_code": exit_code},
    }


def _render_ya_trace(
    tmp_path: Path,
    events: list[dict],
    *,
    evlog_events: list[dict] | None = None,
    root_start_s: float = 0,
    root_end_s: float = 20,
    exit_code: int = 0,
):
    ya_out = tmp_path / "out"
    _write_jsonl(
        ya_out / "suite" / "test-results" / "unittest" / "ytest.report.trace",
        events,
    )
    evlog = None
    if evlog_events is not None:
        evlog_path = tmp_path / "ya_evlog.jsonl"
        _write_jsonl(evlog_path, evlog_events)
        evlog = load_ya_evlog(evlog_path)
    return build_ya_trace(
        YaTraceCollection.load(ya_out).traces,
        root_start_ns=Ns(round(root_start_s * 1_000_000_000)),
        root_end_ns=Ns(round(root_end_s * 1_000_000_000)),
        exit_code=exit_code,
        resource=ResourceAttributes(),
        evlog=evlog,
    )


__all__ = [
    "ClassifiedNode",
    "Interval",
    "Ns",
    "Path",
    "ResourceAttributes",
    "TestChunk",
    "YaCriticalPathEntry",
    "YaEvent",
    "YaEvlog",
    "YaEvlogRecord",
    "YaTraceCollection",
    "YaTraceInputs",
    "_attributes",
    "_chunk",
    "_failed_node",
    "_node_started",
    "_render_ya_trace",
    "_run_worker",
    "_stage",
    "_statistics",
    "_subtest",
    "_worker",
    "_worker_detail",
    "_write_jsonl",
    "build_ya_trace",
    "decode_attributes",
    "json",
    "load_ya_evlog",
    "make_span",
    "os",
    "pytest",
    "span_duration_ns",
    "span_status_code",
    "sys",
    "ya_trace_report_module",
    "yatrace_limits",
]
