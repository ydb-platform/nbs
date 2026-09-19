from __future__ import annotations

import json
import logging
import math
from pathlib import Path
from typing import Any, Mapping

from . import limits
from .evlog_data import YaEvlog
from .metrics import finite_number
from .node import YaNode

LOGGER = logging.getLogger(__name__)
MALFORMED_LINE_PREVIEW_CHARACTERS = 1_024
WORKER_DETAIL_TAGS = frozenset(
    {"setup", "exec_cmd", "post_cmd", "node_result", "finalize"}
)


def _malformed_line_preview(line: str) -> str:
    line = line.rstrip("\r\n")
    if len(line) <= MALFORMED_LINE_PREVIEW_CHARACTERS:
        return line
    marker = "... characters omitted ..."
    retained = MALFORMED_LINE_PREVIEW_CHARACTERS - len(marker)
    prefix = (retained + 1) // 2
    suffix = retained - prefix
    return line[:prefix] + marker + line[-suffix:]


def _safe_statistics_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    result = {}
    for key, item in list(value.items())[:256]:
        if isinstance(item, (bool, int)) or (
            isinstance(item, float) and math.isfinite(item)
        ):
            result[str(key)] = item
        elif isinstance(item, str):
            result[str(key)] = item[:8_192]
    return result


def _selected_evlog_statistics(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, Any] = {}
    for key in (
        "cache_hit",
        "dist_cache_stat",
        "execution_stages_msec",
        "graph_lang_usage",
    ):
        selected = _safe_statistics_mapping(value.get(key))
        if selected:
            result[key] = selected
    task_execution_msec = finite_number(value.get("task_execution_msec"))
    if task_execution_msec is not None:
        result["task_execution_msec"] = task_execution_msec

    critical_path = []
    if isinstance(value.get("critical_path"), list):
        for item in value["critical_path"]:
            if not isinstance(item, Mapping):
                continue
            selected = _safe_statistics_mapping(
                {
                    key: item.get(key)
                    for key in (
                        "type",
                        "elapsed",
                        "start_ts",
                        "end_ts",
                        "text",
                        "host",
                        "uid",
                    )
                }
            )
            if selected:
                critical_path.append(selected)
    if critical_path:
        result["critical_path"] = critical_path
    return result


def load_ya_evlog(path: Path | None) -> YaEvlog:
    stages: list[YaNode] = []
    nodes: list[YaNode] = []
    statistics: dict[str, Any] = {}
    failures: dict[str, int | None] = {}
    malformed_json_count = 0
    first_malformed: tuple[int, str, str] | None = None

    def finalize() -> YaEvlog:
        return YaEvlog.from_raw(
            stages=stages,
            nodes=nodes,
            statistics=statistics,
            failures=failures,
            malformed_json_record_count=malformed_json_count,
        )

    if path is None:
        return finalize()
    if not path.is_file():
        LOGGER.warning("Ya event log does not exist: %s", path)
        return finalize()
    if path.stat().st_size > limits.MAX_YA_EVLOG_BYTES:
        raise ValueError(f"Ya event log exceeds {limits.MAX_YA_EVLOG_BYTES} bytes")

    event_count = 0
    last_finished_by_thread: dict[str, YaNode] = {}
    with path.open(encoding="utf-8", errors="replace") as stream:
        line_number = 0
        while True:
            line = stream.readline(limits.MAX_YA_EVLOG_LINE_CHARACTERS + 1)
            if not line:
                break
            line_number += 1
            if len(line) > limits.MAX_YA_EVLOG_LINE_CHARACTERS:
                raise ValueError(
                    "Ya event log line exceeds "
                    f"{limits.MAX_YA_EVLOG_LINE_CHARACTERS} characters "
                    f"in {path}:{line_number}"
                )
            if not line.strip():
                continue
            event_count += 1
            if event_count > limits.MAX_YA_EVLOG_EVENTS:
                raise ValueError(
                    f"Ya event log exceeds {limits.MAX_YA_EVLOG_EVENTS} events"
                )
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as error:
                malformed_json_count += 1
                if first_malformed is None:
                    first_malformed = (
                        line_number,
                        str(error),
                        _malformed_line_preview(line),
                    )
                continue
            if not isinstance(raw, Mapping):
                LOGGER.warning("Skipping non-object event in %s:%s", path, line_number)
                continue
            namespace = raw.get("namespace")
            event = raw.get("event")
            raw_value = raw.get("value")
            if (
                namespace == "dump_debug"
                and event == "log"
                and isinstance(raw_value, Mapping)
                and raw_value.get("key") == "stats"
            ):
                statistics = _selected_evlog_statistics(raw_value.get("value"))
                continue
            if (
                namespace == "devtools.ya.build.reports.failed_node_info"
                and event == "node-failed"
                and isinstance(raw_value, Mapping)
            ):
                uid = str(raw_value.get("uid", ""))
                raw_exit_code = raw_value.get("exit_code")
                exit_code = (
                    raw_exit_code
                    if isinstance(raw_exit_code, int)
                    and not isinstance(raw_exit_code, bool)
                    else None
                )
                if uid:
                    failures[uid] = exit_code
                continue
            raw_thread_name = raw.get("thread_name")
            thread_name = raw_thread_name if isinstance(raw_thread_name, str) else ""
            if namespace == "worker_threads" and event == "node-started":
                if thread_name:
                    last_finished_by_thread.pop(thread_name, None)
                continue
            if (
                namespace == "worker_threads"
                and event == "node-detailed"
                and isinstance(raw_value, Mapping)
            ):
                parent = last_finished_by_thread.get(thread_name)
                if parent is None or raw_value.get("tag") not in WORKER_DETAIL_TAGS:
                    continue
                detail = YaNode.from_raw(
                    raw_value,
                    thread=thread_name,
                )
                if (
                    detail is not None
                    and parent.start_ns <= detail.start_ns
                    and detail.end_ns <= parent.end_ns
                ):
                    parent.details.append(detail)
                continue
            if (namespace, event) not in {
                ("stages", "stage-finished"),
                ("worker_threads", "node-finished"),
            } or not isinstance(raw_value, Mapping):
                continue
            record = YaNode.from_raw(
                raw_value,
                thread=thread_name,
            )
            if record is None:
                continue
            if namespace == "stages":
                stages.append(record)
            else:
                nodes.append(record)
                if thread_name:
                    last_finished_by_thread[thread_name] = record
    if first_malformed is not None:
        line_number, error, preview = first_malformed
        record_label = "record" if malformed_json_count == 1 else "records"
        LOGGER.warning(
            "Ya event log %s contains %s malformed JSON %s; first at line %s: "
            "%s; line=%r",
            path,
            malformed_json_count,
            record_label,
            line_number,
            error,
            preview,
        )
    return finalize()
