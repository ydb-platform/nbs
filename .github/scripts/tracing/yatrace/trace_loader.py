"""Load discovered ``ytest.report.trace`` files."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Sequence

from . import limits
from .event import YaEvent
from .trace_file import YaTraceFile

LOGGER = logging.getLogger(__name__)


def _load_ya_trace_files(
    ya_out: Path,
    paths: Sequence[Path],
) -> list[YaTraceFile]:
    traces: list[YaTraceFile] = []
    event_count = 0
    if len(paths) > limits.MAX_YA_TRACE_FILES:
        raise ValueError(f"Found more than {limits.MAX_YA_TRACE_FILES} ya trace files")

    path_stats = [(path, path.stat()) for path in paths]
    if (
        sum(path_stat.st_size for _, path_stat in path_stats)
        > limits.MAX_YA_TRACE_BYTES
    ):
        raise ValueError(f"Ya traces exceed {limits.MAX_YA_TRACE_BYTES} bytes")

    for path, _ in path_stats:
        suite, result_folder = YaTraceFile.identity(ya_out, path)
        events = []
        malformed_json_record_count = 0
        with path.open(encoding="utf-8", errors="replace") as stream:
            for line_number, line in enumerate(stream, 1):
                line = line.strip("\r\t\n\x00 ")
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError as error:
                    malformed_json_record_count += 1
                    LOGGER.warning("Skipping %s:%s: %s", path, line_number, error)
                    continue
                event = YaEvent.from_raw(raw, event_count)
                if event is None:
                    continue
                events.append(event)
                event_count += 1
                if event_count > limits.MAX_YA_EVENTS:
                    raise ValueError(f"Ya traces exceed {limits.MAX_YA_EVENTS} events")
        traces.append(
            YaTraceFile(
                path=path,
                suite=suite,
                result_folder=result_folder,
                events=events,
                malformed_json_record_count=malformed_json_record_count,
            )
        )
    return traces
