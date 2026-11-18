from __future__ import annotations

import logging
import stat
from dataclasses import dataclass
from pathlib import Path

from .trace_loader import load_trace_files
from .trace_model import Chunk, SuiteTrace, TestEvent

LOGGER = logging.getLogger(__name__)


def discover_trace_paths(
    root: Path,
    *,
    modified_since: float | None = None,
) -> list[Path]:
    root = root.resolve()
    result = []
    for path in sorted(root.rglob("ytest.report.trace")):
        try:
            relative = path.relative_to(root)
            path_stat = path.lstat()
            resolved = path.resolve(strict=True)
        except (OSError, ValueError) as error:
            LOGGER.warning("Skipping ya trace input %s: %s", path, error)
            continue
        if not stat.S_ISREG(path_stat.st_mode) or resolved != root / relative:
            LOGGER.warning("Skipping unsafe ya trace input: %s", path)
            continue
        if modified_since is None or path_stat.st_mtime >= modified_since:
            result.append(path)
    return result


@dataclass(slots=True)
class YaTraceCollection:
    root: Path
    traces: list[SuiteTrace]

    @classmethod
    def load(
        cls,
        root: Path,
        *,
        modified_since: float | None = None,
    ) -> YaTraceCollection:
        root = root.resolve()
        return cls(
            root,
            load_trace_files(
                root,
                discover_trace_paths(root, modified_since=modified_since),
            ),
        )

    def finished_test(
        self,
        suite: str,
        test_class: str,
        name: str,
    ) -> TestEvent | None:
        for trace in reversed(self.traces):
            if (
                trace.suite == suite
                and (event := trace.finished_test(test_class, name)) is not None
            ):
                return event
        return None

    def chunk_event_candidates(
        self,
        suite: str,
        index: int,
        total: int,
    ) -> list[tuple[SuiteTrace, Chunk]]:
        return [
            (trace, chunk)
            for trace in self.traces
            if trace.suite == suite
            for chunk in trace.matching_chunks(index, total)
        ]

    def select_chunk_event(
        self,
        suite: str,
        index: int,
        total: int,
        failure_text: str | None = None,
    ) -> Chunk | None:
        candidates = self.chunk_event_candidates(suite, index, total)
        if len(candidates) == 1:
            return candidates[0][1]
        if not failure_text:
            return None
        for trace, chunk in candidates:
            if f"test-results/{trace.result_folder}" in failure_text:
                return chunk
        root = str(self.root)
        for _, chunk in candidates:
            if any(
                path.replace("$(BUILD_ROOT)", root) in failure_text
                for path in (chunk.record.logs.values() if chunk.record else ())
            ):
                return chunk
        return None
