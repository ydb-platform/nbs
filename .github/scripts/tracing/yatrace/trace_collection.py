"""A collection of parsed ya test traces."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .event import YaEvent
from .trace_file import YaTraceFile


@dataclass
class YaTraceCollection:
    root: Path
    traces: list[YaTraceFile]

    @classmethod
    def load(
        cls,
        root: Path,
        *,
        modified_since: float | None = None,
    ) -> YaTraceCollection:
        from .trace_inputs import YaTraceInputs

        return YaTraceInputs.discover(
            root,
            modified_since=modified_since,
        ).parse()

    def finished_test(
        self,
        suite: str,
        test_class: str,
        subtest: str,
    ) -> YaEvent | None:
        for trace in reversed(self.traces):
            if trace.suite != suite:
                continue
            event = trace.finished_test(test_class, subtest)
            if event is not None:
                return event
        return None

    def chunk_event_candidates(
        self,
        suite: str,
        chunk_index: int,
        chunks_total: int,
    ) -> list[tuple[YaTraceFile, YaEvent]]:
        return [
            (trace, event)
            for trace in self.traces
            if trace.suite == suite
            for event in trace.chunk_events(chunk_index, chunks_total)
        ]

    def select_chunk_event(
        self,
        suite: str,
        chunk_index: int,
        chunks_total: int,
        failure_text: str | None = None,
    ) -> YaEvent | None:
        candidates = self.chunk_event_candidates(
            suite,
            chunk_index,
            chunks_total,
        )
        if len(candidates) == 1:
            return candidates[0][1]
        if not failure_text:
            return None

        for trace, event in candidates:
            if f"test-results/{trace.result_folder}" in failure_text:
                return event

        root = str(self.root)
        for _, event in candidates:
            for path in event.log_paths.values():
                if path.replace("$(BUILD_ROOT)", root) in failure_text:
                    return event
        return None
