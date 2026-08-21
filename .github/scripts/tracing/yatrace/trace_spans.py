from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from ..otlp import (
    Interval,
    Ns,
    Span,
    set_span_status,
    update_span_attributes,
)
from ..projector import SpanProjector
from .metrics import metric_attributes, normalize_metric_name
from .trace_model import Chunk, SuiteTrace, TestEvent, TraceRecord, relative_build_path


def _error_attributes(record: TraceRecord, prefix: str) -> dict[str, Any]:
    if not record.errors:
        return {}
    return {
        f"{prefix}.error.count": len(record.errors),
        f"{prefix}.error.statuses": [status for status, _ in record.errors],
        f"{prefix}.error.messages": [message for _, message in record.errors],
    }


def _log_attributes(record: TraceRecord, prefix: str) -> dict[str, str]:
    attributes = {}
    for name, path in list(record.logs.items())[:32]:
        relative = relative_build_path(path)
        if relative is None:
            continue
        if name == "logsdir":
            attributes[f"{prefix}.logs_directory.path"] = relative
        elif normalized := normalize_metric_name(name):
            attributes[f"{prefix}.log.{normalized}.path"] = relative
    return attributes


def _test_attributes(
    event: TestEvent,
    *,
    status: str,
    timestamp_source: str,
    timestamp_inferred: bool,
    incomplete: bool,
) -> dict[str, Any]:
    attributes: dict[str, Any] = {
        "test.framework": "ya",
        "test.suite": event.test_class,
        "test.name": event.name,
        "test.status": status,
        "test.duration.reported_seconds": event.duration_ns.to_s(),
        "ya.test.result.timestamp.source": timestamp_source,
    }
    optional = {
        "test.type": event.test_type,
        "test.path": event.path,
        "ya.test.result.timestamp.inferred": timestamp_inferred,
        "test.incomplete": incomplete,
    }
    attributes.update({key: value for key, value in optional.items() if value})
    attributes.update(metric_attributes(event.record.metrics, prefix="ya.test.metric"))
    attributes.update(_error_attributes(event.record, "ya.test"))
    attributes.update(_log_attributes(event.record, "ya.test"))
    return attributes


@dataclass(frozen=True, slots=True)
class YaTraceSpanProjector:
    source: SuiteTrace

    @staticmethod
    def _sequence_start(
        chunk: Chunk,
        interval: Interval,
        attempts: list[tuple[TestEvent, bool]],
    ) -> Ns | None:
        if not attempts or any(incomplete for _, incomplete in attempts):
            return None
        keys = {(event.test_class, event.name) for event, _ in attempts}
        for test_class, name in keys:
            parts = name.split("/")
            if any(
                (test_class, "/".join(parts[:end])) in keys
                for end in range(1, len(parts))
            ):
                return None
        total = Ns(sum(event.duration_ns for event, _ in attempts))
        if total > len(interval):
            return None

        latest = Ns(interval.end - total)
        metrics = chunk.record.metrics if chunk.record is not None else {}
        delay = Ns.from_s(metrics.get("suite_delay_until_first_test_secs"))
        startup = Ns.from_s_or_zero(metrics.get("suite_binary_startup_secs"))
        if delay is None:
            return latest
        proposed = Ns(interval.start + max(0, delay - startup))
        return Ns(min(proposed, latest))

    @staticmethod
    def _stage_spans(
        parent: SpanProjector,
        interval: Interval,
        metrics: Mapping[str, Any],
        identity: str,
    ) -> tuple[list[Span], Ns]:
        cursor = interval.start
        reported_total = Ns(0)
        spans = []
        stages = parent.scoped("ya.test.stage")
        for metric_name, raw_duration in metrics.items():
            name = str(metric_name)
            if not name.startswith("suite_") or not name.endswith("_(seconds)"):
                continue
            stage_name = name[len("suite_") : -len("_(seconds)")]
            duration = Ns.from_s(raw_duration)
            if duration is None or not duration or not stage_name:
                continue
            reported_total = Ns(reported_total + duration)
            end = interval.clamp(Ns(cursor + duration))
            if end <= cursor:
                continue
            spans.append(
                stages.make(
                    identity,
                    "ya.test.stage",
                    stage_name,
                    len(spans),
                    name=f"test stage: {stage_name.replace('_', ' ')}",
                    interval=Interval(cursor, end),
                    attributes={
                        "ya.test.stage.name": stage_name,
                        "ya.test.stage.reported_seconds": duration.to_s(),
                        "ya.test.stage.timing.source": "ya-chunk-cumulative-stage-duration",
                        "test.timing.inferred": True,
                    },
                )
            )
            cursor = end
        return spans, reported_total

    @staticmethod
    def _test_spans(
        parent: SpanProjector,
        chunk: Chunk,
        interval: Interval,
        identity: str,
    ) -> tuple[list[Span], bool]:
        spans = []
        failed = False
        tests = parent.scoped("ya.test")
        ordered_attempts = sorted(
            chunk.attempts,
            key=lambda attempt: (
                (attempt.finish or attempt.start).record.order
                if attempt.finish or attempt.start
                else -1
            ),
        )
        results = [
            (result, attempt.finish is None)
            for attempt in ordered_attempts
            if (result := attempt.finish or attempt.start) is not None
        ]
        sequence_start = YaTraceSpanProjector._sequence_start(
            chunk,
            interval,
            [
                (result, incomplete)
                for result, incomplete in results
                if result.status not in {"deselected", "not_launched"}
            ],
        )
        cursor = sequence_start
        for result, incomplete in results:
            status = "incomplete" if incomplete else result.status
            failed = failed or incomplete or result.status_code == 2
            timestamp = result.record.timestamp_ns
            timestamp_inferred = timestamp is None
            timestamp = interval.end if timestamp is None else timestamp
            sequenced = cursor is not None and result.status not in {
                "deselected",
                "not_launched",
            }
            test_interval = (
                Interval(cursor, Ns(cursor + result.duration_ns))
                if sequenced
                else Interval(timestamp, timestamp)
            )
            attributes = _test_attributes(
                result,
                status=status,
                timestamp_source=(
                    "chunk-end-fallback"
                    if timestamp_inferred
                    else (
                        "subtest-finished.timestamp"
                        if not incomplete
                        else "subtest-started.timestamp"
                    )
                ),
                timestamp_inferred=timestamp_inferred,
                incomplete=incomplete,
            )
            if sequenced:
                attributes.update(
                    {
                        "test.timing.inferred": True,
                        "test.timing.source": "chunk-order-and-reported-duration",
                    }
                )
            spans.append(
                tests.make(
                    identity,
                    result.test_class,
                    result.name,
                    result.test_type,
                    result.path,
                    result.record.order,
                    name=f"{result.test_class}::{result.name}",
                    interval=test_interval,
                    attributes=attributes,
                    status_code=2 if incomplete else result.status_code,
                    status_message=(
                        status if incomplete or result.status_code == 2 else ""
                    ),
                )
            )
            if sequenced:
                cursor = test_interval.end
        return spans, failed

    def _suite_span(
        self,
        parent: SpanProjector,
        root: Interval,
        trace_index: int,
    ) -> Span | None:
        record = self.source.suite_record
        if record is None:
            return None
        timestamps = self.source.suite_timestamps
        start = root.clamp(min(timestamps) if timestamps else root.start)
        end = Ns(max(start, root.clamp(max(timestamps) if timestamps else start)))
        status_code = 2 if record.failing_errors else 0
        status = record.failing_errors[0][0] or "error" if status_code else ""
        return parent.scoped("ya.suite").make(
            "ya.suite",
            self.source.suite,
            self.source.result_folder,
            trace_index,
            name=f"{self.source.suite} [{self.source.result_folder} suite]",
            interval=Interval(start, end),
            attributes={
                "test.suite": self.source.suite,
                "ya.test_results.folder": self.source.result_folder,
                **_error_attributes(record, "ya.suite"),
                **metric_attributes(record.metrics, prefix="ya.suite.metric"),
            },
            status_code=status_code,
            status_message=status,
        )

    def project(
        self,
        parent: SpanProjector,
        root: Interval,
        trace_index: int,
    ) -> int:
        if (suite := self._suite_span(parent, root, trace_index)) is not None:
            parent.scoped("ya.suite").add(suite)
        for record_index, chunk_data in enumerate(self.source.chunks):
            interval = chunk_data.interval(root)
            identity = (
                f"{self.source.suite}:{self.source.result_folder}:{trace_index}:"
                f"{chunk_data.key or record_index}:{chunk_data.filename}"
            )
            chunk_id = parent.span_id(identity)
            chunk_parent = parent.under(chunk_id)
            record = chunk_data.record
            attributes: dict[str, Any] = {
                "test.suite": self.source.suite,
                "ya.test_results.folder": self.source.result_folder,
            }
            if chunk_data.index is not None:
                attributes["ya.chunk.chunk_index"] = chunk_data.index
            if chunk_data.total is not None:
                attributes["ya.chunk.nchunks"] = chunk_data.total
            if chunk_data.filename:
                attributes["ya.chunk.filename"] = chunk_data.filename
            if record is not None:
                attributes.update(_error_attributes(record, "ya.chunk"))
                attributes.update(_log_attributes(record, "ya.chunk"))
                attributes.update(
                    metric_attributes(record.metrics, prefix="ya.chunk.metric")
                )
            failing_errors = record.failing_errors if record is not None else ()
            status = failing_errors[0][0] or "error" if failing_errors else ""
            chunk_label = (
                f"record {record_index + 1}"
                if chunk_data.key is None
                else f"chunk {chunk_data.index + 1}/{chunk_data.total}"
            )
            chunk = parent.scoped("ya.chunk").make(
                identity,
                name=f"{self.source.suite} [{self.source.result_folder} {chunk_label}]",
                interval=interval,
                attributes=attributes,
                status_code=2 if failing_errors else 0,
                status_message=status,
            )
            metrics = record.metrics if record is not None else {}
            test_spans, test_failed = self._test_spans(
                chunk_parent, chunk_data, interval, identity
            )
            if test_spans:
                update_span_attributes(chunk, {"ya.test.result.count": len(test_spans)})
            stage_spans, stage_total = self._stage_spans(
                chunk_parent, interval, metrics, identity
            )
            if stage_spans:
                update_span_attributes(
                    chunk,
                    {
                        "ya.test.stage.count": len(stage_spans),
                        "ya.test.stage.reported_total_seconds": stage_total.to_s(),
                        "ya.test.stage.timeline.inferred": True,
                        "ya.test.stage.timeline.residual_seconds": Ns(
                            abs(len(interval) - stage_total)
                        ).to_s(),
                    },
                )
            if test_failed:
                set_span_status(chunk, 2, "one or more tests failed")
            parent.scoped("ya.chunk").add(chunk)
            stages = parent.scoped("ya.test.stage")
            tests = parent.scoped("ya.test")
            for span in stage_spans:
                stages.add(span)
            for span in test_spans:
                tests.add(span)
        return len(self.source.chunks)
