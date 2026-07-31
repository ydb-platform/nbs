from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterable, Mapping

from ..otlp import (
    Interval,
    Ns,
    ResourceAttributes,
    Span,
    Trace,
    make_span,
    set_span_status,
    span_status_code,
    stable_span_id,
    update_span_attributes,
)
from .event import YaEvent
from .metrics import _metric_attributes
from .test_timing import YaTestTiming

if TYPE_CHECKING:
    from .trace_file import YaTraceFile


@dataclass(frozen=True, slots=True)
class YaTraceSpanBuilder:
    source: YaTraceFile

    @staticmethod
    def _stage_spans(
        trace_id: bytes,
        chunk_span_id: bytes,
        chunk: Interval,
        metrics: Mapping[str, Any],
        identity: str,
    ) -> tuple[list[Span], Ns]:
        prefix = "suite_"
        suffix = "_(seconds)"
        cursor = chunk.start
        reported_total = Ns(0)
        result: list[Span] = []
        for metric_name, raw_duration in metrics.items():
            name = str(metric_name)
            if not name.startswith(prefix) or not name.endswith(suffix):
                continue
            stage_name = name[len(prefix) : -len(suffix)]
            duration = Ns.from_s(raw_duration)
            if duration is None or not duration or not stage_name:
                continue
            reported_total = Ns(reported_total + duration)
            end = chunk.clamp(Ns(cursor + duration))
            if end <= cursor:
                continue
            result.append(
                make_span(
                    trace_id=trace_id,
                    span_id=stable_span_id(
                        trace_id,
                        identity,
                        "ya.test.stage",
                        stage_name,
                        len(result),
                    ),
                    parent_span_id=chunk_span_id,
                    name=f"test stage: {stage_name.replace('_', ' ')}",
                    start_ns=cursor,
                    end_ns=end,
                    attributes={
                        "ya.test.stage.name": stage_name,
                        "ya.test.stage.reported_seconds": duration.to_s(),
                        "ya.test.stage.timing.source": (
                            "ya-chunk-cumulative-stage-duration"
                        ),
                        "test.timing.inferred": True,
                    },
                )
            )
            cursor = end
        return result, reported_total

    @staticmethod
    def _test_spans(
        trace_id: bytes,
        chunk_span_id: bytes,
        chunk: Interval,
        events: Iterable[YaEvent],
        identity: str,
        inferred_test_start_ns: Ns | None = None,
    ) -> list[Span]:
        grouped: dict[tuple[str, str, str, str], list[YaEvent]] = defaultdict(list)
        for event in events:
            if event.name in {"subtest-started", "subtest-finished"}:
                grouped[event.logical_test_key].append(event)

        result: list[Span] = []
        for (
            test_class,
            subtest,
            test_type,
            test_path,
        ), test_events in sorted(grouped.items()):
            starts = sorted(
                (event for event in test_events if event.name == "subtest-started"),
                key=lambda event: event.order,
            )
            finishes = [
                event for event in test_events if event.name == "subtest-finished"
            ]
            finishes.sort(key=lambda event: event.order)
            start = starts[0] if starts else None
            finish = None
            for candidate in finishes:
                if (
                    finish is None
                    or finish.status[0] in {"crashed", "deselected", "not_launched"}
                    or candidate.status[0] != "deselected"
                ):
                    finish = candidate
            if finish is not None and finish.status[0] in {
                "deselected",
                "not_launched",
            }:
                start = None

            timing = YaTestTiming.resolve(
                start,
                finish,
                chunk=chunk,
                inferred_start=inferred_test_start_ns,
                only_test=len(grouped) == 1,
            )
            if timing is None:
                continue
            status, status_code = timing.status

            result.append(
                make_span(
                    trace_id=trace_id,
                    span_id=stable_span_id(
                        trace_id,
                        identity,
                        test_class,
                        subtest,
                        test_type,
                        test_path,
                        timing.order,
                    ),
                    parent_span_id=chunk_span_id,
                    name=f"{test_class}::{subtest}",
                    start_ns=timing.interval.start,
                    end_ns=timing.interval.end,
                    attributes=timing.attributes(test_class, subtest),
                    status_code=status_code,
                    status_message=status if status_code == 2 else "",
                )
            )
        return result

    def _suite_span(
        self,
        *,
        trace_id: bytes,
        root_span_id: bytes,
        root: Interval,
        trace_index: int,
    ) -> Span | None:
        source = self.source
        suite_event = source.suite_event()
        if suite_event is None:
            return None
        timestamps = [
            event.timestamp_ns
            for event in source.events
            if event.name == "suite-event" and event.timestamp_ns is not None
        ]
        start_ns = root.clamp(min(timestamps) if timestamps else root.start)
        end_ns = Ns(
            max(
                start_ns,
                root.clamp(max(timestamps) if timestamps else start_ns),
            )
        )
        status_code = 2 if suite_event.failing_errors else 0
        status = suite_event.failing_errors[0][0] or "error" if status_code else ""
        attributes: dict[str, Any] = {
            "test.suite": source.suite,
            "ya.test_results.folder": source.result_folder,
            **suite_event.error_attributes("ya.suite"),
            **_metric_attributes(
                suite_event.value.get("metrics"),
                prefix="ya.suite.metric",
            ),
        }
        return make_span(
            trace_id=trace_id,
            span_id=stable_span_id(
                trace_id,
                "ya.suite",
                source.suite,
                source.result_folder,
                trace_index,
            ),
            parent_span_id=root_span_id,
            name=f"{source.suite} [{source.result_folder} suite]",
            start_ns=start_ns,
            end_ns=end_ns,
            attributes=attributes,
            status_code=status_code,
            status_message=status,
        )

    def build(
        self,
        *,
        trace: Trace,
        trace_id: bytes,
        root_span_id: bytes,
        root_start_ns: Ns,
        root_end_ns: Ns,
        resource: ResourceAttributes,
        trace_index: int,
    ) -> int:
        source = self.source
        writer = trace.writer(resource)
        root = Interval(root_start_ns, root_end_ns)
        suite_span = self._suite_span(
            trace_id=trace_id,
            root_span_id=root_span_id,
            root=root,
            trace_index=trace_index,
        )
        if suite_span is not None:
            writer.add(suite_span, scope_name="ya.suite")

        chunk_records = source.chunk_records()
        for record_index, (chunk_event, chunk_events) in enumerate(chunk_records):
            chunk_interval, chunk_value = source._chunk_interval(
                chunk_event,
                chunk_events,
                root,
            )
            chunk_key = YaEvent.parse_chunk_key(chunk_value)
            identity = (
                f"{source.suite}:{source.result_folder}:{trace_index}:"
                f"{chunk_key or record_index}:{chunk_event.chunk_filename if chunk_event else ''}"
            )
            chunk_span_id = stable_span_id(trace_id, identity)
            attributes = {
                "test.suite": source.suite,
                "ya.test_results.folder": source.result_folder,
            }
            for field_name in ("chunk_index", "nchunks"):
                if field_name in chunk_value:
                    attributes[f"ya.chunk.{field_name}"] = chunk_value[field_name]
            if chunk_event is not None and chunk_event.chunk_filename:
                attributes["ya.chunk.filename"] = chunk_event.chunk_filename
            if chunk_event is not None:
                attributes.update(chunk_event.error_attributes("ya.chunk"))
                attributes.update(chunk_event.log_attributes("ya.chunk"))
            attributes.update(
                _metric_attributes(
                    chunk_value.get("metrics"),
                    prefix="ya.chunk.metric",
                )
            )
            chunk_status = ""
            chunk_status_code = 0
            if chunk_event is not None and chunk_event.failing_errors:
                chunk_status_code = 2
                chunk_status = chunk_event.failing_errors[0][0] or "error"
            if chunk_key is None:
                chunk_label = f"record {record_index + 1}"
            else:
                chunk_label = f"chunk {chunk_key[0] + 1}/{chunk_key[1]}"
            chunk = make_span(
                trace_id=trace_id,
                span_id=chunk_span_id,
                parent_span_id=root_span_id,
                name=f"{source.suite} [{source.result_folder} {chunk_label}]",
                start_ns=chunk_interval.start,
                end_ns=chunk_interval.end,
                attributes=attributes,
                status_code=chunk_status_code,
                status_message=chunk_status if chunk_status_code == 2 else "",
            )
            metrics = chunk_value.get("metrics", {})
            test_start_ns = None
            if isinstance(metrics, Mapping):
                delay_ns = Ns.from_s_or_zero(
                    metrics.get("suite_delay_until_first_test_secs")
                )
                startup_ns = Ns.from_s_or_zero(metrics.get("suite_binary_startup_secs"))
                first_test_offset_ns = max(0, delay_ns - startup_ns)
                if delay_ns or startup_ns:
                    test_start_ns = Ns(chunk_interval.start + first_test_offset_ns)
            test_spans = self._test_spans(
                trace_id,
                chunk_span_id,
                chunk_interval,
                chunk_events,
                identity,
                inferred_test_start_ns=test_start_ns,
            )
            stage_spans: list[Span] = []
            if isinstance(metrics, Mapping):
                stage_spans, reported_stage_total = self._stage_spans(
                    trace_id,
                    chunk_span_id,
                    chunk_interval,
                    metrics,
                    identity,
                )
                if stage_spans:
                    update_span_attributes(
                        chunk,
                        {
                            "ya.test.stage.count": len(stage_spans),
                            "ya.test.stage.reported_total_seconds": (
                                reported_stage_total.to_s()
                            ),
                            "ya.test.stage.timeline.inferred": True,
                            "ya.test.stage.timeline.residual_seconds": Ns(
                                abs(len(chunk_interval) - reported_stage_total)
                            ).to_s(),
                        },
                    )
            if any(span_status_code(span) == 2 for span in test_spans):
                set_span_status(chunk, 2, "one or more tests failed")
            writer.add(chunk, scope_name="ya.chunk")
            for stage_span in stage_spans:
                writer.add(stage_span, scope_name="ya.test.stage")
            for test_span in test_spans:
                writer.add(test_span, scope_name="ya.test")
        return len(chunk_records)
