from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Sequence

from ..otlp import (
    Interval,
    Ns,
    ResourceAttributes,
    Span,
    Trace,
    decode_attributes,
    make_span,
    span_status_code,
    stable_span_id,
    update_span_attributes,
)
from .evlog_record import YaEvlogRecord

_ChunkSpan = tuple[Span, dict[str, Any], Interval]


class _YaTestOperations:
    @staticmethod
    def _test_chunk_spans(trace: Trace) -> list[_ChunkSpan]:
        return [
            (
                span,
                decode_attributes(span.attributes),
                Interval(
                    Ns(span.start_time_unix_nano or 0),
                    Ns(span.end_time_unix_nano or 0),
                ),
            )
            for span in trace.spans("ya.chunk")
        ]

    @staticmethod
    def _test_chunk_match_score(
        record: YaEvlogRecord,
        chunk: _ChunkSpan,
        *,
        identity: tuple[str, str] | None,
        record_chunk_index: int | None,
    ) -> tuple[int, int, int, int] | None:
        _, attributes, interval = chunk
        chunk_identity = (
            str(attributes.get("test.suite", "")),
            str(attributes.get("ya.test_results.folder", "")),
        )
        identity_matches = bool(identity and identity == chunk_identity)
        overlap = record.interval.overlap(interval)
        if identity is not None and not identity_matches:
            return None
        if identity is None and not overlap:
            return None

        chunk_index = attributes.get("ya.chunk.chunk_index")
        index_matches = (
            record_chunk_index is not None
            and isinstance(chunk_index, int)
            and not isinstance(chunk_index, bool)
            and record_chunk_index == chunk_index
        )
        if (
            record_chunk_index is not None
            and chunk_index is not None
            and not index_matches
        ):
            return None
        return (
            int(identity_matches),
            int(index_matches),
            overlap,
            -record.interval.boundary_distance(interval),
        )

    def _build_test_operations(
        self,
        *,
        trace: Trace,
        trace_id: bytes,
        parent_span_id: bytes,
        classified: Sequence[tuple[YaEvlogRecord, str, str]],
        resource: ResourceAttributes,
    ) -> dict[str, Any]:
        chunks = self._test_chunk_spans(trace)
        chunks_by_identity: dict[tuple[str, str], list[int]] = defaultdict(list)
        chunks_by_identity_and_index: dict[tuple[str, str, int], list[int]] = (
            defaultdict(list)
        )
        for chunk_index, (_, attributes, _) in enumerate(chunks):
            identity = (
                str(attributes.get("test.suite", "")),
                str(attributes.get("ya.test_results.folder", "")),
            )
            if not all(identity):
                continue
            chunks_by_identity[identity].append(chunk_index)
            report_chunk_index = attributes.get("ya.chunk.chunk_index")
            if isinstance(report_chunk_index, int) and not isinstance(
                report_chunk_index, bool
            ):
                chunks_by_identity_and_index[(*identity, report_chunk_index)].append(
                    chunk_index
                )
        graph_records = [
            (record, kind) for record, kind, _ in classified if kind.startswith("test_")
        ]
        test_records = [
            record for record, kind in graph_records if kind == "test_execute"
        ]
        if not chunks and not graph_records:
            return {}

        intervals = [
            *[record.interval for record, _ in graph_records],
            *[interval for _, _, interval in chunks],
        ]
        start_ns = min(interval.start for interval in intervals)
        end_ns = max(interval.end for interval in intervals)
        operations_span_id = stable_span_id(
            trace_id,
            "ya.test.operations",
            start_ns,
            end_ns,
        )

        available_chunks = set(range(len(chunks)))
        matches: dict[int, int] = {}
        for record_index, record in enumerate(test_records):
            identity = record.test_result_identity
            record_chunk_index = record.test_result_chunk_index
            if identity is None:
                candidate_indices = available_chunks
            elif record_chunk_index is None:
                candidate_indices = chunks_by_identity.get(identity, ())
            else:
                candidate_indices = chunks_by_identity_and_index.get(
                    (*identity, record_chunk_index),
                    (),
                )
            candidates = [
                (score, chunk_index)
                for chunk_index in candidate_indices
                if chunk_index in available_chunks
                if (
                    score := self._test_chunk_match_score(
                        record,
                        chunks[chunk_index],
                        identity=identity,
                        record_chunk_index=record_chunk_index,
                    )
                )
                is not None
            ]
            if not candidates:
                continue
            _, chunk_index = max(candidates)
            matches[record_index] = chunk_index
            available_chunks.remove(chunk_index)

        tests_by_parent: dict[bytes, list[Span]] = defaultdict(list)
        for test_span in trace.spans("ya.test"):
            tests_by_parent[test_span.parent_span_id].append(test_span)

        failed_test_uids = {
            record.uid
            for record, _ in graph_records
            if record.uid and record.uid in self.failures
        }
        for record_index, chunk_index in matches.items():
            record = test_records[record_index]
            chunk_span = chunks[chunk_index][0]
            worker_attributes = record.test_worker_attributes()
            update_span_attributes(chunk_span, worker_attributes)
            test_size = worker_attributes.get("test.size")
            if test_size:
                for test_span in tests_by_parent.get(chunk_span.span_id, ()):
                    update_span_attributes(test_span, {"test.size": test_size})
            failed = bool(
                (record.uid and record.uid in self.failures)
                or span_status_code(chunk_span) == 2
            )
            worker_span = record.matched_test_worker_span(
                trace_id=trace_id,
                parent_span_id=operations_span_id,
                chunk_span=chunk_span,
                index=record_index,
                failed=failed,
                exit_code=(
                    self.failures.get(record.uid)
                    if record.uid and record.uid in self.failures
                    else None
                ),
            )
            chunk_span.parent_span_id = worker_span.span_id
            trace.add_span(
                worker_span,
                resource=resource,
                scope_name="ya.test.worker",
            )
            for detail_index, detail in enumerate(record.details):
                trace.add_span(
                    record.test_worker_phase_span(
                        detail,
                        trace_id=trace_id,
                        parent_span_id=worker_span.span_id,
                        index=detail_index,
                    ),
                    resource=resource,
                    scope_name="ya.test.worker.phase",
                )

        for chunk_index in available_chunks:
            chunks[chunk_index][0].parent_span_id = operations_span_id
        for suite_span in trace.spans("ya.suite"):
            suite_span.parent_span_id = operations_span_id

        unmatched_records = [
            (record_index, record)
            for record_index, record in enumerate(test_records)
            if record_index not in matches
        ]
        for record_index, record in unmatched_records:
            failed = bool(record.uid and record.uid in self.failures)
            node_span = record.test_node_span(
                trace_id=trace_id,
                parent_span_id=operations_span_id,
                index=record_index,
                unmatched=True,
                failed=failed,
                exit_code=self.failures.get(record.uid) if failed else None,
            )
            trace.add_span(
                node_span,
                resource=resource,
                scope_name="ya.test.node",
            )
            for detail_index, detail in enumerate(record.details):
                trace.add_span(
                    record.test_worker_phase_span(
                        detail,
                        trace_id=trace_id,
                        parent_span_id=node_span.span_id,
                        index=detail_index,
                    ),
                    resource=resource,
                    scope_name="ya.test.worker.phase",
                )

        processing_records = [
            (record, kind) for record, kind in graph_records if kind != "test_execute"
        ]
        for record_index, (record, _) in enumerate(
            processing_records,
            start=len(test_records),
        ):
            failed = bool(record.uid and record.uid in self.failures)
            node_span = record.test_node_span(
                trace_id=trace_id,
                parent_span_id=operations_span_id,
                index=record_index,
                unmatched=False,
                failed=failed,
                exit_code=self.failures.get(record.uid) if failed else None,
            )
            trace.add_span(
                node_span,
                resource=resource,
                scope_name="ya.test.node",
            )
            for detail_index, detail in enumerate(record.details):
                trace.add_span(
                    record.test_worker_phase_span(
                        detail,
                        trace_id=trace_id,
                        parent_span_id=node_span.span_id,
                        index=detail_index,
                    ),
                    resource=resource,
                    scope_name="ya.test.worker.phase",
                )

        failed_chunks = sum(
            span_status_code(chunk_span) == 2 for chunk_span, _, _ in chunks
        )
        attributes = {
            "ya.test.wall_time.kind": "worker-node-execution-envelope",
            "ya.test.worker.execute.count": len(test_records),
            "ya.test.worker.unmatched.count": len(unmatched_records),
            "ya.test.worker.cumulative_seconds": Ns(
                sum(len(record.interval) for record in test_records)
            ).to_s(),
            "ya.test.chunk.count": len(chunks),
        }
        processing_counts = Counter(
            kind.removeprefix("test_") for _, kind in processing_records
        )
        for kind, count in sorted(processing_counts.items()):
            attributes[f"ya.test.graph_node.{kind}.count"] = count
        trace.add_span(
            make_span(
                trace_id=trace_id,
                span_id=operations_span_id,
                parent_span_id=parent_span_id,
                name="test operations",
                start_ns=start_ns,
                end_ns=end_ns,
                attributes=attributes,
                status_code=2 if failed_test_uids or failed_chunks else 0,
                status_message=(
                    "one or more test operations failed"
                    if failed_test_uids or failed_chunks
                    else ""
                ),
            ),
            resource=resource,
            scope_name="ya.test.operations",
        )
        return {
            "ya.test.worker.execute.count": len(test_records),
            "ya.test.worker.unmatched.count": len(unmatched_records),
            "ya.test.chunk.count": len(chunks),
        }
