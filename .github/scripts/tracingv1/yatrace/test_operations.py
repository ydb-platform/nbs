from __future__ import annotations

__test__ = False

from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from ..otlp import (
    Ns,
    Span,
    SpanWriter,
    Trace,
    make_span,
    span_status_code,
    stable_span_id,
    update_span_attributes,
)
from .evlog_record import YaEvlogRecord
from .node import ClassifiedNode
from .test_chunk import TestChunk
from .worker_spans import WorkerSpanFactory


@dataclass(frozen=True, slots=True, order=True)
class TestChunkScore:
    identity_matches: int
    index_matches: int
    overlap_ns: int
    negative_boundary_distance_ns: int


@dataclass(frozen=True, slots=True, order=True)
class TestChunkMatch:
    score: TestChunkScore
    chunk_index: int


@dataclass(frozen=True, slots=True)
class TestChunkIndexes:
    by_identity: Mapping[tuple[str, str], list[int]]
    by_identity_and_index: Mapping[tuple[str, str, int], list[int]]


@dataclass(frozen=True, slots=True)
class TestOperationPlan:
    chunks: list[TestChunk]
    graph_nodes: list[ClassifiedNode]
    test_nodes: list[ClassifiedNode]
    indexes: TestChunkIndexes
    start_ns: Ns
    end_ns: Ns
    span_id: bytes


@dataclass(frozen=True, slots=True)
class TestOperationMatches:
    by_test_node: dict[int, int]
    available_chunks: set[int]


@dataclass(frozen=True, slots=True)
class YaTestOperations:
    classified: Sequence[ClassifiedNode]
    failures: Mapping[str, int | None]

    @staticmethod
    def _chunk_indexes(chunks: list[TestChunk]) -> TestChunkIndexes:
        by_identity: dict[tuple[str, str], list[int]] = defaultdict(list)
        by_identity_and_index: dict[tuple[str, str, int], list[int]] = defaultdict(list)
        for index, chunk in enumerate(chunks):
            if chunk.identity is None:
                continue
            by_identity[chunk.identity].append(index)
            if chunk.chunk_index is not None:
                by_identity_and_index[(*chunk.identity, chunk.chunk_index)].append(
                    index
                )
        return TestChunkIndexes(by_identity, by_identity_and_index)

    def _plan(self, trace: Trace, trace_id: bytes) -> TestOperationPlan | None:
        chunks = [TestChunk.from_span(span) for span in trace.spans("ya.chunk")]
        graph_nodes = [node for node in self.classified if node.is_test]
        if not chunks and not graph_nodes:
            return None
        test_nodes = [node for node in graph_nodes if node.kind == "test_execute"]
        intervals = [
            *[node.interval for node in graph_nodes],
            *[chunk.interval for chunk in chunks],
        ]
        start_ns = min(interval.start for interval in intervals)
        end_ns = max(interval.end for interval in intervals)
        return TestOperationPlan(
            chunks=chunks,
            graph_nodes=graph_nodes,
            test_nodes=test_nodes,
            indexes=self._chunk_indexes(chunks),
            start_ns=start_ns,
            end_ns=end_ns,
            span_id=stable_span_id(
                trace_id,
                "ya.test.operations",
                start_ns,
                end_ns,
            ),
        )

    @staticmethod
    def _chunk_match_score(
        record: YaEvlogRecord,
        chunk: TestChunk,
    ) -> TestChunkScore | None:
        identity = record.test_result_identity
        identity_matches = bool(identity and identity == chunk.identity)
        overlap = chunk.overlap(record.interval)
        if (identity is not None and not identity_matches) or (
            identity is None and not overlap
        ):
            return None
        record_chunk_index = record.test_result_chunk_index
        index_matches = (
            record_chunk_index is not None and record_chunk_index == chunk.chunk_index
        )
        if (
            record_chunk_index is not None
            and chunk.chunk_index is not None
            and not index_matches
        ):
            return None
        return TestChunkScore(
            int(identity_matches),
            int(index_matches),
            overlap,
            -record.interval.boundary_distance(chunk.interval),
        )

    @staticmethod
    def _candidate_chunk_indexes(
        record: YaEvlogRecord,
        plan: TestOperationPlan,
        available: set[int],
    ) -> Sequence[int]:
        identity = record.test_result_identity
        chunk_index = record.test_result_chunk_index
        if identity is None:
            return available
        if chunk_index is None:
            return plan.indexes.by_identity.get(identity, ())
        return plan.indexes.by_identity_and_index.get((*identity, chunk_index), ())

    def _match_chunks(self, plan: TestOperationPlan) -> TestOperationMatches:
        available = set(range(len(plan.chunks)))
        matches: dict[int, int] = {}
        for record_index, node in enumerate(plan.test_nodes):
            candidates = [
                TestChunkMatch(score, chunk_index)
                for chunk_index in self._candidate_chunk_indexes(
                    node.record,
                    plan,
                    available,
                )
                if chunk_index in available
                if (
                    score := self._chunk_match_score(
                        node.record,
                        plan.chunks[chunk_index],
                    )
                )
                is not None
            ]
            if not candidates:
                continue
            chunk_index = max(candidates).chunk_index
            matches[record_index] = chunk_index
            available.remove(chunk_index)
        return TestOperationMatches(matches, available)

    @staticmethod
    def _add_worker_phases(
        writer: SpanWriter,
        record: YaEvlogRecord,
        *,
        trace_id: bytes,
        parent_span_id: bytes,
    ) -> None:
        for index, detail in enumerate(record.details):
            writer.add(
                WorkerSpanFactory(record).test_worker_phase_span(
                    detail,
                    trace_id=trace_id,
                    parent_span_id=parent_span_id,
                    index=index,
                ),
                scope_name="ya.test.worker.phase",
            )

    def _add_test_node(
        self,
        writer: SpanWriter,
        node: ClassifiedNode,
        *,
        trace_id: bytes,
        parent_span_id: bytes,
        index: int,
        unmatched: bool,
    ) -> None:
        failed = bool(node.uid and node.uid in self.failures)
        span = WorkerSpanFactory(node.record).test_node_span(
            trace_id=trace_id,
            parent_span_id=parent_span_id,
            index=index,
            unmatched=unmatched,
            failed=failed,
            exit_code=self.failures.get(node.uid) if failed else None,
        )
        writer.add(span, scope_name="ya.test.node")
        self._add_worker_phases(
            writer,
            node.record,
            trace_id=trace_id,
            parent_span_id=span.span_id,
        )

    def _emit_matched_workers(
        self,
        plan: TestOperationPlan,
        matches: TestOperationMatches,
        tests_by_parent: Mapping[bytes, Sequence[Span]],
        *,
        writer: SpanWriter,
        trace_id: bytes,
    ) -> None:
        for record_index, chunk_index in matches.by_test_node.items():
            node = plan.test_nodes[record_index]
            chunk_span = plan.chunks[chunk_index].span
            factory = WorkerSpanFactory(node.record)
            worker_attributes = factory.test_worker_attributes()
            update_span_attributes(chunk_span, worker_attributes)
            if test_size := worker_attributes.get("test.size"):
                for test_span in tests_by_parent.get(chunk_span.span_id, ()):
                    update_span_attributes(test_span, {"test.size": test_size})
            failed = bool(
                (node.uid and node.uid in self.failures)
                or span_status_code(chunk_span) == 2
            )
            worker_span = factory.matched_test_worker_span(
                trace_id=trace_id,
                parent_span_id=plan.span_id,
                chunk_span=chunk_span,
                index=record_index,
                failed=failed,
                exit_code=self.failures.get(node.uid) if node.uid else None,
            )
            chunk_span.parent_span_id = worker_span.span_id
            writer.add(worker_span, scope_name="ya.test.worker")
            self._add_worker_phases(
                writer,
                node.record,
                trace_id=trace_id,
                parent_span_id=worker_span.span_id,
            )

    def _emit_graph_nodes(
        self,
        plan: TestOperationPlan,
        matches: TestOperationMatches,
        *,
        writer: SpanWriter,
        trace_id: bytes,
    ) -> int:
        unmatched = [
            (index, node)
            for index, node in enumerate(plan.test_nodes)
            if index not in matches.by_test_node
        ]
        for index, node in unmatched:
            self._add_test_node(
                writer,
                node,
                trace_id=trace_id,
                parent_span_id=plan.span_id,
                index=index,
                unmatched=True,
            )
        processing = [node for node in plan.graph_nodes if node.kind != "test_execute"]
        for index, node in enumerate(processing, start=len(plan.test_nodes)):
            self._add_test_node(
                writer,
                node,
                trace_id=trace_id,
                parent_span_id=plan.span_id,
                index=index,
                unmatched=False,
            )
        return len(unmatched)

    def _operation_attributes(
        self,
        plan: TestOperationPlan,
        unmatched_count: int,
    ) -> dict[str, Any]:
        attributes = {
            "ya.test.wall_time.kind": "worker-node-execution-envelope",
            "ya.test.worker.execute.count": len(plan.test_nodes),
            "ya.test.worker.unmatched.count": unmatched_count,
            "ya.test.worker.cumulative_seconds": Ns(
                sum(len(node.interval) for node in plan.test_nodes)
            ).to_s(),
            "ya.test.chunk.count": len(plan.chunks),
        }
        counts = Counter(
            node.kind.removeprefix("test_")
            for node in plan.graph_nodes
            if node.kind != "test_execute"
        )
        for kind, count in sorted(counts.items()):
            attributes[f"ya.test.graph_node.{kind}.count"] = count
        return attributes

    def build(
        self,
        *,
        writer: SpanWriter,
        trace_id: bytes,
        parent_span_id: bytes,
    ) -> dict[str, Any]:
        trace = writer.trace
        plan = self._plan(trace, trace_id)
        if plan is None:
            return {}
        matches = self._match_chunks(plan)
        tests_by_parent: dict[bytes, list[Span]] = defaultdict(list)
        for span in trace.spans("ya.test"):
            tests_by_parent[span.parent_span_id].append(span)
        self._emit_matched_workers(
            plan,
            matches,
            tests_by_parent,
            writer=writer,
            trace_id=trace_id,
        )
        for chunk_index in matches.available_chunks:
            plan.chunks[chunk_index].span.parent_span_id = plan.span_id
        for suite_span in trace.spans("ya.suite"):
            suite_span.parent_span_id = plan.span_id
        unmatched_count = self._emit_graph_nodes(
            plan,
            matches,
            writer=writer,
            trace_id=trace_id,
        )
        failed = any(
            node.uid and node.uid in self.failures for node in plan.graph_nodes
        ) or any(span_status_code(chunk.span) == 2 for chunk in plan.chunks)
        attributes = self._operation_attributes(plan, unmatched_count)
        writer.add(
            make_span(
                trace_id=trace_id,
                span_id=plan.span_id,
                parent_span_id=parent_span_id,
                name="test operations",
                start_ns=plan.start_ns,
                end_ns=plan.end_ns,
                attributes=attributes,
                status_code=2 if failed else 0,
                status_message="one or more test operations failed" if failed else "",
            ),
            scope_name="ya.test.operations",
        )
        return {
            "ya.test.worker.execute.count": len(plan.test_nodes),
            "ya.test.worker.unmatched.count": unmatched_count,
            "ya.test.chunk.count": len(plan.chunks),
        }
