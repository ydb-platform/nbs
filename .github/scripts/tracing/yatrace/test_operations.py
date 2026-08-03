from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from ..otlp import Interval, Ns, Span, span_status_code, update_span_attributes
from ..projector import SpanProjector
from .node import YaNode
from .span_chunk import TestChunk
from .worker_spans import WorkerSpanProjector


def _chunk_score(node: YaNode, chunk: TestChunk) -> tuple[int, int, int, int] | None:
    identity_matches = bool(node.test_identity and node.test_identity == chunk.identity)
    overlap = chunk.overlap(node.interval)
    if (node.test_identity is not None and not identity_matches) or (
        node.test_identity is None and not overlap
    ):
        return None
    index_matches = (
        node.test_chunk_index is not None and node.test_chunk_index == chunk.chunk_index
    )
    if (
        node.test_chunk_index is not None
        and chunk.chunk_index is not None
        and not index_matches
    ):
        return None
    return (
        int(identity_matches),
        int(index_matches),
        overlap,
        -node.interval.boundary_distance(chunk.interval),
    )


def _match_chunks(
    nodes: Sequence[YaNode], chunks: Sequence[TestChunk]
) -> tuple[dict[int, int], set[int]]:
    by_identity: dict[tuple[str, str], list[int]] = defaultdict(list)
    by_chunk: dict[tuple[str, str, int], list[int]] = defaultdict(list)
    for index, chunk in enumerate(chunks):
        if chunk.identity is None:
            continue
        by_identity[chunk.identity].append(index)
        if chunk.chunk_index is not None:
            by_chunk[(*chunk.identity, chunk.chunk_index)].append(index)

    available = set(range(len(chunks)))
    matches: dict[int, int] = {}
    for node_index, node in enumerate(nodes):
        if node.test_identity is None:
            indexes: Sequence[int] = tuple(available)
        elif node.test_chunk_index is None:
            indexes = by_identity.get(node.test_identity, ())
        else:
            indexes = by_chunk.get((*node.test_identity, node.test_chunk_index), ())
        candidates = [
            (score, chunk_index)
            for chunk_index in indexes
            if chunk_index in available
            if (score := _chunk_score(node, chunks[chunk_index])) is not None
        ]
        if candidates:
            chunk_index = max(candidates)[1]
            matches[node_index] = chunk_index
            available.remove(chunk_index)
    return matches, available


@dataclass(frozen=True, slots=True)
class YaTestOperations:
    nodes: Sequence[YaNode]
    failures: Mapping[str, int | None]

    def _node(
        self,
        node: YaNode,
        parent: SpanProjector,
        *,
        index: int,
        unmatched: bool,
    ) -> None:
        failed = bool(node.uid and node.uid in self.failures)
        span = WorkerSpanProjector(node, parent).test_node(
            index=index,
            unmatched=unmatched,
            failed=failed,
            exit_code=self.failures.get(node.uid) if failed else None,
        )
        WorkerSpanProjector(node, parent.under(span)).test_worker_phases()

    def project(self, parent: SpanProjector) -> dict[str, Any]:
        chunks = [TestChunk.from_span(span) for span in parent.trace.spans("ya.chunk")]
        graph_nodes = [node for node in self.nodes if node.is_test]
        if not chunks and not graph_nodes:
            return {}
        test_nodes = [node for node in graph_nodes if node.kind == "test_execute"]
        intervals = [
            *[node.interval for node in graph_nodes],
            *[chunk.interval for chunk in chunks],
        ]
        interval = Interval(
            min(item.start for item in intervals),
            max(item.end for item in intervals),
        )
        operation_id = parent.span_id(
            "ya.test.operations", interval.start, interval.end
        )
        context = parent.under(operation_id)
        matches, available = _match_chunks(test_nodes, chunks)
        tests_by_parent: dict[bytes, list[Span]] = defaultdict(list)
        for span in parent.trace.spans("ya.test"):
            tests_by_parent[span.parent_span_id].append(span)

        for node_index, chunk_index in matches.items():
            node = test_nodes[node_index]
            chunk_span = chunks[chunk_index].span
            worker = WorkerSpanProjector(node, context)
            worker_attributes = worker.test_attributes()
            update_span_attributes(chunk_span, worker_attributes)
            if test_size := worker_attributes.get("test.size"):
                for test_span in tests_by_parent.get(chunk_span.span_id, ()):
                    update_span_attributes(test_span, {"test.size": test_size})
            failed = bool(
                (node.uid and node.uid in self.failures)
                or span_status_code(chunk_span) == 2
            )
            worker_span = worker.matched_test_worker(
                chunk_span,
                index=node_index,
                failed=failed,
                exit_code=self.failures.get(node.uid) if node.uid else None,
            )
            chunk_span.parent_span_id = worker_span.span_id
            WorkerSpanProjector(node, context.under(worker_span)).test_worker_phases()

        for chunk_index in available:
            chunks[chunk_index].span.parent_span_id = operation_id
        for suite_span in parent.trace.spans("ya.suite"):
            suite_span.parent_span_id = operation_id
        unmatched = [
            (index, node)
            for index, node in enumerate(test_nodes)
            if index not in matches
        ]
        for index, node in unmatched:
            self._node(node, context, index=index, unmatched=True)
        processing = [node for node in graph_nodes if node.kind != "test_execute"]
        for index, node in enumerate(processing, start=len(test_nodes)):
            self._node(node, context, index=index, unmatched=False)

        failed = any(
            node.uid and node.uid in self.failures for node in graph_nodes
        ) or any(span_status_code(chunk.span) == 2 for chunk in chunks)
        attributes = {
            "ya.test.wall_time.kind": "worker-node-execution-envelope",
            "ya.test.worker.execute.count": len(test_nodes),
            "ya.test.worker.unmatched.count": len(unmatched),
            "ya.test.worker.cumulative_seconds": Ns(
                sum(len(node.interval) for node in test_nodes)
            ).to_s(),
            "ya.test.chunk.count": len(chunks),
        }
        for kind, count in sorted(
            Counter(
                node.kind.removeprefix("test_")
                for node in graph_nodes
                if node.kind != "test_execute"
            ).items()
        ):
            attributes[f"ya.test.graph_node.{kind}.count"] = count
        parent.scoped("ya.test.operations").emit(
            "ya.test.operations",
            interval.start,
            interval.end,
            name="test operations",
            interval=interval,
            attributes=attributes,
            status_code=2 if failed else 0,
            status_message=("one or more test operations failed" if failed else ""),
        )
        return {
            "ya.test.worker.execute.count": len(test_nodes),
            "ya.test.worker.unmatched.count": len(unmatched),
            "ya.test.chunk.count": len(chunks),
        }
