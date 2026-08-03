"""Create OTLP spans for parsed ya worker records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..otlp import Interval, Ns, Span, make_span, stable_span_id
from .critical_path import YaCriticalPathEntry
from .evlog_record import YaEvlogRecord


@dataclass(frozen=True, slots=True)
class WorkerSpanFactory:
    record: YaEvlogRecord

    def test_worker_attributes(self) -> dict[str, Any]:
        record = self.record
        kind, tool = record.kind_and_tool
        attributes: dict[str, Any] = {
            "ya.test.worker.kind": kind,
            "ya.test.worker.tag": record.tag,
        }
        if tool:
            attributes["ya.test.worker.tool"] = tool
        if record.test_size:
            attributes["test.size"] = record.test_size
        if record.uid:
            attributes["ya.test.worker.uid"] = record.uid
        if record.thread_name:
            attributes["ya.test.worker.thread"] = record.thread_name
        if record.outputs:
            attributes["ya.test.worker.outputs"] = record.outputs
        identity = record.test_result_identity
        if identity is not None:
            attributes["test.suite"] = identity[0]
            attributes["ya.test_results.folder"] = identity[1]
        return attributes

    def test_node_span(
        self,
        *,
        trace_id: bytes,
        parent_span_id: bytes,
        index: int,
        unmatched: bool,
        failed: bool,
        exit_code: int | None,
    ) -> Span:
        record = self.record
        kind, _ = record.kind_and_tool
        identity = record.test_result_identity
        labels = {
            "test_aggregate": "test results aggregation",
            "test_merge": "test results merge",
            "test_list": "test list result",
            "test_cache_restore": "test result cache restore",
            "test_materialize": "test result materialization",
            "test_cache_store": "test result cache store",
            "test_orchestration": "test support operation",
        }
        label = labels.get(kind, "test worker")
        if identity is not None:
            if kind == "test_execute":
                label = f"{identity[0]} [{identity[1]} test worker]"
            else:
                label = f"{label}: {identity[0]} [{identity[1]}]"
        attributes = self.test_worker_attributes()
        attributes["ya.test.graph_node.kind"] = kind
        if unmatched:
            attributes["ya.test.worker.unmatched"] = True
        if failed and exit_code is not None:
            attributes["process.exit.code"] = exit_code
        return make_span(
            trace_id=trace_id,
            span_id=stable_span_id(
                trace_id,
                "ya.test.node",
                record.name,
                record.tag,
                record.start_ns,
                index,
            ),
            parent_span_id=parent_span_id,
            name=label,
            start_ns=record.start_ns,
            end_ns=record.end_ns,
            attributes=attributes,
            status_code=2 if failed else 0,
            status_message="test graph node failed" if failed else "",
        )

    def matched_test_worker_span(
        self,
        *,
        trace_id: bytes,
        parent_span_id: bytes,
        chunk_span: Span,
        index: int,
        failed: bool,
        exit_code: int | None,
    ) -> Span:
        record = self.record
        chunk = Interval(
            Ns(chunk_span.start_time_unix_nano or 0),
            Ns(chunk_span.end_time_unix_nano or 0),
        )
        envelope = Interval(
            min(record.start_ns, chunk.start),
            max(record.end_ns, chunk.end),
        )
        attributes = self.test_worker_attributes()
        attributes.update(
            {
                "ya.test.worker.reported_seconds": Ns(len(record.interval)).to_s(),
                "ya.test.worker.timing.source": "ya-evlog",
                "ya.test.worker.timeline.adjusted": envelope != record.interval,
            }
        )
        if failed and exit_code is not None:
            attributes["process.exit.code"] = exit_code
        return make_span(
            trace_id=trace_id,
            span_id=stable_span_id(
                trace_id,
                "ya.test.worker",
                record.uid,
                record.start_ns,
                chunk_span.span_id,
                index,
            ),
            parent_span_id=parent_span_id,
            name=f"test worker: {chunk_span.name}",
            start_ns=envelope.start,
            end_ns=envelope.end,
            attributes=attributes,
            status_code=2 if failed else 0,
            status_message="test worker or reported chunk failed" if failed else "",
        )

    def test_worker_phase_span(
        self,
        detail: YaEvlogRecord,
        *,
        trace_id: bytes,
        parent_span_id: bytes,
        index: int,
    ) -> Span:
        record = self.record
        labels = {
            "exec_cmd": "exec command",
            "post_cmd": "post command",
            "node_result": "node result",
        }
        label = labels.get(detail.tag, detail.tag.replace("_", " "))
        return make_span(
            trace_id=trace_id,
            span_id=stable_span_id(
                trace_id,
                "ya.test.worker.phase",
                record.uid,
                detail.tag,
                detail.start_ns,
                index,
            ),
            parent_span_id=parent_span_id,
            name=f"worker phase: {label}",
            start_ns=detail.start_ns,
            end_ns=detail.end_ns,
            attributes={
                "ya.test.worker.phase": detail.tag,
                "ya.test.worker.timing.source": "ya-evlog",
                **({"ya.test.worker.uid": record.uid} if record.uid else {}),
            },
        )

    def _build_span_name(self, kind: str, tool: str) -> str:
        record = self.record
        output = next(iter(record.outputs), "")
        labels = {
            "cache_restore": "cache restore",
            "materialize": "materialize",
            "cache_store": "cache store",
            "execute": tool or "execute",
            "orchestration": tool or record.name,
        }
        label = labels[kind]
        if kind in {"cache_restore", "materialize", "cache_store"} and tool:
            label += f" [{tool}]"
        return f"{label}: {output}" if output else label

    def build_node_span(
        self,
        *,
        trace_id: bytes,
        parent_span_id: bytes,
        kind: str,
        tool: str,
        index: int,
        critical_entry: YaCriticalPathEntry | None,
        failed: bool,
        exit_code: int | None,
    ) -> Span:
        record = self.record
        attributes: dict[str, Any] = {
            "ya.build.kind": kind,
            "ya.build.tag": record.tag,
            "ya.build.node.name": record.name,
            "ya.build.timing.scope": "worker-node",
        }
        if tool:
            attributes["ya.build.tool"] = tool
        if record.outputs:
            attributes["ya.build.outputs"] = record.outputs
        if record.thread_name:
            attributes["ya.worker.thread"] = record.thread_name
        if record.cache_source:
            attributes["ya.build.cache.source"] = record.cache_source
        if record.uid:
            attributes["ya.build.node.uid"] = record.uid
        if critical_entry is not None:
            attributes.update(critical_entry.span_attributes(test=False))
        if failed:
            attributes["ya.build.failed"] = True
            if exit_code is not None:
                attributes["process.exit.code"] = exit_code
        return make_span(
            trace_id=trace_id,
            span_id=stable_span_id(
                trace_id,
                "ya.build.node",
                record.name,
                record.tag,
                record.start_ns,
                index,
            ),
            parent_span_id=parent_span_id,
            name=self._build_span_name(kind, tool),
            start_ns=record.start_ns,
            end_ns=record.end_ns,
            attributes=attributes,
            status_code=2 if failed else 0,
            status_message="build node failed" if failed else "",
        )

    def build_command_span(
        self,
        detail: YaEvlogRecord,
        *,
        trace_id: bytes,
        parent_span_id: bytes,
        tool: str,
        index: int,
        failed: bool,
    ) -> Span:
        record = self.record
        output = next(iter(record.outputs), "")
        label = f"{tool} command" if tool else "exec command"
        attributes: dict[str, Any] = {
            "ya.build.detail.stage": "exec_cmd",
            "ya.build.timing.scope": "command",
        }
        if tool:
            attributes["ya.build.tool"] = tool
        if record.uid:
            attributes["ya.build.node.uid"] = record.uid
        if record.outputs:
            attributes["ya.build.outputs"] = record.outputs
        if record.thread_name:
            attributes["ya.worker.thread"] = record.thread_name
        if failed:
            attributes["ya.build.node.failed"] = True
        return make_span(
            trace_id=trace_id,
            span_id=stable_span_id(
                trace_id,
                "ya.build.command",
                parent_span_id,
                detail.start_ns,
                detail.end_ns,
                index,
            ),
            parent_span_id=parent_span_id,
            name=f"{label}: {output}" if output else label,
            start_ns=detail.start_ns,
            end_ns=detail.end_ns,
            attributes=attributes,
        )
