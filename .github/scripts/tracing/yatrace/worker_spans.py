from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..otlp import Interval, Ns, Span
from ..projector import SpanProjector
from .critical_path import YaCriticalPathEntry
from .node import YaNode


@dataclass(frozen=True, slots=True)
class WorkerSpanProjector:
    node: YaNode
    parent: SpanProjector

    def test_attributes(self) -> dict[str, Any]:
        node = self.node
        identity = node.test_identity
        return {
            "ya.test.worker.kind": node.kind,
            "ya.test.worker.tag": node.tag,
            **({"ya.test.worker.tool": node.tool} if node.tool else {}),
            **({"test.size": node.test_size} if node.test_size else {}),
            **({"ya.test.worker.uid": node.uid} if node.uid else {}),
            **({"ya.test.worker.thread": node.thread} if node.thread else {}),
            **({"ya.test.worker.outputs": node.outputs} if node.outputs else {}),
            **(
                {
                    "test.suite": identity[0],
                    "ya.test_results.folder": identity[1],
                }
                if identity is not None
                else {}
            ),
        }

    def test_node(
        self,
        *,
        index: int,
        unmatched: bool,
        failed: bool,
        exit_code: int | None,
    ) -> Span:
        node = self.node
        kind = node.kind
        identity = node.test_identity
        label = {
            "test_aggregate": "test results aggregation",
            "test_merge": "test results merge",
            "test_list": "test list result",
            "test_cache_restore": "test result cache restore",
            "test_materialize": "test result materialization",
            "test_cache_store": "test result cache store",
            "test_orchestration": "test support operation",
        }.get(kind, "test worker")
        if identity is not None:
            label = (
                f"{identity[0]} [{identity[1]} test worker]"
                if kind == "test_execute"
                else f"{label}: {identity[0]} [{identity[1]}]"
            )
        attributes = self.test_attributes()
        attributes.update(
            {
                "ya.test.graph_node.kind": kind,
                **({"ya.test.worker.unmatched": True} if unmatched else {}),
                **(
                    {"process.exit.code": exit_code}
                    if failed and exit_code is not None
                    else {}
                ),
            }
        )
        return self.parent.scoped("ya.test.node").emit(
            "ya.test.node",
            node.name,
            node.tag,
            node.start_ns,
            index,
            name=label,
            interval=node.interval,
            attributes=attributes,
            status_code=2 if failed else 0,
            status_message="test graph node failed" if failed else "",
        )

    def matched_test_worker(
        self,
        chunk_span: Span,
        *,
        index: int,
        failed: bool,
        exit_code: int | None,
    ) -> Span:
        node = self.node
        chunk = Interval(
            Ns(chunk_span.start_time_unix_nano or 0),
            Ns(chunk_span.end_time_unix_nano or 0),
        )
        interval = Interval(
            min(node.start_ns, chunk.start),
            max(node.end_ns, chunk.end),
        )
        attributes = self.test_attributes()
        attributes.update(
            {
                "ya.test.worker.reported_seconds": Ns(len(node.interval)).to_s(),
                "ya.test.worker.timing.source": "ya-evlog",
                "ya.test.worker.timeline.adjusted": interval != node.interval,
                **(
                    {"process.exit.code": exit_code}
                    if failed and exit_code is not None
                    else {}
                ),
            }
        )
        return self.parent.scoped("ya.test.worker").emit(
            "ya.test.worker",
            node.uid,
            node.start_ns,
            chunk_span.span_id,
            index,
            name=f"test worker: {chunk_span.name}",
            interval=interval,
            attributes=attributes,
            status_code=2 if failed else 0,
            status_message=("test worker or reported chunk failed" if failed else ""),
        )

    def test_worker_phases(self) -> None:
        node = self.node
        labels = {
            "exec_cmd": "exec command",
            "post_cmd": "post command",
            "node_result": "node result",
        }
        phases = self.parent.scoped("ya.test.worker.phase")
        for index, detail in enumerate(node.details):
            label = labels.get(detail.tag, detail.tag.replace("_", " "))
            phases.emit(
                "ya.test.worker.phase",
                node.uid,
                detail.tag,
                detail.start_ns,
                index,
                name=f"worker phase: {label}",
                interval=detail.interval,
                attributes={
                    "ya.test.worker.phase": detail.tag,
                    "ya.test.worker.timing.source": "ya-evlog",
                    **({"ya.test.worker.uid": node.uid} if node.uid else {}),
                },
            )

    def build_node(
        self,
        *,
        kind: str,
        tool: str,
        index: int,
        critical_entry: YaCriticalPathEntry | None,
        failed: bool,
        exit_code: int | None,
    ) -> Span:
        node = self.node
        attributes: dict[str, Any] = {
            "ya.build.kind": kind,
            "ya.build.tag": node.tag,
            "ya.build.node.name": node.name,
            "ya.build.timing.scope": "worker-node",
            **({"ya.build.tool": tool} if tool else {}),
            **({"ya.build.outputs": node.outputs} if node.outputs else {}),
            **({"ya.worker.thread": node.thread} if node.thread else {}),
            **(
                {"ya.build.cache.source": node.cache_source}
                if node.cache_source
                else {}
            ),
            **({"ya.build.node.uid": node.uid} if node.uid else {}),
        }
        if critical_entry is not None:
            attributes.update(critical_entry.span_attributes(test=False))
        if failed:
            attributes.update(
                {
                    "ya.build.failed": True,
                    **(
                        {"process.exit.code": exit_code}
                        if exit_code is not None
                        else {}
                    ),
                }
            )
        output = next(iter(node.outputs), "")
        label = {
            "cache_restore": "cache restore",
            "materialize": "materialize",
            "cache_store": "cache store",
            "execute": tool or "execute",
            "orchestration": tool or node.name,
        }[kind]
        if kind in {"cache_restore", "materialize", "cache_store"} and tool:
            label += f" [{tool}]"
        return self.parent.scoped("ya.build.node").emit(
            "ya.build.node",
            node.name,
            node.tag,
            node.start_ns,
            index,
            name=f"{label}: {output}" if output else label,
            interval=node.interval,
            attributes=attributes,
            status_code=2 if failed else 0,
            status_message="build node failed" if failed else "",
        )

    def build_command(
        self,
        detail: YaNode,
        *,
        tool: str,
        index: int,
        failed: bool,
    ) -> Span:
        node = self.node
        output = next(iter(node.outputs), "")
        label = f"{tool} command" if tool else "exec command"
        return self.parent.scoped("ya.build.command").emit(
            "ya.build.command",
            self.parent.parent_span_id,
            detail.start_ns,
            detail.end_ns,
            index,
            name=f"{label}: {output}" if output else label,
            interval=detail.interval,
            attributes={
                "ya.build.detail.stage": "exec_cmd",
                "ya.build.timing.scope": "command",
                **({"ya.build.tool": tool} if tool else {}),
                **({"ya.build.node.uid": node.uid} if node.uid else {}),
                **({"ya.build.outputs": node.outputs} if node.outputs else {}),
                **({"ya.worker.thread": node.thread} if node.thread else {}),
                **({"ya.build.node.failed": True} if failed else {}),
            },
        )
