from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping

from ..otlp import Interval, Ns, Span, make_span, stable_span_id
from .critical_path import TEST_NODE_MARKERS, YaCriticalPathEntry

BUILD_ROOT_RE = re.compile(r"\$\(BUILD_ROOT\)/([^\s)]+)")
NODE_UID_RE = re.compile(r"^[^(]+\(([^$()\s]+)\$\(BUILD_ROOT\)")
OUTPUTLESS_NODE_UID_RE = re.compile(r"^[^(]+\(([^$()\s]+)\)$")
TAG_WRAPPER_RE = re.compile(
    r"^(restore|restore_from_dist_cache|result|put_in_cache|"
    r"put_in_dist_cache|write_through_caches)\[([^\]]+)\]$"
)
TEST_SIZE_BY_TAG = {"TS": "small", "TM": "medium", "TL": "large"}
TEST_EXECUTE_TAGS = frozenset({"TS", "TM", "TL", "YT"})
TEST_GRAPH_NODE_MARKERS = TEST_NODE_MARKERS | frozenset({"TR", "YT"})
TEST_PROCESSING_KINDS = {
    "TA": "test_aggregate",
    "TR": "test_merge",
}
TEST_WRAPPER_KINDS = {
    "restore": "test_cache_restore",
    "restore_from_dist_cache": "test_cache_restore",
    "result": "test_materialize",
    "put_in_cache": "test_cache_store",
    "put_in_dist_cache": "test_cache_store",
    "write_through_caches": "test_cache_store",
}


@dataclass(slots=True)
class YaEvlogRecord:
    name: str
    tag: str
    start_ns: Ns
    end_ns: Ns
    thread_name: str = ""
    node_uid: str = ""
    details: list[YaEvlogRecord] = field(default_factory=list)

    @classmethod
    def from_raw(
        cls,
        value: Mapping[str, Any],
        *,
        thread_name: str = "",
    ) -> YaEvlogRecord | None:
        interval = cls._time_range(value.get("time"))
        if interval is None:
            return None
        return cls(
            name=str(value.get("name", "unknown")),
            tag=str(value.get("tag", "")),
            start_ns=interval.start,
            end_ns=interval.end,
            thread_name=thread_name,
            node_uid=str(value.get("uid") or ""),
        )

    @staticmethod
    def _time_range(value: Any) -> Interval | None:
        if not isinstance(value, list) or len(value) != 2:
            return None
        start_ns = Ns.from_s(value[0])
        end_ns = Ns.from_s(value[1])
        if start_ns is None or end_ns is None or end_ns < start_ns:
            return None
        return Interval(start_ns, end_ns)

    @property
    def interval(self) -> Interval:
        return Interval(self.start_ns, self.end_ns)

    def clipped(self, bounds: Interval) -> YaEvlogRecord | None:
        clipped = self.interval.intersection(bounds)
        if clipped is None:
            return None
        details = []
        for detail in self.details:
            clipped_detail = detail.clipped(clipped)
            if clipped_detail is not None:
                details.append(clipped_detail)
        return YaEvlogRecord(
            name=self.name,
            tag=self.tag,
            start_ns=clipped.start,
            end_ns=clipped.end,
            thread_name=self.thread_name,
            node_uid=self.node_uid,
            details=details,
        )

    @property
    def kind_and_tool(self) -> tuple[str, str]:
        match = TAG_WRAPPER_RE.fullmatch(self.tag)
        if match:
            wrapper, tool = match.groups()
            if tool in TEST_GRAPH_NODE_MARKERS:
                return TEST_WRAPPER_KINDS[wrapper], tool
            return {
                "restore": "cache_restore",
                "restore_from_dist_cache": "cache_restore",
                "result": "materialize",
                "put_in_cache": "cache_store",
                "put_in_dist_cache": "cache_store",
                "write_through_caches": "cache_store",
            }[wrapper], tool
        if self.tag in TEST_GRAPH_NODE_MARKERS:
            if self.tag in TEST_EXECUTE_TAGS:
                if self.tag == "TL" and self.test_result_identity is None:
                    return "test_list", self.tag
                return "test_execute", self.tag
            return TEST_PROCESSING_KINDS[self.tag], self.tag
        if "/test-results/" in self.name:
            return "test_orchestration", self.tag
        if self.name.startswith("Run("):
            return "execute", self.tag
        return "orchestration", self.tag

    @property
    def test_size(self) -> str:
        kind, tool = self.kind_and_tool
        return TEST_SIZE_BY_TAG.get(tool, "") if kind == "test_execute" else ""

    @property
    def outputs(self) -> list[str]:
        return list(dict.fromkeys(BUILD_ROOT_RE.findall(self.name)))[:16]

    @property
    def uid(self) -> str:
        if self.node_uid:
            return self.node_uid
        match = NODE_UID_RE.match(self.name) or OUTPUTLESS_NODE_UID_RE.match(self.name)
        return match.group(1) if match else ""

    @property
    def cache_source(self) -> str:
        if self.tag.startswith(("restore_from_dist_cache[", "put_in_dist_cache[")):
            return "distributed"
        if self.tag.startswith(("restore[", "put_in_cache[")):
            return "local"
        return ""

    @property
    def test_result_identity(self) -> tuple[str, str] | None:
        for output in self.outputs:
            suite, marker, relative = output.partition("/test-results/")
            if not marker or not suite or not relative:
                continue
            result_folder = relative.split("/", 1)[0]
            if result_folder:
                return suite, result_folder
        return None

    @property
    def test_result_chunk_index(self) -> int | None:
        for output in self.outputs:
            _, marker, relative = output.partition("/test-results/")
            if not marker:
                continue
            parts = relative.split("/")
            if len(parts) < 2:
                continue
            match = re.fullmatch(r"chunk(\d+)", parts[1])
            if match:
                return int(match.group(1))
        return None

    def test_worker_attributes(self) -> dict[str, Any]:
        kind, tool = self.kind_and_tool
        attributes: dict[str, Any] = {
            "ya.test.worker.kind": kind,
            "ya.test.worker.tag": self.tag,
        }
        if tool:
            attributes["ya.test.worker.tool"] = tool
        if self.test_size:
            attributes["test.size"] = self.test_size
        if self.uid:
            attributes["ya.test.worker.uid"] = self.uid
        if self.thread_name:
            attributes["ya.test.worker.thread"] = self.thread_name
        if self.outputs:
            attributes["ya.test.worker.outputs"] = self.outputs
        identity = self.test_result_identity
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
        kind, _ = self.kind_and_tool
        identity = self.test_result_identity
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
                self.name,
                self.tag,
                self.start_ns,
                index,
            ),
            parent_span_id=parent_span_id,
            name=label,
            start_ns=self.start_ns,
            end_ns=self.end_ns,
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
        chunk = Interval(
            Ns(chunk_span.start_time_unix_nano or 0),
            Ns(chunk_span.end_time_unix_nano or 0),
        )
        envelope = Interval(
            min(self.start_ns, chunk.start),
            max(self.end_ns, chunk.end),
        )
        attributes = self.test_worker_attributes()
        attributes.update(
            {
                "ya.test.worker.reported_seconds": Ns(len(self.interval)).to_s(),
                "ya.test.worker.timing.source": "ya-evlog",
                "ya.test.worker.timeline.adjusted": envelope != self.interval,
            }
        )
        if failed and exit_code is not None:
            attributes["process.exit.code"] = exit_code
        return make_span(
            trace_id=trace_id,
            span_id=stable_span_id(
                trace_id,
                "ya.test.worker",
                self.uid,
                self.start_ns,
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
                self.uid,
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
                **({"ya.test.worker.uid": self.uid} if self.uid else {}),
            },
        )

    def span_name(self, kind: str, tool: str) -> str:
        output = next(iter(self.outputs), "")
        labels = {
            "cache_restore": "cache restore",
            "materialize": "materialize",
            "cache_store": "cache store",
            "execute": tool or "execute",
            "orchestration": tool or self.name,
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
        attributes: dict[str, Any] = {
            "ya.build.kind": kind,
            "ya.build.tag": self.tag,
            "ya.build.node.name": self.name,
            "ya.build.timing.scope": "worker-node",
        }
        if tool:
            attributes["ya.build.tool"] = tool
        if self.outputs:
            attributes["ya.build.outputs"] = self.outputs
        if self.thread_name:
            attributes["ya.worker.thread"] = self.thread_name
        if self.cache_source:
            attributes["ya.build.cache.source"] = self.cache_source
        if self.uid:
            attributes["ya.build.node.uid"] = self.uid
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
                self.name,
                self.tag,
                self.start_ns,
                index,
            ),
            parent_span_id=parent_span_id,
            name=self.span_name(kind, tool),
            start_ns=self.start_ns,
            end_ns=self.end_ns,
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
        output = next(iter(self.outputs), "")
        label = f"{tool} command" if tool else "exec command"
        attributes: dict[str, Any] = {
            "ya.build.detail.stage": "exec_cmd",
            "ya.build.timing.scope": "command",
        }
        if tool:
            attributes["ya.build.tool"] = tool
        if self.uid:
            attributes["ya.build.node.uid"] = self.uid
        if self.outputs:
            attributes["ya.build.outputs"] = self.outputs
        if self.thread_name:
            attributes["ya.worker.thread"] = self.thread_name
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
