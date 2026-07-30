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
        if "/test-results/" in self.name:
            match = TAG_WRAPPER_RE.fullmatch(self.tag)
            return "test", match.group(2) if match else self.tag
        match = TAG_WRAPPER_RE.fullmatch(self.tag)
        if match:
            wrapper, tool = match.groups()
            if tool in TEST_NODE_MARKERS:
                return "test", tool
            return {
                "restore": "cache_restore",
                "restore_from_dist_cache": "cache_restore",
                "result": "materialize",
                "put_in_cache": "cache_store",
                "put_in_dist_cache": "cache_store",
                "write_through_caches": "cache_store",
            }[wrapper], tool
        if self.tag in TEST_NODE_MARKERS:
            return "test", self.tag
        if self.name.startswith("Run("):
            return "execute", self.tag
        return "orchestration", self.tag

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
