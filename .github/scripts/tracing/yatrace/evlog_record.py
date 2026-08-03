from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping

from ..otlp import Interval, Ns
from .critical_path import TEST_NODE_MARKERS

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
