from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from typing import Any, Mapping

from ..otlp import Interval, Ns

BUILD_ROOT_RE = re.compile(r"\$\(BUILD_ROOT\)/([^\s)]+)")
NODE_UID_RE = re.compile(r"^[^(]+\(([^$()\s]+)\$\(BUILD_ROOT\)")
OUTPUTLESS_NODE_UID_RE = re.compile(r"^[^(]+\(([^$()\s]+)\)$")
TAG_WRAPPER_RE = re.compile(
    r"^(restore|restore_from_dist_cache|result|put_in_cache|"
    r"put_in_dist_cache|write_through_caches)\[([^\]]+)\]$"
)
TEST_NODE_MARKERS = frozenset({"TA", "TL", "TM", "TS"})
TEST_GRAPH_MARKERS = TEST_NODE_MARKERS | frozenset({"TR", "YT"})
TEST_EXECUTE_TAGS = frozenset({"TS", "TM", "TL", "YT"})
TEST_SIZES = {"TS": "small", "TM": "medium", "TL": "large"}
TEST_PROCESSING_KINDS = {"TA": "test_aggregate", "TR": "test_merge"}
TEST_WRAPPER_KINDS = {
    "restore": "test_cache_restore",
    "restore_from_dist_cache": "test_cache_restore",
    "result": "test_materialize",
    "put_in_cache": "test_cache_store",
    "put_in_dist_cache": "test_cache_store",
    "write_through_caches": "test_cache_store",
}
BUILD_WRAPPER_KINDS = {
    "restore": "cache_restore",
    "restore_from_dist_cache": "cache_restore",
    "result": "materialize",
    "put_in_cache": "cache_store",
    "put_in_dist_cache": "cache_store",
    "write_through_caches": "cache_store",
}


def _test_identity(outputs: tuple[str, ...]) -> tuple[str, str] | None:
    for output in outputs:
        suite, marker, relative = output.partition("/test-results/")
        folder = relative.split("/", 1)[0]
        if marker and suite and folder:
            return suite, folder
    return None


def _chunk_index(outputs: tuple[str, ...]) -> int | None:
    for output in outputs:
        _, marker, relative = output.partition("/test-results/")
        parts = relative.split("/")
        if (
            marker
            and len(parts) > 1
            and (match := re.fullmatch(r"chunk(\d+)", parts[1]))
        ):
            return int(match.group(1))
    return None


def _kind_and_tool(
    name: str,
    tag: str,
    test_identity: tuple[str, str] | None,
) -> tuple[str, str]:
    if match := TAG_WRAPPER_RE.fullmatch(tag):
        wrapper, tool = match.groups()
        kinds = (
            TEST_WRAPPER_KINDS if tool in TEST_GRAPH_MARKERS else BUILD_WRAPPER_KINDS
        )
        return kinds[wrapper], tool
    if tag in TEST_GRAPH_MARKERS:
        if tag in TEST_EXECUTE_TAGS:
            return (
                "test_list" if tag == "TL" and test_identity is None else "test_execute"
            ), tag
        return TEST_PROCESSING_KINDS[tag], tag
    if "/test-results/" in name:
        return "test_orchestration", tag
    return ("execute" if name.startswith("Run(") else "orchestration"), tag


@dataclass(slots=True)
class YaNode:
    name: str
    tag: str
    interval: Interval
    kind: str
    tool: str
    thread: str = ""
    uid: str = ""
    outputs: tuple[str, ...] = ()
    cache_source: str = ""
    test_identity: tuple[str, str] | None = None
    test_chunk_index: int | None = None
    test_size: str = ""
    details: list[YaNode] = field(default_factory=list)

    @classmethod
    def from_raw(
        cls,
        value: Mapping[str, Any],
        *,
        thread: str = "",
    ) -> YaNode | None:
        raw_time = value.get("time")
        if not isinstance(raw_time, list) or len(raw_time) != 2:
            return None
        start = Ns.from_s(raw_time[0])
        end = Ns.from_s(raw_time[1])
        if start is None or end is None or end < start:
            return None
        name = str(value.get("name", "unknown"))
        tag = str(value.get("tag", ""))
        outputs = tuple(dict.fromkeys(BUILD_ROOT_RE.findall(name)))[:16]
        test_identity = _test_identity(outputs)
        kind, tool = _kind_and_tool(name, tag, test_identity)
        raw_uid = str(value.get("uid") or "")
        uid_match = NODE_UID_RE.match(name) or OUTPUTLESS_NODE_UID_RE.match(name)
        uid = raw_uid or (uid_match.group(1) if uid_match else "")
        cache_source = ""
        if tag.startswith(("restore_from_dist_cache[", "put_in_dist_cache[")):
            cache_source = "distributed"
        elif tag.startswith(("restore[", "put_in_cache[")):
            cache_source = "local"
        return cls(
            name=name,
            tag=tag,
            interval=Interval(start, end),
            kind=kind,
            tool=tool,
            thread=thread,
            uid=uid,
            outputs=outputs,
            cache_source=cache_source,
            test_identity=test_identity,
            test_chunk_index=_chunk_index(outputs),
            test_size=TEST_SIZES.get(tool, "") if kind == "test_execute" else "",
        )

    @property
    def start_ns(self) -> Ns:
        return self.interval.start

    @property
    def end_ns(self) -> Ns:
        return self.interval.end

    @property
    def is_test(self) -> bool:
        return self.kind.startswith("test_")

    def clipped(self, bounds: Interval) -> YaNode | None:
        interval = self.interval.intersection(bounds)
        if interval is None:
            return None
        return replace(
            self,
            interval=interval,
            details=[
                clipped
                for detail in self.details
                if (clipped := detail.clipped(interval)) is not None
            ],
        )
