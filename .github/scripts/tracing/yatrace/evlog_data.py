from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping, Sequence

from .critical_path import YaCriticalPath
from .node import YaNode
from .statistics import YaBuildStatistics


@dataclass(frozen=True, slots=True)
class YaEvlog:
    stages: tuple[YaNode, ...]
    nodes: tuple[YaNode, ...]
    statistics: YaBuildStatistics
    critical_path: YaCriticalPath
    failures: Mapping[str, int | None]
    malformed_json_record_count: int = 0

    @classmethod
    def from_raw(
        cls,
        *,
        stages: Sequence[YaNode] = (),
        nodes: Sequence[YaNode] = (),
        statistics: Mapping[str, Any] | None = None,
        failures: Mapping[str, int | None] | None = None,
        malformed_json_record_count: int = 0,
    ) -> YaEvlog:
        nodes = tuple(nodes)
        build_statistics = YaBuildStatistics.from_raw(statistics)
        return cls(
            stages=tuple(stages),
            nodes=nodes,
            statistics=build_statistics,
            critical_path=YaCriticalPath.from_evlog(build_statistics.values, nodes),
            failures=MappingProxyType(dict(failures or {})),
            malformed_json_record_count=malformed_json_record_count,
        )
