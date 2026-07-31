from __future__ import annotations

import re
from copy import deepcopy
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from . import limits
from .critical_path import YaCriticalPathEntry
from .metrics import _metric_name, _number

if TYPE_CHECKING:
    from .node import ClassifiedNode

SAFE_TOOL_RE = re.compile(r"^[a-zA-Z0-9_.+-]{1,32}$")


@dataclass(frozen=True, slots=True)
class YaBuildStatistics:
    values: Mapping[str, Any]

    @classmethod
    def from_raw(cls, values: Mapping[str, Any] | None = None) -> YaBuildStatistics:
        return cls(deepcopy(dict(values or {})))

    def graph_attributes(self) -> dict[str, Any]:
        attributes: dict[str, Any] = {}
        cache = self.values.get("cache_hit")
        if isinstance(cache, Mapping):
            cache_hit_percent = _number(cache.get("cache_hit"))
            if cache_hit_percent is not None:
                attributes["ya.build.cache.considered_task.hit.ratio"] = (
                    cache_hit_percent / 100
                )
            cache_fields = {
                "run_tasks": "ya.build.task.total.count",
                "executed_tasks": "ya.build.task.considered.count",
                "cached_tasks": "ya.build.cache.considered_task.hit.count",
                "dyn_cached_tasks": (
                    "ya.build.cache.considered_task.dynamic_hit.count"
                ),
                "not_cached_tasks": "ya.build.cache.considered_task.miss.count",
                "tests_tasks": "ya.build.task.uncached_test.count",
                "failed_tasks": "ya.build.task.failed.count",
                "ok_tasks": "ya.build.task.uncached_non_test.count",
                "avoided_tasks": "ya.build.task.avoided.count",
            }
            for source, destination in cache_fields.items():
                value = _number(cache.get(source))
                if value is not None:
                    attributes[destination] = value
            total_tasks = _number(cache.get("run_tasks"))
            cached_tasks = _number(cache.get("cached_tasks"))
            avoided_tasks = _number(cache.get("avoided_tasks"))
            if (
                total_tasks is not None
                and total_tasks > 0
                and cached_tasks is not None
                and avoided_tasks is not None
            ):
                attributes["ya.build.task.avoided.ratio"] = avoided_tasks / total_tasks
                attributes["ya.build.task.reused_or_avoided.ratio"] = (
                    cached_tasks + avoided_tasks
                ) / total_tasks
            attributes["ya.build.cache.statistics.source"] = "ya"

        dist_cache = self.values.get("dist_cache_stat")
        if isinstance(dist_cache, Mapping):
            dist_cache_fields = {
                "get_count": "ya.build.dist_cache.get.count",
                "get_data_size": "ya.build.dist_cache.get.bytes",
                "put_count": "ya.build.dist_cache.put.count",
                "put_data_size": "ya.build.dist_cache.put.bytes",
            }
            for source, destination in dist_cache_fields.items():
                value = _number(dist_cache.get(source))
                if value is not None:
                    attributes[destination] = value

        execution_stages = self.values.get("execution_stages_msec")
        if isinstance(execution_stages, Mapping):
            for name, value in execution_stages.items():
                milliseconds = _number(value)
                normalized = _metric_name(name)
                if milliseconds is not None and normalized:
                    attributes[f"ya.build.execution.stage.{normalized}.seconds"] = (
                        milliseconds / 1_000
                    )
        task_execution_msec = _number(self.values.get("task_execution_msec"))
        if task_execution_msec is not None:
            attributes["ya.build.execution.total.seconds"] = task_execution_msec / 1_000

        languages = self.values.get("graph_lang_usage")
        if isinstance(languages, Mapping):
            attributes["ya.build.graph.languages"] = sorted(
                str(language) for language in languages
            )[:128]
        return attributes

    def build_attributes(
        self,
        build_records: Sequence[ClassifiedNode],
        critical_entries: Sequence[YaCriticalPathEntry],
    ) -> tuple[
        dict[str, Any],
        Sequence[YaCriticalPathEntry],
    ]:
        attributes: dict[str, Any] = {}
        tool_counts: dict[str, Counter[str]] = defaultdict(Counter)
        for node in build_records:
            if node.kind in {"cache_restore", "execute"} and SAFE_TOOL_RE.fullmatch(
                node.tool
            ):
                tool_counts[_metric_name(node.tool)][node.kind] += 1
        ranked_tools = sorted(
            tool_counts,
            key=lambda tool: (
                -(tool_counts[tool]["cache_restore"] + tool_counts[tool]["execute"]),
                tool,
            ),
        )[: limits.MAX_WORKER_TOOL_STATS]
        for normalized in ranked_tools:
            for kind in ("cache_restore", "execute"):
                count = tool_counts[normalized][kind]
                if count:
                    attributes[f"ya.build.worker.tool.{normalized}.{kind}.count"] = (
                        count
                    )

        if critical_entries:
            work_msec = sum(entry.elapsed_ms or 0 for entry in critical_entries)
            start_times = [
                entry.start_ms
                for entry in critical_entries
                if entry.start_ms is not None
            ]
            end_times = [
                entry.end_ms for entry in critical_entries if entry.end_ms is not None
            ]
            attributes["ya.build.critical_path.node.count"] = len(critical_entries)
            attributes["ya.build.critical_path.work.seconds"] = work_msec / 1_000
            if start_times and end_times:
                attributes["ya.build.critical_path.elapsed.seconds"] = (
                    max(end_times) - min(start_times)
                ) / 1_000
            attributes["ya.build.critical_path.summary"] = [
                entry.summary for entry in critical_entries[:128]
            ]
        return attributes, critical_entries
