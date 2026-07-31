from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .build_operations import _YaEvlogSpanBuilder
from .critical_path import _YaEvlogCriticalPath
from .evlog_record import YaEvlogRecord
from .statistics import _YaEvlogStatistics
from .test_operations import _YaTestOperations


@dataclass
class YaEvlog(
    _YaEvlogCriticalPath,
    _YaEvlogStatistics,
    _YaTestOperations,
    _YaEvlogSpanBuilder,
):
    stages: list[YaEvlogRecord]
    nodes: list[YaEvlogRecord]
    statistics: dict[str, Any] = field(default_factory=dict)
    failures: dict[str, int | None] = field(default_factory=dict)
