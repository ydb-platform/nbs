"""Helpers for converting ya statistics into OTLP attributes."""

from __future__ import annotations

import math
import re
from typing import Any, Mapping

SAFE_METRIC_RE = re.compile(r"[^a-zA-Z0-9_.-]+")


def _metric_name(name: object) -> str:
    normalized = SAFE_METRIC_RE.sub("_", str(name)).strip("_").lower()
    return re.sub(r"_+", "_", normalized)[:200]


def _metric_attributes(metrics: Any, *, prefix: str) -> dict[str, Any]:
    if not isinstance(metrics, Mapping):
        return {}
    attributes = {}
    for key, value in list(metrics.items())[:256]:
        if not isinstance(value, (str, bool, int, float)):
            continue
        if isinstance(value, float) and not math.isfinite(value):
            continue
        normalized = _metric_name(key)
        if normalized:
            attributes[f"{prefix}.{normalized}"] = value
    return attributes


def _number(value: Any) -> int | float | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    return None
