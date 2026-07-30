from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

from ..otlp import Interval, Ns
from .metrics import _number

CRITICAL_TASK_SUFFIX_RE = re.compile(r"(?:(?:-CACHED|-DYN_UID_CACHE))+$")
TEST_NODE_MARKERS = frozenset({"TA", "TL", "TM", "TS"})


@dataclass(frozen=True, slots=True)
class YaCriticalPathEntry:
    index: int
    base_type: str
    raw_type: str
    text: str
    uid: str
    elapsed_ms: int | float | None
    start_ms: int | float | None
    end_ms: int | float | None
    interval: Interval | None
    is_test: bool

    @classmethod
    def from_raw(
        cls,
        index: int,
        value: Mapping[str, Any],
    ) -> YaCriticalPathEntry:
        raw_type = str(value.get("type", ""))
        base_type = CRITICAL_TASK_SUFFIX_RE.sub("", raw_type)
        text = str(value.get("text", ""))
        start_ns = Ns.from_ms(value.get("start_ts"))
        end_ns = Ns.from_ms(value.get("end_ts"))
        interval = (
            Interval(start_ns, end_ns)
            if start_ns is not None and end_ns is not None and end_ns >= start_ns
            else None
        )
        return cls(
            index=index,
            base_type=base_type,
            raw_type=raw_type,
            text=text,
            uid=str(value.get("uid", "")),
            elapsed_ms=_number(value.get("elapsed")),
            start_ms=_number(value.get("start_ts")),
            end_ms=_number(value.get("end_ts")),
            interval=interval,
            is_test=base_type in TEST_NODE_MARKERS or "/test-results/" in text,
        )

    @property
    def reported_seconds(self) -> float | None:
        return self.elapsed_ms / 1_000 if self.elapsed_ms is not None else None

    def span_attributes(self, *, test: bool) -> dict[str, Any]:
        scope = "test" if test else "build"
        prefix = f"ya.{scope}.critical_path"
        attributes: dict[str, Any] = {
            prefix: True,
            f"{prefix}.index": self.index,
        }
        if test:
            attributes.update(
                {
                    f"{prefix}.inferred": True,
                    f"{prefix}.granularity": "test-chunk",
                }
            )
            if self.raw_type:
                attributes[f"{prefix}.type"] = self.raw_type
            if self.text:
                attributes[f"{prefix}.text"] = self.text
        if self.reported_seconds is not None:
            attributes[f"{prefix}.reported_seconds"] = self.reported_seconds
        return attributes

    @property
    def summary(self) -> str:
        return (
            f"{self.raw_type or 'unknown'}: {self.text or 'unknown'} "
            f"({self.reported_seconds or 0:.3f}s)"
        )
