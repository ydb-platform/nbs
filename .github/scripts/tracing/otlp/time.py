from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


class Ns(int):
    """A non-negative nanosecond value."""

    def __new__(cls, value: int | float | str) -> Ns:
        nanoseconds = int(value)
        if nanoseconds < 0:
            raise ValueError("nanoseconds must be non-negative")
        return int.__new__(cls, nanoseconds)

    @classmethod
    def _from_scaled(cls, value: Any, multiplier: int) -> Ns | None:
        try:
            number = float(value)
            scaled = number * multiplier
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(scaled) or scaled < 0:
            return None
        return cls(scaled)

    @classmethod
    def from_s(cls, value: Any) -> Ns | None:
        return cls._from_scaled(value, SECOND)

    @classmethod
    def from_s_or_zero(cls, value: Any) -> Ns:
        return cls.from_s(value) or cls(0)

    @classmethod
    def from_ms(cls, value: Any) -> Ns | None:
        return cls._from_scaled(value, MILLISECOND)

    def to_s(self) -> float:
        return self / SECOND

    def to_ms(self) -> float:
        return self / MILLISECOND

    def is_second_aligned(self) -> bool:
        return self % SECOND == 0


MILLISECOND = Ns(1_000_000)
SECOND = Ns(1_000_000_000)


@dataclass(frozen=True, slots=True)
class Interval:
    start: Ns
    end: Ns

    def __post_init__(self) -> None:
        object.__setattr__(self, "start", Ns(self.start))
        object.__setattr__(self, "end", Ns(self.end))
        if self.end < self.start:
            raise ValueError("interval end precedes start")

    def __len__(self) -> int:
        return self.end - self.start

    def clamp(self, value: Ns) -> Ns:
        return Ns(max(self.start, min(value, self.end)))

    def overlap(self, other: Interval) -> Ns:
        return Ns(max(0, min(self.end, other.end) - max(self.start, other.start)))

    def boundary_distance(self, other: Interval) -> Ns:
        return Ns(abs(self.start - other.start) + abs(self.end - other.end))

    def intersection(self, other: Interval) -> Interval | None:
        start = max(self.start, other.start)
        end = min(self.end, other.end)
        return Interval(start, end) if start < end else None
