from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP


def _rounded_ns(value: object, multiplier: int) -> int | None:
    if isinstance(value, bool):
        return None

    try:
        if isinstance(value, float):
            number = Decimal.from_float(value)
        elif isinstance(value, (int, Decimal)):
            number = Decimal(value)
        else:
            return None
        scaled = number * multiplier
    except (InvalidOperation, OverflowError, ValueError):
        return None

    if not scaled.is_finite() or scaled < 0:
        return None
    return int(scaled.to_integral_value(rounding=ROUND_HALF_UP))


@dataclass(frozen=True, order=True, slots=True)
class Ns:
    """An immutable, non-negative number of nanoseconds."""

    value: int

    def __post_init__(self) -> None:
        if isinstance(self.value, bool) or not isinstance(self.value, int):
            raise TypeError("nanoseconds must be an integer")
        if self.value < 0:
            raise ValueError("nanoseconds must be non-negative")

    @classmethod
    def _from_scaled(cls, value: object, multiplier: int) -> Ns | None:
        nanoseconds = _rounded_ns(value, multiplier)
        return cls(nanoseconds) if nanoseconds is not None else None

    @classmethod
    def from_s(cls, value: object) -> Ns | None:
        """Convert seconds to the nearest nanosecond, rounding half up."""
        return cls._from_scaled(value, SECOND.value)

    @classmethod
    def from_s_or_zero(cls, value: object) -> Ns:
        return cls.from_s(value) or cls(0)

    @classmethod
    def from_ms(cls, value: object) -> Ns | None:
        """Convert milliseconds to the nearest nanosecond, rounding half up."""
        return cls._from_scaled(value, MILLISECOND.value)

    def to_s(self) -> float:
        return self.value / SECOND.value

    def to_ms(self) -> float:
        return self.value / MILLISECOND.value

    def is_second_aligned(self) -> bool:
        return self.value % SECOND.value == 0

    def __int__(self) -> int:
        return self.value

    def __add__(self, other: object) -> Ns:
        if not isinstance(other, Ns):
            raise TypeError("Ns can only be added to Ns")
        return Ns(self.value + other.value)

    def __sub__(self, other: object) -> Ns:
        if not isinstance(other, Ns):
            raise TypeError("Ns can only be subtracted from Ns")
        return Ns(self.value - other.value)


MILLISECOND = Ns(1_000_000)
SECOND = Ns(1_000_000_000)


def _require_ns(value: object, field: str) -> Ns:
    if not isinstance(value, Ns):
        raise TypeError(f"{field} must be Ns")
    return value


@dataclass(frozen=True, slots=True)
class Interval:
    start: Ns
    end: Ns

    def __post_init__(self) -> None:
        _require_ns(self.start, "start")
        _require_ns(self.end, "end")
        if self.end < self.start:
            raise ValueError("interval end precedes start")

    def __len__(self) -> int:
        return int(self.end - self.start)

    def clamp(self, value: Ns) -> Ns:
        _require_ns(value, "value")
        return max(self.start, min(value, self.end))

    def overlap(self, other: Interval) -> Ns:
        start = max(self.start, other.start)
        end = min(self.end, other.end)
        return end - start if start < end else Ns(0)

    def boundary_distance(self, other: Interval) -> Ns:
        start_distance = abs(self.start.value - other.start.value)
        end_distance = abs(self.end.value - other.end.value)
        return Ns(start_distance + end_distance)

    def intersection(self, other: Interval) -> Interval | None:
        start = max(self.start, other.start)
        end = min(self.end, other.end)
        return Interval(start, end) if start < end else None
