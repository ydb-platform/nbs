from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from .time import Interval, Ns


def test_ns_from_s_rounds_float_to_nearest_nanosecond() -> None:
    assert Ns.from_s(1.001) == Ns(1_001_000_000)


def test_ns_conversion_preserves_decimal_precision() -> None:
    assert Ns.from_s(Decimal("1750000000.123456789")) == Ns(1_750_000_000_123_456_789)
    assert Ns.from_ms(Decimal("1.0000005")) == Ns(1_000_001)


@pytest.mark.parametrize("value", ["1.0", True, -1, float("inf"), float("nan")])
def test_ns_from_s_rejects_invalid_values(value: object) -> None:
    assert Ns.from_s(value) is None


@pytest.mark.parametrize("value", [1.5, "1", True, -1, Decimal("1")])
def test_ns_requires_non_negative_integer_nanoseconds(value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        Ns(value)  # type: ignore[arg-type]


def test_ns_is_frozen() -> None:
    value = Ns(1)

    with pytest.raises(FrozenInstanceError):
        value.value = 2  # type: ignore[misc]


def test_ns_arithmetic_preserves_type() -> None:
    assert Ns(1) + Ns(2) == Ns(3)
    assert Ns(3) - Ns(2) == Ns(1)

    with pytest.raises(TypeError):
        Ns(1) + 2


def test_interval_requires_ns_boundaries() -> None:
    with pytest.raises(TypeError):
        Interval(1, 2)  # type: ignore[arg-type]


def test_interval_operations_preserve_ns() -> None:
    interval = Interval(Ns(1), Ns(4))

    assert len(interval) == 3
    assert interval.clamp(Ns(5)) == Ns(4)
    assert interval.overlap(Interval(Ns(3), Ns(5))) == Ns(1)
    assert interval.intersection(Interval(Ns(3), Ns(5))) == Interval(Ns(3), Ns(4))
