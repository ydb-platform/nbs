from __future__ import annotations

__test__ = False

from dataclasses import dataclass
from typing import Any

from ..otlp import Interval, Ns, Span, decode_attributes


@dataclass(frozen=True, slots=True)
class TestChunk:
    __test__ = False

    span: Span
    attributes: dict[str, Any]
    interval: Interval

    @classmethod
    def from_span(cls, span: Span) -> TestChunk:
        return cls(
            span=span,
            attributes=decode_attributes(span.attributes),
            interval=Interval(
                Ns(span.start_time_unix_nano or 0),
                Ns(span.end_time_unix_nano or 0),
            ),
        )

    @property
    def suite(self) -> str:
        return str(self.attributes.get("test.suite", ""))

    @property
    def result_folder(self) -> str:
        return str(self.attributes.get("ya.test_results.folder", ""))

    @property
    def identity(self) -> tuple[str, str] | None:
        identity = (self.suite, self.result_folder)
        return identity if all(identity) else None

    @property
    def chunk_index(self) -> int | None:
        value = self.attributes.get("ya.chunk.chunk_index")
        return value if isinstance(value, int) and not isinstance(value, bool) else None

    def overlap(self, interval: Interval) -> Ns:
        return self.interval.overlap(interval)
