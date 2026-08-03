from __future__ import annotations

from dataclasses import dataclass

from ..otlp import Interval, Ns
from .evlog_record import YaEvlogRecord


@dataclass(frozen=True, slots=True)
class ClassifiedNode:
    record: YaEvlogRecord
    kind: str
    tool: str

    @classmethod
    def from_record(cls, record: YaEvlogRecord) -> ClassifiedNode:
        kind, tool = record.kind_and_tool
        return cls(record, kind, tool)

    @property
    def interval(self) -> Interval:
        return self.record.interval

    @property
    def start_ns(self) -> Ns:
        return self.record.start_ns

    @property
    def end_ns(self) -> Ns:
        return self.record.end_ns

    @property
    def uid(self) -> str:
        return self.record.uid

    @property
    def is_test(self) -> bool:
        return self.kind.startswith("test_")

    def clipped(self, bounds: Interval) -> ClassifiedNode | None:
        record = self.record.clipped(bounds)
        return type(self)(record, self.kind, self.tool) if record is not None else None
