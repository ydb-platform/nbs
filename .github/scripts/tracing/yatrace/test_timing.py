"""Resolve incomplete ya test events into bounded intervals."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..otlp import Interval, Ns
from .event import YaEvent


@dataclass(frozen=True, slots=True)
class YaTestTiming:
    interval: Interval
    event: YaEvent
    order: int
    source: str
    inferred: bool
    incomplete: bool = False

    @classmethod
    def resolve(
        cls,
        start: YaEvent | None,
        finish: YaEvent | None,
        *,
        chunk: Interval,
        inferred_start: Ns | None,
        only_test: bool,
    ) -> YaTestTiming | None:
        if start is not None:
            start_ns = chunk.clamp(start.timestamp_ns or chunk.start)
            if finish is None:
                return cls(
                    interval=Interval(start_ns, chunk.end),
                    event=start,
                    order=start.order,
                    source="subtest-start-and-chunk-end",
                    inferred=start.timestamp_ns is None,
                    incomplete=True,
                )

            duration_ns = Ns.from_s_or_zero(finish.value.get("time"))
            end_ns = Ns(
                max(
                    start_ns,
                    chunk.clamp(
                        finish.timestamp_ns or Ns(start_ns + duration_ns),
                    ),
                )
            )
            return cls(
                interval=Interval(start_ns, end_ns),
                event=finish,
                order=start.order,
                source="subtest-events",
                inferred=start.timestamp_ns is None or finish.timestamp_ns is None,
            )

        if finish is None:
            return None

        duration_ns = Ns.from_s_or_zero(finish.value.get("time"))
        if (
            inferred_start is not None
            and only_test
            and finish.status[0] not in {"deselected", "not_launched"}
        ):
            start_ns = chunk.clamp(inferred_start)
            end_ns = Ns(
                max(
                    start_ns,
                    chunk.clamp(Ns(start_ns + duration_ns)),
                )
            )
            source = "chunk-delay-and-test-duration"
        else:
            end_ns = chunk.clamp(finish.timestamp_ns or chunk.end)
            start_ns = Ns(max(chunk.start, end_ns - duration_ns))
            source = "finish-event-and-test-duration"
        return cls(
            interval=Interval(start_ns, end_ns),
            event=finish,
            order=finish.order,
            source=source,
            inferred=True,
        )

    @property
    def status(self) -> tuple[str, int]:
        return ("incomplete", 2) if self.incomplete else self.event.status

    def attributes(self, test_class: str, subtest: str) -> dict[str, Any]:
        return self.event.test_attributes(
            test_class,
            subtest,
            inferred=self.inferred,
            timing_source=self.source,
            incomplete=self.incomplete,
        )
