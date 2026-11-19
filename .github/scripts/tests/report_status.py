"""Test result statuses shared by reports and the PR status plaque."""

from __future__ import annotations

from enum import Enum


class TestStatus(Enum):
    PASS = 0
    FAIL = 1
    ERROR = 2
    SKIP = 3
    MUTE = 4
    FAIL_BUILD = 5

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, TestStatus):
            return NotImplemented
        return self.value < other.value

    @property
    def label(self) -> str:
        return {
            TestStatus.PASS: "PASS",
            TestStatus.FAIL: "FAIL",
            TestStatus.FAIL_BUILD: "FAIL BUILD",
            TestStatus.ERROR: "ERROR",
            TestStatus.SKIP: "SKIP",
            TestStatus.MUTE: "MUTE",
        }[self]

    @property
    def summary_header(self) -> str:
        return {
            TestStatus.PASS: "PASSED",
            TestStatus.ERROR: "ERRORS",
            TestStatus.FAIL: "FAILED",
            TestStatus.FAIL_BUILD: "FAILED BUILD",
            TestStatus.SKIP: "SKIPPED",
            TestStatus.MUTE: "MUTED",
        }[self]

    @property
    def report_anchor(self) -> str:
        return self.name

    @property
    def is_failure(self) -> bool:
        return self in (
            TestStatus.ERROR,
            TestStatus.FAIL,
            TestStatus.FAIL_BUILD,
        )

    @classmethod
    def summary_table_order(cls) -> tuple["TestStatus", ...]:
        return (
            cls.PASS,
            cls.ERROR,
            cls.FAIL,
            cls.FAIL_BUILD,
            cls.SKIP,
            cls.MUTE,
        )

    @classmethod
    def html_report_order(cls) -> tuple["TestStatus", ...]:
        return (
            cls.ERROR,
            cls.FAIL,
            cls.FAIL_BUILD,
            cls.SKIP,
            cls.MUTE,
            cls.PASS,
        )
