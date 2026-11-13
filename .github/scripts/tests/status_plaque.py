"""State and HTML rendering for the PR build/test status table."""

from __future__ import annotations

import dataclasses
import json
import re
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from ..set_build_and_test_targets import JOIN_GROUPS

START = "<!-- status-plaque -->"
END = "<!-- /status-plaque -->"
DATA_RE = re.compile(r"<!-- status-plaque-data (.*?) -->")
BLOCK_RE = re.compile(rf"{re.escape(START)}.*?{re.escape(END)}", re.DOTALL)
COUNT_STATUSES = ("PASS", "ERROR", "FAIL", "FAIL_BUILD", "SKIP", "MUTE")
ICONS = {
    "pending": "⏳",
    "running": "⏳",
    "built": "⏳",
    "completed": "🟢",
    "failed_build": "🔴",
    "failed": "🔴",
    "cancelled": "🚫",
    "timed_out": "⏰",
    "skipped": "⚫",
    "report_failed": "⚠️",
}
LABELS = {
    "pending": "Queued",
    "running": "Running",
    "built": "Build passed; tests pending",
    "completed": "Passed",
    "failed_build": "Build failed",
    "failed": "Failed",
    "cancelled": "Cancelled",
    "timed_out": "Timed out",
    "skipped": "Skipped",
    "report_failed": "Report missing",
}
MESSAGES = {
    "pending": "Queued",
    "running": "Building / preparing tests",
    "built": "Build passed; waiting for test results",
    "completed": "Tests not run",
    "failed_build": "Build failed",
    "failed": "Workflow failed before reporting completion",
    "cancelled": "Workflow cancelled",
    "timed_out": "Workflow timed out",
    "skipped": "Workflow skipped",
    "report_failed": "Workflow finished, but report update failed",
}


def format_duration(seconds: int | str | None) -> str:
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        return "—"
    if seconds < 0:
        return "—"
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


@dataclasses.dataclass
class Report:
    iteration: int
    title: str
    target: str
    commit: str
    total: int
    counts: dict[str, int]
    url: str = ""
    build_error_url: str = ""
    build_failed_count: int = 0
    history_url: str = ""
    elapsed: str = "0"

    @property
    def failed(self) -> bool:
        return (
            not self.total
            or self.build_failed_count > 0
            or any(
                self.counts.get(status, 0) for status in ("ERROR", "FAIL", "FAIL_BUILD")
            )
        )

    @property
    def message(self) -> str:
        if self.total:
            return ""
        if self.count("FAIL_BUILD"):
            return "Build failed; no test results"
        return "No test results found; check the job logs"

    def count(self, status: str) -> int:
        count = self.counts.get(status, 0)
        return max(count, self.build_failed_count) if status == "FAIL_BUILD" else count

    def count_url(self, status: str) -> str:
        if not self.count(status):
            return ""
        if status == "FAIL_BUILD" and not self.counts.get(status, 0):
            return self.build_error_url
        return f"{self.url}#{status}" if self.url else ""


def target_for_component(component: str) -> str:
    for group in JOIN_GROUPS:
        if component == "_".join(group):
            return ",".join(f"cloud/{name}/" for name in group)
    if component in ("all", "none"):
        return "All targets" if component == "all" else "Sanitized targets"
    return f"cloud/{component}/"


def component_for_target(target: str) -> str:
    for group in JOIN_GROUPS:
        if target == ",".join(f"cloud/{name}/" for name in group):
            return "_".join(group)
    match = re.fullmatch(r"cloud/([^/]+)/?", target)
    return match.group(1) if match else "all"


@dataclasses.dataclass
class Workload:
    component: str
    status: str = "pending"
    job_url: str = ""
    build: str = ""
    build_error_url: str = ""
    reports: list[Report] = dataclasses.field(default_factory=list)
    build_duration_seconds: int | None = None

    @property
    def target(self) -> str:
        return next(
            (report.target for report in self.reports if report.target),
            target_for_component(self.component),
        )

    def update(
        self,
        status: str,
        job_url: str,
        build_error_url: str,
        build_duration_seconds: int | None = None,
    ) -> None:
        # A running update starts a new job attempt. Repeated notifications for
        # the same running job must not erase reports that have already arrived.
        if status == "running" and (
            self.status not in ("pending", "running", "built")
            or (job_url and job_url != self.job_url)
        ):
            self.reports.clear()
            self.build = ""
            self.build_error_url = ""
            self.build_duration_seconds = None
        elif self.status == "failed_build" and status not in (
            "running",
            "failed_build",
        ):
            return
        self.status = status
        self.job_url = job_url or self.job_url
        if build_duration_seconds is not None:
            self.build_duration_seconds = build_duration_seconds
        if status == "built":
            self.build = "completed"
        elif status == "failed_build":
            self.build = "failed_build"
            self.build_error_url = build_error_url or self.build_error_url

    def add_report(self, report: Report) -> None:
        self.reports = [
            old
            for old in self.reports
            if (old.iteration, old.title) != (report.iteration, report.title)
        ]
        self.reports.append(report)
        self.reports.sort(key=lambda item: (item.iteration, item.title))
        if report.count("FAIL_BUILD"):
            self.build = "failed_build"
            self.build_error_url = report.build_error_url or self.build_error_url
        elif report.total and self.build != "failed_build":
            self.build = "completed"

    def overall(self, report: Report | None) -> str:
        if self.status == "cancelled":
            return "cancelled"
        if self.status not in ("pending", "running", "built", "completed"):
            return "failed"
        if self.build == "failed_build" or (report and report.failed):
            return "failed"
        return "completed" if self.status == "completed" else "running"


@dataclasses.dataclass
class Plaque:
    build_preset: str
    workloads: list[Workload] = dataclasses.field(default_factory=list)

    def workload(self, component: str) -> Workload:
        for workload in self.workloads:
            if workload.component == component:
                return workload
        workload = Workload(component)
        self.workloads.append(workload)
        return workload

    @classmethod
    def from_comment(cls, body: str) -> Plaque | None:
        match = DATA_RE.search(body)
        if match is None:
            return None
        data = json.loads(match.group(1))
        return cls(
            data["build_preset"],
            [
                Workload(
                    **{key: value for key, value in row.items() if key != "reports"},
                    reports=[Report(**report) for report in row["reports"]],
                )
                for row in data["workloads"]
            ],
        )

    def render(self) -> str:
        # Keep update state separate from the presentation. Escaping angle
        # brackets also prevents values containing '-->' from closing the comment.
        payload = json.dumps(dataclasses.asdict(self), separators=(",", ":"))
        payload = payload.replace("<", "\\u003c").replace(">", "\\u003e")
        return "\n".join(
            [START, f"<!-- status-plaque-data {payload} -->", self.render_table(), END]
        )

    def replace(self, body: str) -> str:
        return BLOCK_RE.sub(lambda _: self.render(), body, count=1)  # noqa: U101

    def render_table(self) -> str:
        env = Environment(
            loader=FileSystemLoader(str(Path(__file__).with_name("templates"))),
            undefined=StrictUndefined,
            autoescape=True,
            trim_blocks=True,
            lstrip_blocks=True,
        )
        env.filters["duration"] = format_duration
        return env.get_template("status_plaque.html").render(
            workloads=self.workloads,
            build_preset=self.build_preset,
            icons=ICONS,
            labels=LABELS,
            messages=MESSAGES,
            count_statuses=COUNT_STATUSES,
        )
