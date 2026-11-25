"""State and HTML rendering for the PR build/test status table."""

from __future__ import annotations

import base64
import binascii
import dataclasses
import json
import logging
import re
import zlib
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from ..helpers import RunMetadata, format_duration
from ..set_build_and_test_targets import JOIN_GROUPS
from .report_status import TestStatus

START = "<!-- status-plaque -->"
END = "<!-- /status-plaque -->"
DATA_RE = re.compile(r"<!-- status-plaque-data (.*?) -->")
BLOCK_RE = re.compile(rf"{re.escape(START)}.*?{re.escape(END)}", re.DOTALL)
COMMENT_SIZE_LIMIT = 60_000
LOGGER = logging.getLogger(__name__)
STATE_NOTICE = "Some stored results were unavailable. Check the workflow reports."
STATE_ENCODING = "zlib-v1:"
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
    "unknown": "⚠️",
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
    "unknown": "Unknown status",
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
    "unknown": "Unknown status; check the workflow",
}


@dataclasses.dataclass
class Report:
    iteration: int
    title: str
    target: str
    commit: str
    total: int
    counts: dict[str, int]
    filename: str = ""
    build_errors: bool = False
    build_failed_count: int = 0
    elapsed: str = "0"
    exit_code: int | None = None
    trace: bool = False
    otlp: bool = False

    @property
    def failed(self) -> bool:
        return (
            not self.total
            or self.exit_code not in (None, 0)
            or self.build_failed_count > 0
            or any(
                self.counts.get(status.name, 0)
                for status in TestStatus
                if status.is_failure
            )
        )

    @property
    def execution_message(self) -> str:
        if self.exit_code in (None, 0):
            return ""
        if self.exit_code == 1 and any(
            self.count(status.name) for status in TestStatus if status.is_failure
        ):
            return ""
        if self.exit_code == 124:
            return "Test execution timed out (exit code 124)"
        return f"Test execution failed (exit code {self.exit_code})"

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

    def count_url(self, status: str, report_url: str, build_error_url: str) -> str:
        if not self.count(status):
            return ""
        if status == "FAIL_BUILD" and not self.counts.get(status, 0):
            return build_error_url
        return f"{report_url}#{status}" if report_url else ""


def target_for_component(component: str) -> str:
    for group in JOIN_GROUPS:
        if component == "_".join(group):
            return ",".join(group)
    if component in ("all", "none"):
        return "All targets" if component == "all" else "Sanitized targets"
    return component


def component_for_target(target: str) -> str:
    for group in JOIN_GROUPS:
        if target == ",".join(f"cloud/{name}/" for name in group):
            return "_".join(group)
    match = re.fullmatch(r"cloud/([^/]+)/?", target)
    return match.group(1) if match else "all"


@dataclasses.dataclass(frozen=True)
class WorkflowOutcome:
    execution: str | None = None
    workflow: str | None = None
    cleanup_only_failure: bool = False


@dataclasses.dataclass
class Workload:
    component: str
    status: str = "pending"
    job_id: int | None = None  # Zero links to the run when job lookup was unavailable.
    build: str = ""
    build_errors: bool = False
    reports: list[Report] = dataclasses.field(default_factory=list)
    build_duration_seconds: int | None = None
    workflow_status: str = ""
    execution_status: str = ""
    cleanup_only_failure: bool = False
    run_attempt: int = 1

    @property
    def workflow_state(self) -> str:
        return self.workflow_status or self.status

    @property
    def workflow_failed(self) -> bool:
        return self.workflow_state in ("failed", "failed_build", "timed_out")

    @property
    def latest_reports(self) -> list[Report]:
        latest_iteration = max((report.iteration for report in self.reports), default=0)
        return [
            report for report in self.reports if report.iteration == latest_iteration
        ]

    def can_merge_after_cleanup_failure(self) -> bool:
        if not self.cleanup_only_failure or self.execution_status != "completed":
            return False
        if self.overall() != "completed":
            return False
        if not self.reports:
            return self.status == "completed" and self.build == "completed"
        return all(report.exit_code == 0 for report in self.latest_reports)

    @property
    def target(self) -> str:
        target = next(
            (report.target for report in self.reports if report.target),
            target_for_component(self.component),
        )
        return ",".join(
            part.strip().removeprefix("cloud/").removesuffix("/")
            for part in target.split(",")
        )

    def update(
        self,
        status: str,
        job_id: int | None,
        build_errors: bool,
        build_duration_seconds: int | None = None,
    ) -> None:
        # A running update starts a new job attempt. Repeated notifications for
        # the same running job must not erase reports that have already arrived.
        if status == "running" and (
            self.status not in ("pending", "running", "built")
            or (job_id is not None and job_id != self.job_id)
        ):
            self.reports.clear()
            self.build = ""
            self.build_errors = False
            self.build_duration_seconds = None
            self.workflow_status = ""
            self.execution_status = ""
            self.cleanup_only_failure = False
        elif self.status == "failed_build" and status not in (
            "running",
            "failed_build",
        ):
            return
        self.status = status
        if job_id is not None:
            self.job_id = job_id
        if build_duration_seconds is not None:
            self.build_duration_seconds = build_duration_seconds
        if status == "built":
            self.build = "completed"
        elif status == "failed_build":
            self.build = "failed_build"
            self.build_errors = build_errors or self.build_errors

    def add_report(self, report: Report) -> None:
        # Older plaques copied attempt build errors into the shared build state.
        # An explicit failure of the separate build step remains authoritative.
        if (
            self.status != "failed_build"
            and self.build == "failed_build"
            and any(item.count("FAIL_BUILD") for item in self.reports)
        ):
            self.build = ""
            self.build_errors = False
        self.reports = [
            old
            for old in self.reports
            if (old.iteration, old.title) != (report.iteration, report.title)
        ]
        self.reports.append(report)
        self.reports.sort(key=lambda item: (item.iteration, item.title))
        if report.total and not report.count("FAIL_BUILD") and not self.build:
            self.build = "completed"

    def build_state(self, report: Report) -> str:
        if report.count("FAIL_BUILD"):
            return "failed_build"
        return self.build if report.iteration == 0 else ""

    def overall(self) -> str:
        states = (self.status, self.build, self.workflow_status, self.execution_status)
        if not self.status or any(
            state and (state not in ICONS or state == "unknown") for state in states
        ):
            return "unknown"
        # A retry can pass its subset while the full build/test job still fails.
        if self.build == "failed_build" or self.execution_status in (
            "failed",
            "failed_build",
            "timed_out",
            "cancelled",
        ):
            return "failed"
        if any(report.failed for report in self.latest_reports):
            return "failed"
        if self.reports or self.status == "completed":
            return "completed"
        if self.status == "cancelled":
            return "cancelled"
        if self.status not in ("pending", "running", "built", "completed"):
            return "failed"
        return "running"


def _read_workload(data) -> Workload:
    if not isinstance(data, dict):
        raise ValueError("Expected a workload object")
    reports = data["reports"]
    if not isinstance(reports, list):
        raise ValueError("Expected a list of reports")
    fields = dict(data)
    fields["reports"] = [Report(**report) for report in reports]
    return Workload(**fields)


def _unavailable_workload(data) -> Workload | None:
    if not isinstance(data, dict):
        return None
    component = data.get("component")
    if not isinstance(component, str) or not component:
        return None
    job_id, attempt = data.get("job_id"), data.get("run_attempt")
    return Workload(
        component,
        job_id=job_id if type(job_id) is int and job_id >= 0 else None,
        run_attempt=attempt if type(attempt) is int and attempt > 0 else 1,
        status="report_failed",
        workflow_status="report_failed",
    )


@dataclasses.dataclass
class Plaque:
    build_preset: str
    workloads: list[Workload] = dataclasses.field(default_factory=list)
    notice: str = ""
    run: RunMetadata = dataclasses.field(default_factory=RunMetadata.from_env)
    history_url: str = ""
    too_large: bool = False

    def workload(self, component: str, run_attempt: int | None = None) -> Workload:
        for index, workload in enumerate(self.workloads):
            if workload.component == component:
                if run_attempt is not None and run_attempt > workload.run_attempt:
                    workload = Workload(component, run_attempt=run_attempt)
                    self.workloads[index] = workload
                return workload
        workload = Workload(component, run_attempt=run_attempt or 1)
        if not self.too_large:
            self.workloads.append(workload)
        return workload

    @classmethod
    def from_comment(cls, body: str) -> Plaque | None:
        match = DATA_RE.search(body)
        if (
            match is None
            and START not in body
            and "<!-- status-plaque-data" not in body
        ):
            return None
        header = re.search(r"^<!-- status .*?build_preset=([^,\n]*),", body)
        plaque = cls(header.group(1) if header else "Build and test results")
        try:
            if match is None:
                raise ValueError("Missing stored plaque state")
            payload = match.group(1)
            if payload.startswith(STATE_ENCODING):
                payload = zlib.decompress(
                    base64.b64decode(payload[len(STATE_ENCODING) :], validate=True)
                )
            data = json.loads(payload)
            if not isinstance(data, dict):
                raise ValueError("Expected stored plaque state")
            build_preset, workloads = data["build_preset"], data["workloads"]
            notice, history_url = data.get("notice", ""), data.get("history_url", "")
            too_large = data.get("too_large", False)
            if (
                not isinstance(notice, str)
                or not isinstance(history_url, str)
                or type(too_large) is not bool
            ):
                raise ValueError("Invalid plaque metadata")
            plaque.run = RunMetadata(**data["run"])
            plaque.notice, plaque.history_url, plaque.too_large = (
                notice,
                history_url,
                too_large,
            )
            if not isinstance(build_preset, str) or not isinstance(workloads, list):
                raise ValueError("Invalid plaque preset or workloads")
            plaque.build_preset = build_preset
            for data in workloads:
                try:
                    plaque.workloads.append(_read_workload(data))
                except (KeyError, TypeError, ValueError) as error:
                    LOGGER.warning("Cannot restore workload: %s", error)
                    row = _unavailable_workload(data)
                    if row is not None:
                        plaque.workloads.append(row)
                    plaque.mark_state_unavailable()
        except (KeyError, TypeError, ValueError, binascii.Error, zlib.error) as error:
            LOGGER.warning("Cannot restore status plaque: %s", error)
            plaque.workloads.clear()
            plaque.mark_state_unavailable()
        return plaque

    def mark_state_unavailable(self) -> None:
        if STATE_NOTICE not in self.notice:
            self.notice = f"{self.notice} {STATE_NOTICE}".strip()

    def render(self) -> str:
        # Keep ordinary JSON objects; Base64 makes the compressed state safe
        # inside the hidden HTML comment.
        raw = json.dumps(dataclasses.asdict(self), separators=(",", ":")).encode(
            "utf-8"
        )
        payload = STATE_ENCODING + base64.b64encode(zlib.compress(raw)).decode("ascii")
        return "\n".join(
            [
                START,
                f"<!-- status-plaque-data {payload} -->",
                self.render_table(),
                END,
            ]
        )

    def replace(self, body: str) -> str:
        rendered = self.render()
        match = BLOCK_RE.search(body)
        if match:
            return body[: match.start()] + rendered + body[match.end() :]
        # An incomplete managed block is not a legacy Markdown comment. Replace
        # its damaged contents so an old green table cannot survive recovery.
        for marker in (START, "<!-- status-plaque-data"):
            if marker in body:
                return body.split(marker, 1)[0] + rendered
        return f"{body}\n{rendered}"

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
            run=self.run,
            history_url=self.history_url,
            workloads=self.workloads,
            build_preset=self.build_preset,
            icons=ICONS,
            labels=LABELS,
            messages=MESSAGES,
            count_statuses=TestStatus.summary_table_order(),
            notice=self.notice,
            workflow_url=self.run.url,
        )


def fit_comment(body: str, limit: int = COMMENT_SIZE_LIMIT) -> str:
    """Bound the entire API payload, including its header and hidden state."""
    if len(body) <= limit:
        return body
    plaque = Plaque.from_comment(body) or Plaque("Build and test results")
    plaque.workloads.clear()
    plaque.history_url = ""
    plaque.too_large = True
    plaque.notice = "Results are too large for this comment. See individual results in the workflow run."
    header = body.splitlines()[0] if body.startswith("<!-- status ") else ""
    return f"{header}\n{plaque.render()}"
