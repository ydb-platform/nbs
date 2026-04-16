#!/usr/bin/env python3
from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
import re
import sys
from contextlib import contextmanager
from enum import Enum
from operator import attrgetter
from pathlib import Path
from typing import IO, Iterable, Protocol, TypeAlias
from xml.etree import ElementTree as ET

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from ..helpers import setup_logger
from .junit_utils import get_property_value, iter_xml_files

LOGGER = logging.getLogger(__name__)
TitlePathTriplet: TypeAlias = tuple[str, str, str]
TitlePathPair: TypeAlias = tuple[str, str]
LogUrls: TypeAlias = dict[str, str]
WORKLOAD_STATUS_START = "<!-- workload-status -->"
WORKLOAD_STATUS_END = "<!-- /workload-status -->"
WORKLOAD_CHECKS_START = "<!-- workload-checks -->"
WORKLOAD_CHECKS_END = "<!-- /workload-checks -->"


class IssueCommentLike(Protocol):
    id: int
    body: str

    def edit(self, body: str) -> None: ...  # noqa: U100


class PullRequestHeadLike(Protocol):
    sha: str


class PullRequestLike(Protocol):
    number: int
    head: PullRequestHeadLike

    def get_issue_comments(self) -> Iterable[IssueCommentLike]: ...
    def create_issue_comment(self, body: str) -> None: ...  # noqa: U100


@contextmanager
def _summary_output_stream(summary_fn: str | None) -> Iterable[IO[str]]:
    if summary_fn:
        with open(summary_fn, "at") as fp:
            yield fp
        return

    yield sys.stdout


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


@dataclasses.dataclass
class TestResult:
    classname: str
    name: str
    status: TestStatus
    log_urls: LogUrls
    elapsed: float
    is_timed_out: bool

    @property
    def status_display(self) -> str:
        return self.status.label

    @property
    def elapsed_display(self) -> str:
        m, s = divmod(self.elapsed, 60)
        parts = []
        if m > 0:
            parts.append(f"{int(m)}m")
        parts.append(f"{s:.3f}s")
        return " ".join(parts)

    @property
    def full_name(self) -> str:
        return f"{self.classname}/{self.name}"

    @classmethod
    def from_junit(cls, testcase: ET.Element) -> TestResult:
        classname = testcase.get("classname") or ""
        name = testcase.get("name") or ""
        is_timed_out = False

        if testcase.find("failure") is not None:
            status = TestStatus.FAIL
            text = testcase.find("failure").text
            if text is not None and "Killed by timeout" in text:
                LOGGER.info("%s, %s is_timed_out = True", classname, name)
                is_timed_out = True
            if text is not None and "skipped due to a failed build" in text:
                status = TestStatus.FAIL_BUILD
        elif testcase.find("error") is not None:
            status = TestStatus.ERROR
        elif get_property_value(testcase, "mute") is not None:
            status = TestStatus.MUTE
        elif testcase.find("skipped") is not None:
            status = TestStatus.SKIP
        else:
            status = TestStatus.PASS

        logs_directory = get_property_value(testcase, "url:logs_directory")
        if logs_directory is not None:
            logs_directory = f"{logs_directory}/index.html"

        log_urls: dict[str, str | None] = {
            "DIR": logs_directory,
            "Log": get_property_value(testcase, "url:Log"),
            "log": get_property_value(testcase, "url:log"),
            "stdout": get_property_value(testcase, "url:stdout"),
            "stderr": get_property_value(testcase, "url:stderr"),
            "backtrace": get_property_value(testcase, "url:backtrace"),
            "recipe_stderr": get_property_value(testcase, "url:recipe stderr"),
            "recipe_stdout": get_property_value(testcase, "url:recipe stdout"),
        }
        log_urls = {k: v for k, v in log_urls.items() if v}

        elapsed = testcase.get("time")

        try:
            elapsed = float(elapsed)
        except (TypeError, ValueError):
            LOGGER.warning(
                "Unable to cast elapsed time for %s::%s value=%r",
                classname,
                name,
                elapsed,
            )
            elapsed = 0.0

        return cls(classname, name, status, log_urls, elapsed, is_timed_out)


class TestSummaryLine:
    def __init__(self, title: str):
        self.title = title
        self.tests: list[TestResult] = []
        self.is_failed = False
        self.report_fn: str | None = None
        self.report_url: str | None = None
        self.counter: dict[TestStatus, int] = {s: 0 for s in TestStatus}

    def add(self, test: TestResult) -> None:
        self.is_failed |= test.status.is_failure
        self.counter[test.status] += 1
        self.tests.append(test)

    def add_report(self, fn: str, url: str) -> None:
        self.report_fn = fn
        self.report_url = url

    @property
    def test_count(self) -> int:
        return len(self.tests)

    @property
    def passed(self) -> int:
        return self.counter[TestStatus.PASS]

    @property
    def errors(self) -> int:
        return self.counter[TestStatus.ERROR]

    @property
    def failed(self) -> int:
        return self.counter[TestStatus.FAIL]

    @property
    def failed_build(self) -> int:
        return self.counter[TestStatus.FAIL_BUILD]

    @property
    def skipped(self) -> int:
        return self.counter[TestStatus.SKIP]

    @property
    def muted(self) -> int:
        return self.counter[TestStatus.MUTE]

    def count(self, status: TestStatus) -> int:
        return self.counter[status]


class TestSummary:
    def __init__(self):
        self.lines: list[TestSummaryLine] = []
        self.is_failed = False

    def add_line(self, line: TestSummaryLine) -> None:
        self.is_failed |= line.is_failed
        self.lines.append(line)

    @property
    def is_empty(self) -> bool:
        return len(self.lines) == 0

    @staticmethod
    def render_line(items: Iterable[str]) -> str:
        return f"| {' | '.join(items)} |"

    def render(
        self, add_footnote: bool = False, build_failed_count: int = 0
    ) -> list[str]:
        github_srv = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
        repo = os.environ.get("GITHUB_REPOSITORY", "ydb-platform/nbs")

        footnote_url = f"{github_srv}/{repo}/tree/main/.github/config"

        footnote = (
            "[^1]"
            if add_footnote
            else f'<sup>[?]({footnote_url} "All mute rules are defined here")</sup>'
        )

        columns = ["TESTS"]
        for status in TestStatus.summary_table_order():
            if status == TestStatus.MUTE:
                columns.append(f"{status.summary_header}{footnote}")
            else:
                columns.append(status.summary_header)

        need_first_column = len(self.lines) > 1
        if need_first_column:
            columns.insert(0, "")

        result = [self.render_line(columns)]

        if need_first_column:
            result.append(self.render_line([":---"] + ["---:"] * (len(columns) - 1)))
        else:
            result.append(self.render_line(["---:"] * len(columns)))

        for line in self.lines:
            report_url = line.report_url
            row = []
            if need_first_column:
                row.append(line.title)

            row.append(render_pm(line.test_count, report_url, 0))
            for status in TestStatus.summary_table_order():
                count = line.count(status)
                if status == TestStatus.FAIL_BUILD:
                    count = max(build_failed_count, count)
                status_url = (
                    f"{report_url}#{status.report_anchor}" if report_url else None
                )
                row.append(render_pm(count, status_url, 0))
            result.append(self.render_line(row))

        if add_footnote:
            result.append("")
            result.append(f"[^1]: All mute rules are defined [here]({footnote_url}).")

        return result


def get_build_failed_count() -> int:
    try:
        return int(os.environ.get("BUILD_FAILED_COUNT", "0"))
    except ValueError:
        return 0


def render_pm(value: int, url: str | None, diff: int | None = None) -> str:
    if value and url:
        text = f"[{value}]({url})"
    else:
        text = str(value)

    if diff is not None and diff != 0:
        if diff < 0:
            sign = "-"
        else:
            sign = "+"

        text = f"{text} {sign}{abs(diff)}"

    return text


def render_summary_markdown(
    summary: TestSummary,
    *,
    add_footnote: bool = False,
    build_failed_count: int = 0,
) -> str:
    if summary.is_empty:
        return ""
    return "\n".join(
        summary.render(
            add_footnote=add_footnote,
            build_failed_count=build_failed_count,
        )
    )


def render_testlist_html(rows: list[TestResult], fn: str, summary_url: str) -> None:
    templates_path = Path(__file__).with_name("templates")

    env = Environment(
        loader=FileSystemLoader(str(templates_path)), undefined=StrictUndefined
    )

    status_test: dict[TestStatus, list[TestResult]] = {}
    has_any_log: set[TestStatus] = set()

    for t in rows:
        status_test.setdefault(t.status, []).append(t)
        if any(t.log_urls.values()):
            has_any_log.add(t.status)

    for status in status_test.keys():
        status_test[status].sort(key=attrgetter("full_name"))

    status_order = list(TestStatus.html_report_order())
    status_order = [s for s in status_order if s in status_test]

    content = env.get_template("summary.html").render(
        status_order=status_order,
        tests=status_test,
        has_any_log=has_any_log,
        summary_url=summary_url,
    )

    with open(fn, "w") as fp:
        fp.write(content)


def write_summary(summary: TestSummary, summary_out_env_path: str = "") -> None:
    summary_fn = summary_out_env_path or os.environ.get("GITHUB_STEP_SUMMARY")

    with _summary_output_stream(summary_fn) as fp:
        if summary.is_empty:
            fp.write(
                ":red_circle: Test run completed, no test results found. Please check build logs."
            )
        else:
            for line in summary.render(
                add_footnote=True,
                build_failed_count=get_build_failed_count(),
            ):
                fp.write(f"{line}\n")

        fp.write("\n")


def gen_summary(
    summary_url_prefix: str,
    summary_out_folder: str,
    paths: list[TitlePathTriplet],
) -> TestSummary:
    summary = TestSummary()

    for title, html_fn, path in paths:
        summary_line = TestSummaryLine(title)

        for _fn, _suite, case in iter_xml_files(path):
            test_result = TestResult.from_junit(case)
            summary_line.add(test_result)

        if not summary_line.tests:
            continue

        report_url = f"{summary_url_prefix}{html_fn}"

        render_testlist_html(
            sorted(summary_line.tests, key=lambda x: x.full_name),
            os.path.join(summary_out_folder, html_fn),
            summary_url=summary_url_prefix,
        )
        summary_line.add_report(html_fn, report_url)
        summary.add_line(summary_line)

    return summary


def gen_summary_counts(paths: list[TitlePathPair]) -> TestSummary:
    summary = TestSummary()

    for title, path in paths:
        summary_line = TestSummaryLine(title)

        for _fn, _suite, case in iter_xml_files(path):
            test_result = TestResult.from_junit(case)
            summary_line.add(test_result)

        if not summary_line.tests:
            continue

        summary.add_line(summary_line)

    return summary


def get_comment_text(
    pr: PullRequestLike,
    summary: TestSummary,
    build_preset: str,
    test_history_url: str,
    test_target: str,
    test_time: str,
) -> list[str]:
    test_target_message = f" target: **{test_target}**" if test_target else ""
    test_time_message = (
        f" (test time: {test_time}s)" if test_time and test_time != "0" else ""
    )

    if summary.is_empty:
        empty_summary = (
            f":red_circle: **{build_preset}**{test_target_message}{test_time_message}"
        )
        empty_summary += (
            f" Test run completed, no test results found for commit {pr.head.sha}."
        )
        return [empty_summary, "Please check build logs."]

    if summary.is_failed or get_build_failed_count() > 0:
        result = f":red_circle: **{build_preset}**{test_target_message}{test_time_message}: some tests FAILED"
    else:
        result = f":green_circle: **{build_preset}**{test_target_message}{test_time_message}: all tests PASSED"

    body = [f"{result} for commit {pr.head.sha}."]

    if test_history_url:
        body.append("")
        body.append(f"[Test history]({test_history_url})")

    body.extend(summary.render(build_failed_count=get_build_failed_count()))

    return body


def get_workload_status_text(
    build_preset: str,
    workload_status: str,
) -> list[str]:
    if workload_status == "in_progress":
        status_note = [
            "> [!IMPORTANT]",
            f"> Workload for **{build_preset}** is not finished yet. This comment will be updated after all workloads complete.",
        ]
    else:
        status_note = [
            "> [!NOTE]",
            f"> All workloads for **{build_preset}** have completed.",
        ]

    return [WORKLOAD_STATUS_START, *status_note, WORKLOAD_STATUS_END]


def get_comment_header(
    pr_number: int,
    run_number: int,
    build_preset: str,
    is_dry_run: bool,
) -> str:
    return (
        f"<!-- status pr={pr_number}, run={run_number}, "
        f"build_preset={build_preset}, dry_run={is_dry_run} -->"
    )


def get_workload_label(component: str) -> str:
    if component == "all":
        return "all"
    if component == "none":
        return "sanitized"
    return component.replace("_", " + ")


def get_workload_check_marker(component: str) -> str:
    return f"<!-- workload-check component={component} -->"


def get_workload_check_line(
    component: str,
    status: str,
    job_url: str = "",
) -> str:
    icons = {
        "pending": ":white_circle:",
        "running": ":hourglass_flowing_sand:",
        "completed": ":white_check_mark:",
    }
    label = get_workload_label(component)
    if job_url:
        label = f"[{label}]({job_url})"
    return f"- {icons[status]} {label} {get_workload_check_marker(component)}"


def get_workload_checks_text(
    build_preset: str,
    components: list[str],
) -> list[str]:
    return [
        WORKLOAD_CHECKS_START,
        "> [!TIP]",
        f"> Planned checks for **{build_preset}**.",
        "",
        *[get_workload_check_line(component, "pending") for component in components],
        WORKLOAD_CHECKS_END,
    ]


def _insert_managed_block(
    body: str,
    block: str,
    *,
    after_marker: str,
) -> str:
    if after_marker in body:
        return body.replace(after_marker, f"{after_marker}\n\n{block}", 1)

    lines = body.splitlines()
    lines.extend(["", block])
    return "\n".join(lines)


def replace_workload_status_block(
    body: str,
    build_preset: str,
    workload_status: str,
) -> str:
    status_block = "\n".join(get_workload_status_text(build_preset, workload_status))
    pattern = re.compile(
        rf"{re.escape(WORKLOAD_STATUS_START)}.*?{re.escape(WORKLOAD_STATUS_END)}",
        re.DOTALL,
    )

    if pattern.search(body):
        return pattern.sub(status_block, body, count=1)

    lines = body.splitlines()
    insert_at = len(lines)
    for idx, line in enumerate(lines):
        if line.startswith("> [!NOTE]") or line.startswith("> [!IMPORTANT]"):
            continue
        if line.startswith("> This is "):
            continue
        if line == "":
            continue
        insert_at = idx
        break

    updated_lines = lines[:insert_at] + [status_block, ""] + lines[insert_at:]
    return "\n".join(updated_lines)


def replace_workload_checks_block(
    body: str,
    build_preset: str,
    components: list[str],
) -> str:
    checks_block = "\n".join(get_workload_checks_text(build_preset, components))
    pattern = re.compile(
        rf"{re.escape(WORKLOAD_CHECKS_START)}.*?{re.escape(WORKLOAD_CHECKS_END)}",
        re.DOTALL,
    )
    if pattern.search(body):
        return pattern.sub(checks_block, body, count=1)

    return _insert_managed_block(
        body,
        checks_block,
        after_marker=WORKLOAD_STATUS_END,
    )


def update_workload_check_block(
    body: str,
    component: str,
    status: str,
    job_url: str = "",
) -> str:
    marker = get_workload_check_marker(component)
    pattern = re.compile(rf"^.*{re.escape(marker)}$", re.MULTILINE)
    match = pattern.search(body)
    if match is None:
        return body
    if not job_url:
        job_url_match = re.search(r"\]\((?P<url>[^)]+)\)", match.group(0))
        job_url = job_url_match.group("url") if job_url_match else ""
    return pattern.sub(
        get_workload_check_line(component, status, job_url),
        body,
        count=1,
    )


def complete_workload_checks_block(body: str) -> str:
    pattern = re.compile(
        r"^.*<!-- workload-check component=(?P<component>[^ ]+) -->$",
        re.MULTILINE,
    )

    def _replace(match: re.Match[str]) -> str:
        component = match.group("component")
        job_url_match = re.search(r"\]\((?P<url>[^)]+)\)", match.group(0))
        job_url = job_url_match.group("url") if job_url_match else ""
        return get_workload_check_line(component, "completed", job_url)

    return pattern.sub(_replace, body)


def find_pr_comment(
    pr: PullRequestLike,
    header: str,
) -> IssueCommentLike | None:
    for comment in pr.get_issue_comments():
        if comment.body.startswith(header):
            LOGGER.info("Found comment with id=%s", comment.id)
            return comment
    return None


def get_base_comment_body(
    header: str,
    build_preset: str,
    is_dry_run: bool,
    workload_status: str,
    workload_components: list[str] | None = None,
) -> list[str]:
    body = [header]

    if is_dry_run:
        body.extend(
            [
                "> [!NOTE]",
                "> This is a simulation, not a real result. If you see this, everything is as it should be.",
                "",
            ]
        )
    body.extend(
        [
            "> [!NOTE]",
            "> This is an automated comment that will be appended during run.",
            "",
        ]
    )
    body.extend(get_workload_status_text(build_preset, workload_status))
    body.append("")
    if workload_components is not None:
        body.extend(get_workload_checks_text(build_preset, workload_components))
        body.append("")
    return body


def update_pr_comment(
    run_number: int,
    pr: PullRequestLike,
    summary: TestSummary,
    build_preset: str,
    test_history_url: str,
    test_target: str,
    test_time: str,
    is_dry_run: bool,
    workload_status: str,
) -> None:
    header = get_comment_header(pr.number, run_number, build_preset, is_dry_run)
    comment = find_pr_comment(pr, header)

    if comment is None:
        body = get_base_comment_body(
            header,
            build_preset,
            is_dry_run,
            workload_status,
        )
    else:
        body = [
            replace_workload_status_block(
                comment.body,
                build_preset,
                workload_status,
            ),
            "",
            "",
        ]

    body.extend(
        get_comment_text(
            pr,
            summary,
            build_preset,
            test_history_url,
            test_target,
            test_time,
        )
    )

    body = "\n".join(body)

    if comment is None:
        LOGGER.info("Creating new comment")
        pr.create_issue_comment(body)
    else:
        LOGGER.info("Updating existing comment")
        comment.edit(body)


def update_pr_comment_workload_status(
    run_number: int,
    pr: PullRequestLike,
    build_preset: str,
    is_dry_run: bool,
    workload_status: str,
) -> None:
    header = get_comment_header(pr.number, run_number, build_preset, is_dry_run)
    comment = find_pr_comment(pr, header)

    if comment is None:
        LOGGER.info("No existing comment found for workload status update")
        return

    LOGGER.info("Updating workload status for comment id=%s", comment.id)
    comment.edit(
        replace_workload_status_block(
            comment.body,
            build_preset,
            workload_status,
        )
    )

def initialize_pr_comment(
    run_number: int,
    pr: PullRequestLike,
    build_preset: str,
    is_dry_run: bool,
    workload_status: str,
    workload_components: list[str],
) -> None:
    header = get_comment_header(pr.number, run_number, build_preset, is_dry_run)
    comment = find_pr_comment(pr, header)

    if comment is None:
        LOGGER.info("Creating new pre-run comment")
        pr.create_issue_comment(
            "\n".join(
                get_base_comment_body(
                    header,
                    build_preset,
                    is_dry_run,
                    workload_status,
                    workload_components,
                )
            )
        )
        return

    LOGGER.info("Refreshing existing pre-run comment id=%s", comment.id)
    body = replace_workload_status_block(comment.body, build_preset, workload_status)
    body = replace_workload_checks_block(body, build_preset, workload_components)
    comment.edit(body)


def update_pr_comment_workload_check(
    run_number: int,
    pr: PullRequestLike,
    build_preset: str,
    component: str,
    is_dry_run: bool,
    workload_check_status: str,
    job_url: str = "",
) -> None:
    header = get_comment_header(pr.number, run_number, build_preset, is_dry_run)
    comment = find_pr_comment(pr, header)

    if comment is None:
        LOGGER.info("No existing comment found for workload check update")
        return

    LOGGER.info("Updating workload check for comment id=%s", comment.id)
    comment.edit(
        update_workload_check_block(
            comment.body,
            component,
            workload_check_status,
            job_url,
        )
    )


def complete_pr_comment_workload_checks(
    run_number: int,
    pr: PullRequestLike,
    build_preset: str,
    is_dry_run: bool,
) -> None:
    header = get_comment_header(pr.number, run_number, build_preset, is_dry_run)
    comment = find_pr_comment(pr, header)

    if comment is None:
        LOGGER.info("No existing comment found for workload checks completion")
        return

    LOGGER.info("Completing workload checks for comment id=%s", comment.id)
    comment.edit(complete_workload_checks_block(comment.body))


def parse_title_html_path_args(args: list[str]) -> list[TitlePathTriplet]:
    if len(args) % 3 != 0:
        raise ValueError("Invalid argument count")
    it = iter(args)
    return list(zip(it, it, it))


def main() -> None:
    setup_logger(name=__name__, fmt="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary-out-path", required=True)
    parser.add_argument(
        "--summary-out-env-path",
        required=False,
        help="File to write out summary instead of GITHUB_STEP_SUMMARY",
        default="",
    )
    parser.add_argument("--summary-url-prefix", required=True)
    parser.add_argument("--test-history-url", required=False)
    parser.add_argument(
        "--build-preset", default="default-linux-x86-64-relwithdebinfo", required=False
    )
    parser.add_argument("--test-target", default="", required=False)
    parser.add_argument(
        "--is-dry-run",
        default=False,
        action="store_true",
        help="Add mark in comments that this is simulation, not real result",
    )
    parser.add_argument("--test-time", default="0", required=False)
    parser.add_argument(
        "--workload-status",
        choices=("in_progress", "completed"),
        default="completed",
        help="Update the PR comment workload state",
    )
    parser.add_argument(
        "--update-workload-status-only",
        default=False,
        action="store_true",
        help="Only update the workload status block in an existing PR comment",
    )
    parser.add_argument("args", nargs="*", metavar="TITLE html_out path")
    args = parser.parse_args()

    summary: TestSummary | None = None
    if args.args:
        try:
            title_path = parse_title_html_path_args(args.args)
        except ValueError as error:
            LOGGER.error(str(error))
            raise SystemExit(-1) from error

        summary = gen_summary(args.summary_url_prefix, args.summary_out_path, title_path)
        write_summary(summary, args.summary_out_env_path)
    elif not args.update_workload_status_only:
        LOGGER.error("No summary inputs provided")
        raise SystemExit(-1)

    if os.environ.get("GITHUB_EVENT_NAME") not in (
        "pull_request",
        "pull_request_target",
    ):
        return

    from github import Auth as GithubAuth, Github
    from github.PullRequest import PullRequest

    gh = Github(auth=GithubAuth.Token(os.environ["GITHUB_TOKEN"]))

    with open(os.environ["GITHUB_EVENT_PATH"]) as fp:
        event = json.load(fp)

    run_number = int(os.environ.get("GITHUB_RUN_NUMBER", "0"))
    pr = gh.create_from_raw_data(PullRequest, event["pull_request"])
    if args.update_workload_status_only:
        update_pr_comment_workload_status(
            run_number,
            pr,
            args.build_preset,
            args.is_dry_run,
            args.workload_status,
        )
    else:
        assert summary is not None
        update_pr_comment(
            run_number,
            pr,
            summary,
            args.build_preset,
            args.test_history_url,
            args.test_target,
            args.test_time,
            args.is_dry_run,
            args.workload_status,
        )


if __name__ == "__main__":
    main()
