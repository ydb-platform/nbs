#!/usr/bin/env python3
import argparse
import dataclasses
import json
import logging
import os
import sys
from contextlib import nullcontext
from enum import Enum
from operator import attrgetter
from pathlib import Path
from typing import Dict, Iterable

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from .junit_utils import get_property_value, iter_xml_files

LOGGER = logging.getLogger(__name__)


class TestStatus(Enum):
    PASS = 0
    FAIL = 1
    ERROR = 2
    SKIP = 3
    MUTE = 4
    FAIL_BUILD = 5

    def __lt__(self, other):
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
    log_urls: Dict[str, str]
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
    def from_junit(cls, testcase):
        classname, name = testcase.get("classname"), testcase.get("name")
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

        log_urls = {
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
        self.tests = []
        self.is_failed = False
        self.report_fn = None
        self.report_url = None
        self.counter = {s: 0 for s in TestStatus}

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
                row.append(render_pm(count, f"{report_url}#{status.report_anchor}", 0))
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


def render_pm(value, url, diff=None):
    if value:
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


def render_testlist_html(rows, fn: str, summary_url: str) -> None:
    templates_path = Path(__file__).with_name("templates")

    env = Environment(
        loader=FileSystemLoader(str(templates_path)), undefined=StrictUndefined
    )

    status_test = {}
    has_any_log = set()

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

    fp_ctx = (
        open(summary_fn, "at") if summary_fn else nullcontext(sys.stdout)
    )  # noqa: SIM115
    with fp_ctx as fp:
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


def gen_summary(summary_url_prefix: str, summary_out_folder: str, paths):
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


def get_comment_text(
    pr,
    summary: TestSummary,
    build_preset: str,
    test_history_url: str,
    test_target: str,
    test_time: str,
):
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


def update_pr_comment(
    run_number: int,
    pr,
    summary: TestSummary,
    build_preset: str,
    test_history_url: str,
    test_target: str,
    test_time: str,
    is_dry_run: bool,
):
    header = f"<!-- status pr={pr.number}, run={run_number}, build_preset={build_preset}, dry_run={is_dry_run} -->"

    body = None
    comment = None

    for c in pr.get_issue_comments():
        if c.body.startswith(header):
            LOGGER.info("Found comment with id=%s", c.id)
            comment = c
            body = [c.body]
            break

    if body is None:
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
    else:
        body.extend(["", ""])

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


def parse_title_html_path_args(args: list[str]) -> list[tuple[str, str, str]]:
    if len(args) % 3 != 0:
        raise ValueError("Invalid argument count")
    it = iter(args)
    return list(zip(it, it, it))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
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
    parser.add_argument("args", nargs="+", metavar="TITLE html_out path")
    args = parser.parse_args()

    try:
        title_path = parse_title_html_path_args(args.args)
    except ValueError as error:
        LOGGER.error(str(error))
        raise SystemExit(-1) from error

    summary = gen_summary(args.summary_url_prefix, args.summary_out_path, title_path)
    write_summary(summary, args.summary_out_env_path)

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
    update_pr_comment(
        run_number,
        pr,
        summary,
        args.build_preset,
        args.test_history_url,
        args.test_target,
        args.test_time,
        args.is_dry_run,
    )


if __name__ == "__main__":
    main()
