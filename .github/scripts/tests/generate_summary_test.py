from __future__ import annotations

import json
from pathlib import Path

from scripts.tests import generate_summary as gs


def test_from_junit_marks_fail_build_timeout_and_logs_directory(mk_testcase) -> None:
    case = mk_testcase(
        failure="Killed by timeout; skipped due to a failed build",
        props={
            "url:logs_directory": "https://logs/path",
            "url:stdout": "https://stdout",
        },
    )

    result = gs.TestResult.from_junit(case)

    assert result.status == gs.TestStatus.FAIL_BUILD
    assert result.is_timed_out is True
    assert result.log_urls["DIR"] == "https://logs/path/index.html"
    assert result.log_urls["stdout"] == "https://stdout"


def test_from_junit_normalizes_non_finite_elapsed(mk_testcase) -> None:
    for value in ("NaN", "Infinity", "-Infinity"):
        result = gs.TestResult.from_junit(mk_testcase(time=value))

        assert result.elapsed == 0.0


def test_gen_summary_creates_html_and_aggregates_counters(
    tmp_path: Path, mk_testcase, write_junit_xml
) -> None:
    xml_path = tmp_path / "junit.xml"
    write_junit_xml(
        xml_path,
        mk_testcase(classname="a", name="pass"),
        mk_testcase(classname="a", name="fail", failure="boom"),
        mk_testcase(classname="a", name="mute", props={"mute": "rule-1"}),
    )

    summary = gs.gen_summary(
        "https://summary/",
        str(tmp_path),
        [("Tests", "ya-test.html", str(xml_path))],
    )

    assert summary.is_empty is False
    assert len(summary.lines) == 1

    line = summary.lines[0]
    assert line.test_count == 3
    assert line.passed == 1
    assert line.failed == 1
    assert line.muted == 1

    html = (tmp_path / "ya-test.html").read_text()
    assert 'id="FAIL"' in html
    assert "a/fail" in html
    assert "Summary dir file listing" in html


def test_gen_summary_links_build_errors_in_fail_build_html(
    tmp_path: Path, mk_testcase, write_junit_xml
) -> None:
    xml_path = tmp_path / "junit.xml"
    write_junit_xml(
        xml_path,
        mk_testcase(
            classname="",
            name="unittest",
            failure=(
                "skipped due to a failed build\n"
                "Depends on broken: cloud/blockstore/libs/common/ut"
            ),
        ),
        suite_name="cloud/blockstore/libs/common/ut",
    )

    gs.gen_summary(
        "",
        str(tmp_path),
        [("Tests", "ya-test.html", str(xml_path))],
        build_error_log_url="build_errors.html",
    )

    html = (tmp_path / "ya-test.html").read_text()
    assert '<h1 id="FAIL_BUILD">FAIL_BUILD (1)</h1>' in html
    assert 'href="build_errors.html">Build errors</a>' in html
    assert (
        'href="build_errors.html#cloud-blockstore-libs-common-ut">BUILD ERRORS</a>'
        in html
    )
    assert "cloud/blockstore/libs/common/ut/unittest" in html
    assert "<td>/unittest</td>" not in html


def test_gen_summary_counts_renders_plain_number_table(
    tmp_path: Path, mk_testcase, write_junit_xml
) -> None:
    xml_path = tmp_path / "junit.xml"
    write_junit_xml(
        xml_path,
        mk_testcase(classname="a", name="pass"),
        mk_testcase(classname="a", name="fail", failure="boom"),
        mk_testcase(classname="a", name="mute", props={"mute": "rule-1"}),
    )

    summary = gs.gen_summary_counts([("GA Scripts pytest", str(xml_path))])
    markdown = gs.render_summary_markdown(summary)

    assert (
        "| TESTS | PASSED | ERRORS | FAILED | FAILED BUILD | SKIPPED | MUTED"
        in markdown
    )
    assert "| 3 | 1 | 0 | 1 | 0 | 0 | 1 |" in markdown
    assert "[3](" not in markdown
    assert "[1](None#PASS)" not in markdown


def test_write_summary_writes_markdown_table_and_footnote(tmp_path: Path) -> None:
    line = gs.TestSummaryLine("Tests")
    line.add(gs.TestResult("cls", "ok", gs.TestStatus.PASS, {}, 1.2, False))
    line.add_report("ya-test.html", "https://summary/ya-test.html")

    summary = gs.TestSummary()
    summary.add_line(line)

    out = tmp_path / "summary_env"
    gs.write_summary(summary, str(out))

    content = out.read_text()
    assert (
        "| TESTS | PASSED | ERRORS | FAILED | FAILED BUILD | SKIPPED | MUTED" in content
    )
    assert "[1](https://summary/ya-test.html)" in content
    assert "[^1]: All mute rules are defined" in content


def test_write_summary_json_keeps_counts_per_report(tmp_path: Path) -> None:
    line = gs.TestSummaryLine("Tests")
    line.add(gs.TestResult("cls", "ok", gs.TestStatus.PASS, {}, 1.2, False))
    line.add_report("ya-test.html", "https://summary/ya-test.html")

    summary = gs.TestSummary()
    summary.add_line(line)

    gs.write_summary_json(summary, str(tmp_path), build_failed_count=2)

    payload = json.loads((tmp_path / "summary.json").read_text())
    assert payload["aggregate_build_failed_count"] == 2
    assert payload["reports"][0]["counts"]["FAIL_BUILD"] == 0
    assert payload["reports"][0]["total"] == 1
    assert len(payload["reports"][0]["tests"]) == 1


def test_write_summary_json_normalizes_non_finite_elapsed(tmp_path: Path) -> None:
    line = gs.TestSummaryLine("Tests")
    line.add(gs.TestResult("cls", "nan", gs.TestStatus.PASS, {}, float("nan"), False))

    summary = gs.TestSummary()
    summary.add_line(line)

    gs.write_summary_json(summary, str(tmp_path))

    content = (tmp_path / "summary.json").read_text()
    assert "NaN" not in content

    payload = json.loads(content)
    assert payload["reports"][0]["tests"][0]["elapsed_seconds"] == 0.0


def test_get_comment_text_respects_build_failed_count(monkeypatch) -> None:
    class _Head:
        sha = "abc123"

    class _PR:
        head = _Head()

    line = gs.TestSummaryLine("Tests")
    line.add(gs.TestResult("cls", "ok", gs.TestStatus.PASS, {}, 0.1, False))
    line.add_report("ya-test.html", "https://summary/ya-test.html")

    summary = gs.TestSummary()
    summary.add_line(line)

    monkeypatch.setenv("BUILD_FAILED_COUNT", "1")
    body = gs.get_comment_text(_PR(), summary, "linux", "", "", "0")

    assert body[0].startswith(":red_circle:")
    assert "some tests FAILED" in body[0]


def test_parse_title_html_path_args_rejects_incomplete_triplet() -> None:
    try:
        gs.parse_title_html_path_args(["title", "out.html"])
    except ValueError as err:
        assert "Invalid argument count" in str(err)
    else:
        raise AssertionError("ValueError was expected")


def test_status_metadata_defines_orders_and_labels() -> None:
    assert gs.TestStatus.FAIL.label == "FAIL"
    assert gs.TestStatus.FAIL_BUILD.summary_header == "FAILED BUILD"
    assert gs.TestStatus.MUTE.report_anchor == "MUTE"
    assert gs.TestStatus.FAIL.is_failure is True
    assert gs.TestStatus.SKIP.is_failure is False

    assert gs.TestStatus.summary_table_order() == (
        gs.TestStatus.PASS,
        gs.TestStatus.ERROR,
        gs.TestStatus.FAIL,
        gs.TestStatus.FAIL_BUILD,
        gs.TestStatus.SKIP,
        gs.TestStatus.MUTE,
    )
    assert gs.TestStatus.html_report_order() == (
        gs.TestStatus.ERROR,
        gs.TestStatus.FAIL,
        gs.TestStatus.FAIL_BUILD,
        gs.TestStatus.SKIP,
        gs.TestStatus.MUTE,
        gs.TestStatus.PASS,
    )


def test_write_summary_renders_expected_table_row(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("BUILD_FAILED_COUNT", "0")
    line = gs.TestSummaryLine("Tests")
    line.add(gs.TestResult("cls", "ok", gs.TestStatus.PASS, {}, 1.0, False))
    line.add(gs.TestResult("cls", "bad", gs.TestStatus.FAIL, {}, 1.0, False))
    line.add(gs.TestResult("cls", "muted", gs.TestStatus.MUTE, {}, 1.0, False))
    line.add_report("ya-test.html", "https://summary/ya-test.html")

    summary = gs.TestSummary()
    summary.add_line(line)

    out = tmp_path / "summary_env"
    gs.write_summary(summary, str(out))
    content = out.read_text()

    assert (
        "| [3](https://summary/ya-test.html) | "
        "[1](https://summary/ya-test.html#PASS) | 0 | "
        "[1](https://summary/ya-test.html#FAIL) | 0 | 0 | "
        "[1](https://summary/ya-test.html#MUTE) |"
    ) in content


def test_write_summary_omits_fail_build_log_link(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("BUILD_FAILED_COUNT", "0")
    line = gs.TestSummaryLine("Tests")
    line.add(
        gs.TestResult(
            "cls",
            "build_failed",
            gs.TestStatus.FAIL_BUILD,
            {},
            1.0,
            False,
        )
    )
    line.add_report("ya-test.html", "https://summary/ya-test.html")

    summary = gs.TestSummary()
    summary.add_line(line)

    out = tmp_path / "summary_env"
    gs.write_summary(summary, str(out))
    content = out.read_text()

    assert "[1](https://summary/ya-test.html#FAIL_BUILD)" in content
    assert "https://summary/build_errors.html" not in content


def test_update_pr_comment_creates_new_comment(monkeypatch) -> None:
    monkeypatch.setenv("BUILD_FAILED_COUNT", "0")

    class FakeHead:
        sha = "deadbeef"

    class FakePR:
        number = 77
        head = FakeHead()

        def __init__(self) -> None:
            self.created = []

        def get_issue_comments(self) -> list[object]:
            return []

        def create_issue_comment(self, body: str) -> None:
            self.created.append(body)

    summary = gs.TestSummary()
    line = gs.TestSummaryLine("Tests")
    line.add(gs.TestResult("cls", "ok", gs.TestStatus.PASS, {}, 0.1, False))
    line.add_report("ya-test.html", "https://summary/ya-test.html")
    summary.add_line(line)

    pr = FakePR()
    gs.update_pr_comment(
        run_number=12,
        pr=pr,
        summary=summary,
        build_preset="linux",
        test_history_url="",
        test_target="",
        test_time="0",
        is_dry_run=True,
        workload_status="in_progress",
    )

    assert len(pr.created) == 1
    body = pr.created[0]
    assert (
        "<!-- status pr=77, run=12, build_preset=linux, dry_run=True, " "revision=0 -->"
    ) in body
    assert "This is a simulation, not a real result" in body
    assert "<!-- workload-status -->" in body
    assert "Workload for **linux** is not finished yet" in body
    assert "all tests PASSED for commit deadbeef." in body


def test_update_pr_comment_updates_existing_comment() -> None:
    class FakeHead:
        sha = "abc123"

    class FakeComment:
        id = 1001

        def __init__(self, body: str) -> None:
            self.body = body
            self.edits = []

        def edit(self, body: str) -> None:
            self.body = body
            self.edits.append(body)

    class FakePR:
        number = 77
        head = FakeHead()

        def __init__(self, comment: FakeComment) -> None:
            self._comment = comment
            self.created = []

        def get_issue_comments(self) -> list[FakeComment]:
            return [self._comment]

        def create_issue_comment(self, body: str) -> None:
            self.created.append(body)

    header = (
        "<!-- status pr=77, run=12, build_preset=linux, dry_run=False, "
        "revision=3 -->"
    )
    existing = FakeComment(
        "\n".join(
            [
                header,
                "> [!NOTE]",
                "> This is an automated comment that will be appended during run.",
                "",
                gs.WORKLOAD_STATUS_START,
                "> [!NOTE]",
                "> All workloads for **linux** have completed.",
                gs.WORKLOAD_STATUS_END,
                "",
                "old text",
            ]
        )
    )
    pr = FakePR(existing)

    summary = gs.TestSummary()
    line = gs.TestSummaryLine("Tests")
    line.add(gs.TestResult("cls", "failed", gs.TestStatus.FAIL, {}, 0.1, False))
    line.add_report("ya-test.html", "https://summary/ya-test.html")
    summary.add_line(line)

    gs.update_pr_comment(
        run_number=12,
        pr=pr,
        summary=summary,
        build_preset="linux",
        test_history_url="",
        test_target="",
        test_time="0",
        is_dry_run=False,
        workload_status="in_progress",
    )

    assert len(pr.created) == 0
    assert len(existing.edits) == 1
    assert "All workloads for **linux** have completed." not in existing.body
    assert "Workload for **linux** is not finished yet" in existing.body
    assert "old text" in existing.body
    assert "some tests FAILED for commit abc123." in existing.body
    assert gs.get_comment_revision(existing.body) == 4


def test_update_pr_comment_retries_lost_concurrent_summary_edit(
    monkeypatch,
) -> None:
    monkeypatch.setattr(gs, "WORKLOAD_COMMENT_EDIT_VERIFY_DELAY_SECONDS", 0)

    class FakeHead:
        sha = "abc123"

    class FakeComment:
        id = 1001

        def __init__(self, body: str) -> None:
            self.body = body
            self.edits = []

        def edit(self, body: str) -> None:
            self.edits.append(body)
            self.body = body
            if len(self.edits) == 1:
                self.body = gs.bump_comment_revision(
                    "\n".join([initial_body, "", "", concurrent_summary])
                )

    class FakePR:
        number = 77
        head = FakeHead()

        def __init__(self, comment: FakeComment) -> None:
            self._comment = comment
            self.created = []

        def get_issue_comments(self) -> list[FakeComment]:
            return [self._comment]

        def create_issue_comment(self, body: str) -> None:
            self.created.append(body)

    initial_body = "\n".join(
        [
            (
                "<!-- status pr=77, run=12, build_preset=linux, "
                "dry_run=False, revision=0 -->"
            ),
            "> [!NOTE]",
            "> This is an automated comment that will be appended during run.",
            "",
            gs.WORKLOAD_STATUS_START,
            "> [!NOTE]",
            "> This comment will be updated after all workloads complete.",
            gs.WORKLOAD_STATUS_END,
        ]
    )
    concurrent_summary = (
        ":green_circle: **linux** target: **cloud/blockstore/**: "
        "all tests PASSED for commit abc123."
    )
    existing = FakeComment(initial_body)
    pr = FakePR(existing)

    summary = gs.TestSummary()
    line = gs.TestSummaryLine("Tests")
    line.add(gs.TestResult("cls", "ok", gs.TestStatus.PASS, {}, 0.1, False))
    line.add_report("ya-test.html", "https://summary/ya-test.html")
    summary.add_line(line)

    gs.update_pr_comment(
        run_number=12,
        pr=pr,
        summary=summary,
        build_preset="linux",
        test_history_url="",
        test_target="cloud/filestore/",
        test_time="0",
        is_dry_run=False,
        workload_status="in_progress",
    )

    assert len(pr.created) == 0
    assert len(existing.edits) == 2
    assert concurrent_summary in existing.body
    assert "target: **cloud/filestore/**" in existing.body
    assert "all tests PASSED for commit abc123." in existing.body
    assert gs.get_comment_revision(existing.body) == 2


def test_update_pr_comment_workload_status_only_preserves_existing_body() -> None:
    class FakeHead:
        sha = "abc123"

    class FakeComment:
        id = 1001

        def __init__(self, body: str) -> None:
            self.body = body
            self.edits = []

        def edit(self, body: str) -> None:
            self.body = body
            self.edits.append(body)

    class FakePR:
        number = 77
        head = FakeHead()

        def __init__(self, comment: FakeComment) -> None:
            self._comment = comment

        def get_issue_comments(self) -> list[FakeComment]:
            return [self._comment]

        def create_issue_comment(self, body: str) -> None:  # noqa: U100
            raise AssertionError("should not create a new comment")

    existing = FakeComment(
        "\n".join(
            [
                (
                    "<!-- status pr=77, run=12, build_preset=linux, "
                    "dry_run=False, revision=7 -->"
                ),
                "> [!NOTE]",
                "> This is an automated comment that will be appended during run.",
                "",
                gs.WORKLOAD_STATUS_START,
                "> [!IMPORTANT]",
                "> Workload for **linux** is not finished yet. This comment will be updated after all workloads complete.",
                gs.WORKLOAD_STATUS_END,
                "",
                ":green_circle: **linux**: all tests PASSED for commit abc123.",
            ]
        )
    )
    pr = FakePR(existing)

    gs.update_pr_comment_workload_status(
        run_number=12,
        pr=pr,
        build_preset="linux",
        is_dry_run=False,
        workload_status="completed",
    )

    assert len(existing.edits) == 1
    assert "Workload for **linux** is not finished yet" not in existing.body
    assert "All workloads for **linux** have completed." in existing.body
    assert gs.get_comment_revision(existing.body) == 8
    assert (
        ":green_circle: **linux**: all tests PASSED for commit abc123." in existing.body
    )


def test_update_pr_comment_workload_status_upgrades_legacy_header() -> None:
    class FakeHead:
        sha = "abc123"

    class FakeComment:
        id = 1001

        def __init__(self, body: str) -> None:
            self.body = body
            self.edits = []

        def edit(self, body: str) -> None:
            self.body = body
            self.edits.append(body)

    class FakePR:
        number = 77
        head = FakeHead()

        def __init__(self, comment: FakeComment) -> None:
            self._comment = comment

        def get_issue_comments(self) -> list[FakeComment]:
            return [self._comment]

        def create_issue_comment(self, body: str) -> None:  # noqa: U100
            raise AssertionError("should not create a new comment")

    existing = FakeComment(
        "\n".join(
            [
                "<!-- status pr=77, run=12, build_preset=linux, dry_run=False -->",
                gs.WORKLOAD_STATUS_START,
                "> [!IMPORTANT]",
                "> Workload for **linux** is not finished yet.",
                gs.WORKLOAD_STATUS_END,
            ]
        )
    )
    pr = FakePR(existing)

    gs.update_pr_comment_workload_status(
        run_number=12,
        pr=pr,
        build_preset="linux",
        is_dry_run=False,
        workload_status="completed",
    )

    assert len(existing.edits) == 1
    assert existing.body.startswith(
        "<!-- status pr=77, run=12, build_preset=linux, dry_run=False, "
        "revision=1 -->"
    )


def test_initialize_pr_comment_creates_workload_checks_block() -> None:
    class FakeHead:
        sha = "abc123"

    class FakePR:
        number = 77
        head = FakeHead()

        def __init__(self) -> None:
            self.created = []

        def get_issue_comments(self) -> list[object]:
            return []

        def create_issue_comment(self, body: str) -> None:
            self.created.append(body)

    pr = FakePR()
    gs.initialize_pr_comment(
        run_number=12,
        pr=pr,
        build_preset="linux",
        is_dry_run=False,
        workload_status="in_progress",
        workload_components=["blockstore", "tasks_storage"],
    )

    assert len(pr.created) == 1
    body = pr.created[0]
    assert gs.WORKLOAD_CHECKS_START in body
    assert "Planned checks for **linux**." in body
    assert "blockstore" in body
    assert "tasks + storage" in body


def test_update_pr_comment_workload_check_creates_minimal_comment_when_missing(
    monkeypatch,
) -> None:
    monkeypatch.setattr(gs, "WORKLOAD_COMMENT_EDIT_VERIFY_DELAY_SECONDS", 0)

    class FakeHead:
        sha = "abc123"

    class FakeComment:
        def __init__(self, comment_id: int, body: str) -> None:
            self.id = comment_id
            self.body = body
            self.edits = []

        def edit(self, body: str) -> None:
            self.body = body
            self.edits.append(body)

    class FakePR:
        number = 77
        head = FakeHead()

        def __init__(self) -> None:
            self.comments = []

        def get_issue_comments(self) -> list[FakeComment]:
            return self.comments

        def create_issue_comment(self, body: str) -> None:
            self.comments.append(FakeComment(len(self.comments) + 1, body))

    pr = FakePR()

    gs.update_pr_comment_workload_check(
        run_number=12,
        pr=pr,
        build_preset="linux",
        component="blockstore",
        is_dry_run=False,
        workload_check_status="completed",
        job_url="https://github.example/job/123",
    )

    assert len(pr.comments) == 1
    body = pr.comments[0].body
    assert gs.WORKLOAD_CHECKS_START in body
    assert "Planned checks for **linux**." in body
    assert gs.get_workload_check_status(body, "blockstore") == "completed"
    assert "https://github.example/job/123" in body
    assert gs.get_comment_revision(body) == 1


def test_update_pr_comment_workload_check_adds_missing_component_row(
    monkeypatch,
) -> None:
    monkeypatch.setattr(gs, "WORKLOAD_COMMENT_EDIT_VERIFY_DELAY_SECONDS", 0)

    class FakeHead:
        sha = "abc123"

    class FakeComment:
        id = 1001

        def __init__(self, body: str) -> None:
            self.body = body
            self.edits = []

        def edit(self, body: str) -> None:
            self.body = body
            self.edits.append(body)

    class FakePR:
        number = 77
        head = FakeHead()

        def __init__(self, comment: FakeComment) -> None:
            self._comment = comment
            self.created = []

        def get_issue_comments(self) -> list[FakeComment]:
            return [self._comment]

        def create_issue_comment(self, body: str) -> None:
            self.created.append(body)

    existing = FakeComment(
        "\n".join(
            [
                (
                    "<!-- status pr=77, run=12, build_preset=linux, "
                    "dry_run=False, revision=0 -->"
                ),
                *gs.get_workload_status_text("linux", "in_progress"),
                "",
                gs.WORKLOAD_CHECKS_START,
                gs.get_workload_check_line("blockstore", "completed"),
                gs.WORKLOAD_CHECKS_END,
            ]
        )
    )
    pr = FakePR(existing)

    gs.update_pr_comment_workload_check(
        run_number=12,
        pr=pr,
        build_preset="linux",
        component="filestore",
        is_dry_run=False,
        workload_check_status="running",
        job_url="https://github.example/job/456",
    )

    assert pr.created == []
    assert len(existing.edits) == 1
    assert gs.get_workload_check_status(existing.body, "blockstore") == "completed"
    assert gs.get_workload_check_status(existing.body, "filestore") == "running"
    assert "https://github.example/job/456" in existing.body
    assert gs.get_comment_revision(existing.body) == 1


def test_update_pr_comment_workload_check_does_not_downgrade_workload_status(
    monkeypatch,
) -> None:
    monkeypatch.setattr(gs, "WORKLOAD_COMMENT_EDIT_VERIFY_DELAY_SECONDS", 0)

    class FakeHead:
        sha = "abc123"

    class FakeComment:
        id = 1001

        def __init__(self, body: str) -> None:
            self.body = body

        def edit(self, body: str) -> None:
            self.body = body

    class FakePR:
        number = 77
        head = FakeHead()

        def __init__(self, comment: FakeComment) -> None:
            self._comment = comment

        def get_issue_comments(self) -> list[FakeComment]:
            return [self._comment]

        def create_issue_comment(self, body: str) -> None:  # noqa: U100
            raise AssertionError("should not create a new comment")

    existing = FakeComment(
        "\n".join(
            [
                (
                    "<!-- status pr=77, run=12, build_preset=linux, "
                    "dry_run=False, revision=0 -->"
                ),
                *gs.get_workload_status_text("linux", "completed"),
                "",
                gs.WORKLOAD_CHECKS_START,
                gs.WORKLOAD_CHECKS_END,
            ]
        )
    )
    pr = FakePR(existing)

    gs.update_pr_comment_workload_check(
        run_number=12,
        pr=pr,
        build_preset="linux",
        component="blockstore",
        is_dry_run=False,
        workload_check_status="completed",
    )

    assert "All workloads for **linux** have completed." in existing.body
    assert "is not finished yet" not in existing.body
    assert gs.get_workload_check_status(existing.body, "blockstore") == "completed"


def test_update_pr_comment_workload_check_preserves_existing_job_url() -> None:
    class FakeHead:
        sha = "abc123"

    class FakeComment:
        id = 1001

        def __init__(self, body: str) -> None:
            self.body = body
            self.edits = []

        def edit(self, body: str) -> None:
            self.body = body
            self.edits.append(body)

    class FakePR:
        number = 77
        head = FakeHead()

        def __init__(self, comment: FakeComment) -> None:
            self._comment = comment

        def get_issue_comments(self) -> list[FakeComment]:
            return [self._comment]

        def create_issue_comment(self, body: str) -> None:  # noqa: U100
            raise AssertionError("should not create a new comment")

    existing = FakeComment(
        "\n".join(
            [
                (
                    "<!-- status pr=77, run=12, build_preset=linux, "
                    "dry_run=False, revision=0 -->"
                ),
                gs.WORKLOAD_CHECKS_START,
                gs.get_workload_check_line(
                    "blockstore",
                    "running",
                    "https://github.example/job/123",
                ),
                gs.WORKLOAD_CHECKS_END,
            ]
        )
    )
    pr = FakePR(existing)

    gs.update_pr_comment_workload_check(
        run_number=12,
        pr=pr,
        build_preset="linux",
        component="blockstore",
        is_dry_run=False,
        workload_check_status="completed",
    )

    assert len(existing.edits) == 1
    assert ":white_check_mark:" in existing.body
    assert "https://github.example/job/123" in existing.body
    assert gs.get_comment_revision(existing.body) == 1


def test_update_pr_comment_workload_check_retries_lost_concurrent_edit(
    monkeypatch,
) -> None:
    monkeypatch.setattr(gs, "WORKLOAD_COMMENT_EDIT_VERIFY_DELAY_SECONDS", 0)

    class FakeHead:
        sha = "abc123"

    class FakeComment:
        id = 1001

        def __init__(self, body: str) -> None:
            self.body = body
            self.edits = []

        def edit(self, body: str) -> None:
            self.edits.append(body)
            self.body = body
            if len(self.edits) == 1:
                self.body = gs.bump_comment_revision(
                    gs.update_workload_check_block(
                        initial_body,
                        "filestore",
                        "completed",
                    )
                )

    class FakePR:
        number = 77
        head = FakeHead()

        def __init__(self, comment: FakeComment) -> None:
            self._comment = comment

        def get_issue_comments(self) -> list[FakeComment]:
            return [self._comment]

        def create_issue_comment(self, body: str) -> None:  # noqa: U100
            raise AssertionError("should not create a new comment")

    initial_body = "\n".join(
        [
            (
                "<!-- status pr=77, run=12, build_preset=linux, "
                "dry_run=False, revision=0 -->"
            ),
            gs.WORKLOAD_CHECKS_START,
            gs.get_workload_check_line("blockstore", "running"),
            gs.get_workload_check_line("filestore", "running"),
            gs.WORKLOAD_CHECKS_END,
        ]
    )
    existing = FakeComment(initial_body)
    pr = FakePR(existing)

    gs.update_pr_comment_workload_check(
        run_number=12,
        pr=pr,
        build_preset="linux",
        component="blockstore",
        is_dry_run=False,
        workload_check_status="completed",
    )

    assert len(existing.edits) == 2
    assert gs.get_workload_check_status(existing.body, "blockstore") == "completed"
    assert gs.get_workload_check_status(existing.body, "filestore") == "completed"
    assert gs.get_comment_revision(existing.body) == 2


def test_update_workload_check_block_does_not_downgrade_completed_status() -> None:
    body = "\n".join(
        [
            gs.WORKLOAD_CHECKS_START,
            gs.get_workload_check_line("blockstore", "completed"),
            gs.WORKLOAD_CHECKS_END,
        ]
    )

    updated = gs.update_workload_check_block(
        body,
        "blockstore",
        "running",
        "https://github.example/job/123",
    )

    assert updated == body


def test_complete_workload_checks_block_preserves_failed_build_rows() -> None:
    body = "\n".join(
        [
            gs.WORKLOAD_CHECKS_START,
            gs.get_workload_check_line(
                "blockstore",
                "failed_build",
                "https://github.example/job/123",
            ),
            gs.get_workload_check_line(
                "filestore",
                "running",
                "https://github.example/job/456",
            ),
            gs.WORKLOAD_CHECKS_END,
        ]
    )

    updated = gs.complete_workload_checks_block(body)

    assert ":red_circle:" in updated
    assert "build failed" in updated
    assert ":white_check_mark:" in updated
    assert "https://github.example/job/123" in updated
    assert "https://github.example/job/456" in updated


def test_update_workload_check_block_adds_failed_build_log_link() -> None:
    body = "\n".join(
        [
            gs.WORKLOAD_CHECKS_START,
            gs.get_workload_check_line(
                "blockstore",
                "running",
                "https://github.example/job/123",
            ),
            gs.WORKLOAD_CHECKS_END,
        ]
    )

    updated = gs.update_workload_check_block(
        body,
        "blockstore",
        "failed_build",
        build_error_log_url="https://logs/build_errors.html",
    )

    assert "build failed" in updated
    assert "[log](https://logs/build_errors.html)" in updated
    assert "file.cpp:1:1: error: broken" not in updated

    completed = gs.complete_workload_checks_block(updated)
    assert "[log](https://logs/build_errors.html)" in completed


def test_update_workload_check_block_preserves_failed_build_log_on_completion() -> None:
    body = "\n".join(
        [
            gs.WORKLOAD_CHECKS_START,
            gs.get_workload_check_line(
                "blockstore",
                "failed_build",
                build_error_log_url="https://logs/build_errors.html",
            ),
            gs.WORKLOAD_CHECKS_END,
        ]
    )

    updated = gs.update_workload_check_block(body, "blockstore", "completed")

    assert "https://logs/build_errors.html" in updated
    assert gs.get_workload_check_status(updated, "blockstore") == "failed_build"
