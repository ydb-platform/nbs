import xml.etree.ElementTree as ET

from scripts.tests import generate_summary as gs

def test_from_junit_marks_fail_build_timeout_and_logs_directory(mk_testcase):
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


def test_gen_summary_creates_html_and_aggregates_counters(
    tmp_path, mk_testcase, write_junit_xml
):
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


def test_write_summary_writes_markdown_table_and_footnote(tmp_path):
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


def test_get_comment_text_respects_build_failed_count(monkeypatch):
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


def test_parse_title_html_path_args_rejects_incomplete_triplet():
    try:
        gs.parse_title_html_path_args(["title", "out.html"])
    except ValueError as err:
        assert "Invalid argument count" in str(err)
    else:
        raise AssertionError("ValueError was expected")


def test_status_metadata_defines_orders_and_labels():
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


def test_write_summary_renders_expected_table_row(tmp_path, monkeypatch):
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


def test_update_pr_comment_creates_new_comment(monkeypatch):
    monkeypatch.setenv("BUILD_FAILED_COUNT", "0")

    class FakeHead:
        sha = "deadbeef"

    class FakePR:
        number = 77
        head = FakeHead()

        def __init__(self):
            self.created = []

        def get_issue_comments(self):
            return []

        def create_issue_comment(self, body):
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
    )

    assert len(pr.created) == 1
    body = pr.created[0]
    assert "<!-- status pr=77, run=12, build_preset=linux, dry_run=True -->" in body
    assert "This is a simulation, not a real result" in body
    assert "all tests PASSED for commit deadbeef." in body


def test_update_pr_comment_updates_existing_comment():
    class FakeHead:
        sha = "abc123"

    class FakeComment:
        id = 1001

        def __init__(self, body):
            self.body = body
            self.edits = []

        def edit(self, body):
            self.body = body
            self.edits.append(body)

    class FakePR:
        number = 77
        head = FakeHead()

        def __init__(self, comment):
            self._comment = comment
            self.created = []

        def get_issue_comments(self):
            return [self._comment]

        def create_issue_comment(self, body):
            self.created.append(body)

    header = "<!-- status pr=77, run=12, build_preset=linux, dry_run=False -->"
    existing = FakeComment(header + "\nold text")
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
    )

    assert len(pr.created) == 0
    assert len(existing.edits) == 1
    assert "some tests FAILED for commit abc123." in existing.body
