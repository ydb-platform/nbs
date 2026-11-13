from __future__ import annotations

from types import SimpleNamespace
from xml.etree import ElementTree as ET

import pytest

from scripts.tests import generate_summary as gs
from scripts.tests.status_plaque import Plaque, Report

PRESET = "linux-x86_64-relwithdebinfo"


class Comment:
    id = 1

    def __init__(self, body: str):
        self.body = body
        self.edits = []

    def edit(self, body: str) -> None:
        self.body = body
        self.edits.append(body)


class PullRequest:
    number = 77
    head = SimpleNamespace(sha="abc123")

    def __init__(self):
        self.comments = []

    def get_issue_comments(self) -> list[Comment]:
        return self.comments

    def create_issue_comment(self, body: str) -> None:
        self.comments.append(Comment(body))


@pytest.fixture
def pr(monkeypatch):
    monkeypatch.setattr(gs, "WORKLOAD_COMMENT_EDIT_VERIFY_DELAY_SECONDS", 0)
    monkeypatch.setenv("BUILD_FAILED_COUNT", "0")
    pr = PullRequest()
    gs.initialize_pr_comment(
        12, pr, PRESET, False, "in_progress", ["filestore", "blockstore"]
    )
    return pr


def publish(pr, component="filestore", iteration=0, failed=False):
    summary = gs.TestSummary()
    line = gs.TestSummaryLine("Tests")
    line.add(
        gs.TestResult(
            "suite",
            "test",
            gs.TestStatus.FAIL if failed else gs.TestStatus.PASS,
            {},
            1.0,
            False,
        )
    )
    line.add_report("ya-test.html", f"https://reports/{component}/{iteration}.html")
    summary.add_line(line)
    gs.update_pr_comment(
        12,
        pr,
        summary,
        PRESET,
        "",
        f"cloud/{component}/",
        "1",
        False,
        "in_progress",
        component=component,
        test_iteration=iteration,
    )


def parsed_table(plaque):
    table = ET.fromstring(f"<root>{plaque.render_table()}</root>").find("table")
    # Check the effective width, including cells inherited from earlier rows.
    width = len(table.findall("thead/tr/th"))
    remaining = [0] * width
    for row in table.findall("tbody/tr"):
        occupied = [value > 0 for value in remaining]
        remaining = [max(0, value - 1) for value in remaining]
        column = 0
        for cell in row:
            while column < width and occupied[column]:
                column += 1
            span = int(cell.get("colspan", "1"))
            assert column + span <= width
            assert not any(occupied[column : column + span])
            for idx in range(column, column + span):
                occupied[idx] = True
                remaining[idx] = int(cell.get("rowspan", "1")) - 1
            column += span
        assert all(occupied)
    assert not any(remaining)
    return table


def texts(row):
    return ["".join(cell.itertext()).strip() for cell in row]


@pytest.mark.parametrize(
    "status,message",
    [
        ("pending", "Queued"),
        ("running", "Building / preparing tests"),
        ("built", "Build passed; waiting for test results"),
        ("failed_build", "Build failed"),
        ("failed", "Workflow failed before reporting completion"),
        ("cancelled", "Workflow cancelled"),
        ("timed_out", "Workflow timed out"),
        ("skipped", "Workflow skipped"),
        ("report_failed", "Workflow finished, but report update failed"),
    ],
)
def test_no_results_uses_merged_message(status, message):
    plaque = Plaque(PRESET)
    plaque.workload("disk_manager").update(
        status, "https://job/1", "https://logs/build.html"
    )
    table = parsed_table(plaque)
    row = table.find("tbody/tr")
    merged = next(cell for cell in row if cell.get("colspan"))
    assert message in "".join(merged.itertext())
    assert row.find("td/a").get("href") == "https://job/1"
    if status == "failed_build":
        assert merged.find("a").get("href") == "https://logs/build.html"


def test_build_only_completion(pr):
    body = pr.comments[0].body
    body = gs.update_workload_check_block(body, "filestore", "built")
    body = gs.update_workload_check_block(body, "filestore", "completed")
    row = parsed_table(Plaque.from_comment(body)).find("tbody/tr")
    assert texts(row)[0] == texts(row)[5] == texts(row)[-1] == "🟢"
    assert texts(row)[6] == "Tests not run"


def test_standalone_completed_summary_does_not_remain_running(pr):
    summary = gs.TestSummary()
    line = gs.TestSummaryLine("Tests")
    line.add(gs.TestResult("suite", "test", gs.TestStatus.PASS, {}, 1.0, False))
    summary.add_line(line)
    gs.update_pr_comment(
        12, pr, summary, PRESET, "", "cloud/filestore/", "1", False, "completed"
    )
    plaque = Plaque.from_comment(pr.comments[0].body)
    row = parsed_table(plaque).find("tbody/tr")
    assert texts(row)[0] == texts(row)[-1] == "🟢"


def test_retries_share_target_and_keep_each_report(pr):
    comment = pr.comments[0]
    comment.body = gs.update_workload_check_block(
        comment.body, "filestore", "running", "https://job/1"
    )
    comment.body = gs.update_workload_check_block(comment.body, "filestore", "built")
    publish(pr, iteration=0, failed=True)
    publish(pr, iteration=1, failed=True)
    publish(pr, iteration=2)
    publish(pr, iteration=2)  # A retried comment update is idempotent.
    gs.update_pr_comment_workload_check(12, pr, PRESET, "filestore", False, "completed")

    plaque = Plaque.from_comment(comment.body)
    rows = parsed_table(plaque).findall("tbody/tr")
    assert len(rows) == 4
    assert comment.body.count("<table>") == 1
    assert texts(rows[0]) == [
        "🔴",
        "cloud/filestore/",
        "linux",
        "x86_64",
        "relwithdebinfo",
        "🟢",
        "0",
        "🔴",
        "1",
        "0",
        "0",
        "1",
        "0",
        "0",
        "0",
        "—",
    ]
    assert rows[0][1].get("rowspan") == "3"
    assert texts(rows[1])[:4] == ["🔴", "—", "1", "🔴"]
    assert texts(rows[2])[:4] == ["🟢", "—", "2", "🟢"]
    assert texts(rows[2])[-1] == "🟢"
    assert rows[0][11].find("a").get("href") == "https://reports/filestore/0.html#FAIL"
    assert plaque.workload("filestore").reports[-1].commit == "abc123"


def test_summary_keeps_other_components_during_concurrent_edit(pr):
    comment = pr.comments[0]
    initial = comment.body
    original_edit = comment.edit

    def edit(body):
        original_edit(body)
        if len(comment.edits) == 1:
            comment.body = initial
            publish(pr, component="blockstore")

    comment.edit = edit
    publish(pr)

    assert len(comment.edits) == 3
    plaque = Plaque.from_comment(comment.body)
    assert len(plaque.workload("filestore").reports) == 1
    assert len(plaque.workload("blockstore").reports) == 1
    parsed_table(plaque)


def test_workload_completion_keeps_concurrent_summary(pr):
    comment = pr.comments[0]
    initial = gs.update_workload_check_block(comment.body, "filestore", "running")
    comment.body = initial
    original_edit = comment.edit

    def edit(body):
        original_edit(body)
        if len(comment.edits) == 1:
            comment.body = initial
            publish(pr, component="blockstore")

    comment.edit = edit
    gs.update_pr_comment_workload_check(12, pr, PRESET, "filestore", False, "completed")
    plaque = Plaque.from_comment(comment.body)
    assert plaque.workload("filestore").status == "completed"
    assert len(plaque.workload("blockstore").reports) == 1


def test_rerun_clears_only_its_previous_attempt(pr):
    publish(pr, failed=True)
    publish(pr, component="blockstore")
    body = gs.update_workload_check_block(pr.comments[0].body, "filestore", "completed")
    body = gs.update_workload_check_block(
        body, "filestore", "running", "https://job/new"
    )
    plaque = Plaque.from_comment(body)
    assert plaque.workload("filestore").reports == []
    assert plaque.workload("filestore").build == ""
    assert len(plaque.workload("blockstore").reports) == 1
    assert gs.is_workload_check_update_applied(
        body, "filestore", "running", "https://job/new", require_exact_match=True
    )


def test_duplicate_start_and_initialization_preserve_results(pr):
    comment = pr.comments[0]
    comment.body = gs.update_workload_check_block(
        comment.body, "filestore", "running", "https://job/1"
    )
    publish(pr)
    comment.body = gs.update_workload_check_block(
        comment.body, "filestore", "running", "https://job/1"
    )
    gs.initialize_pr_comment(
        12, pr, PRESET, False, "in_progress", ["filestore", "tasks_storage"]
    )
    plaque = Plaque.from_comment(comment.body)
    assert len(plaque.workload("filestore").reports) == 1
    assert plaque.workload("filestore").job_url == "https://job/1"
    assert [row.component for row in plaque.workloads] == [
        "filestore",
        "blockstore",
        "tasks_storage",
    ]


@pytest.mark.parametrize(
    "conclusion", ["cancelled", "timed_out", "skipped", "failure", "success", None]
)
def test_finalization_keeps_reports_and_build_state(pr, conclusion):
    publish(pr)
    body = gs.complete_workload_checks_block(
        pr.comments[0].body, lambda _url, _component: conclusion  # noqa: U101
    )
    plaque = Plaque.from_comment(body)
    row = plaque.workload("filestore")
    assert row.status == gs.workload_check_status_for_job_conclusion(conclusion)
    assert row.build == "completed"
    assert len(row.reports) == 1
    table = parsed_table(plaque)
    assert texts(table.find("tbody/tr"))[0] != "🟢"


def test_build_failure_and_log_survive_completion(pr):
    body = gs.update_workload_check_block(
        pr.comments[0].body,
        "filestore",
        "failed_build",
        build_error_log_url="https://logs/build.html",
    )
    body = gs.update_workload_check_block(body, "filestore", "completed")
    body = gs.complete_workload_checks_block(body)
    row = Plaque.from_comment(body).workload("filestore")
    assert row.status == "failed_build"
    assert row.build_error_url == "https://logs/build.html"


@pytest.mark.parametrize("build_failed_count", [0, 2])
def test_empty_summary_is_an_error_with_merged_cells(
    pr, monkeypatch, build_failed_count
):
    monkeypatch.setenv("BUILD_FAILED_COUNT", str(build_failed_count))
    gs.update_pr_comment(
        12,
        pr,
        gs.TestSummary("https://logs/build.html"),
        PRESET,
        "",
        "cloud/filestore/",
        "3",
        False,
        "in_progress",
        component="filestore",
    )
    plaque = Plaque.from_comment(pr.comments[0].body)
    row = parsed_table(plaque).find("tbody/tr")
    assert texts(row)[0] == "🔴"
    assert row[7].get("colspan") == "8"
    assert row[7].find("a").get("href") == "https://logs/build.html"
    if build_failed_count:
        assert "Build failed" in texts(row)[7]
    else:
        assert "No test results" in texts(row)[7]


def test_counts_link_to_each_category_and_aggregate_build_log():
    plaque = Plaque(PRESET)
    report = Report(
        0,
        "Tests",
        "cloud/filestore/",
        "abc123",
        6,
        {status: 1 for status in gs.TestStatus.__members__},
        url="https://reports/tests.html",
    )
    row = plaque.workload("filestore")
    row.add_report(report)
    table = parsed_table(plaque)
    cells = table.find("tbody/tr")
    for idx, status in enumerate(gs.TestStatus.summary_table_order(), 9):
        assert (
            cells[idx].find("a").get("href")
            == f"https://reports/tests.html#{status.name}"
        )
    report.counts["FAIL_BUILD"] = 0
    report.build_failed_count = 2
    report.build_error_url = "https://logs/build.html"
    assert report.count("FAIL_BUILD") == 2
    assert report.count_url("FAIL_BUILD") == "https://logs/build.html"


def test_html_and_hidden_state_escape_values():
    plaque = Plaque("linux-arm64-release-asan")
    row = plaque.workload("filestore")
    row.job_url = 'https://job/?a=1&b="quoted"'
    report = Report(
        0,
        "Tests",
        '<script>alert("x")</script> -->',
        "abc123",
        1,
        {"PASS": 1},
        url="https://reports/?a=1&b=2",
    )
    row.add_report(report)
    assert Plaque.from_comment(plaque.render()) == plaque
    assert "<script>" not in plaque.render()
    cells = parsed_table(plaque).find("tbody/tr")
    assert cells[1].find("a").get("href") == row.job_url
    assert texts(cells)[1] == report.target
    assert texts(cells)[2:5] == ["linux", "arm64", "release-asan"]
