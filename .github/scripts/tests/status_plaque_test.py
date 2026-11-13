from __future__ import annotations

from types import SimpleNamespace
from xml.etree import ElementTree as ET

import pytest

from scripts.tests import generate_summary as gs
from scripts.tests import finalize_workload_comments as fwc
from scripts.tests.status_plaque import Plaque, Report, WorkflowOutcome, format_duration

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


def publish(
    pr, component="filestore", iteration=0, failed=False, elapsed="1", exit_code=0
):
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
        elapsed,
        False,
        "in_progress",
        component=component,
        test_iteration=iteration,
        test_exit_code=exit_code,
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
    body = gs.update_workload_check_block(
        body, "filestore", "built", build_duration_seconds=65
    )
    body = gs.update_workload_check_block(body, "filestore", "completed")
    row = parsed_table(Plaque.from_comment(body)).find("tbody/tr")
    assert texts(row)[0] == texts(row)[-1] == "🟢"
    assert texts(row)[2] == "🟢 1m 5s"
    assert texts(row)[3] == "Tests not run"


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
    comment.body = gs.update_workload_check_block(
        comment.body, "filestore", "built", build_duration_seconds=125
    )
    publish(pr, iteration=0, failed=True, elapsed="6925")
    publish(pr, iteration=1, failed=True, elapsed="241")
    publish(pr, iteration=2, elapsed="87")
    publish(pr, iteration=2, elapsed="87")  # A retried comment update is idempotent.
    gs.update_pr_comment_workload_check(12, pr, PRESET, "filestore", False, "completed")

    plaque = Plaque.from_comment(comment.body)
    rows = parsed_table(plaque).findall("tbody/tr")
    assert len(rows) == 4
    assert comment.body.count("<table>") == 1
    assert texts(rows[0]) == [
        "🔴",
        "cloud/filestore/",
        "🟢 2m 5s",
        "0",
        "🔴 1h 55m 25s",
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
    assert texts(rows[1])[:4] == ["🔴", "—", "1", "🔴 4m 1s"]
    assert texts(rows[2])[:4] == ["🟢", "—", "2", "🟢 1m 27s"]
    assert texts(rows[2])[-1] == "🟢"
    assert rows[0][8].find("a").get("href") == "https://reports/filestore/0.html#FAIL"
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
    pr.comments[0].body = gs.update_workload_check_block(
        pr.comments[0].body, "filestore", "built", build_duration_seconds=125
    )
    publish(pr, failed=True)
    publish(pr, component="blockstore")
    body = gs.update_workload_check_block(pr.comments[0].body, "filestore", "completed")
    body = gs.complete_workload_checks_block(
        body, lambda _url, _component: "failure"  # noqa: U101
    )
    body = gs.update_workload_check_block(
        body, "filestore", "running", "https://job/new"
    )
    plaque = Plaque.from_comment(body)
    assert plaque.workload("filestore").reports == []
    assert plaque.workload("filestore").build == ""
    assert plaque.workload("filestore").build_duration_seconds is None
    assert plaque.workload("filestore").workflow_status == ""
    assert not plaque.workload("filestore").workflow_failed
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
    assert table.find("tbody/tr/td/span").text == "🟢"
    assert row.workflow_state == (
        "completed"
        if conclusion == "success"
        else gs.workload_check_status_for_job_conclusion(conclusion)
    )
    assert (table.find("tbody/tr/td/sup") is not None) == (
        conclusion in ("failure", "timed_out")
    )


@pytest.mark.parametrize("result", ["passed", "failed", "build_only"])
def test_vm_cleanup_failure_preserves_finished_build_test_result(pr, result):
    if result == "build_only":
        gs.update_pr_comment_workload_check(
            12, pr, PRESET, "filestore", False, "built", build_duration_seconds=125
        )
    else:
        publish(pr, failed=result == "failed")
    gs.update_pr_comment_workload_check(12, pr, PRESET, "filestore", False, "completed")
    marker = fwc.workload_job_marker("relwithdebinfo", "filestore", "native")
    jobs = [
        SimpleNamespace(
            name=f"Build and test / Build and test {marker}", conclusion="success"
        ),
        SimpleNamespace(
            name=f"Build and test / Release self-hosted runner {marker}",
            conclusion="failure",
        ),
    ]

    def jobs_for_run(run_id):
        assert run_id == 123
        return jobs

    def resolve(job_url, component):
        return fwc.resolve_workflow_outcome(
            job_url,
            "relwithdebinfo",
            component,
            "native",
            123,
            jobs_for_run,
        )

    gs.complete_pr_comment_workload_checks(
        12, pr, PRESET, False, workflow_outcome_resolver=resolve
    )
    plaque = Plaque.from_comment(pr.comments[0].body)
    workload = plaque.workload("filestore")
    assert workload.status == "completed"
    assert workload.workflow_status == "failed"
    assert workload.execution_status == "completed"
    assert workload.cleanup_only_failure
    assert Plaque.from_comment(plaque.render()) == plaque
    cells = parsed_table(plaque).find("tbody/tr")
    assert cells[0].find("span").text == ("🔴" if result == "failed" else "🟢")
    warning = cells[0].find("sup")
    assert warning.text == "❗"
    assert warning.get("title") == (
        "Part of workflow failed"
        if result == "failed"
        else "Part of workflow failed but you can merge changes"
    )
    assert cells[-1].find("span").text == "🔴"


def test_build_failure_and_log_survive_completion(pr):
    body = gs.update_workload_check_block(
        pr.comments[0].body,
        "filestore",
        "failed_build",
        build_error_log_url="https://logs/build.html",
        build_duration_seconds=125,
    )
    body = gs.update_workload_check_block(body, "filestore", "completed")
    body = gs.complete_workload_checks_block(body)
    row = Plaque.from_comment(body).workload("filestore")
    assert row.status == "failed_build"
    assert row.build_error_url == "https://logs/build.html"
    assert row.build_duration_seconds == 125
    table = parsed_table(Plaque.from_comment(body))
    assert (
        table.find('.//td[@colspan="10"]/span[@title="Build duration"]').text == "2m 5s"
    )


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
    assert row[4].get("colspan") == "8"
    assert row[4].find("a").get("href") == "https://logs/build.html"
    assert row[4].find('span[@title="Tests duration"]').text == "3s"
    if build_failed_count:
        assert "Build failed" in texts(row)[4]
    else:
        assert "No test results" in texts(row)[4]


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
    for idx, status in enumerate(gs.TestStatus.summary_table_order(), 6):
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
    html = ET.fromstring(f"<root>{plaque.render_table()}</root>")
    assert html.find("p/strong").text == "linux-arm64-release-asan"
    assert len(html.find("table/thead/tr")) == 13


@pytest.mark.parametrize(
    "seconds,expected",
    [
        (0, "0s"),
        (59, "59s"),
        (60, "1m 0s"),
        (3600, "1h 0m 0s"),
        ("3661", "1h 1m 1s"),
        (None, "—"),
        ("", "—"),
        (-1, "—"),
        ("invalid", "—"),
    ],
)
def test_duration_format(seconds, expected):
    assert format_duration(seconds) == expected


@pytest.mark.parametrize("seconds", [None, 0, 3661])
def test_workload_update_records_build_duration(pr, seconds):
    gs.update_pr_comment_workload_check(
        run_number=12,
        pr=pr,
        build_preset=PRESET,
        component="filestore",
        is_dry_run=False,
        workload_check_status="built",
        build_duration_seconds=seconds,
    )

    plaque = Plaque.from_comment(pr.comments[0].body)
    assert plaque.workload("filestore").build_duration_seconds == seconds
    cell = parsed_table(plaque).find('tbody/tr/td/span[@title="Build duration"]')
    if seconds is None:
        assert cell is None
    else:
        assert cell.text == format_duration(seconds)


def test_duration_update_retries_when_only_the_timing_is_lost(pr):
    comment = pr.comments[0]
    initial = gs.update_workload_check_block(comment.body, "filestore", "built")
    comment.body = initial
    original_edit = comment.edit

    def edit(body):
        original_edit(body)
        if len(comment.edits) == 1:
            comment.body = initial

    comment.edit = edit
    gs.update_pr_comment_workload_check(
        12, pr, PRESET, "filestore", False, "built", build_duration_seconds=125
    )
    assert len(comment.edits) == 2
    plaque = Plaque.from_comment(comment.body)
    assert plaque.workload("filestore").build_duration_seconds == 125


def test_existing_plaque_without_build_duration_still_loads(pr):
    body = pr.comments[0].body.replace(',"build_duration_seconds":null', "")
    plaque = Plaque.from_comment(body)
    assert plaque.workload("filestore").build_duration_seconds is None
    assert 'title="Build duration"' not in plaque.render_table()


def test_repeated_build_failure_can_add_missing_duration(pr):
    body = gs.update_workload_check_block(
        pr.comments[0].body,
        "filestore",
        "failed_build",
        build_error_log_url="https://logs/build.html",
    )
    body = gs.update_workload_check_block(
        body, "filestore", "failed_build", build_duration_seconds=125
    )
    workload = Plaque.from_comment(body).workload("filestore")
    assert workload.status == "failed_build"
    assert workload.build_error_url == "https://logs/build.html"
    assert workload.build_duration_seconds == 125


@pytest.mark.parametrize("exit_code", [1, 12, 124, 137])
def test_passing_partial_junit_does_not_hide_execution_failure(pr, exit_code):
    publish(pr, exit_code=exit_code)
    gs.update_pr_comment_workload_check(12, pr, PRESET, "filestore", False, "completed")
    body = gs.complete_workload_checks_block(
        pr.comments[0].body,
        workflow_outcome_resolver=lambda *_: WorkflowOutcome(  # noqa: U101
            "failure", "failure"
        ),
    )
    plaque = Plaque.from_comment(body)
    report = plaque.workload("filestore").reports[0]
    assert report.exit_code == exit_code
    assert report.count("PASS") == 1
    assert report.failed
    cells = parsed_table(plaque).find("tbody/tr")
    assert cells[0].find("span").text == cells[4].find("span").text == "🔴"
    assert f"exit code {exit_code}" in texts(cells)[4]
    assert "you can merge" not in plaque.render_table()


@pytest.mark.parametrize("exit_code", [None, 0])
def test_failed_test_job_never_gets_merge_hint_from_passing_counts(pr, exit_code):
    publish(pr, exit_code=exit_code)
    body = gs.complete_workload_checks_block(
        pr.comments[0].body,
        workflow_outcome_resolver=lambda *_: WorkflowOutcome(  # noqa: U101
            "failure", "failure"
        ),
    )
    plaque = Plaque.from_comment(body)
    assert "you can merge" not in plaque.render_table()
    if exit_code is None:
        assert parsed_table(plaque).find("tbody/tr/td/span").text == "🔴"


def test_unverified_execution_does_not_get_cleanup_merge_hint(pr):
    publish(pr, exit_code=None)
    body = gs.complete_workload_checks_block(
        pr.comments[0].body,
        workflow_outcome_resolver=lambda *_: WorkflowOutcome(  # noqa: U101
            "success", "failure", True
        ),
    )
    assert "you can merge" not in Plaque.from_comment(body).render_table()


@pytest.mark.parametrize("exit_codes,expected_hints", [([0, 124], 0), ([124, 0], 1)])
def test_cleanup_merge_hint_requires_successful_latest_attempt(
    pr, exit_codes, expected_hints
):
    for iteration, exit_code in enumerate(exit_codes):
        publish(pr, iteration=iteration, exit_code=exit_code)
    body = gs.complete_workload_checks_block(
        pr.comments[0].body,
        workflow_outcome_resolver=lambda *_: WorkflowOutcome(  # noqa: U101
            "success", "failure", True
        ),
    )
    assert (
        Plaque.from_comment(body).render_table().count("you can merge")
        == expected_hints
    )


@pytest.mark.parametrize("arrival_order", [(0, 1), (1, 0)])
def test_attempt_build_errors_do_not_override_successful_retry(
    pr, monkeypatch, arrival_order
):
    for iteration in arrival_order:
        monkeypatch.setenv("BUILD_FAILED_COUNT", "2" if iteration == 0 else "0")
        publish(pr, iteration=iteration, exit_code=8 if iteration == 0 else 0)
    plaque = Plaque.from_comment(pr.comments[0].body)
    row = plaque.workload("filestore")
    assert row.build == "completed"
    assert row.reports[0].count("FAIL_BUILD") == 2
    assert row.reports[1].count("FAIL_BUILD") == 0
    cells = parsed_table(plaque).findall("tbody/tr")
    assert cells[0][0].find("span").text == "🔴"
    assert cells[0][2].find("span").text == "🔴"
    assert cells[1][0].find("span").text == cells[1][3].find("span").text == "🟢"
    assert row.build_error_url == ""


def test_existing_shared_attempt_failure_is_cleared_by_successful_retry(
    pr, monkeypatch
):
    monkeypatch.setenv("BUILD_FAILED_COUNT", "1")
    publish(pr, iteration=0, exit_code=8)
    plaque = Plaque.from_comment(pr.comments[0].body)
    row = plaque.workload("filestore")
    row.build = "failed_build"
    row.build_error_url = "https://logs/old-build-error.html"
    pr.comments[0].body = plaque.replace(pr.comments[0].body)
    monkeypatch.setenv("BUILD_FAILED_COUNT", "0")
    publish(pr, iteration=1)
    plaque = Plaque.from_comment(pr.comments[0].body)
    row = plaque.workload("filestore")
    assert row.overall(row.reports[0]) == "failed"
    assert row.overall(row.reports[1]) == "completed"
    assert row.build == "completed"
    assert row.build_error_url == ""


def test_dedicated_build_failure_is_not_cleared_by_a_test_report(pr):
    gs.update_pr_comment_workload_check(
        12, pr, PRESET, "filestore", False, "failed_build"
    )
    publish(pr)
    row = Plaque.from_comment(pr.comments[0].body).workload("filestore")
    assert row.build == "failed_build"
    assert row.overall(row.reports[0]) == "failed"


def test_junit_summary_records_execution_exit_code(
    pr, tmp_path, mk_testcase, write_junit_xml
):
    junit = tmp_path / "junit.xml"
    write_junit_xml(junit, mk_testcase())
    summary = gs.gen_summary_counts([("Tests", str(junit))])
    gs.update_pr_comment(
        run_number=12,
        pr=pr,
        summary=summary,
        build_preset=PRESET,
        test_history_url="",
        test_target="cloud/filestore/",
        test_time="0",
        is_dry_run=False,
        workload_status="completed",
        component="filestore",
        test_exit_code=124,
    )
    row = Plaque.from_comment(pr.comments[0].body).workload("filestore")
    assert row.reports[0].count("PASS") == 1
    assert row.reports[0].exit_code == 124
    assert row.overall(row.reports[0]) == "failed"
