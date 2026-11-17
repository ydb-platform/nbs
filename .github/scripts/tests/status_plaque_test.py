from __future__ import annotations

import base64
import dataclasses
import json
import zlib
from functools import partial
from types import SimpleNamespace
from xml.etree import ElementTree as ET

import pytest

from scripts.helpers import RunMetadata, format_duration
from scripts.tests import generate_summary as gs
from scripts.tests import finalize_workload_comments as fwc
from scripts.tests.status_plaque import (
    DATA_RE,
    STATE_ENCODING,
    Plaque,
    Report,
    Workload,
    WorkflowOutcome,
    fit_comment,
)

PRESET = "linux-x86_64-relwithdebinfo"
ORIGIN = "https://github-actions-s3.storage.eu-north2.nebius.cloud"
RUN_URL = "https://github.com/ydb-platform/nbs/actions/runs/43840619498"
SUMMARY_BASE = (
    f"{ORIGIN}/ydb-platform/nbs/Check-github-workflow/43840619498/1/nebius-x86-64"
)
TRACE_BASE = f"{ORIGIN}/reports/ydb-platform/nbs/pr-github-actions.yaml/43840619498/1/variants/nebius-x86-64-relwithdebinfo"


@pytest.fixture(autouse=True)
def run_environment(monkeypatch):
    monkeypatch.setenv("GITHUB_REPOSITORY", "ydb-platform/nbs")
    monkeypatch.setenv("GITHUB_RUN_ID", "43840619498")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
    monkeypatch.setenv("GITHUB_WORKFLOW", "Check github workflow")
    monkeypatch.setenv(
        "GITHUB_WORKFLOW_REF",
        "ydb-platform/nbs/.github/workflows/pr-github-actions.yaml@refs/pull/7937/merge",
    )
    monkeypatch.setenv("S3_WEBSITE_PREFIX", SUMMARY_BASE)
    monkeypatch.setenv("S3_REPORTS_WEBSITE_PREFIX", TRACE_BASE)


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
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
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
    line.add_report(
        "ya-test.html",
        f"{SUMMARY_BASE}/summary/{component}/{iteration + 1}/ya-test.html",
    )
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


def decoded_state(body):
    payload = DATA_RE.search(body).group(1)
    assert payload.startswith(STATE_ENCODING)
    compressed = base64.b64decode(payload[len(STATE_ENCODING) :], validate=True)
    return json.loads(zlib.decompress(compressed))


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
    plaque.workload("disk_manager").update(status, 1, True)
    table = parsed_table(plaque)
    row = table.find("tbody/tr")
    merged = next(cell for cell in row if cell.get("colspan"))
    assert message in "".join(merged.itertext())
    assert row.find("td/a").get("href") == f"{RUN_URL}/job/1"
    if status == "failed_build":
        assert (
            merged.find("a").get("href")
            == f"{SUMMARY_BASE}/build_logs/disk_manager/build_errors.html"
        )


@pytest.mark.parametrize(
    "field", ["status", "build", "workflow_status", "execution_status"]
)
@pytest.mark.parametrize("with_report", [False, True])
def test_unknown_status_is_visible_and_never_allows_merge(field, with_report):
    plaque = Plaque(PRESET)
    row = plaque.workload("filestore")
    row.status = row.build = row.execution_status = "completed"
    row.workflow_status = "failed"
    row.cleanup_only_failure = True
    if with_report:
        row.add_report(
            Report(
                0, "Tests", "cloud/filestore/", "abc123", 1, {"PASS": 1}, exit_code=0
            )
        )
    setattr(row, field, "future_status")

    html = plaque.render_table()

    table = ET.fromstring(f"<root>{html}</root>").find("table")
    cells = table.find("tbody/tr")
    assert "⚠️" in "".join(cells[0].itertext())
    assert "Unknown status" in html
    assert "you can merge" not in html


def test_unknown_status_does_not_satisfy_completed_update():
    plaque = Plaque(PRESET, [Workload("filestore", status="future_status")])
    assert not gs.is_workload_check_update_applied(
        plaque.render(), "filestore", "completed"
    )


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
        comment.body, "filestore", "running", f"{RUN_URL}/job/1"
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
        "filestore",
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
    assert (
        rows[0][8].find("a").get("href")
        == f"{SUMMARY_BASE}/summary/filestore/1/ya-test.html#FAIL"
    )
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
        body, "filestore", "running", f"{RUN_URL}/job/3"
    )
    plaque = Plaque.from_comment(body)
    assert plaque.workload("filestore").reports == []
    assert plaque.workload("filestore").build == ""
    assert plaque.workload("filestore").build_duration_seconds is None
    assert plaque.workload("filestore").workflow_status == ""
    assert not plaque.workload("filestore").workflow_failed
    assert len(plaque.workload("blockstore").reports) == 1
    assert gs.is_workload_check_update_applied(
        body, "filestore", "running", f"{RUN_URL}/job/3", require_exact_match=True
    )


@pytest.mark.parametrize("run_attempt", [1, 2])
def test_duplicate_start_and_initialization_preserve_results(
    pr, monkeypatch, run_attempt
):
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", str(run_attempt))
    comment = pr.comments[0]
    comment.body = gs.update_workload_check_block(
        comment.body, "filestore", "running", f"{RUN_URL}/job/1"
    )
    publish(pr)
    comment.body = gs.update_workload_check_block(
        comment.body, "filestore", "running", f"{RUN_URL}/job/1"
    )
    gs.initialize_pr_comment(
        12, pr, PRESET, False, "in_progress", ["filestore", "tasks_storage"]
    )
    plaque = Plaque.from_comment(comment.body)
    assert len(plaque.workload("filestore").reports) == 1
    assert plaque.workload("filestore").job_id == 1
    assert plaque.workload("filestore").run_attempt == run_attempt
    assert [row.component for row in plaque.workloads] == [
        "filestore",
        "blockstore",
        "tasks_storage",
    ]


@pytest.mark.parametrize("execution", ["failure", "skipped"])
def test_rerun_failure_before_worker_start_cannot_reuse_passing_results(
    pr, monkeypatch, execution
):
    for component in ["filestore", "blockstore"]:
        gs.update_pr_comment_workload_check(
            12,
            pr,
            PRESET,
            component,
            False,
            "built",
            job_url=f"{RUN_URL}/job/{1 if component == 'filestore' else 2}",
            build_duration_seconds=125,
        )
        publish(pr, component=component)
        gs.update_pr_comment_workload_check(
            12, pr, PRESET, component, False, "completed"
        )
    pr.comments[0].body = gs.complete_workload_checks_block(
        pr.comments[0].body,
        workflow_outcome_resolver=lambda *_: WorkflowOutcome(  # noqa: U101
            "success", "success"
        ),
    )

    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
    gs.initialize_pr_comment(
        12, pr, PRESET, False, "in_progress", ["filestore", "blockstore"]
    )
    plaque = Plaque.from_comment(pr.comments[0].body)
    for row in plaque.workloads:
        assert row.run_attempt == 2
        assert row.status == "pending"
        assert row.reports == []
        assert row.build == row.workflow_status == row.execution_status == ""
        assert row.build_duration_seconds is None
        assert row.job_id is None

    def resolve(job_url, component):
        assert job_url == ""
        assert component in ("filestore", "blockstore")
        return WorkflowOutcome(execution, "failure")

    # Provisioning or checkout fails before the worker can post "running".
    body = gs.complete_workload_checks_block(
        pr.comments[0].body, workflow_outcome_resolver=resolve
    )
    plaque = Plaque.from_comment(body)
    assert "/job/" not in body
    for row in parsed_table(plaque).findall("tbody/tr"):
        assert row[0].find("span").text == "🔴"
        assert row[1].find("a") is None


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
        build_error_log_url=f"{SUMMARY_BASE}/build_logs/filestore/build_errors.html",
        build_duration_seconds=125,
    )
    body = gs.update_workload_check_block(body, "filestore", "completed")
    body = gs.complete_workload_checks_block(body)
    row = Plaque.from_comment(body).workload("filestore")
    assert row.status == "failed_build"
    assert row.build_errors
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
        gs.TestSummary(f"{SUMMARY_BASE}/summary/filestore/1/build_errors.html"),
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
    assert (
        row[4].find("a").get("href")
        == f"{SUMMARY_BASE}/summary/filestore/1/build_errors.html"
    )
    assert row[4].find('span[@title="Tests duration"]').text == "3s"
    if build_failed_count:
        assert "Build failed" in texts(row)[4]
    else:
        assert "No test results" in texts(row)[4]


def test_counts_link_to_each_category_and_aggregate_build_log():
    report = Report(
        0,
        "Tests",
        "cloud/filestore/",
        "abc123",
        6,
        {
            status: 1
            for status in ("PASS", "ERROR", "FAIL", "FAIL_BUILD", "SKIP", "MUTE")
        },
        filename="ya-test.html",
    )
    plaque = Plaque(PRESET, [Workload("filestore", reports=[report])])
    cells = parsed_table(plaque).find("tbody/tr")
    for status, cell in zip(
        ("PASS", "ERROR", "FAIL", "FAIL_BUILD", "SKIP", "MUTE"), cells[6:12]
    ):
        assert (
            cell.find("a").get("href")
            == f"{SUMMARY_BASE}/summary/filestore/1/ya-test.html#{status}"
        )
    report.counts["FAIL_BUILD"] = 0
    report.build_failed_count = 2
    report.build_errors = True
    cells = parsed_table(plaque).find("tbody/tr")
    assert texts(cells)[9] == "2"
    assert (
        cells[9].find("a").get("href")
        == f"{SUMMARY_BASE}/summary/filestore/1/build_errors.html"
    )


def test_state_stores_shared_metadata_and_preserves_retries():
    plaque = Plaque(PRESET)
    for job_id, component in enumerate(
        ("filestore", "tasks_storage_fastshard"), 133209463414
    ):
        row = plaque.workload(component, run_attempt=2)
        row.update("built", job_id, False, build_duration_seconds=125)
        for iteration in range(3):
            row.add_report(
                Report(
                    iteration,
                    "Tests",
                    f"cloud/{component}/",
                    "abc123",
                    3,
                    {"FAIL": 3} if iteration == 0 else {"PASS": 3},
                    filename="ya-test.html",
                    elapsed="75",
                    exit_code=1 if iteration == 0 else 0,
                    trace=True,
                    otlp=True,
                )
            )
    body = plaque.render()
    data = decoded_state(body)
    assert data == dataclasses.asdict(plaque)
    assert data["run"]["run_id"] == 43840619498
    assert data["workloads"][0]["job_id"] == 133209463414
    assert "https://" not in json.dumps(data["workloads"])
    assert Plaque.from_comment(body) == plaque
    assert Plaque.from_comment(body).render_table() == plaque.render_table()
    assert len(DATA_RE.search(body).group(1)) < len(json.dumps(data)) // 2


@pytest.mark.parametrize(
    "component,component_path",
    [("tasks_storage_fastshard", "tasks_storage_fastshard/"), ("all", "")],
)
def test_links_use_run_metadata_and_one_based_artifact_attempts(
    component, component_path
):
    plaque = Plaque(PRESET)
    row = plaque.workload(component)
    row.update("failed_build", 133209463414, True)
    report = Report(
        0,
        "Tests",
        "",
        "abc123",
        3,
        {"PASS": 3},
        filename="ya-test.html",
        trace=True,
        otlp=True,
    )
    row.add_report(report)
    html = plaque.render_table()
    assert f"{RUN_URL}/job/133209463414" in html
    assert f"{SUMMARY_BASE}/summary/{component_path}1/ya-test.html#PASS" in html
    assert f"{TRACE_BASE}/{component_path}1/trace.html" in html
    assert f"{TRACE_BASE}/{component_path}1/trace.otlp.jsonl.gz" in html
    assert (
        plaque.run.build_error_url(row.component, row.run_attempt)
        == f"{SUMMARY_BASE}/build_logs/{component_path}build_errors.html"
    )
    assert "_url" not in json.dumps(decoded_state(plaque.render())["workloads"])


def test_metadata_survives_other_environments_and_mixed_run_attempts(monkeypatch):
    plaque = Plaque(PRESET)
    for component, attempt in (("filestore", 2), ("blockstore", 1)):
        row = plaque.workload(component, attempt)
        row.add_report(
            Report(
                1,
                "Tests",
                "",
                "abc123",
                1,
                {"PASS": 1},
                filename="ya-test.html",
                trace=True,
            )
        )
    body = plaque.render()
    monkeypatch.setenv("GITHUB_RUN_ID", "999")
    monkeypatch.setenv("GITHUB_REPOSITORY", "other/repo")
    monkeypatch.delenv("S3_WEBSITE_PREFIX")
    monkeypatch.delenv("S3_REPORTS_WEBSITE_PREFIX")
    restored = Plaque.from_comment(body)
    assert restored == plaque
    html = restored.render_table()
    assert "/43840619498/2/nebius-x86-64/summary/filestore/2/ya-test.html" in html
    assert "/43840619498/1/nebius-x86-64/summary/blockstore/2/ya-test.html" in html
    assert (
        "/43840619498/2/variants/nebius-x86-64-relwithdebinfo/filestore/2/trace.html"
        in html
    )


@pytest.mark.parametrize(
    "counts",
    [
        {},
        {"PASS": 0},
        {"FAIL": 2},
        {"PASS": 3, "ERROR": 0, "FAIL": 0, "FAIL_BUILD": 0, "SKIP": 0, "MUTE": 0},
        {"PASS": 3, "FUTURE_STATUS": 1},
    ],
)
def test_compressed_state_preserves_count_categories(counts):
    plaque = Plaque(PRESET)
    plaque.workload("filestore").add_report(
        Report(0, "Tests", "cloud/filestore/", "abc123", 3, counts)
    )
    assert Plaque.from_comment(plaque.render()) == plaque


@pytest.mark.parametrize(
    "summary_variant,trace_variant",
    [
        ("nebius-arm64", "nebius-arm64-relwithdebinfo"),
        ("nebius-x86-64-asan", "nebius-x86-64-release-asan"),
    ],
)
def test_artifact_links_use_configured_variants(
    monkeypatch, summary_variant, trace_variant
):
    monkeypatch.setenv(
        "S3_WEBSITE_PREFIX", SUMMARY_BASE.rsplit("/", 1)[0] + "/" + summary_variant
    )
    monkeypatch.setenv(
        "S3_REPORTS_WEBSITE_PREFIX", TRACE_BASE.rsplit("/", 1)[0] + "/" + trace_variant
    )
    plaque = Plaque(PRESET)
    row = plaque.workload("filestore")
    report = Report(0, "Tests", "", "abc123", 1, {"PASS": 1}, filename="ya-test.html")
    assert (
        f"/{summary_variant}/summary/filestore/1/ya-test.html"
        in plaque.run.report_url(
            row.component, row.run_attempt, report.iteration, report.filename
        )
    )
    assert f"/variants/{trace_variant}/filestore/1/trace.html" in plaque.run.report_url(
        row.component, row.run_attempt, report.iteration, "trace.html", trace=True
    )


def test_trace_link_uses_summary_directory_without_separate_trace_storage(monkeypatch):
    monkeypatch.delenv("S3_REPORTS_WEBSITE_PREFIX")
    plaque = Plaque(PRESET)
    row = plaque.workload("filestore")
    report = Report(0, "Tests", "", "abc123", 1, {"PASS": 1})
    assert (
        plaque.run.report_url(
            row.component, row.run_attempt, report.iteration, "trace.html", trace=True
        )
        == f"{SUMMARY_BASE}/summary/filestore/1/trace.html"
    )


def test_worker_adds_artifact_metadata_after_initialization(pr, monkeypatch):
    plaque = Plaque.from_comment(pr.comments[0].body)
    plaque.run = RunMetadata(repository="ydb-platform/nbs", run_id=43840619498)
    pr.comments[0].body = plaque.replace(pr.comments[0].body)
    publish(pr)
    assert (
        f"{SUMMARY_BASE}/summary/filestore/1/ya-test.html#PASS" in pr.comments[0].body
    )
    monkeypatch.delenv("S3_WEBSITE_PREFIX")
    monkeypatch.delenv("S3_REPORTS_WEBSITE_PREFIX")
    gs.update_pr_comment_workload_check(12, pr, PRESET, "filestore", False, "completed")
    assert (
        f"{SUMMARY_BASE}/summary/filestore/1/ya-test.html#PASS" in pr.comments[0].body
    )


def test_job_lookup_fallback_links_to_run(pr):
    body = gs.update_workload_check_block(
        pr.comments[0].body, "filestore", "running", RUN_URL
    )
    plaque = Plaque.from_comment(body)
    assert plaque.workload("filestore").job_id == 0
    assert plaque.run.job_url(plaque.workload("filestore").job_id) == RUN_URL
    assert gs.is_workload_check_update_applied(
        body, "filestore", "running", RUN_URL, require_exact_match=True
    )


def test_legacy_state_is_compacted_on_update(pr):
    publish(pr, failed=True)
    publish(pr, component="blockstore")
    comment = pr.comments[0]
    original = Plaque.from_comment(comment.body)
    legacy_data = dataclasses.asdict(original)
    for workload in legacy_data["workloads"]:
        del workload["run_attempt"]
        for report in workload["reports"]:
            del report["trace"]
            del report["otlp"]
    legacy_state = f"<!-- status-plaque-data {json.dumps(legacy_data)} -->"
    comment.body = comment.body.replace(
        DATA_RE.search(comment.body).group(0), legacy_state, 1
    )
    assert Plaque.from_comment(comment.body) == original

    publish(pr, iteration=1)

    restored = Plaque.from_comment(comment.body)
    assert restored.workload("blockstore") == original.workload("blockstore")
    reports = restored.workload("filestore").reports
    assert reports[0] == original.workload("filestore").reports[0]
    assert reports[0].failed
    assert reports[1].iteration == 1
    assert not reports[1].failed
    assert DATA_RE.search(comment.body).group(1).startswith(STATE_ENCODING)


def test_oversized_comment_shows_only_workflow_link():
    plaque = Plaque(PRESET)
    for component in ("filestore", "blockstore"):
        for iteration in range(6):
            plaque.workload(component).add_report(
                Report(
                    iteration,
                    "Tests",
                    "",
                    "abc123",
                    1,
                    {"FAIL": 1},
                    filename="ya-test.html",
                )
            )
    header = gs.get_comment_header(77, 12, PRESET, False)
    body = f"{header}\n{plaque.render()}"
    assert fit_comment(body, limit=len(body)) == body
    bounded = fit_comment(body, limit=5000)
    assert len(bounded) <= 5000
    assert bounded.startswith(header)
    assert "<table>" not in bounded
    assert "See individual results in the workflow run" in bounded
    assert f'href="{RUN_URL}"' in bounded
    restored = Plaque.from_comment(bounded)
    assert restored.too_large
    assert restored.workloads == []
    assert all(len(row.reports) == 6 for row in plaque.workloads)


def test_comment_size_limit_does_not_trigger_conflict_retries(pr, monkeypatch):
    monkeypatch.setattr(gs, "fit_comment", partial(fit_comment, limit=5000))
    for iteration in range(10):
        publish(pr, iteration=iteration)
    comment = pr.comments[0]
    assert len(comment.edits) <= 10
    assert all(len(body) <= 5000 for body in comment.edits)
    plaque = Plaque.from_comment(comment.body)
    assert plaque.too_large
    assert plaque.workloads == []
    assert "<table>" not in comment.body


def test_html_and_hidden_state_escape_values():
    plaque = Plaque("linux-arm64-release-asan")
    row = plaque.workload("filestore")
    row.job_id = 123
    report = Report(
        0,
        "Tests",
        '<script>alert("x")</script> -->',
        "abc123",
        1,
        {"PASS": 1},
        filename="ya-test.html",
    )
    row.add_report(report)
    assert Plaque.from_comment(plaque.render()) == plaque
    assert "<script>" not in plaque.render()
    cells = parsed_table(plaque).find("tbody/tr")
    assert cells[1].find("a").get("href") == f"{RUN_URL}/job/123"
    assert texts(cells)[1] == report.target
    html = ET.fromstring(f"<root>{plaque.render_table()}</root>")
    assert html.find("p/strong").text == "linux-arm64-release-asan"
    assert len(html.find("table/thead/tr")) == 13


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
    data = decoded_state(pr.comments[0].body)
    del data["workloads"][0]["build_duration_seconds"]
    plaque = Plaque.from_comment(f"<!-- status-plaque-data {json.dumps(data)} -->")
    assert plaque.workload("filestore").build_duration_seconds is None
    assert 'title="Build duration"' not in plaque.render_table()


def test_repeated_build_failure_can_add_missing_duration(pr):
    body = gs.update_workload_check_block(
        pr.comments[0].body,
        "filestore",
        "failed_build",
        build_error_log_url=f"{SUMMARY_BASE}/build_logs/filestore/build_errors.html",
    )
    body = gs.update_workload_check_block(
        body, "filestore", "failed_build", build_duration_seconds=125
    )
    workload = Plaque.from_comment(body).workload("filestore")
    assert workload.status == "failed_build"
    assert workload.build_errors
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
    assert not row.build_errors


def test_existing_shared_attempt_failure_is_cleared_by_successful_retry(
    pr, monkeypatch
):
    monkeypatch.setenv("BUILD_FAILED_COUNT", "1")
    publish(pr, iteration=0, exit_code=8)
    plaque = Plaque.from_comment(pr.comments[0].body)
    row = plaque.workload("filestore")
    row.build = "failed_build"
    row.build_errors = True
    pr.comments[0].body = plaque.replace(pr.comments[0].body)
    monkeypatch.setenv("BUILD_FAILED_COUNT", "0")
    publish(pr, iteration=1)
    plaque = Plaque.from_comment(pr.comments[0].body)
    row = plaque.workload("filestore")
    assert row.overall(row.reports[0]) == "failed"
    assert row.overall(row.reports[1]) == "completed"
    assert row.build == "completed"
    assert not row.build_errors


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
