#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError
from github import Auth as GithubAuth, Github
from github.PullRequest import PullRequest
from github.Repository import Repository
from github.WorkflowRun import WorkflowRun as GithubWorkflowRun

from .helpers import (
    find_current_job_url,
    format_github_response_debug,
    github_api_headers,
    get_build_preset_from_workflow_name,
    get_s3_report_uri,
    get_s3_report_url,
    get_s3_workflow_reports_path,
    parse_s3_path,
    setup_logger,
)
from .tests import generate_summary as gs

LABEL_TO_WORKFLOWS = {
    "nightly-tests": ["nightly.yaml"],
    "nightly-asan": ["nightly-asan.yaml"],
    "nightly-tsan": ["nightly-tsan.yaml"],
    "nightly-msan": ["nightly-msan.yaml"],
    "nightly-ubsan": ["nightly-ubsan.yaml"],
    "nightly-sanitizers": [
        "nightly-asan.yaml",
        "nightly-tsan.yaml",
        "nightly-msan.yaml",
        "nightly-ubsan.yaml",
    ],
    "nightly-all": [
        "nightly.yaml",
        "nightly-asan.yaml",
        "nightly-tsan.yaml",
        "nightly-msan.yaml",
        "nightly-ubsan.yaml",
    ],
}

COMMENT_MARKER_PREFIX = "<!-- nbs-nightly-pr-check"
CANCEL_VERIFY_INTERVAL_SECONDS = 5
CANCEL_VERIFY_TIMEOUT_SECONDS = 300
INITIAL_DISCOVERY_INTERVAL_SECONDS = 5
INITIAL_DISCOVERY_TIMEOUT_SECONDS = 60
logger = setup_logger()

S3_REPORT_CONTEXT_ENV_KEYS = (
    "S3_BUCKET",
    "S3_BUCKET_PATH",
    "S3_REPORTS_BUCKET_PATH",
    "S3_WEBSITE_SUFFIX",
)


@dataclass
class WorkflowRun:
    workflow: str
    label: str
    run_id: int | None = None
    url: str = ""
    status: str = "dispatching"
    conclusion: str | None = None
    summaries: list[dict[str, str]] = field(default_factory=list)
    error: str = ""


@dataclass(frozen=True)
class CancelRequestResult:
    accepted: bool
    status_code: int
    debug: str


class CancellationRequested(Exception):
    pass


def request_cancellation(signum: int, _frame: Any) -> None:  # noqa: U101
    raise CancellationRequested(f"received signal {signum}")


def load_event() -> dict[str, Any]:
    with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as fp:
        return json.load(fp)


def get_github_context(
    token: str, repository: str, event: dict[str, Any]
) -> tuple[Repository, PullRequest]:
    gh = Github(auth=GithubAuth.Token(token))
    repo = gh.get_repo(repository)
    pr = gh.create_from_raw_data(PullRequest, event["pull_request"])
    return repo, pr


def selected_workflows(labels: set[str]) -> list[WorkflowRun]:
    workflows: dict[str, str] = {}
    for label, label_workflows in LABEL_TO_WORKFLOWS.items():
        if label not in labels:
            continue
        for workflow in label_workflows:
            workflows[workflow] = label

    return [
        WorkflowRun(workflow=workflow, label=label)
        for workflow, label in sorted(workflows.items())
    ]


def dispatch_workflow(
    repo: Repository,
    workflow: str,
    ref: str,
    marker: str,
) -> None:
    logger.info("Dispatching %s on ref %s with marker %s", workflow, ref, marker)
    repo.get_workflow(workflow).create_dispatch(
        ref=ref,
        inputs={"comment": marker},
        throw=True,
    )


def find_workflow_run(
    repo: Repository,
    workflow: str,
    branch: str,
    marker: str,
    dispatched_at: datetime | None = None,
) -> GithubWorkflowRun | None:
    candidates = []
    workflow_runs = repo.get_workflow(workflow).get_runs(
        branch=branch,
        event="workflow_dispatch",
    )
    for index, run in enumerate(workflow_runs):
        if index >= 50:
            break
        title = run.display_title or run.name or ""
        if marker not in title:
            continue
        created_at = run.created_at
        if created_at is not None and created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        if dispatched_at is not None and created_at and created_at < dispatched_at:
            continue
        candidates.append(run)

    if not candidates:
        logger.info(
            "No run found yet for workflow=%s branch=%s marker=%s",
            workflow,
            branch,
            marker,
        )
        return None

    result = sorted(candidates, key=lambda run: run.run_number, reverse=True)[0]
    logger.info(
        "Found run workflow=%s id=%s status=%s conclusion=%s url=%s",
        workflow,
        result.id,
        result.status,
        result.conclusion,
        result.html_url,
    )
    return result


def get_summary_label(workflow: str, summary_json_uri: str = "") -> str:
    build_preset = get_build_preset_from_workflow_name(workflow)
    if summary_json_uri:
        component = get_summary_component(summary_json_uri)
        if component:
            return f"{build_preset or workflow}/{component}"
    if build_preset is not None:
        return build_preset
    return workflow


def link_count(value: int, url: str, anchor: str | None = None) -> str:
    if value == 0:
        return "0"
    href = f"{url}#{anchor}" if anchor else url
    return f"[{value}]({href})"


def get_run_attempt(workflow_run: GithubWorkflowRun) -> int:
    return getattr(workflow_run, "run_attempt", None) or 1


def get_reports_bucket() -> str:
    bucket = os.environ.get("S3_BUCKET", "").strip()
    if bucket:
        return bucket

    for env_name in ("S3_REPORTS_BUCKET_PATH", "S3_BUCKET_PATH"):
        env_value = os.environ.get(env_name, "").strip()
        if not env_value:
            continue
        try:
            bucket, _key = parse_s3_path(env_value)
        except ValueError as error:
            logger.warning(
                "Cannot derive S3 bucket from %s=%r: %s", env_name, env_value, error
            )
            continue
        logger.info("Derived S3 bucket %s from %s", bucket, env_name)
        return bucket

    logger.warning(
        "Cannot derive S3 bucket: none of %s are set to a usable value",
        ", ".join(S3_REPORT_CONTEXT_ENV_KEYS),
    )
    return ""


def log_s3_report_context() -> None:
    logger.info(
        "S3 report context: %s",
        {
            env_name: os.environ.get(env_name, "")
            for env_name in S3_REPORT_CONTEXT_ENV_KEYS
        },
    )


def fetch_summary_payload(s3: Any, summary_json_uri: str) -> dict[str, Any] | None:
    logger.info("Fetching summary json %s", summary_json_uri)
    try:
        bucket, key = parse_s3_path(summary_json_uri)
        response = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except (
        BotoCoreError,
        ClientError,
        json.JSONDecodeError,
        KeyError,
        UnicodeDecodeError,
        ValueError,
    ) as error:
        logger.info("Failed to fetch summary json %s: %s", summary_json_uri, error)
        return None


def list_summary_json_uris(
    s3: Any,
    repo: Repository,
    workflow_file: str,
    workflow_run: GithubWorkflowRun,
) -> list[str]:
    prefix = get_s3_workflow_reports_path(
        repository=repo.full_name,
        workflow_file=workflow_file,
        run_id=workflow_run.id,
        run_attempt=get_run_attempt(workflow_run),
    )
    reports_bucket = get_reports_bucket()
    prefix_uri = get_s3_report_uri(prefix, bucket=reports_bucket)
    if not prefix_uri:
        logger.warning(
            "Cannot list summary reports: empty S3 URI for workflow=%s run_id=%s attempt=%s bucket=%r prefix=%r",
            workflow_file,
            workflow_run.id,
            get_run_attempt(workflow_run),
            reports_bucket,
            prefix,
        )
        return []

    try:
        bucket, key_prefix = parse_s3_path(prefix_uri)
        logger.info(
            "Listing summary reports for workflow=%s run_id=%s attempt=%s under bucket=%s prefix=%s uri=%s",
            workflow_file,
            workflow_run.id,
            get_run_attempt(workflow_run),
            bucket,
            key_prefix,
            prefix_uri,
        )
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        object_count = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
            contents = page.get("Contents", [])
            object_count += len(contents)
            keys.extend(
                content["Key"]
                for content in contents
                if content["Key"].endswith("/summary.json")
            )
    except (BotoCoreError, ClientError, KeyError, ValueError) as error:
        logger.warning("Failed to list summary reports under %s: %s", prefix_uri, error)
        return []

    def sort_key(key: str) -> tuple[int, str]:
        try:
            retry = int(key.rsplit("/", 2)[-2])
        except ValueError:
            retry = 0
        return retry, key

    logger.info(
        "Found %d objects and %d summary reports under %s: %s",
        object_count,
        len(keys),
        prefix_uri,
        keys[:20],
    )
    return [
        get_s3_report_uri(key, bucket=bucket)
        for key in sorted(keys, key=sort_key, reverse=True)
    ]


def get_report_url_from_summary_uri(summary_json_uri: str) -> str:
    try:
        bucket, key = parse_s3_path(summary_json_uri)
    except ValueError:
        return ""
    if not key.endswith("/summary.json"):
        return ""
    return get_s3_report_url(
        f"{key.removesuffix('/summary.json')}/ya-test.html",
        bucket=bucket,
    )


def get_summary_component(summary_json_uri: str) -> str:
    try:
        _bucket, key = parse_s3_path(summary_json_uri)
    except ValueError:
        return ""

    parts = key.split("/")
    if len(parts) < 8 or parts[0] != "reports" or parts[-1] != "summary.json":
        return ""

    relative_parts = parts[6:-1]
    if len(relative_parts) <= 1:
        return ""
    return "/".join(relative_parts[:-1])


def render_summary_markdown(
    payload: dict[str, Any] | None,
    summary_json_url: str,
    fallback_report_url: str = "",
) -> str:
    if payload is None:
        return ""

    logger.info(
        "Using summary json %s schema=%s version=%s",
        summary_json_url,
        payload.get("schema") if isinstance(payload, dict) else None,
        payload.get("schema_version") if isinstance(payload, dict) else None,
    )
    reports = payload.get("reports") if isinstance(payload, dict) else None
    if not reports:
        logger.info("No summary reports found in %s", summary_json_url)
        return ""

    report = reports[0]
    if not isinstance(report, dict):
        logger.info("Invalid summary report in %s", summary_json_url)
        return ""
    report_url = (
        report.get("report_url")
        or get_report_url_from_summary_uri(summary_json_url)
        or fallback_report_url
    )
    counts = report.get("counts") or {}
    total = int(report.get("total") or sum(int(value) for value in counts.values()))
    statuses = gs.TestStatus.summary_table_order()
    headers = ["TESTS"] + [status.summary_header for status in statuses]
    separators = ["---:"] * len(headers)
    values = [link_count(total, report_url)]
    values.extend(
        link_count(int(counts.get(status.name, 0)), report_url, status.report_anchor)
        for status in statuses
    )

    return "\n".join(
        [
            f"| {' | '.join(headers)} |",
            f"| {' | '.join(separators)} |",
            f"| {' | '.join(values)} |",
        ]
    )


def build_summary_entry(
    workflow_file: str,
    summary_json_uri: str,
    summary_markdown: str,
) -> dict[str, str]:
    return {
        "label": get_summary_label(workflow_file, summary_json_uri),
        "summary_markdown": summary_markdown,
    }


def fetch_run_summaries(
    s3: Any,
    repo: Repository,
    workflow_file: str,
    run: GithubWorkflowRun,
) -> list[dict[str, str]]:
    summaries = []
    for summary_json_uri in list_summary_json_uris(s3, repo, workflow_file, run):
        summary_markdown = render_summary_markdown(
            fetch_summary_payload(s3, summary_json_uri),
            summary_json_uri,
        )
        if summary_markdown:
            summaries.append(
                build_summary_entry(workflow_file, summary_json_uri, summary_markdown)
            )

    if summaries:
        return summaries

    logger.warning(
        "No summary reports found for workflow=%s run_id=%s attempt=%s. Expected at least one summary.json under %s",
        workflow_file,
        run.id,
        get_run_attempt(run),
        get_s3_report_uri(
            get_s3_workflow_reports_path(
                repository=repo.full_name,
                workflow_file=workflow_file,
                run_id=run.id,
                run_attempt=get_run_attempt(run),
            ),
            bucket=get_reports_bucket(),
        ),
    )
    return []


def cancel_workflow_run(repo: Repository, run_id: int) -> CancelRequestResult:
    url = f"https://api.github.com/repos/{repo.full_name}/actions/runs/{run_id}/cancel"
    logger.info("Requesting cancellation for run_id=%s via %s", run_id, url)
    response = requests.post(
        url,
        headers=github_api_headers(os.environ.get("GITHUB_TOKEN")),
        timeout=30,
    )
    debug = format_github_response_debug(response)
    accepted = response.status_code == 202
    if accepted:
        logger.info("Cancellation request for run_id=%s accepted: %s", run_id, debug)
    else:
        logger.warning(
            "Cancellation request for run_id=%s was not accepted: %s",
            run_id,
            debug,
        )
    return CancelRequestResult(
        accepted=accepted,
        status_code=response.status_code,
        debug=debug,
    )


def status_icon(run: WorkflowRun) -> str:
    if run.error:
        return ":red_circle:"
    if run.status != "completed":
        return ":yellow_circle:"
    if run.conclusion == "success":
        return ":green_circle:"
    return ":red_circle:"


def status_text(run: WorkflowRun) -> str:
    if run.error:
        return run.error
    if run.conclusion:
        return run.conclusion
    return run.status


def render_comment(
    marker: str,
    runs: list[WorkflowRun],
    final: bool,
    collector_url: str = "",
) -> str:
    comment_marker = get_comment_marker(marker)
    workflow_status = "finished" if final else "running"
    lines = [
        comment_marker,
        "> [!TIP]",
        f"> Nightly workflows are **{workflow_status}**.",
        "",
    ]
    if collector_url:
        lines.extend([f"Collector: [nightly-builds job]({collector_url})", ""])
    for run in runs:
        workflow = f"`{run.workflow}`"
        if run.url:
            workflow = f"[`{run.workflow}`]({run.url})"
        lines.append(f"- {status_icon(run)} {workflow} - `{status_text(run)}`")

    summaries = [
        summary
        for run in runs
        for summary in run.summaries
        if summary.get("summary_markdown")
    ]
    if summaries:
        lines.extend(["", "Summaries:"])
        for summary in summaries[:10]:
            lines.append(f"`{summary.get('label', 'summary')}`")
            lines.extend(summary["summary_markdown"].splitlines())
        if len(summaries) > 10:
            lines.append(f"- ... and {len(summaries) - 10} more summaries")

    return "\n".join(lines)


def get_comment_marker(marker: str) -> str:
    return f'{COMMENT_MARKER_PREFIX} marker="{marker}" -->'


def upsert_comment(pr: PullRequest, marker: str, body: str) -> None:
    comment_marker = get_comment_marker(marker)
    comment = gs.find_pr_comment(pr, comment_marker)
    if comment is None:
        logger.info("Creating nightly PR comment for PR #%s", pr.number)
        pr.create_issue_comment(body)
        return

    def replace_comment_body(current_body: str) -> str:
        del current_body
        return body

    logger.info("Updating nightly PR comment id=%s for PR #%s", comment.id, pr.number)
    gs.edit_pr_comment(
        pr=pr,
        header_prefix=comment_marker,
        update_body=replace_comment_body,
        is_applied=lambda current_body: current_body == body
        or current_body == gs.bump_comment_revision(body),
        operation=f"nightly PR comment {marker}",
    )


def refresh_run(s3: Any, repo: Repository, run: WorkflowRun) -> None:
    if run.run_id is None:
        return

    workflow_run = repo.get_workflow_run(run.run_id)
    run.status = workflow_run.status or run.status
    run.conclusion = workflow_run.conclusion
    run.url = workflow_run.html_url or run.url
    if run.status == "completed":
        run.summaries = fetch_run_summaries(s3, repo, run.workflow, workflow_run)
    logger.info(
        "Refreshed %s: id=%s status=%s conclusion=%s",
        run.workflow,
        run.run_id,
        run.status,
        run.conclusion,
    )


def refresh_runs(s3: Any, repo: Repository, runs: list[WorkflowRun]) -> None:
    for run in runs:
        refresh_run(s3, repo, run)


def wait_for_cancellation(
    s3: Any, repo: Repository, run: WorkflowRun, timeout_seconds: int
) -> bool:
    if run.run_id is None:
        return False

    deadline = time.monotonic() + timeout_seconds
    while True:
        refresh_run(s3, repo, run)
        if run.status == "completed" or run.conclusion == "cancelled":
            logger.info(
                "Run %s id=%s stopped after cancellation request: conclusion=%s",
                run.workflow,
                run.run_id,
                run.conclusion,
            )
            return True

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            logger.warning(
                "Run %s id=%s is still %s after %ss cancellation wait",
                run.workflow,
                run.run_id,
                run.status,
                timeout_seconds,
            )
            return False

        logger.info(
            "Waiting for run %s id=%s to stop after cancellation request: status=%s",
            run.workflow,
            run.run_id,
            run.status,
        )
        time.sleep(min(CANCEL_VERIFY_INTERVAL_SECONDS, remaining))


def all_done(runs: list[WorkflowRun]) -> bool:
    return all(run.error or run.status == "completed" for run in runs)


def any_failed(runs: list[WorkflowRun]) -> bool:
    for run in runs:
        if run.error:
            return True
        if run.status != "completed":
            return True
        if run.conclusion != "success":
            return True
    return False


def discover_missing_runs(
    repo: Repository,
    runs: list[WorkflowRun],
    head_ref: str,
    marker: str,
    dispatched_at: datetime | None,
) -> None:
    for run in runs:
        if run.error or run.run_id is not None:
            continue
        workflow_run = find_workflow_run(
            repo,
            run.workflow,
            head_ref,
            marker,
            dispatched_at,
        )
        if workflow_run is None:
            continue
        run.run_id = int(workflow_run.id)
        run.url = workflow_run.html_url or ""


def all_discovered(runs: list[WorkflowRun]) -> bool:
    return all(run.error or run.run_id is not None for run in runs)


def wait_for_initial_discovery(
    repo: Repository,
    runs: list[WorkflowRun],
    head_ref: str,
    marker: str,
    dispatched_at: datetime,
) -> None:
    deadline = time.monotonic() + INITIAL_DISCOVERY_TIMEOUT_SECONDS
    while True:
        discover_missing_runs(repo, runs, head_ref, marker, dispatched_at)
        if all_discovered(runs):
            return

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            missing = [
                run.workflow for run in runs if run.run_id is None and not run.error
            ]
            logger.warning("Timed out waiting to discover spawned runs: %s", missing)
            return

        time.sleep(min(INITIAL_DISCOVERY_INTERVAL_SECONDS, remaining))


def cancel_started_runs(
    s3: Any,
    repo: Repository,
    runs: list[WorkflowRun],
    cancellation_reason: str = "collector job was cancelled",
) -> None:
    for run in runs:
        if run.run_id is None:
            if not run.error:
                logger.warning(
                    "Cannot cancel %s: spawned run was not discovered",
                    run.workflow,
                )
                run.error = (
                    f"{cancellation_reason} before the spawned run was discovered"
                )
                run.status = "completed"
                run.conclusion = "cancelled"
            continue

        if run.status == "completed" or run.conclusion == "cancelled":
            logger.info(
                "Skipping cancellation for completed run %s id=%s status=%s conclusion=%s url=%s",
                run.workflow,
                run.run_id,
                run.status,
                run.conclusion,
                run.url,
            )
            continue

        try:
            logger.info(
                "Cancelling run workflow=%s id=%s status=%s conclusion=%s url=%s",
                run.workflow,
                run.run_id,
                run.status,
                run.conclusion,
                run.url,
            )
            cancel_result = cancel_workflow_run(repo, run.run_id)
            if wait_for_cancellation(
                s3,
                repo,
                run,
                timeout_seconds=CANCEL_VERIFY_TIMEOUT_SECONDS,
            ):
                continue

            if cancel_result.accepted:
                run.error = (
                    f"{cancellation_reason}; GitHub accepted cancellation, "
                    f"but the nightly run stayed {run.status}"
                )
            else:
                run.error = (
                    f"{cancellation_reason}; GitHub did not accept cancellation "
                    f"({cancel_result.debug}); nightly run stayed {run.status}"
                )
        except Exception as error:
            try:
                refresh_run(s3, repo, run)
            except Exception:
                logger.exception(
                    "Failed to refresh %s id=%s after cancellation failure",
                    run.workflow,
                    run.run_id,
                )
            run.error = f"{cancellation_reason}; failed to cancel run: {error}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=("dispatch-and-collect", "cancel"),
        default="dispatch-and-collect",
    )
    parser.add_argument("--poll-interval-seconds", type=int, default=60)
    parser.add_argument("--timeout-seconds", type=int, default=21600)
    return parser.parse_args()


def load_context() -> tuple[
    str,
    str,
    Repository,
    dict[str, Any],
    PullRequest,
    int,
    str,
    str,
    set[str],
    list[WorkflowRun],
    str,
    str,
]:
    token = os.environ["GITHUB_TOKEN"]
    repository = os.environ["GITHUB_REPOSITORY"]
    event = load_event()
    repo, pr_object = get_github_context(token, repository, event)
    pr = event["pull_request"]
    pr_number = int(pr["number"])
    head_ref = pr["head"]["ref"]
    head_repo = pr["head"]["repo"]["full_name"]
    labels = {label["name"] for label in pr.get("labels", [])}
    runs = selected_workflows(labels)
    marker = f"pr-{pr_number}-run-{os.environ['GITHUB_RUN_ID']}-attempt-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
    collector_url = find_current_job_url(
        os.environ.get("GITHUB_JOB", "nightly-builds"),
        os.environ.get("RUNNER_NAME", ""),
    )
    return (
        token,
        repository,
        repo,
        pr,
        pr_object,
        pr_number,
        head_ref,
        head_repo,
        labels,
        runs,
        marker,
        collector_url,
    )


def cancel_mode() -> int:
    (
        _token,
        _repository,
        repo,
        _pr,
        pr_object,
        pr_number,
        head_ref,
        _head_repo,
        _labels,
        runs,
        marker,
        collector_url,
    ) = load_context()

    if not runs:
        logger.info("No nightly PR labels found; nothing to cancel")
        return 0

    s3 = boto3.client("s3")
    log_s3_report_context()
    logger.info(
        "Cancellation mode started for PR #%s, ref=%s, marker=%s, workflows=%s",
        pr_number,
        head_ref,
        marker,
        [run.workflow for run in runs],
    )
    discover_missing_runs(
        repo,
        runs,
        head_ref,
        marker,
        dispatched_at=None,
    )
    refresh_runs(s3, repo, runs)
    cancel_started_runs(s3, repo, runs)
    upsert_comment(
        pr_object,
        marker,
        render_comment(marker, runs, final=True, collector_url=collector_url),
    )
    return 1 if any(run.error for run in runs) else 0


def main() -> int:
    signal.signal(signal.SIGINT, request_cancellation)
    signal.signal(signal.SIGTERM, request_cancellation)

    args = parse_args()
    if args.mode == "cancel":
        return cancel_mode()

    (
        _token,
        repository,
        repo,
        _pr,
        pr_object,
        pr_number,
        head_ref,
        head_repo,
        _labels,
        runs,
        marker,
        collector_url,
    ) = load_context()

    if not runs:
        logger.info("No nightly PR labels found; nothing to dispatch")
        return 0

    s3 = boto3.client("s3")
    log_s3_report_context()
    logger.info(
        "Dispatch-and-collect mode started for PR #%s, ref=%s, marker=%s, workflows=%s",
        pr_number,
        head_ref,
        marker,
        [run.workflow for run in runs],
    )

    if head_repo != repository:
        for run in runs:
            run.status = "completed"
            run.conclusion = "skipped"
            run.error = "workflow_dispatch for PR nightly checks is supported only for same-repository PR branches"
        upsert_comment(
            pr_object,
            marker,
            render_comment(marker, runs, final=True, collector_url=collector_url),
        )
        return 1

    dispatched_at = datetime.now(timezone.utc)
    try:
        for run in runs:
            try:
                dispatch_workflow(repo, run.workflow, head_ref, marker)
                run.status = "queued"
            except Exception as error:
                run.status = "completed"
                run.conclusion = "failure"
                run.error = str(error)

        wait_for_initial_discovery(repo, runs, head_ref, marker, dispatched_at)
        posted_comment_body = render_comment(
            marker,
            runs,
            final=False,
            collector_url=collector_url,
        )
        upsert_comment(pr_object, marker, posted_comment_body)

        deadline = time.monotonic() + args.timeout_seconds
        while time.monotonic() < deadline:
            discover_missing_runs(
                repo,
                runs,
                head_ref,
                marker,
                dispatched_at,
            )
            refresh_runs(s3, repo, runs)

            if all_done(runs):
                posted_comment_body = render_comment(
                    marker,
                    runs,
                    final=True,
                    collector_url=collector_url,
                )
                upsert_comment(
                    pr_object,
                    marker,
                    posted_comment_body,
                )
                return 1 if any_failed(runs) else 0

            current_comment_body = render_comment(
                marker,
                runs,
                final=False,
                collector_url=collector_url,
            )
            if current_comment_body != posted_comment_body:
                upsert_comment(pr_object, marker, current_comment_body)
                posted_comment_body = current_comment_body

            time.sleep(args.poll_interval_seconds)

    except CancellationRequested:
        discover_missing_runs(repo, runs, head_ref, marker, dispatched_at)
        refresh_runs(s3, repo, runs)
        cancel_started_runs(s3, repo, runs)
        upsert_comment(
            pr_object,
            marker,
            render_comment(marker, runs, final=True, collector_url=collector_url),
        )
        return 1

    discover_missing_runs(repo, runs, head_ref, marker, dispatched_at)
    refresh_runs(s3, repo, runs)
    timed_out_runs = [run for run in runs if run.status != "completed"]
    if timed_out_runs:
        logger.warning(
            "Collector timed out after %ss; cancelling unfinished nightly runs: %s",
            args.timeout_seconds,
            [
                {
                    "workflow": run.workflow,
                    "run_id": run.run_id,
                    "status": run.status,
                    "conclusion": run.conclusion,
                    "url": run.url,
                }
                for run in timed_out_runs
            ],
        )
        cancel_started_runs(
            s3,
            repo,
            timed_out_runs,
            cancellation_reason="collector timed out",
        )
        for run in timed_out_runs:
            if not run.error:
                run.error = (
                    "timed out waiting for workflow completion; cancellation requested"
                )
            if run.status != "completed":
                run.status = "completed"
                run.conclusion = "timed_out"

    upsert_comment(
        pr_object,
        marker,
        render_comment(marker, runs, final=True, collector_url=collector_url),
    )
    return 1 if timed_out_runs or any_failed(runs) else 0


if __name__ == "__main__":
    sys.exit(main())
