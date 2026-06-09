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

from github import Auth as GithubAuth, Github
from github.PullRequest import PullRequest
from github.Repository import Repository
from github.WorkflowRun import WorkflowRun as GithubWorkflowRun

from .helpers import setup_logger
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
logger = setup_logger()


@dataclass
class WorkflowRun:
    workflow: str
    label: str
    run_id: int | None = None
    url: str = ""
    status: str = "dispatching"
    conclusion: str | None = None
    jobs: list[dict[str, Any]] = field(default_factory=list)
    error: str = ""


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


def get_workflow_jobs(run: GithubWorkflowRun) -> list[dict[str, Any]]:
    return [
        {
            "name": job.name,
            "html_url": job.html_url,
            "status": job.status,
            "conclusion": job.conclusion,
        }
        for job in run.jobs()
    ]


def cancel_workflow_run(repo: Repository, run_id: int) -> None:
    logger.info("Requesting cancellation for run_id=%s", run_id)
    repo.get_workflow_run(run_id).cancel()


def status_icon(run: WorkflowRun) -> str:
    if run.error:
        return ":red_circle:"
    if run.status != "completed":
        return ":yellow_circle:"
    if run.conclusion == "success":
        return ":green_circle:"
    return ":red_circle:"


def render_comment(
    marker: str,
    pr_number: int,
    head_ref: str,
    runs: list[WorkflowRun],
    final: bool,
) -> str:
    comment_marker = get_comment_marker(marker)
    lines = [
        comment_marker,
        f"### Nightly PR checks for #{pr_number}",
        "",
        f"Marker: `{marker}`",
        f"Ref: `{head_ref}`",
        "",
        "| Workflow | Status | Result | Trigger |",
        "| --- | --- | --- | --- |",
    ]

    for run in runs:
        workflow = f"`{run.workflow}`"
        if run.url:
            workflow = f"[`{run.workflow}`]({run.url})"
        result = run.error or run.conclusion or ""
        lines.append(
            f"| {workflow} | {status_icon(run)} `{run.status}` | `{result}` | `{run.label}` |"
        )

    failed_jobs = [
        (run, job)
        for run in runs
        for job in run.jobs
        if job.get("conclusion") not in (None, "success", "skipped")
    ]
    if failed_jobs:
        lines.extend(
            [
                "",
                "#### Failed jobs",
                "",
                "| Workflow | Job | Result |",
                "| --- | --- | --- |",
            ]
        )
        for run, job in failed_jobs[:25]:
            job_name = job.get("name", "")
            job_url = job.get("html_url", run.url)
            job_result = job.get("conclusion") or job.get("status") or ""
            lines.append(
                f"| `{run.workflow}` | [{job_name}]({job_url}) | `{job_result}` |"
            )
        if len(failed_jobs) > 25:
            lines.append(f"| ... | {len(failed_jobs) - 25} more failed jobs | |")

    lines.extend(
        [
            "",
            "Detailed test summaries and S3 report links are available in the spawned workflow runs.",
        ]
    )
    if not final:
        lines.append(
            "This comment will be updated until all requested nightly workflows complete."
        )

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


def refresh_runs(repo: Repository, runs: list[WorkflowRun]) -> None:
    for run in runs:
        if run.run_id is None:
            continue
        workflow_run = repo.get_workflow_run(run.run_id)
        run.status = workflow_run.status or run.status
        run.conclusion = workflow_run.conclusion
        run.url = workflow_run.html_url or run.url
        run.jobs = get_workflow_jobs(workflow_run)
        logger.info(
            "Refreshed %s: id=%s status=%s conclusion=%s",
            run.workflow,
            run.run_id,
            run.status,
            run.conclusion,
        )


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
    dispatched_at: datetime,
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


def cancel_started_runs(repo: Repository, runs: list[WorkflowRun]) -> None:
    for run in runs:
        if run.run_id is None:
            if not run.error:
                logger.warning(
                    "Cannot cancel %s: spawned run was not discovered",
                    run.workflow,
                )
                run.error = (
                    "collector was cancelled before the spawned run was discovered"
                )
                run.status = "completed"
                run.conclusion = "cancelled"
            continue

        if run.status == "completed":
            logger.info(
                "Skipping cancellation for completed run %s id=%s",
                run.workflow,
                run.run_id,
            )
            continue

        try:
            cancel_workflow_run(repo, run.run_id)
            run.error = "collector job was cancelled; requested cancellation for this nightly run"
            run.status = "completed"
            run.conclusion = "cancelled"
        except Exception as error:
            run.error = f"collector job was cancelled; failed to cancel run: {error}"
            run.status = "completed"
            run.conclusion = "cancelled"


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
    )


def cancel_mode() -> int:
    (
        token,
        repository,
        repo,
        _pr,
        pr_object,
        pr_number,
        head_ref,
        _head_repo,
        _labels,
        runs,
        marker,
    ) = load_context()

    if not runs:
        logger.info("No nightly PR labels found; nothing to cancel")
        return 0

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
    refresh_runs(repo, runs)
    cancel_started_runs(repo, runs)
    upsert_comment(
        pr_object, marker, render_comment(marker, pr_number, head_ref, runs, final=True)
    )
    return 0


def main() -> int:
    signal.signal(signal.SIGINT, request_cancellation)
    signal.signal(signal.SIGTERM, request_cancellation)

    args = parse_args()
    if args.mode == "cancel":
        return cancel_mode()

    (
        token,
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
    ) = load_context()

    if not runs:
        logger.info("No nightly PR labels found; nothing to dispatch")
        return 0

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
            render_comment(marker, pr_number, head_ref, runs, final=True),
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

        upsert_comment(
            pr_object,
            marker,
            render_comment(marker, pr_number, head_ref, runs, final=False),
        )

        deadline = time.monotonic() + args.timeout_seconds
        while time.monotonic() < deadline:
            discover_missing_runs(
                repo,
                runs,
                head_ref,
                marker,
                dispatched_at,
            )
            refresh_runs(repo, runs)

            if all_done(runs):
                upsert_comment(
                    pr_object,
                    marker,
                    render_comment(marker, pr_number, head_ref, runs, final=True),
                )
                return 1 if any_failed(runs) else 0

            time.sleep(args.poll_interval_seconds)

    except CancellationRequested:
        discover_missing_runs(repo, runs, head_ref, marker, dispatched_at)
        refresh_runs(repo, runs)
        cancel_started_runs(repo, runs)
        upsert_comment(
            pr_object,
            marker,
            render_comment(marker, pr_number, head_ref, runs, final=True),
        )
        return 1

    for run in runs:
        if run.status != "completed":
            run.error = "timed out waiting for workflow completion"
            run.status = "completed"
            run.conclusion = "timed_out"

    upsert_comment(
        pr_object, marker, render_comment(marker, pr_number, head_ref, runs, final=True)
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
