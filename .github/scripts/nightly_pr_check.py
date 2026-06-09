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
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from .helpers import setup_logger

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

COMMENT_MARKER = "<!-- nbs-nightly-pr-check -->"
GITHUB_API = "https://api.github.com"
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


def api_request(
    method: str, path: str, token: str, data: dict[str, Any] | None = None
) -> Any:
    body = None
    if data is not None:
        body = json.dumps(data).encode("utf-8")

    request = Request(
        f"{GITHUB_API}{path}",
        data=body,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urlopen(request) as response:
            if response.status in (202, 204):
                return None
            response_body = response.read()
            if not response_body:
                return None
            return json.loads(response_body)
    except HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"GitHub API {method} {path} failed: {error.code} {error_body}"
        ) from error


def repo_path(repository: str, suffix: str) -> str:
    owner, repo = repository.split("/", 1)
    return f"/repos/{quote(owner)}/{quote(repo)}{suffix}"


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
    repository: str,
    token: str,
    workflow: str,
    ref: str,
    marker: str,
) -> None:
    logger.info("Dispatching %s on ref %s with marker %s", workflow, ref, marker)
    path = repo_path(repository, f"/actions/workflows/{quote(workflow)}/dispatches")
    api_request(
        "POST",
        path,
        token,
        {
            "ref": ref,
            "inputs": {
                "comment": marker,
            },
        },
    )


def find_workflow_run(
    repository: str,
    token: str,
    workflow: str,
    branch: str,
    marker: str,
    dispatched_at: datetime | None = None,
) -> dict[str, Any] | None:
    query = urlencode(
        {
            "event": "workflow_dispatch",
            "branch": branch,
            "per_page": 50,
        }
    )
    path = repo_path(repository, f"/actions/workflows/{quote(workflow)}/runs?{query}")
    payload = api_request("GET", path, token)
    candidates = []
    for run in payload.get("workflow_runs", []):
        title = run.get("display_title") or run.get("name") or ""
        if marker not in title:
            continue
        created_at = parse_github_time(run.get("created_at"))
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

    result = sorted(candidates, key=lambda run: run.get("run_number", 0), reverse=True)[
        0
    ]
    logger.info(
        "Found run workflow=%s id=%s status=%s conclusion=%s url=%s",
        workflow,
        result.get("id"),
        result.get("status"),
        result.get("conclusion"),
        result.get("html_url"),
    )
    return result


def get_workflow_run(repository: str, token: str, run_id: int) -> dict[str, Any]:
    return api_request("GET", repo_path(repository, f"/actions/runs/{run_id}"), token)


def get_workflow_jobs(repository: str, token: str, run_id: int) -> list[dict[str, Any]]:
    jobs = []
    page = 1
    while True:
        query = urlencode({"per_page": 100, "page": page})
        payload = api_request(
            "GET", repo_path(repository, f"/actions/runs/{run_id}/jobs?{query}"), token
        )
        jobs.extend(payload.get("jobs", []))
        if len(jobs) >= payload.get("total_count", 0):
            return jobs
        page += 1


def cancel_workflow_run(repository: str, token: str, run_id: int) -> None:
    logger.info("Requesting cancellation for run_id=%s", run_id)
    api_request("POST", repo_path(repository, f"/actions/runs/{run_id}/cancel"), token)


def parse_github_time(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


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
    lines = [
        COMMENT_MARKER,
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


def upsert_comment(repository: str, token: str, pr_number: int, body: str) -> None:
    comments_path = repo_path(repository, f"/issues/{pr_number}/comments")
    comments = api_request("GET", f"{comments_path}?per_page=100", token)

    for comment in comments:
        if COMMENT_MARKER not in comment.get("body", ""):
            continue
        comment_id = comment["id"]
        logger.info("Updating sticky PR comment id=%s", comment_id)
        api_request(
            "PATCH",
            repo_path(repository, f"/issues/comments/{comment_id}"),
            token,
            {"body": body},
        )
        return

    logger.info("Creating sticky PR comment for PR #%s", pr_number)
    api_request("POST", comments_path, token, {"body": body})


def refresh_runs(repository: str, token: str, runs: list[WorkflowRun]) -> None:
    for run in runs:
        if run.run_id is None:
            continue
        payload = get_workflow_run(repository, token, run.run_id)
        run.status = payload.get("status") or run.status
        run.conclusion = payload.get("conclusion")
        run.url = payload.get("html_url") or run.url
        run.jobs = get_workflow_jobs(repository, token, run.run_id)
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
    repository: str,
    token: str,
    runs: list[WorkflowRun],
    head_ref: str,
    marker: str,
    dispatched_at: datetime,
) -> None:
    for run in runs:
        if run.error or run.run_id is not None:
            continue
        workflow_run = find_workflow_run(
            repository,
            token,
            run.workflow,
            head_ref,
            marker,
            dispatched_at,
        )
        if workflow_run is None:
            continue
        run.run_id = int(workflow_run["id"])
        run.url = workflow_run.get("html_url") or ""


def cancel_started_runs(repository: str, token: str, runs: list[WorkflowRun]) -> None:
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
            cancel_workflow_run(repository, token, run.run_id)
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


def load_context() -> (
    tuple[str, str, dict[str, Any], int, str, str, set[str], list[WorkflowRun], str]
):
    token = os.environ["GITHUB_TOKEN"]
    repository = os.environ["GITHUB_REPOSITORY"]
    event = load_event()
    pr = event["pull_request"]
    pr_number = int(pr["number"])
    head_ref = pr["head"]["ref"]
    head_repo = pr["head"]["repo"]["full_name"]
    labels = {label["name"] for label in pr.get("labels", [])}
    runs = selected_workflows(labels)
    marker = f"pr-{pr_number}-run-{os.environ['GITHUB_RUN_ID']}-attempt-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
    return token, repository, pr, pr_number, head_ref, head_repo, labels, runs, marker


def cancel_mode() -> int:
    token, repository, _pr, pr_number, head_ref, _head_repo, _labels, runs, marker = (
        load_context()
    )

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
        repository,
        token,
        runs,
        head_ref,
        marker,
        dispatched_at=None,
    )
    refresh_runs(repository, token, runs)
    cancel_started_runs(repository, token, runs)
    upsert_comment(
        repository,
        token,
        pr_number,
        render_comment(marker, pr_number, head_ref, runs, final=True),
    )
    return 0


def main() -> int:
    signal.signal(signal.SIGINT, request_cancellation)
    signal.signal(signal.SIGTERM, request_cancellation)

    args = parse_args()
    if args.mode == "cancel":
        return cancel_mode()

    token, repository, _pr, pr_number, head_ref, head_repo, _labels, runs, marker = (
        load_context()
    )

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
            repository,
            token,
            pr_number,
            render_comment(marker, pr_number, head_ref, runs, final=True),
        )
        return 1

    dispatched_at = datetime.now(timezone.utc)
    try:
        for run in runs:
            try:
                dispatch_workflow(repository, token, run.workflow, head_ref, marker)
                run.status = "queued"
            except Exception as error:
                run.status = "completed"
                run.conclusion = "failure"
                run.error = str(error)

        upsert_comment(
            repository,
            token,
            pr_number,
            render_comment(marker, pr_number, head_ref, runs, final=False),
        )

        deadline = time.monotonic() + args.timeout_seconds
        while time.monotonic() < deadline:
            discover_missing_runs(
                repository,
                token,
                runs,
                head_ref,
                marker,
                dispatched_at,
            )
            refresh_runs(repository, token, runs)
            upsert_comment(
                repository,
                token,
                pr_number,
                render_comment(marker, pr_number, head_ref, runs, final=all_done(runs)),
            )

            if all_done(runs):
                return 1 if any_failed(runs) else 0

            time.sleep(args.poll_interval_seconds)

    except CancellationRequested:
        discover_missing_runs(repository, token, runs, head_ref, marker, dispatched_at)
        refresh_runs(repository, token, runs)
        cancel_started_runs(repository, token, runs)
        upsert_comment(
            repository,
            token,
            pr_number,
            render_comment(marker, pr_number, head_ref, runs, final=True),
        )
        return 1

    for run in runs:
        if run.status != "completed":
            run.error = "timed out waiting for workflow completion"
            run.status = "completed"
            run.conclusion = "timed_out"

    upsert_comment(
        repository,
        token,
        pr_number,
        render_comment(marker, pr_number, head_ref, runs, final=True),
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
