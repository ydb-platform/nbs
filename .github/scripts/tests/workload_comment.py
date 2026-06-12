#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os

from github import Auth as GithubAuth, Github
from github.PullRequest import PullRequest

from ..helpers import find_current_job_url, setup_logger
from . import generate_summary as gs


def get_pull_request() -> PullRequest:
    gh = Github(auth=GithubAuth.Token(os.environ["GITHUB_TOKEN"]))

    with open(os.environ["GITHUB_EVENT_PATH"]) as fp:
        event = json.load(fp)

    return gh.create_from_raw_data(PullRequest, event["pull_request"])


def iter_components(matrix_include: str) -> list[str]:
    matrix = json.loads(matrix_include)
    return [entry["component"] for entry in matrix.get("include", [])]


def write_output(path: str, value: str) -> None:
    if not path:
        return
    with open(path, "w") as fp:
        fp.write(value)


def main() -> None:
    setup_logger(name=__name__, fmt="%(levelname)s %(message)s")
    setup_logger(name=gs.__name__, fmt="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init")
    init_parser.add_argument("--matrix-include", required=True)
    init_parser.add_argument("--build-preset", required=True)
    init_parser.add_argument(
        "--workload-status",
        choices=("in_progress", "completed"),
        default="in_progress",
    )
    init_parser.add_argument(
        "--is-dry-run",
        default=False,
        action="store_true",
    )

    update_parser = subparsers.add_parser("update")
    update_parser.add_argument("--build-preset", required=True)
    update_parser.add_argument("--component", required=True)
    update_parser.add_argument(
        "--workload-check-status",
        choices=("running", "completed", "failed_build"),
        required=True,
    )
    update_parser.add_argument("--current-job-name", default="")
    update_parser.add_argument("--runner-name", default="")
    update_parser.add_argument("--job-url-out", default="")
    update_parser.add_argument("--build-error-log-url", default="")
    update_parser.add_argument(
        "--is-dry-run",
        default=False,
        action="store_true",
    )

    args = parser.parse_args()

    if os.environ.get("GITHUB_EVENT_NAME") not in (
        "pull_request",
        "pull_request_target",
    ):
        return

    pr = get_pull_request()
    run_number = int(os.environ.get("GITHUB_RUN_NUMBER", "0"))

    if args.command == "init":
        gs.initialize_pr_comment(
            run_number=run_number,
            pr=pr,
            build_preset=args.build_preset,
            is_dry_run=args.is_dry_run,
            workload_status=args.workload_status,
            workload_components=iter_components(args.matrix_include),
        )
        return

    job_url = ""
    if args.current_job_name:
        job_url = find_current_job_url(args.current_job_name, args.runner_name)
        write_output(args.job_url_out, job_url)
    gs.update_pr_comment_workload_check(
        run_number=run_number,
        pr=pr,
        build_preset=args.build_preset,
        component=args.component,
        is_dry_run=args.is_dry_run,
        workload_check_status=args.workload_check_status,
        job_url=job_url,
        build_error_log_url=args.build_error_log_url,
    )


if __name__ == "__main__":
    main()
