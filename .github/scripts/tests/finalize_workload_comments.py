#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os

from github import Auth as GithubAuth, Github
from github.PullRequest import PullRequest

from ..helpers import setup_logger
from . import generate_summary as gs


def get_pull_request() -> PullRequest:
    gh = Github(auth=GithubAuth.Token(os.environ["GITHUB_TOKEN"]))

    with open(os.environ["GITHUB_EVENT_PATH"]) as fp:
        event = json.load(fp)

    return gh.create_from_raw_data(PullRequest, event["pull_request"])


def iter_build_presets(matrix_include: str) -> list[str]:
    matrix = json.loads(matrix_include)
    return sorted({entry["build_preset"] for entry in matrix.get("include", [])})


def main() -> None:
    setup_logger(name=__name__, fmt="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--matrix-include", required=True)
    parser.add_argument(
        "--platform-name",
        required=True,
        help="Normalized platform prefix, for example linux-x86_64",
    )
    parser.add_argument(
        "--workload-status",
        choices=("in_progress", "completed"),
        default="completed",
    )
    parser.add_argument(
        "--is-dry-run",
        default=False,
        action="store_true",
        help="Match comments created from dry-run summaries",
    )
    args = parser.parse_args()

    if os.environ.get("GITHUB_EVENT_NAME") not in (
        "pull_request",
        "pull_request_target",
    ):
        return

    pr = get_pull_request()
    run_number = int(os.environ.get("GITHUB_RUN_NUMBER", "0"))

    for build_preset in iter_build_presets(args.matrix_include):
        gs.update_pr_comment_workload_status(
            run_number=run_number,
            pr=pr,
            build_preset=f"{args.platform_name}-{build_preset}",
            is_dry_run=args.is_dry_run,
            workload_status=args.workload_status,
        )


if __name__ == "__main__":
    main()
