#!/usr/bin/env python3

import os
import argparse
import datetime
import numpy as np
from github import Github
from tabulate import tabulate
from collections import defaultdict
from dateutil import parser as dateparser
from dateutil.relativedelta import relativedelta
from .helpers import setup_logger, get_jobs_raw, Job
from typing import List

logger = setup_logger()


# --- Utilities ---
def parse_datetime(value, now=None):
    now = now or datetime.datetime.now(datetime.timezone.utc)
    try:
        if value.endswith("d"):
            days = int(value.rstrip("d"))
            return now - relativedelta(days=days)
        elif value == "now":
            return now
        else:
            return dateparser.isoparse(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid datetime or relative format: {value}"
        )


def output_results(all_jobs: List[Job], summary, threshold: int):
    print("=== Job Wait Times ===")
    print(
        tabulate(
            [
                [
                    f"{job.run_id}:{job.id}",
                    job.workflow.replace(".yaml", "").replace(".yml", ""),
                    job.name,
                    job.runner_type,
                    job.created_at.isoformat() if job.created_at else "N/A",
                    job.started_at.isoformat() if job.started_at else "N/A",
                    (job.started_at - job.created_at).total_seconds(),
                ]
                for job in all_jobs
                if (job.started_at - job.created_at).total_seconds() >= threshold
            ],
            headers=[
                "Id",
                "Workflow",
                "Job Name",
                "Runner Type",
                "Queued At",
                "Started At",
                "Wait (s)",
            ],
        )
    )

    print("=== Summary ===")
    print(
        tabulate(
            [
                [
                    runner,
                    data["count"],
                    int(data["total_wait"]),
                    (
                        round(data["total_wait"] / data["count"], 2)
                        if data["count"]
                        else 0
                    ),
                    int(np.percentile(data["waits"], 1)) if data["waits"] else "N/A",
                    int(np.percentile(data["waits"], 50)) if data["waits"] else "N/A",
                    int(np.percentile(data["waits"], 80)) if data["waits"] else "N/A",
                    int(np.percentile(data["waits"], 90)) if data["waits"] else "N/A",
                    int(np.percentile(data["waits"], 95)) if data["waits"] else "N/A",
                    int(np.percentile(data["waits"], 99)) if data["waits"] else "N/A",
                ]
                for runner, data in summary.items()
            ],
            headers=[
                "Runner Type",
                "Job Count",
                "Total Wait (s)",
                "Avg Wait (s)",
                "P1 (s)",
                "Median (s)",
                "P80 (s)",
                "P90 (s)",
                "P95 (s)",
                "P99 (s)",
            ],
        )
    )


def main(start, end, threshold):
    logger.info(f"Fetching workflow runs from {start} to {end}")
    all_jobs = []
    summary = defaultdict(lambda: {"total_wait": 0.0, "count": 0, "waits": []})

    runs = repo.get_workflow_runs()
    for run in runs:
        created_at = run.created_at
        if created_at < start:
            logger.info("Reached runs before target window; stopping.")
            break
        if created_at >= end:
            continue

        try:
            jobs = get_jobs_raw(GITHUB_TOKEN, repo.full_name, run.id)
        except Exception as e:
            logger.warning(f"Failed to get jobs for run {run.id}: {e}")
            continue

        for job in jobs:
            name = job.name
            created_at = job.created_at
            started_at = job.started_at
            conclusion = job.conclusion
            wait_sec = (job.started_at - job.created_at).total_seconds()
            labels = job.labels

            if conclusion == "skipped":
                logger.debug(f"Job {name} was skipped; skipping.")
                continue

            if conclusion == "cancelled" and len(labels) == 0:
                logger.debug(f"Job {name} was cancelled; skipping.")
                continue

            name_string = name.split("/")[-1].strip()
            # remove anything inside [] brackets
            name_string = name_string.split("[")[0].strip()
            all_jobs.append(
                Job(
                    workflow=run.path.split("/")[-1],
                    id=job.id,
                    run_id=run.id,
                    runner_name=job.runner_name,
                    completed_at=job.completed_at,
                    name=name_string,
                    status=job.status,
                    conclusion=job.conclusion,
                    runner_type=job.runner_type,
                    created_at=created_at,
                    started_at=started_at,
                )
            )

            if wait_sec is not None:
                summary[job.runner_type]["total_wait"] += wait_sec
                summary[job.runner_type]["count"] += 1
                summary[job.runner_type]["waits"].append(wait_sec)

    output_results(all_jobs, summary, threshold)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analyze GitHub Actions job wait times."
    )
    parser.add_argument(
        "--since",
        type=str,
        default="1d",
        help="Start of time window (e.g. 2d, 2025-05-20T00:00:00Z)",
    )
    parser.add_argument(
        "--until",
        type=str,
        default="now",
        help="End of time window (e.g. now, 2025-05-21T00:00:00Z)",
    )
    parser.add_argument(
        "--owner",
        type=str,
        default="librarian-test",
        help="GitHub repository owner ",
    )
    parser.add_argument(
        "--repo",
        type=str,
        default="nbs",
        help="GitHub repository name ",
    )
    parser.add_argument(
        "--token",
        type=str,
        help="GitHub token for authentication",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=120,
        help="Minimum wait time in seconds to include in results",
    )
    args = parser.parse_args()

    now = datetime.datetime.now(datetime.timezone.utc)
    start = parse_datetime(args.since, now)
    end = parse_datetime(args.until, now)

    GITHUB_TOKEN = args.token if args.token else os.getenv("GITHUB_TOKEN")
    if not GITHUB_TOKEN:
        raise EnvironmentError(
            "GITHUB_TOKEN environment variable not set or passed as argument."
        )

    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(f"{args.owner}/{args.repo}")

    main(start, end, args.threshold)
