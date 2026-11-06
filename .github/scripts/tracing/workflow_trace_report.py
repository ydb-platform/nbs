#!/usr/bin/env python3
"""Render GitHub workflow timing with optional links to ya execution reports."""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from ..helpers import get_jobs, github_client, github_output
from .trace_io import MAX_SPANS
from .trace_report import write_trace_bundle
from .workflow_trace import build_workflow_trace
from .workflow_trace_links import load_ya_report_links

logger = logging.getLogger(__name__)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--s3-bucket", default="")
    parser.add_argument("--s3-prefix", default="")
    parser.add_argument(
        "--s3-website-suffix", default=os.environ.get("S3_WEBSITE_SUFFIX", "")
    )
    parser.add_argument("--github-token", default=os.environ.get("GITHUB_TOKEN", ""))
    parser.add_argument(
        "--github-api-url",
        default=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    logging.basicConfig(level=logging.INFO)
    event = json.loads(args.event.read_text(encoding="utf-8"))
    workflow_run = event.get("workflow_run")
    if not isinstance(workflow_run, dict):
        raise ValueError("Event does not contain workflow_run")
    repository = event.get("repository", {}).get("full_name") or os.environ.get(
        "GITHUB_REPOSITORY", ""
    )
    if not repository or not args.github_token:
        raise ValueError("GitHub repository and token are required")
    workflow_run.setdefault("repository", {"full_name": repository})

    github = github_client(args.github_token, base_url=args.github_api_url)
    jobs = get_jobs(
        github,
        repository,
        int(workflow_run["id"]),
        run_attempt=int(workflow_run.get("run_attempt", 1)),
    )
    if len(jobs) > MAX_SPANS:
        raise ValueError(f"Workflow has more than {MAX_SPANS} jobs")
    if all(job.conclusion == "skipped" for job in jobs):
        logger.info("Skipping workflow trace: this attempt has no non-skipped jobs")
        github_output(logger, "rendered", "false")
        return

    report_links = {}
    if args.s3_bucket or args.s3_prefix:
        try:
            s3_client = boto3.client(
                "s3",
                config=Config(
                    connect_timeout=5,
                    read_timeout=5,
                    retries={"total_max_attempts": 2},
                ),
            )
            report_links = load_ya_report_links(
                s3_client=s3_client,
                bucket=args.s3_bucket,
                prefix=args.s3_prefix,
                website_suffix=args.s3_website_suffix,
                workflow_run=workflow_run,
                jobs=jobs,
            )
        except (BotoCoreError, ClientError, OSError, ValueError) as error:
            logger.warning("Rendering without ya report links: %s", error)

    trace, metadata = build_workflow_trace(
        workflow_run, jobs, report_links=report_links
    )
    manifest = write_trace_bundle(
        args.output_dir,
        trace,
        title=f"Workflow trace · {workflow_run.get('name', 'unknown')}",
        metadata=metadata,
        file_prefix="workflow-trace",
    )
    github_output(logger, "rendered", "true")
    print(f"Wrote {manifest['span_count']} workflow spans to {args.output_dir}")


if __name__ == "__main__":
    main()
