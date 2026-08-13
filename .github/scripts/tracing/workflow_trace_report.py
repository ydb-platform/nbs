#!/usr/bin/env python3
"""Build one static trace from GitHub workflow timing and ya OTLP bundles."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Iterable

import boto3

from ..helpers import get_jobs, github_client
from .trace_io import (
    MAX_INPUT_BYTES,
    MAX_SPANS,
    read_otlp_jsonl,
)
from .trace_report import (
    add_artifact_url_arguments,
    write_trace_bundle,
)
from .workflow_trace import build_workflow_trace

MAX_TRACE_INPUTS = 10_000


def _find_inputs(paths: Iterable[Path], output_dir: Path) -> list[Path]:
    inputs = []
    output_dir = output_dir.resolve()
    for path in paths:
        if path.is_dir():
            candidates = path.rglob("trace.otlp.jsonl.gz")
        else:
            candidates = [path]
        for candidate in candidates:
            resolved = candidate.resolve()
            if resolved.is_file() and output_dir not in resolved.parents:
                inputs.append(resolved)
    return sorted(set(inputs))


def download_s3_trace_inputs(
    *,
    s3_client: Any,
    bucket: str,
    prefix: str,
    output_dir: Path,
    max_input_bytes: int = MAX_INPUT_BYTES,
) -> list[Path]:
    if not bucket or not prefix or prefix.startswith("/"):
        raise ValueError("A bucket and relative S3 prefix are required")
    prefix = prefix.rstrip("/") + "/"
    paginator = s3_client.get_paginator("list_objects_v2")
    inventory = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        inventory.extend(
            (item["Key"], item["Size"])
            for item in page.get("Contents", [])
            if item["Key"].endswith("/trace.otlp.jsonl.gz")
        )
        if len(inventory) > MAX_TRACE_INPUTS:
            raise ValueError(f"S3 trace inventory exceeds {MAX_TRACE_INPUTS} objects")

    total_bytes = 0
    inputs: list[Path] = []
    output_dir.mkdir(parents=True, exist_ok=True)
    for index, (key, size) in enumerate(inventory):
        total_bytes += size
        if total_bytes > max_input_bytes:
            raise ValueError(f"S3 traces exceed {max_input_bytes} bytes")
        destination = output_dir / f"{index:05d}.otlp.jsonl.gz"
        s3_client.download_file(bucket, key, str(destination))
        inputs.append(destination)
    return inputs


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", required=True, type=Path)
    parser.add_argument("--input", action="append", type=Path, default=[])
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--s3-bucket", default="")
    parser.add_argument("--s3-prefix", default="")
    parser.add_argument("--s3-input-dir", type=Path)
    parser.add_argument("--github-token", default=os.environ.get("GITHUB_TOKEN", ""))
    parser.add_argument(
        "--github-api-url",
        default=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
    )
    add_artifact_url_arguments(parser)
    return parser


def main() -> None:
    args = _parser().parse_args()
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
    input_paths = _find_inputs(args.input, args.output_dir)
    if args.s3_bucket or args.s3_prefix:
        if not args.s3_bucket or not args.s3_prefix:
            raise ValueError("--s3-bucket and --s3-prefix must be used together")
        s3_input_dir = args.s3_input_dir or args.output_dir.parent / "trace-input"
        s3_client = boto3.client("s3")
        input_paths.extend(
            download_s3_trace_inputs(
                s3_client=s3_client,
                bucket=args.s3_bucket,
                prefix=args.s3_prefix,
                output_dir=s3_input_dir,
            )
        )
    imported = read_otlp_jsonl(
        input_paths,
        max_input_bytes=MAX_INPUT_BYTES,
        max_spans=MAX_SPANS,
    )
    trace, metadata = build_workflow_trace(workflow_run, jobs, imported)
    manifest = write_trace_bundle(
        args.output_dir,
        trace,
        title=f"Workflow trace · {workflow_run.get('name', 'unknown')}",
        metadata=metadata,
        file_prefix="workflow-trace",
        test_log_url_prefix=args.test_log_url_prefix,
        test_data_url_prefix=args.test_data_url_prefix,
    )
    print(
        f"Wrote {manifest['span_count']} spans, including {len(imported)} "
        f"imported ya spans, to {args.output_dir}"
    )


if __name__ == "__main__":
    main()
