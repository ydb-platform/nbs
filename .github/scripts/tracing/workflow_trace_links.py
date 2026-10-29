"""Discover links from small ya bundle manifests without reading their spans."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Mapping, Sequence
from time import monotonic
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError
from github.WorkflowJob import WorkflowJob

from ..helpers import get_s3_report_url

logger = logging.getLogger(__name__)
MAX_MANIFESTS = 1_000
MAX_MANIFEST_BYTES = 256 * 1024
MAX_TOTAL_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_DISCOVERY_SECONDS = 60


def load_ya_report_links(
    *,
    s3_client: Any,
    bucket: str,
    prefix: str,
    website_suffix: str,
    workflow_run: Mapping[str, Any],
    jobs: Sequence[WorkflowJob],
) -> dict[int, list[tuple[str, str]]]:
    if not bucket or not prefix or prefix.startswith("/") or not website_suffix:
        raise ValueError("A bucket, relative S3 prefix and website suffix are required")
    prefix = prefix.rstrip("/") + "/"
    jobs_by_runner: dict[str, list[WorkflowJob]] = defaultdict(list)
    for job in jobs:
        if job.runner_name:
            jobs_by_runner[job.runner_name].append(job)

    links: dict[int, list[tuple[str, str]]] = defaultdict(list)
    count = total_bytes = 0
    deadline = monotonic() + MAX_DISCOVERY_SECONDS
    try:
        pages = s3_client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix
        )
        for page in pages:
            for item in page.get("Contents", []):
                if monotonic() >= deadline:
                    logger.warning("Ya report discovery time limit reached")
                    return links
                key = item["Key"]
                if not key.endswith("/trace.manifest.json"):
                    continue
                if count >= MAX_MANIFESTS or total_bytes >= MAX_TOTAL_MANIFEST_BYTES:
                    logger.warning("Ya report discovery size limit reached")
                    return links
                count += 1
                try:
                    if item["Size"] > MAX_MANIFEST_BYTES:
                        raise ValueError("manifest exceeds size limit")
                    response = s3_client.get_object(Bucket=bucket, Key=key)
                    body = response["Body"]
                    try:
                        payload = body.read(MAX_MANIFEST_BYTES + 1)
                    finally:
                        body.close()
                    total_bytes += len(payload)
                    if total_bytes > MAX_TOTAL_MANIFEST_BYTES:
                        logger.warning("Ya report discovery size limit reached")
                        return links
                    if len(payload) > MAX_MANIFEST_BYTES:
                        raise ValueError("manifest exceeds size limit")
                    manifest = json.loads(payload)
                    if (
                        not isinstance(manifest, dict)
                        or manifest.get("schema") != "nbs-otlp-trace-bundle"
                        or manifest.get("html_file") != "trace.html"
                    ):
                        raise ValueError("unsupported trace manifest")
                    metadata = manifest.get("metadata")
                    if not isinstance(metadata, dict):
                        raise ValueError("missing trace metadata")
                    if (
                        metadata.get("github.repository")
                        != workflow_run["repository"]["full_name"]
                        or str(metadata.get("github.run.id")) != str(workflow_run["id"])
                        or str(metadata.get("github.run.attempt"))
                        != str(workflow_run.get("run_attempt", 1))
                    ):
                        raise ValueError(
                            "trace belongs to a different workflow attempt"
                        )
                    runner = metadata.get("github.runner.name")
                    candidates = (
                        jobs_by_runner.get(runner, [])
                        if isinstance(runner, str)
                        else []
                    )
                    if len(candidates) != 1:
                        raise ValueError("trace runner does not identify a unique job")
                    job = candidates[0]
                    operation = metadata.get("ci.ya.operation")
                    if operation == "build":
                        label = "Build trace"
                    elif operation == "tests":
                        attempt = max(1, int(metadata.get("ci.ya.retry", 1)))
                        label = f"Tests — attempt {attempt}"
                    else:
                        raise ValueError("unknown ya operation")
                    component = metadata.get("ci.component")
                    if component and component != "all":
                        label += f" · {component}"
                    url = get_s3_report_url(
                        key.removesuffix("trace.manifest.json") + "trace.html",
                        bucket=bucket,
                        website_suffix=website_suffix,
                    )
                    links[job.id].append((label, url))
                except (
                    BotoCoreError,
                    ClientError,
                    OSError,
                    ValueError,
                    RecursionError,
                    TypeError,
                    KeyError,
                ) as error:
                    logger.warning("Skipping ya report %s: %s", key, error)
    except (BotoCoreError, ClientError, OSError) as error:
        logger.warning("Could not finish discovering ya reports: %s", error)
    return links
