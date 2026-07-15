#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from collections.abc import Callable

from github.WorkflowJob import WorkflowJob

from ..helpers import (
    BUILD_AND_TEST_JOB_NAME_PREFIX,
    PYGITHUB_RETRY_EXCEPTIONS,
    get_jobs_raw,
    github_client_from_env,
    load_github_event,
    parse_actions_job_url,
    pull_request_from_event,
    setup_logger,
)
from . import generate_summary as gs

JOB_LOOKUP_EXCEPTIONS = (AttributeError, ValueError) + PYGITHUB_RETRY_EXCEPTIONS

WorkflowRunId = int
JobUrl = str
BuildPreset = str
Component = str
JobConclusion = str | None
WorkflowJobs = list[WorkflowJob]
JobsCache = dict[WorkflowRunId, WorkflowJobs]
JobsConclusionCacheKey = tuple[JobUrl, BuildPreset, Component]
JobsConclusionCache = dict[JobsConclusionCacheKey, JobConclusion]
JobsForRun = Callable[[WorkflowRunId], WorkflowJobs]


def platform_name_for_target(default_platform_name: str, target_platform: str) -> str:
    if target_platform == "default-linux-armv9a_grace":
        return "linux-arm64"
    return default_platform_name


def iter_full_build_presets(
    matrix_include: str, default_platform_name: str
) -> list[str]:
    return [
        full_build_preset
        for full_build_preset, _ in iter_build_preset_pairs(
            matrix_include,
            default_platform_name,
        )
    ]


def iter_build_preset_pairs(
    matrix_include: str, default_platform_name: str
) -> list[tuple[str, str]]:
    if not matrix_include.strip():
        return []

    matrix = json.loads(matrix_include)
    return sorted(
        {
            (
                f"{platform_name_for_target(default_platform_name, entry.get('target_platform', ''))}-{entry['build_preset']}",
                entry["build_preset"],
            )
            for entry in matrix.get("include", [])
        }
    )


def workload_job_marker(build_preset: str, component: str) -> str:
    return f"[build_preset={build_preset} component={component}]"


def find_workload_job_conclusion(
    jobs: WorkflowJobs,
    build_preset: BuildPreset,
    component: Component,
) -> JobConclusion:
    marker = workload_job_marker(build_preset, component)
    matches = []
    for job in jobs:
        job_name = getattr(job, "name", None) or ""
        if marker in job_name and BUILD_AND_TEST_JOB_NAME_PREFIX in job_name:
            matches.append(job)
    if len(matches) != 1:
        return None
    return getattr(matches[0], "conclusion", None)


def resolve_job_conclusion(
    job_url: JobUrl,
    build_preset: BuildPreset,
    component: Component,
    current_run_id: WorkflowRunId,
    jobs_for_run: JobsForRun,
) -> JobConclusion:
    job_ref = parse_actions_job_url(job_url)
    if job_ref is not None:
        run_id, job_id = job_ref
        conclusion = None
        for job in jobs_for_run(run_id):
            try:
                matches_job_id = int(job.id) == job_id
            except (AttributeError, TypeError, ValueError):
                continue
            if matches_job_id:
                conclusion = getattr(job, "conclusion", None)
                break
        if conclusion is not None:
            return conclusion

    return find_workload_job_conclusion(
        jobs_for_run(current_run_id),
        build_preset,
        component,
    )


def main() -> None:
    setup_logger(name=__name__, fmt="%(levelname)s %(message)s")
    setup_logger(name=gs.__name__, fmt="%(levelname)s %(message)s")
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

    pr = pull_request_from_event(
        github_client_from_env(),
        load_github_event(),
    )
    run_number = int(os.environ.get("GITHUB_RUN_NUMBER", "0"))
    current_run_id = int(os.environ.get("GITHUB_RUN_ID", "0"))
    jobs_cache: JobsCache = {}
    job_conclusion_cache: JobsConclusionCache = {}

    def jobs_for_run(run_id: WorkflowRunId) -> WorkflowJobs:
        if run_id in jobs_cache:
            return jobs_cache[run_id]
        try:
            jobs = get_jobs_raw(
                os.environ["GITHUB_TOKEN"],
                os.environ["GITHUB_REPOSITORY"],
                run_id,
            )
        except JOB_LOOKUP_EXCEPTIONS:
            jobs = []
        jobs_cache[run_id] = jobs
        return jobs

    def cached_job_conclusion(
        job_url: JobUrl,
        build_preset: BuildPreset,
        component: Component,
    ) -> JobConclusion:
        cache_key: JobsConclusionCacheKey = (job_url, build_preset, component)
        if cache_key in job_conclusion_cache:
            return job_conclusion_cache[cache_key]
        conclusion = resolve_job_conclusion(
            job_url,
            build_preset,
            component,
            current_run_id,
            jobs_for_run,
        )
        job_conclusion_cache[cache_key] = conclusion
        return conclusion

    for full_build_preset, build_preset in iter_build_preset_pairs(
        args.matrix_include, args.platform_name
    ):
        gs.complete_pr_comment_workload_checks(
            run_number=run_number,
            pr=pr,
            build_preset=full_build_preset,
            is_dry_run=args.is_dry_run,
            job_conclusion_resolver=lambda job_url, component, preset=build_preset: (
                cached_job_conclusion(job_url, preset, component)
            ),
        )
        gs.update_pr_comment_workload_status(
            run_number=run_number,
            pr=pr,
            build_preset=full_build_preset,
            is_dry_run=args.is_dry_run,
            workload_status=args.workload_status,
        )


if __name__ == "__main__":
    main()
