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
    get_jobs,
    github_client_from_env,
    load_github_event,
    parse_actions_job_url,
    pull_request_from_event,
    setup_logger,
)
from . import generate_summary as gs
from .status_plaque import WorkflowOutcome

JOB_LOOKUP_EXCEPTIONS = (AttributeError, ValueError) + PYGITHUB_RETRY_EXCEPTIONS

WorkflowRunId = int
JobUrl = str
BuildPreset = str
Component = str
TargetPlatform = str
JobConclusion = str | None
WorkflowJobs = list[WorkflowJob]
JobsCache = dict[WorkflowRunId, WorkflowJobs]
WorkflowOutcomeCacheKey = tuple[JobUrl, BuildPreset, Component, TargetPlatform]
WorkflowOutcomeCache = dict[WorkflowOutcomeCacheKey, WorkflowOutcome]
JobsForRun = Callable[[WorkflowRunId], WorkflowJobs]
NATIVE_TARGET_PLATFORM = "native"


def platform_name_for_target(default_platform_name: str, target_platform: str) -> str:
    if target_platform == "default-linux-armv9a_grace":
        return "linux-arm64"
    return default_platform_name


def iter_full_build_presets(
    matrix_include: str, default_platform_name: str
) -> list[str]:
    return [
        full_build_preset
        for full_build_preset, _, _ in iter_build_preset_contexts(
            matrix_include,
            default_platform_name,
        )
    ]


def iter_build_preset_contexts(
    matrix_include: str, default_platform_name: str
) -> list[tuple[str, str, str]]:
    if not matrix_include.strip():
        return []

    matrix = json.loads(matrix_include)
    return sorted(
        {
            (
                f"{platform_name_for_target(default_platform_name, entry.get('target_platform', ''))}-{entry['build_preset']}",
                entry["build_preset"],
                entry.get("target_platform", "") or NATIVE_TARGET_PLATFORM,
            )
            for entry in matrix.get("include", [])
        }
    )


def workload_job_marker(
    build_preset: str,
    component: str,
    target_platform: str,
) -> str:
    target_platform = target_platform or NATIVE_TARGET_PLATFORM
    return (
        f"[build_preset={build_preset} component={component} "
        f"target_platform={target_platform}]"
    )


def find_workload_job_conclusion(
    jobs: WorkflowJobs,
    build_preset: BuildPreset,
    component: Component,
    target_platform: TargetPlatform,
) -> JobConclusion:
    marker = workload_job_marker(build_preset, component, target_platform)
    matches = []
    for job in jobs:
        job_name = (getattr(job, "name", None) or "").rsplit(" / ", 1)[-1]
        if marker in job_name and job_name.startswith(BUILD_AND_TEST_JOB_NAME_PREFIX):
            matches.append(job)
    if len(matches) != 1:
        return None
    return getattr(matches[0], "conclusion", None)


def matching_workflow_jobs(
    jobs: WorkflowJobs,
    build_preset: BuildPreset,
    component: Component,
    target_platform: TargetPlatform,
    job_id: int | None = None,
) -> WorkflowJobs:
    marker = workload_job_marker(build_preset, component, target_platform)
    return [
        job
        for job in jobs
        if marker in (getattr(job, "name", None) or "").rsplit(" / ", 1)[-1]
        or (job_id is not None and getattr(job, "id", None) == job_id)
    ]


def find_workflow_conclusion(
    jobs: WorkflowJobs,
    build_preset: BuildPreset,
    component: Component,
    target_platform: TargetPlatform,
    job_id: int | None = None,
) -> JobConclusion:
    conclusions = [
        getattr(job, "conclusion", None)
        for job in matching_workflow_jobs(
            jobs, build_preset, component, target_platform, job_id
        )
    ]
    for failure in (
        "failure",
        "action_required",
        "startup_failure",
        "timed_out",
        "cancelled",
    ):
        if failure in conclusions:
            return failure
    if not conclusions or None in conclusions:
        return None
    if all(conclusion == "skipped" for conclusion in conclusions):
        return "skipped"
    if all(
        conclusion in ("success", "neutral", "skipped") for conclusion in conclusions
    ):
        return "success"
    return None


def resolve_job_conclusion(
    job_url: JobUrl,
    build_preset: BuildPreset,
    component: Component,
    target_platform: TargetPlatform,
    current_run_id: WorkflowRunId,
    jobs_for_run: JobsForRun,
    *,
    include_related_jobs: bool = False,
) -> JobConclusion:
    job_ref = parse_actions_job_url(job_url)
    if include_related_jobs:
        # A successful build job can be followed by a failed VM cleanup job.
        # Match the same component and platform without borrowing failures from
        # other matrix entries or a different workflow run.
        conclusion = find_workflow_conclusion(
            jobs_for_run(job_ref[0] if job_ref else current_run_id),
            build_preset,
            component,
            target_platform,
            job_ref[1] if job_ref else None,
        )
        if conclusion is not None:
            return conclusion
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
        target_platform,
    )


def resolve_workflow_outcome(
    job_url: JobUrl,
    build_preset: BuildPreset,
    component: Component,
    target_platform: TargetPlatform,
    current_run_id: WorkflowRunId,
    jobs_for_run: JobsForRun,
) -> WorkflowOutcome:
    job_ref = parse_actions_job_url(job_url)
    jobs = jobs_for_run(job_ref[0] if job_ref else current_run_id)
    related = matching_workflow_jobs(
        jobs, build_preset, component, target_platform, job_ref[1] if job_ref else None
    )
    execution = resolve_job_conclusion(
        job_url, build_preset, component, target_platform, current_run_id, jobs_for_run
    )
    cleanup = []
    other = []
    execution_job_found = False
    for job in related:
        name = (getattr(job, "name", None) or "").rsplit(" / ", 1)[-1]
        if (
            getattr(job, "id", None) == job_ref[1]
            if job_ref
            else name.startswith(BUILD_AND_TEST_JOB_NAME_PREFIX)
        ):
            execution_job_found = True
        conclusion = getattr(job, "conclusion", None)
        if name.startswith("Release self-hosted runner "):
            cleanup.append(conclusion)
        else:
            other.append(conclusion)
    successful = ("success", "neutral", "skipped")
    failed = ("failure", "timed_out", "action_required", "startup_failure")
    cleanup_only_failure = (
        execution == "success"
        and execution_job_found
        and any(conclusion in failed for conclusion in cleanup)
        and all(conclusion in successful + failed for conclusion in cleanup)
        and all(conclusion in successful for conclusion in other)
    )
    return WorkflowOutcome(
        execution=execution,
        workflow=find_workflow_conclusion(
            jobs,
            build_preset,
            component,
            target_platform,
            job_ref[1] if job_ref else None,
        ),
        cleanup_only_failure=cleanup_only_failure,
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

    github = github_client_from_env()
    pr = pull_request_from_event(github, load_github_event())
    run_number = int(os.environ.get("GITHUB_RUN_NUMBER", "0"))
    current_run_id = int(os.environ.get("GITHUB_RUN_ID", "0"))
    jobs_cache: JobsCache = {}
    workflow_outcome_cache: WorkflowOutcomeCache = {}

    def jobs_for_run(run_id: WorkflowRunId) -> WorkflowJobs:
        if run_id in jobs_cache:
            return jobs_cache[run_id]
        try:
            jobs = get_jobs(
                github,
                os.environ["GITHUB_REPOSITORY"],
                run_id,
            )
        except JOB_LOOKUP_EXCEPTIONS:
            jobs = []
        jobs_cache[run_id] = jobs
        return jobs

    def cached_workflow_outcome(
        job_url: JobUrl,
        build_preset: BuildPreset,
        component: Component,
        target_platform: TargetPlatform,
    ) -> WorkflowOutcome:
        cache_key: WorkflowOutcomeCacheKey = (
            job_url,
            build_preset,
            component,
            target_platform,
        )
        if cache_key in workflow_outcome_cache:
            return workflow_outcome_cache[cache_key]
        outcome = resolve_workflow_outcome(
            job_url,
            build_preset,
            component,
            target_platform,
            current_run_id,
            jobs_for_run,
        )
        workflow_outcome_cache[cache_key] = outcome
        return outcome

    for full_build_preset, build_preset, target_platform in iter_build_preset_contexts(
        args.matrix_include, args.platform_name
    ):

        def workflow_outcome_resolver(
            job_url: JobUrl,
            component: Component,
            preset: BuildPreset = build_preset,
            platform: TargetPlatform = target_platform,
        ) -> WorkflowOutcome:
            return cached_workflow_outcome(
                job_url,
                preset,
                component,
                platform,
            )

        gs.complete_pr_comment_workload_checks(
            run_number=run_number,
            pr=pr,
            build_preset=full_build_preset,
            is_dry_run=args.is_dry_run,
            job_conclusion_resolver=lambda url, component: workflow_outcome_resolver(
                url, component
            ).execution,
            workflow_outcome_resolver=workflow_outcome_resolver,
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
