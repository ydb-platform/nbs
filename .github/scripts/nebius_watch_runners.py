#!/usr/bin/env python3
import os
import asyncio
import argparse
from github import Github
from tabulate import tabulate
from .helpers import (
    setup_logger,
    get_jobs_raw,
    compact_workflow_name,
    compact_job_name,
    date_to_hms,
)
import datetime
from typing import List

from nebius.sdk import SDK
from nebius.aio.service_error import RequestError
from nebius.api.nebius.compute.v1 import (
    InstanceServiceClient,
    GetInstanceRequest,
)
from nebius.api.nebius.common.v1 import (
    Operation,
    ListOperationsRequest,
)

logger = setup_logger()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Show self-hosted runners and their active jobs."
    )
    parser.add_argument(
        "--api-endpoint",
        default="api.ai.nebius.cloud",
        help="Cloud API Endpoint",
    )
    parser.add_argument(
        "--service-account-key",
        required=True,
        help="Path to the service account key file",
    )
    parser.add_argument("--owner", required=True, help="GitHub organization or user")
    parser.add_argument("--repo", required=True, help="GitHub repository name")
    parser.add_argument(
        "--token", help="GitHub access token (or set GITHUB_TOKEN env variable)"
    )
    parser.add_argument(
        "--parent-id",
        required=True,
        default="project-e00p3n19h92mcw7vke",
        help="Parent ID where the VM will be created",
    )
    return parser.parse_args()


async def main():
    args = parse_args()
    token = args.token or os.environ.get("GITHUB_TOKEN")
    if not token:
        print(
            "Error: GitHub token must be provided with --token or GITHUB_TOKEN environment variable."
        )
        exit(1)

    sdk = SDK(credentials_file_name=args.service_account_key)
    service = InstanceServiceClient(sdk)
    operation_service = service.operation_service()

    g = Github(token)
    repo = g.get_repo(f"{args.owner}/{args.repo}")

    # Fetch self-hosted runners
    runners = repo.get_self_hosted_runners()

    # Map of runner name -> current job
    active_jobs = {}
    workflow_runs = repo.get_workflow_runs(status="in_progress")
    queued_workflows = repo.get_workflow_runs(
        status="queued",
        created=">"
        + (  # noqa: W503
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=8)
        ).strftime("%Y-%m-%dT%H:%M:%S+00:00"),
    )
    queued_workflows_runs = []
    for run in queued_workflows:
        queued_workflows_runs.append(run.id)

    for run in workflow_runs:
        for job in get_jobs_raw(token, repo.full_name, run.id):
            if job.status in ("in_progress", "queued") and job.runner_name:
                active_jobs[job.runner_name] = {
                    "job_name": job.name,
                    "job_id": job.id,
                    "job_took_raw": job.created_at,
                    "job_took_real": job.started_at,
                    "run_id": run.id,
                    "workflow": run.name,
                }

    # Prepare data
    table = []
    for runner in runners:
        runner_id = runner.id
        name = runner.name
        status = runner.status
        busy = runner.busy
        current_job = active_jobs.get(name)
        took_real = (
            current_job["job_took_real"]
            if current_job
            else datetime.datetime.now(tz=datetime.timezone.utc)
        )
        took_raw = (
            current_job["job_took_raw"]
            if current_job
            else datetime.datetime.now(tz=datetime.timezone.utc)
        )
        runner_label = ", ".join(
            label["name"]
            for label in runner.labels()
            if label["name"].startswith("runner")
        )
        job_info = (
            f'{current_job["job_name"].split("/")[-1].strip()}' if current_job else ""
        )

        workflow_info = f'{current_job["workflow"]}' if current_job else ""

        job_id = f'{current_job["job_id"]}' if current_job else ""
        workflow_id = f'{current_job["run_id"]}' if current_job else ""

        # calculate age of the instance
        try:
            response = await service.get(GetInstanceRequest(id=name))
        except RequestError:
            logger.error(f"Error fetching instance {runner_id}")

        age_str = date_to_hms(response.metadata.created_at)

        ip = "N/A"
        if response.status.state.name == "RUNNING":
            ip = response.status.network_interfaces[0].public_ip_address.address
            ip = ip.split("/")[0]

        # trying to calculate crashes by watching operation list for event "Recover instance"
        crash_count = 0
        operations: List[Operation] = []
        request = ListOperationsRequest(resource_id=name)
        try:
            while True:
                response = await operation_service.list(request)
                operations.extend(response.operations)
                if not response.next_page_token:
                    break
                request.page_token = response.next_page_token

        except RequestError as e:
            logger.error(f"Error fetching operations for instance {name}: %s", e)

        for operation in operations:
            if operation.description == "Recover Instance":
                crash_count += 1

        table.append(
            [
                runner_id,
                age_str,
                ip,
                name,
                status,
                "BUSY" if busy else "FREE",
                runner_label.replace("runner_", "").strip(),
                compact_workflow_name(workflow_info),
                compact_job_name(job_info),
                workflow_id,
                job_id,
                date_to_hms(took_real),
                date_to_hms(took_raw),
                crash_count,
            ]
        )
    # sort by type and then by id
    table = sorted(table, key=lambda x: (x[6], x[0]))

    # Display
    headers = [
        "ID",
        "Age",
        "IP",
        "Runner Name",
        "Status",
        "Busy",
        "Type",
        "Workflow",
        "Job",
        "Workflow ID",
        "Job ID",
        "Real",
        "Raw",
        "Crashes",
    ]
    print(tabulate(table, headers=headers))
    print(f"Queued workflows: {len(queued_workflows_runs)}")


if __name__ == "__main__":
    asyncio.run(main())
