import time
import json
import os
import asyncio
import argparse
from github import Github
from nebius.sdk import SDK
from nebius.api.nebius.compute.v1 import InstanceServiceClient, ListInstancesRequest
from nebius.aio.service_error import RequestError
from .helpers import setup_logger, github_output

logger = setup_logger()


async def main():
    parser = argparse.ArgumentParser(description="Manage GitHub runners on Nebius.")
    parser.add_argument(
        "--api-endpoint", default="api.ai.nebius.cloud", help="Cloud API endpoint"
    )
    parser.add_argument(
        "--service-account-key",
        required=True,
        help="Path to the service account credentials file (JSON)",
    )
    parser.add_argument(
        "--github-repo-owner",
        required=True,
        default="ydb-platform",
        help="GitHub repository owner name",
    )
    parser.add_argument(
        "--github-repo", required=True, default="nbs", help="GitHub repository name"
    )
    parser.add_argument(
        "--parent-id",
        required=True,
        help="Parent folder or project ID for VM placement",
    )
    parser.add_argument(
        "--flavor",
        required=True,
        choices=["light", "heavy"],
        help="VM flavor label to match against",
    )
    parser.add_argument(
        "--vms-older-than",
        type=int,
        required=True,
        help="Minimum VM age (in seconds) before it can be deleted if idle",
    )
    parser.add_argument(
        "--max-vms-to-create",
        type=int,
        required=True,
        help="Maximum number of VMs to create if idle VMs are less than this",
    )
    parser.add_argument(
        "--maximum-amount-of-vms-to-have",
        type=int,
        required=True,
        help="Hard cap on total number of VMs allowed",
    )
    parser.add_argument(
        "--extra-vms-if-needed",
        type=int,
        default=1,
        help="Number of additional VMs to create when most are busy",
    )
    args = parser.parse_args()
    logger.info("Parsed arguments: %s", args)

    github_token = os.environ.get("GITHUB_TOKEN")
    if not github_token:
        raise RuntimeError("GITHUB_TOKEN environment variable is not set")

    now_ts = int(time.time())
    sdk = SDK(credentials_file_name=args.service_account_key)
    github = Github(github_token)
    repo = github.get_repo(f"{args.github_repo_owner}/{args.github_repo}")
    instance_client = InstanceServiceClient(sdk)

    instances = []
    try:
        logger.info("Listing instances from Nebius (with pagination)...")
        async with sdk:
            request = ListInstancesRequest(parent_id=args.parent_id)
            while True:
                response = await instance_client.list(request)
                instances.extend(response.items)
                if not response.next_page_token:
                    break
                request.page_token = response.next_page_token
    except RequestError as err:
        logger.error("Failed to fetch instances from Nebius: %s", err)
        github_output("RUNNING_VMS_COUNT", "0")
        github_output("VMS_TO_REMOVE", "[]")
        github_output("VMS_TO_CREATE", "[]")
        github_output("DATE", str(now_ts))
        return

    logger.info("Fetched %d instances", len(instances))
    runners = list(repo.get_self_hosted_runners())
    vms_to_remove = []
    matched_vm_ids = []
    idle_vm_ids = []
    busy_vm_ids = []

    for instance in instances:
        labels = instance.metadata.labels
        condition = (
            labels.get("repo", "") != args.github_repo
            or labels.get("owner", "") != args.github_repo_owner  # noqa: W503
            or labels.get("runner-flavor", "") != args.flavor  # noqa: W503
            or instance.status.state.name != "RUNNING"  # noqa: W503
        )
        logger.info(
            "Instance %s labels: %s, state: %s",
            instance.metadata.id,
            ", ".join([f"{k}: {v}" for k, v in labels.items()]),
            instance.status.state.name,
        )
        if condition:
            logger.info(
                "Instance %s does not match criteria: %s",
                instance.metadata.id,
                condition,
            )
            continue

        vm_id = instance.metadata.id
        matched_vm_ids.append(vm_id)
        age = int(time.time() - instance.metadata.created_at.timestamp())
        runner = next((r for r in runners if r.name == vm_id), None)
        logger.info(
            "Runner %s found: %s (id: %s, status: %s, busy: %s)",
            vm_id,
            runner,
            getattr(runner, "id", "N/A"),
            getattr(runner, "status", "N/A"),
            getattr(runner, "busy", "N/A"),
        )

        if runner and not runner.busy:
            idle_vm_ids.append(vm_id)
            if age > args.vms_older_than:
                logger.info(
                    "Instance %s is idle and its age is %d seconds (which is older than %d), marking for removal",
                    vm_id,
                    age,
                    args.vms_older_than,
                )
                vms_to_remove.append(vm_id)
        elif runner and runner.busy:
            logger.info("Instance %s is busy, not marking for removal", vm_id)
            busy_vm_ids.append(vm_id)

    logger.info(
        "Total matched VMs: %d (Idle: %d, Busy: %d)",
        len(matched_vm_ids),
        len(idle_vm_ids),
        len(busy_vm_ids),
    )

    # Downscale if too many idle VMs
    excess_idle = len(idle_vm_ids) - args.max_vms_to_create
    logger.info("Excess idle: %d", excess_idle)
    if excess_idle > 0:
        to_remove = idle_vm_ids[:excess_idle]
        vms_to_remove.extend(
            [vm_id for vm_id in to_remove if vm_id not in vms_to_remove]
        )

    # Determine if extra VMs should be created due to busy capacity
    projected_vm_count = len(matched_vm_ids) - len(vms_to_remove)
    idle_remaining = len(idle_vm_ids) - excess_idle

    to_create = 0
    if idle_remaining < args.max_vms_to_create:
        to_create = args.max_vms_to_create - idle_remaining
        logger.info("Need more idle VMs to reach target: creating %d", to_create)
    elif (
        len(busy_vm_ids) >= max(1, projected_vm_count - 1)
        and projected_vm_count < args.maximum_amount_of_vms_to_have  # noqa: W503
    ):
        to_create = min(
            args.extra_vms_if_needed,
            args.maximum_amount_of_vms_to_have - projected_vm_count,
        )
        logger.info("Most VMs are busy, provisioning %d extra VM(s)", to_create)

    if projected_vm_count == 0:
        logger.info(
            "No VMs will be running, creating %d VM(s) to reach the minimum required",
            args.max_vms_to_create,
        )
        to_create = args.max_vms_to_create

    logger.info("PROJECTED_VM_COUNT=%d", projected_vm_count)
    logger.info("FINAL_TO_CREATE=%d", to_create)
    logger.info("FINAL_TO_REMOVE=%d", len(vms_to_remove))

    vms_to_create = (
        [
            f"{args.flavor}-{args.github_repo_owner}-{args.github_repo}-{now_ts}-{i+1}"
            for i in range(to_create)
        ]
        if to_create > 0
        else []
    )

    github_output("VMS_TO_REMOVE", json.dumps(vms_to_remove))
    github_output("VMS_TO_CREATE", json.dumps(vms_to_create))

if __name__ == "__main__":
    asyncio.run(main())
