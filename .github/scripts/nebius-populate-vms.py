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
        help="Number of VMs to create if capacity allows",
    )
    parser.add_argument(
        "--maximum-amount-of-vms-to-have",
        type=int,
        required=True,
        help="Hard cap on total number of VMs allowed",
    )
    parser.add_argument(
        "--min-idle-vms",
        type=int,
        default=1,
        help="Minimum number of idle VMs to maintain before creating more",
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
    logger.info("Instances: %s", instances)
    runners = list(repo.get_self_hosted_runners())
    vms_to_remove = []
    running_vm_names = []
    idle_vm_names = []

    for instance in instances:
        logger.info("Processing instance %s", instance.metadata.name)
        labels = instance.metadata.labels
        logger.info("Instance labels: %s", labels)

        logger.info(
            "Owner: %s == %s = %s",
            labels.get("owner"),
            args.github_repo_owner,
            labels.get("owner") == args.github_repo_owner,
        )
        logger.info(
            "Repo: %s == %s = %s",
            labels.get("repo"),
            args.github_repo,
            labels.get("repo") == args.github_repo,
        )
        logger.info(
            "Flavor: %s == %s = %s",
            labels.get("runner-flavor"),
            args.flavor,
            labels.get("runner-flavor") == args.flavor,
        )
        logger.info(
            "Status: %s == %s = %s",
            instance.status.state.name,
            "RUNNING",
            instance.status.state.name == "RUNNING",
        )

        condition = (
            labels.get("repo", "") == args.github_repo
            and labels.get("owner", "") == args.github_repo_owner  # noqa: W503
            and labels.get("runner-flavor", "") == args.flavor  # noqa: W503
            and instance.status.state.name == "RUNNING"  # noqa: W503
        )
        logger.info(
            "Instance condition: repo: %s, owner: %s, runner-flavor: %s, status: %s overall: %s, not: %s",
            labels.get("repo", "") == args.github_repo,
            labels.get("owner", "") == args.github_repo_owner,
            labels.get("runner-flavor", "") == args.flavor,
            instance.status.state.name == "RUNNING",
            condition,
            not condition,
        )
        if not condition:
            logger.info(
                "Instance %s does not match criteria, skipping", instance.metadata.name
            )
            continue

        vm_name = instance.metadata.name
        vm_id = instance.metadata.id
        created_ts = int(instance.metadata.created_at.timestamp())
        age = now_ts - created_ts
        running_vm_names.append(vm_name)

        logger.info("Instance %s is %d seconds old", vm_name, age)

        runner = next((r for r in runners if r.name == vm_id), None)
        logger.info(
            "Runner %s found: %s (id: %s, status: %s, busy: %s, )",
            vm_id,
            runner,
            runner.id if runner else "N/A",
            runner.status if runner else "N/A",
            runner.busy if runner else "N/A",
        )

        if runner and not runner.busy:
            idle_vm_names.append(vm_name)

        if age > args.vms_older_than:
            if runner and not runner.busy:
                logger.info("Marking VM %s as idle and eligible for removal", vm_name)
                vms_to_remove.append(vm_id)
            else:
                logger.info("VM %s is busy or not found in GitHub", vm_name)

    running_count = len(running_vm_names)
    idle_count = len(idle_vm_names)
    remove_count = len(vms_to_remove)
    available_after_removal = running_count - remove_count
    to_create = 0

    if running_count < args.max_vms_to_create:
        logger.info(
            "Current VM count (%d) is below max allowed to create (%d)",
            running_count,
            args.max_vms_to_create,
        )
        to_create = args.max_vms_to_create - available_after_removal
    elif running_count < args.maximum_amount_of_vms_to_have:
        logger.info(
            "Running count (%d) is >= max_vms_to_create (%d) but < hard limit (%d), allowing up to %d extra",
            running_count,
            args.max_vms_to_create,
            args.maximum_amount_of_vms_to_have,
            args.extra_vm_if_needed,
        )
        to_create = min(
            args.extra_vm_if_needed, args.maximum_amount_of_vms_to_have - running_count
        )
    else:
        logger.info("VM count already at or above hard limit, no creation allowed")

    total_if_created = running_count + to_create - remove_count
    if total_if_created > args.maximum_amount_of_vms_to_have:
        to_create = max(
            0, args.maximum_amount_of_vms_to_have - running_count + remove_count
        )
        logger.info("Capping creation to avoid exceeding maximum VM count")

    vms_to_create = (
        [
            f"{args.flavor}-{args.github_repo_owner}-{args.github_repo}-{now_ts}-{i+1}"
            for i in range(to_create)
        ]
        if to_create > 0
        else []
    )

    logger.info("RUNNING_VMS_COUNT=%d", running_count)
    logger.info("VMS_COUNT_TO_REMOVE=%d", remove_count)
    logger.info("IDLE_VMS_COUNT=%d", idle_count)
    logger.info("MAX_VMS_TO_CREATE=%d", args.max_vms_to_create)
    logger.info("NUMBER_VMS_TO_CREATE=%d", to_create)

    github_output("RUNNING_VMS_COUNT", str(running_count))
    github_output("VMS_TO_REMOVE", json.dumps(vms_to_remove))
    github_output("VMS_TO_CREATE", json.dumps(vms_to_create))
    github_output("DATE", str(now_ts))


if __name__ == "__main__":
    asyncio.run(main())
