import time
import json
import os
import asyncio
import argparse
import datetime
from github import Github
from grpc import StatusCode
from typing import List
from nebius.sdk import SDK
from nebius.api.nebius.compute.v1 import (
    InstanceServiceClient,
    ListInstancesRequest,
    GetInstanceRequest,
)
from nebius.api.nebius.common.v1 import (
    Operation,
    ListOperationsRequest,
)
from nebius.aio.service_error import RequestError
from .helpers import setup_logger, github_output

logger = setup_logger()


async def filter_instances(instances, runners, args, now_ts, operation_service):
    vms_to_remove = []
    broken_to_remove = []
    matched_vm_ids = []
    idle_vm_ids = []
    busy_vm_ids = []

    for instance in instances:
        vm_id = instance.metadata.id
        labels = instance.metadata.labels
        condition = (
            labels.get("repo", "") != args.github_repo
            or labels.get("owner", "") != args.github_repo_owner  # noqa: W503
            or labels.get("runner-flavor", "") != args.flavor  # noqa: W503
            or instance.status.state.name != "RUNNING"  # noqa: W503
        )
        logger.info(
            "Instance %s labels: %s, state: %s",
            vm_id,
            ", ".join([f"{k}: {v}" for k, v in labels.items()]),
            instance.status.state.name,
        )
        if condition:
            logger.info(
                "Instance %s does not match criteria: %s",
                vm_id,
                condition,
            )
            continue

        # trying to calculate crashes by watching operation list for event "Recover instance"
        crash_count = 0
        operations: List[Operation] = []
        request = ListOperationsRequest(resource_id=vm_id)
        try:
            while True:
                response = await operation_service.list(request)
                operations.extend(response.operations)
                if not response.next_page_token:
                    break
                request.page_token = response.next_page_token

        except RequestError as e:
            logger.error(f"Error fetching operations for instance {vm_id}: %s", e)

        for operation in operations:
            if operation.description == "Recover Instance":
                crash_count += 1

        runner = next((r for r in runners if r.name == vm_id), None)
        logger.info(
            "Runner %s found: %s (id: %s, status: %s, busy: %s)",
            vm_id,
            runner,
            getattr(runner, "id", "N/A"),
            getattr(runner, "status", "N/A"),
            getattr(runner, "busy", "N/A"),
        )
        if runner is None or (runner.status == "offline" and runner.busy is False):
            logger.info(
                "Instance %s is not associated with a runner or the runner is offline and not busy, marking for removal",
                vm_id,
            )

            broken_to_remove.append(vm_id)
            continue

        matched_vm_ids.append(vm_id)
        age = int(now_ts - instance.metadata.created_at.timestamp())

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
            elif crash_count > 0:
                logger.info(
                    "Instance %s is idle, has %d crashes, marking for removal",
                    vm_id,
                    crash_count,
                )
                vms_to_remove.append(vm_id)
        elif runner and runner.busy:
            logger.info("Instance %s is busy, not marking for removal", vm_id)
            busy_vm_ids.append(vm_id)

    return matched_vm_ids, idle_vm_ids, busy_vm_ids, vms_to_remove, broken_to_remove


# rules are:
# 1. If all alive VMs are busy and there are VMs to remove, raise an exception.
# 2. If there are no alive VMs, create the maximum number of VMs to create.
# 3. If there are idle VMs, check if the number of idle VMs exceeds the maximum number
# of VMs to create if so, remove the excess idle VMs.
# 4. If there are busy VMs and the number of alive VMs is less than the maximum number
# of VMs to have, create the extra VMs if needed.
# 5. If there are no idle VMs and the number of alive VMs is less than the maximum number
# of VMs to have, create extra vms if needed but not more than the maximum number of VMs
# to create.
# 6. If max_vms_to_create is -1, downscale to 0 VMs to minimum idle vms. 0 if all vms are idle.
def decide_scaling(
    alive: int,
    idle: int,
    busy: int,
    remove: int,
    max_vms_to_create: int,
    maximum_amount_of_vms_to_have: int,
    extra_vms_if_needed: int,
) -> tuple[int, int, int]:
    logger.info("alive=%d, idle=%d, busy=%d, remove=%d", alive, idle, busy, remove)
    logger.info(
        "max_vms_to_create=%d, maximum_amount_of_vms_to_have=%d, extra_vms_if_needed=%d",
        max_vms_to_create,
        maximum_amount_of_vms_to_have,
        extra_vms_if_needed,
    )
    if max_vms_to_create == -1:
        logger.info("max_vms_to_create is -1, downscaling to 0 VMs")
        if busy > 0:
            logger.info(
                "There are busy VMs, not downscaling to 0 VMs, there will be %d VMs",
                alive - busy,
            )
        return 0, alive - busy, busy

    if alive == busy and remove > 0:
        raise ValueError("Cannot remove VMs when all alive VMs are busy. ")
    if idle + busy != alive:
        raise ValueError(
            "Passed values are not valid idle=%d + busy=%d != alive=%d"
            % (alive, idle, busy)
        )

    if alive == 0:
        logger.info(
            "No alive VMs, creating %d VM(s) to meet minimum target", max_vms_to_create
        )
        return max_vms_to_create, 0, max_vms_to_create

    excess_idle = max(0, idle - max_vms_to_create)
    projected_preview = alive - remove - excess_idle
    idle_remaining = idle - excess_idle

    to_create = 0
    logger.info(
        "excess_idle=%d, projected_preview=%d, idle_remaining=%d, busy=%d, ",
        excess_idle,
        projected_preview,
        idle_remaining,
        busy,
    )
    condition_projected_eq0 = projected_preview == 0
    condition_not_enough_vms = idle_remaining + busy < max_vms_to_create
    condition_less_than_maximum = projected_preview < maximum_amount_of_vms_to_have
    condition_idle_less_than_maximum = idle_remaining < max_vms_to_create
    condition_alive_minus_busy = alive - busy >= 0
    condition_extra_vms = (
        condition_less_than_maximum
        and condition_idle_less_than_maximum  # noqa: W503
        and condition_alive_minus_busy  # noqa: W503
    )

    logger.info(
        "Conditions: projected_preview(%d) == 0: %s",
        projected_preview,
        condition_projected_eq0,
    )
    logger.info(
        "Conditions: idle_remaining(%d) + busy(%d) < max_vms_to_create(%d): %s",
        idle_remaining,
        busy,
        max_vms_to_create,
        condition_not_enough_vms,
    )
    logger.info(
        "Conditions [1]: projected_preview(%d) < maximum_amount_of_vms_to_have(%d): %s",
        projected_preview,
        maximum_amount_of_vms_to_have,
        condition_less_than_maximum,
    )
    logger.info(
        "Conditions [2]: idle_remaining(%d) < max_vms_to_create(%d): %s",
        idle_remaining,
        max_vms_to_create,
        condition_idle_less_than_maximum,
    )
    logger.info(
        "Conditions [3]: alive(%d) - busy(%d) > 0: %s",
        alive,
        busy,
        condition_alive_minus_busy,
    )
    logger.info(
        "Conditions (combined): [1=%s] and [2=%s] and [3=%s]: %s",
        condition_less_than_maximum,
        condition_idle_less_than_maximum,
        condition_alive_minus_busy,
        condition_extra_vms,
    )
    if condition_projected_eq0:
        to_create = max_vms_to_create
        logger.info(
            "No VMs projected to remain, creating %d VM(s) to meet minimum target",
            to_create,
        )
    elif condition_not_enough_vms:
        to_create = min(
            max_vms_to_create - (idle_remaining + busy), extra_vms_if_needed
        )
        logger.info(
            "Not enough total VMs available (idle + busy = %d), creating %d VM(s) to reach the minimum required",
            idle_remaining + busy,
            to_create,
        )
    elif condition_extra_vms:
        to_create = min(
            extra_vms_if_needed, maximum_amount_of_vms_to_have - projected_preview
        )
        logger.info("Most VMs are busy, provisioning %d extra VM(s)", to_create)

    total_removals = max(remove, excess_idle)
    projected = alive - total_removals + to_create

    if projected > maximum_amount_of_vms_to_have:
        logger.info(
            "Projected VMs (%d) exceeds maximum allowed (%d), adjusting to maximum",
            projected,
            maximum_amount_of_vms_to_have,
        )
        return 0, 0, projected

    if projected == 0:
        raise ValueError("Projected VMs (%s) is 0, which is not allowed.", projected)

    return to_create, excess_idle, projected


# return True if we have something to create or remove
async def run(github: Github, sdk: SDK, args: argparse.Namespace) -> bool:
    now_ts = int(time.time())
    repo = github.get_repo(f"{args.github_repo_owner}/{args.github_repo}")
    instance_client = InstanceServiceClient(sdk)
    operation_client = instance_client.operation_service()
    instances = []
    try:
        logger.info("Listing instances from Nebius (with pagination)...")
        request = ListInstancesRequest(parent_id=args.parent_id)
        while True:
            response = await instance_client.list(request)
            instances.extend(response.items)
            if not response.next_page_token:
                break
            request.page_token = response.next_page_token
    except RequestError as err:
        logger.error("Failed to fetch instances from Nebius: %s", err)
        github_output(logger, "RUNNING_VMS_COUNT", "0")
        github_output(logger, "VMS_TO_REMOVE", "[]")
        github_output(logger, "VMS_TO_CREATE", "[]")
        github_output(logger, "DATE", str(now_ts))
        return False

    logger.info("Fetched %d instances", len(instances))
    runners = list(repo.get_self_hosted_runners())

    (
        matched_vm_ids,
        idle_vm_ids,
        busy_vm_ids,
        vms_to_remove,
        broken_to_remove,
    ) = await filter_instances(instances, runners, args, now_ts, operation_client)

    logger.info(
        "Total matched VMs: %d (Idle: %d, Busy: %d)",
        len(matched_vm_ids),
        len(idle_vm_ids),
        len(busy_vm_ids),
    )
    # calculating number of workflows that are queued and are waiting for runner with this flavor
    # checking workflows created in the last 8 hours, format >YYYY-MM-DDTHH:MM:SS+00:00
    queued_workflows = repo.get_workflow_runs(
        status="queued",
        created=">"
        + (  # noqa: W503
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=8)
        ).strftime("%Y-%m-%dT%H:%M:%S+00:00"),
    )
    queued_workflows_count = 0
    for workflow in queued_workflows:
        # search through workflow jobs to get labels for jobs with status queued
        jobs = workflow.jobs()
        for job in jobs:
            if job.status != "queued":
                continue
            labels = job.labels
            if f"runner_{args.flavor}" in labels:
                queued_workflows_count += 1
                logger.info(
                    "Found queued workflow %s with flavor %s",
                    workflow.id,
                    args.flavor,
                )
    logger.info(
        "Total queued workflows with flavor %s: %d",
        args.flavor,
        queued_workflows_count,
    )
    extra_vms_if_needed = (
        args.extra_vms_if_needed
        if queued_workflows_count == 0
        else queued_workflows_count
    )
    logger.info(
        "Total queued workflows with flavor %s: %d, resulting extra_vms_if_needed: %d",
        args.flavor,
        queued_workflows_count,
        extra_vms_if_needed,
    )
    to_create, excess_idle, projected_vm_count = decide_scaling(
        len(matched_vm_ids),
        len(idle_vm_ids),
        len(busy_vm_ids),
        len(vms_to_remove),
        args.max_vms_to_create,
        args.maximum_amount_of_vms_to_have,
        extra_vms_if_needed,
    )

    to_remove = vms_to_remove
    if excess_idle > 0:
        for vm_id in idle_vm_ids:
            if vm_id not in to_remove:
                to_remove.append(vm_id)
        logger.info("Resulting sorted to_remove vm list: %s", to_remove)
        to_remove = to_remove[:excess_idle]
        logger.info(
            "Excess idle VMs: %d, marking %d for removal: %s",
            excess_idle,
            len(to_remove),
            to_remove,
        )

    logger.info("PROJECTED_VM_COUNT=%d", projected_vm_count)
    logger.info("TO_CREATE=%d", to_create)
    logger.info("TO_REMOVE=%d", len(to_remove))

    vms_to_create = (
        [
            f"{args.flavor}-{args.github_repo_owner}-{args.github_repo}-{now_ts}-{i+1}"
            for i in range(to_create)
        ]
        if to_create > 0
        else []
    )

    github_output(logger, "VMS_TO_REMOVE", json.dumps(to_remove))
    github_output(logger, "VMS_TO_CREATE", json.dumps(vms_to_create))
    github_output(logger, "BROKEN_VMS_TO_REMOVE", json.dumps(broken_to_remove))
    result = False
    if to_create > 0 or len(to_remove) > 0 or len(broken_to_remove) > 0:
        result = True
        logger.info(
            "We have something to create or remove: %d to create, %d to remove, %d broken to remove",
            to_create,
            len(to_remove),
            len(broken_to_remove),
        )
    # clean up github runners that doesn't have a matching VM and are offline
    # also remove runners that will be removed, so they won't be used
    logger.info("Cleaning up GitHub runners that don't have a matching VM")
    for runner in list(repo.get_self_hosted_runners()):
        logger.info(
            "Runner %s (id: %s, status: %s, busy: %s)",
            runner.name,
            runner.id,
            runner.status,
            runner.busy,
        )
        if runner.status != "offline":
            logger.info("Runner %s is not offline, skipping", runner.name)
            continue

        try:
            request = GetInstanceRequest(id=runner.name)
            instance = await instance_client.get(request)
            if instance.status.state.name == "RUNNING":
                logger.info(
                    "Runner %s has a matching VM, skipping removal", runner.name
                )
                continue
        except RequestError as err:
            if err.status.code == StatusCode.NOT_FOUND:
                logger.info(
                    "Runner %s does not have a matching VM, removing runner",
                    runner.name,
                )
                if not repo.remove_self_hosted_runner(runner):
                    logger.error(
                        "Failed to remove runner %s (id: %s)", runner.name, runner.id
                    )
                    return result
                logger.info("Removed runner %s (id: %s)", runner.name, runner.id)

        if runner.name in to_remove:
            logger.info("Runner %s is marked for removal, removing runner", runner.name)
            if not repo.remove_self_hosted_runner(runner):
                logger.error(
                    "Failed to remove runner %s (id: %s)", runner.name, runner.id
                )
                return result
            logger.info("Removed runner %s (id: %s)", runner.name, runner.id)
    return result


async def main():
    parser = argparse.ArgumentParser(description="Manage GitHub runners on Nebius.")

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
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run the script in a loop, checking every --loop-interval seconds "
        + "until --loop-timeout is reached or we have something to create/remove",  # noqa: W503
    )
    parser.add_argument(
        "--loop-interval",
        type=int,
        default=60,  # initially there was 15s but there is some rate limiting on github api side
    )
    parser.add_argument(
        "--loop-timeout",
        type=int,
        default=3600,
        help="Timeout for the loop in seconds, after which it will stop",
    )
    args = parser.parse_args()
    logger.info("Parsed arguments: %s", args)

    github_token = os.environ.get("GITHUB_TOKEN")
    if not github_token:
        raise RuntimeError("GITHUB_TOKEN environment variable is not set")

    sdk = SDK(credentials_file_name=args.service_account_key)
    github = Github(github_token)

    if args.loop:
        start_time = time.time()
        while True:
            elapsed_time = time.time() - start_time
            if elapsed_time >= args.loop_timeout:
                logger.info("Loop timeout reached, exiting")
                break

            result = False
            try:
                result = await run(github, sdk, args)
            except Exception as e:
                logger.error("Error during run: %s", e)

            if result:
                logger.info("Something to create or remove, exiting loop")
                break

            logger.info(
                "Sleeping for %d seconds before next iteration", args.loop_interval
            )
            await asyncio.sleep(args.loop_interval)
    else:
        async with sdk:
            await run(github, sdk, args)


if __name__ == "__main__":
    asyncio.run(main())
