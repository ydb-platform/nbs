import argparse
import math
import os
from pathlib import Path
import random
import shlex
import string
import time
import grpc
from typing import Optional
import asyncio
import requests
import yaml
from .helpers import (
    setup_logger,
    github_output,
    KeyValueAction,
    SENSITIVE_DATA_VALUES,
    truthy,
    retry,
    GITHUB_API_RETRY_ATTEMPTS,
    GITHUB_API_RETRY_INTERVAL_SEC,
    GITHUB_API_TIMEOUT_SEC,
    GITHUB_RUNNER_LATEST_VERSION,
    format_github_response_debug,
    resolve_github_runner_release,
)
from github import Auth as GithubAuth
from github import Github

from nebius.sdk import SDK
from nebius.aio.cli_config import Config
from nebius.aio.service_error import RequestError
from nebius.api.nebius.common.v1 import ResourceMetadata, GetByNameRequest
from nebius.api.nebius.compute.v1 import (
    DiskServiceClient,
    DiskSpec,
    CreateDiskRequest,
    InstanceServiceClient,
    InstanceSpec,
    CreateInstanceRequest,
    AttachedDiskSpec,
    NetworkInterfaceSpec,
    IPAddress,
    PublicIPAddress,
    ResourcesSpec,
    GetInstanceRequest,
    ListInstancesRequest,
    DeleteInstanceRequest,
    DeleteDiskRequest,
    ExistingDisk,
)

logger = setup_logger()

DISK_NAME_PREFIX = "disk-"
CLOUD_INIT_SCRIPT_TEMPLATE = (
    Path(__file__).parent / "templates" / "nebius_runner_cloud_init.sh"
)
RUNNER_REGISTRATION_RETRY_ATTEMPTS = 60
RUNNER_REGISTRATION_RETRY_INTERVAL_SEC = 10
CREATE_VM_RETRY_ATTEMPTS = 30 * 60 // 5
CREATE_VM_RETRY_INTERVAL_SEC = 5
PRESETS = {
    "cpu-d3": [
        "4vcpu-16gb",
        "8vcpu-32gb",
        "16vcpu-64gb",
        "32vcpu-128gb",
        "48vcpu-192gb",
        "64vcpu-256gb",
        "96vcpu-384gb",
        "128vcpu-512gb",
        "160vcpu-640gb",
        "192vcpu-768gb",
        "224vcpu-896gb",
        "256vcpu-1024gb",
    ],
    "cpu-e2": [
        "2vcpu-8gb",
        "4vcpu-16gb",
        "8vcpu-32gb",
        "16vcpu-64gb",
        "32vcpu-128gb",
        "48vcpu-192gb",
        "64vcpu-256gb",
        "80vcpu-320gb",
    ],
}


def generate_github_label():
    generated_string = "".join(
        random.choices(string.ascii_lowercase + string.digits, k=8)
    )
    logger.info("Generated label: %s", generated_string)
    return generated_string


def fetch_github_team_public_keys(gh: Github, github_org: str, team_slug: str):
    org = gh.get_organization(github_org)
    team = org.get_team_by_slug(team_slug)
    members = [member for member in team.get_members()]

    ssh_keys = []
    logger.info(
        "Fetching SSH keys for members: %s",
        ", ".join([member.login for member in members]),
    )
    member_keys_count = 0
    for member in members:

        for key in member.get_keys():
            member_keys_count += 1
            ssh_keys.append(key.key)

        logger.debug("Fetched %d SSH keys for %s", member_keys_count, member.login)

    logger.debug(f"Fetched SSH keys: {ssh_keys}")
    return ssh_keys


def shell_quote_template_value(value) -> str:
    if isinstance(value, bool):
        value = "true" if value else "false"
    return shlex.quote(str(value))


def render_cloud_init_runner_script(
    owner: str,
    repo: str,
    user: str,
    token: str,
    version: str,
    sha256_by_arch: dict[str, str],
    override_existing_runner: bool,
    label: str,
) -> str:
    template = CLOUD_INIT_SCRIPT_TEMPLATE.read_text()
    return template.format(
        override_existing_runner=shell_quote_template_value(override_existing_runner),
        version=shell_quote_template_value(version),
        sha256_x64=shell_quote_template_value(sha256_by_arch.get("x64", "")),
        sha256_arm64=shell_quote_template_value(sha256_by_arch.get("arm64", "")),
        label=shell_quote_template_value(label),
        repo_url=shell_quote_template_value(f"https://github.com/{owner}/{repo}"),
        runner_user=shell_quote_template_value(user),
        token=shell_quote_template_value(token),
    )


def generate_cloud_init_script(
    user: str,
    ssh_keys: list[str],
    owner: str,
    repo: str,
    token: str,
    version: str,
    sha256_by_arch: dict[str, str],
    override_existing_runner: bool,
    label: str,
):
    if os.environ.get("GITHUB_REPOSITORY"):
        label += (
            f",GITHUB_REPOSITORY_{os.environ['GITHUB_REPOSITORY'].replace('/', '_')}"
        )

    for item in ["GITHUB_SHA", "GITHUB_REF", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT"]:
        if os.environ.get(item):
            label += f",{item}_{os.environ[item]}"

    script = render_cloud_init_runner_script(
        owner=owner,
        repo=repo,
        user=user,
        token=token,
        version=version,
        sha256_by_arch=sha256_by_arch,
        override_existing_runner=override_existing_runner,
        label=label,
    )

    cloud_init = {
        "runcmd": [script],
        "manage_etc_hosts": "true",
        "ssh_pwauth": False,
        "users": [
            {
                "name": user,
                "passwd": os.environ["VM_USER_PASSWD"],
                "lock_passwd": False,
                "shell": "/bin/bash",
            },
            {
                "name": "debug",
                "sudo": "ALL=(ALL) NOPASSWD:ALL",
                "passwd": os.environ["VM_USER_PASSWD"],
                "lock_passwd": False,
                "shell": "/bin/bash",
            },
        ],
    }
    if ssh_keys:
        logger.info("Adding SSH keys to cloud-init")
        for user_config in cloud_init["users"]:
            user_config["ssh_authorized_keys"] = ssh_keys

    logger.info(
        f"Cloud-init: \n{yaml.safe_dump(cloud_init, default_flow_style=False, width=math.inf)}"
    )
    return (
        "#cloud-config\n"
        + yaml.safe_dump(  # noqa: W503
            cloud_init, default_flow_style=False, width=math.inf
        ),  # noqa: W503
        ssh_keys,
    )


def resolve_runner_release_for_update(
    version: str,
    github_token: str,
    override_existing_runner: str,
) -> tuple[str, dict[str, str]]:
    if not truthy(override_existing_runner):
        logger.info("Runner update is disabled, skipping GitHub runner release lookup")
        return "", {}

    release = resolve_github_runner_release(version, github_token)
    logger.info(
        "Resolved GitHub runner release for update: version=%s sha256_by_arch=%s",
        release.version,
        release.sha256_by_arch,
    )
    return release.version, release.sha256_by_arch


@retry(
    attempts=GITHUB_API_RETRY_ATTEMPTS,
    interval_sec=GITHUB_API_RETRY_INTERVAL_SEC,
    retry_exceptions=(requests.exceptions.RequestException, ValueError),
)
def get_runner_token(
    github_repo_owner: str, github_repo: str, github_token: str
) -> str:
    response = requests.post(
        f"https://api.github.com/repos/{github_repo_owner}/{github_repo}/actions/runners/registration-token",
        headers={
            "Authorization": f"Bearer {github_token}",
            "Accept": "application/vnd.github+json",
            "X-Github-Api-Version": "2022-11-28",
        },
        timeout=GITHUB_API_TIMEOUT_SEC,
    )
    logger.debug(
        "GitHub runner registration token response status=%s content_type=%s",
        response.status_code,
        response.headers.get("content-type"),
    )

    try:
        result = response.json()
    except ValueError as e:
        raise ValueError(
            "Failed to parse GitHub runner registration token response "
            f"({format_github_response_debug(response)}): {e}"
        ) from None

    if not response.ok:
        raise ValueError(
            "GitHub runner registration token request failed "
            f"({format_github_response_debug(response)}): {result}"
        )

    token = result.get("token")
    expires_at = result.get("expires_at")
    if token:
        # Mask the token in the logs
        print(f"::add-mask::{token}")
        SENSITIVE_DATA_VALUES["runner_token"] = token
        logger.debug("Got runner registration token valid till: %s", expires_at)
        return token
    else:
        raise ValueError(
            "Failed to get runner registration token "
            f"({format_github_response_debug(response)}): {result}"
        )


def find_runner_by_name(
    client: Github, github_repo_owner: str, github_repo: str, vm_id: str
) -> Optional[str]:
    logger.info(
        "Checking whether VM %s is registered as Github Runner",
        vm_id,
    )

    runners = client.get_repo(
        f"{github_repo_owner}/{github_repo}"
    ).get_self_hosted_runners()

    runner_id = None
    for runner in runners:
        if runner.name == vm_id:
            runner_id = runner.id
            break

    if runner_id is None:
        logger.info("Runner with name %s not found", vm_id)
        return None

    logger.info("Runner with name %s found", vm_id)
    return runner_id


@retry(
    attempts=RUNNER_REGISTRATION_RETRY_ATTEMPTS,
    interval_sec=RUNNER_REGISTRATION_RETRY_INTERVAL_SEC,
    retry_result=lambda result: result is None,
)
def wait_runner_by_name(
    client: Github, github_repo_owner: str, github_repo: str, vm_id: str
) -> Optional[str]:
    return find_runner_by_name(client, github_repo_owner, github_repo, vm_id)


def build_disk_request(
    parent_id: str,
    name: str,
    disk_size: int,
    image_id: str,
) -> CreateDiskRequest:
    return CreateDiskRequest(
        metadata=ResourceMetadata(
            parent_id=parent_id,
            name=DISK_NAME_PREFIX + name,
        ),
        spec=DiskSpec(
            type=DiskSpec.DiskType.NETWORK_SSD_NON_REPLICATED,
            size_gibibytes=disk_size,
            source_image_id=image_id,
        ),
    )


async def create_disk(sdk: SDK, args: argparse.Namespace) -> str:
    service = DiskServiceClient(sdk)
    disk_request = build_disk_request(
        parent_id=args.parent_id,
        name=args.name,
        disk_size=args.disk_size,
        image_id=args.image_id,
    )
    if not args.apply:
        logger.info("Would create disk with request %s", disk_request)
        return "0"
    else:
        logger.info("Creating disk with request %s", disk_request)

    request = None
    try:
        request = await service.create(disk_request)
        await request.wait()
    except TypeError:
        logger.error("Failed to create disk request", exc_info=True)
        raise
    except RequestError as e:
        logger.error("Failed to create disk with request: %s", disk_request)
        logger.error("Response: %s", str(e.status), exc_info=True)
        if request is not None:
            logger.error("Removing created disk with ID %s", request.resource_id)
            await remove_disk_by_id(sdk, args, request.resource_id)
        raise

    logger.info(
        "Created disk with ID %s status:%s",
        request.resource_id,
        request.status(),
    )

    return request.resource_id


def report_create_vm_final_failure(exception: BaseException) -> None:
    if os.environ.get("GITHUB_EVENT_NAME") != "pull_request":
        return

    pr_number = int(os.environ.get("GITHUB_REF").split("/")[-1])
    repo_name = os.environ.get("GITHUB_REPOSITORY")
    github_token = os.environ.get("GITHUB_TOKEN")
    comment = f"VM creation failed after 30 minutes: {exception}"
    gh = Github(auth=GithubAuth.Token(github_token))
    repo = gh.get_repo(repo_name)
    pr = repo.get_pull(pr_number)
    pr.create_issue_comment(comment)


def validate_create_args(
    platform_id: str,
    preset: str,
    disk_type: str,
    disk_size: int,
) -> None:
    if preset not in PRESETS.get(platform_id, []):
        raise Exception(f"Preset {preset} is not available for platform {platform_id}")

    if os.environ.get("VM_USER_PASSWD") is None:
        raise Exception("VM_USER_PASSWD environment variable is not set")

    if os.environ.get("GITHUB_TOKEN") is None:
        raise Exception("GITHUB_TOKEN environment variable is not set")

    if (
        disk_type in ["network-ssd-nonreplicated", "network-ssd-io-m3"]
        and disk_size % 93 != 0  # noqa: W503
    ):
        raise ValueError(
            "Disk size must be a multiple of 93GB for the selected disk type"
        )


def maybe_downgrade_preset(
    platform_id: str,
    preset: str,
    allow_downgrade: bool,
    downgrade_after: int,
    attempt: int,
) -> str:
    # We will downgrade every N failed attempts:
    # if our preset is 80cpu and downgrade_after is 2, on the third
    # attempt it will be downgraded to 64cpu, then to 48cpu.
    logger.info("Attempt %d", attempt)
    logger.info("Current preset %s", preset)
    logger.info("Downgrade after %d", downgrade_after)
    logger.info("Allow downgrade %s", allow_downgrade)
    logger.info("attempt mod downgrade_after = %d", attempt % downgrade_after)
    logger.info("attempt > 0 = %s", attempt > 0)
    if allow_downgrade and attempt % downgrade_after == 0 and attempt > 0:
        current_preset_index = PRESETS[platform_id].index(preset)
        if current_preset_index > 0:
            preset = PRESETS[platform_id][current_preset_index - 1]
            logger.info("Downgrading to %s preset", preset)
    return preset


def build_vm_labels(labels: dict, runner_github_label: str, runner_flavor: str) -> dict:
    labels = dict(labels)
    labels["runner-label"] = runner_github_label
    labels["runner-flavor"] = runner_flavor
    return labels


def build_instance_request(
    parent_id: str,
    name: str,
    platform_id: str,
    preset: str,
    subnet_id: str,
    no_public_ip: bool,
    disk_id: str,
    user_data: str,
    labels: dict,
) -> CreateInstanceRequest:
    return CreateInstanceRequest(
        metadata=ResourceMetadata(
            parent_id=parent_id,
            name=name,
            labels=labels,
        ),
        spec=InstanceSpec(
            stopped=False,
            cloud_init_user_data=user_data,
            resources=ResourcesSpec(platform=platform_id, preset=preset),
            boot_disk=AttachedDiskSpec(
                attach_mode=AttachedDiskSpec.AttachMode.READ_WRITE,
                existing_disk=ExistingDisk(id=disk_id),
                device_id="boot",
            ),
            network_interfaces=[
                NetworkInterfaceSpec(
                    name="eth0",
                    subnet_id=subnet_id,
                    ip_address=IPAddress(),
                    public_ip_address=None if no_public_ip else PublicIPAddress(),
                ),
            ],
        ),
    )


def extract_instance_ips(instance, no_public_ip: bool) -> tuple[str, Optional[str]]:
    network_interface = instance.status.network_interfaces[0]
    external_ipv4 = (
        network_interface.public_ip_address.address.replace("/32", "")
        if no_public_ip is False
        else None
    )
    local_ipv4 = network_interface.ip_address.address.replace("/32", "")
    return local_ipv4, external_ipv4


@retry(
    attempts=CREATE_VM_RETRY_ATTEMPTS,
    interval_sec=CREATE_VM_RETRY_INTERVAL_SEC,
    retry_exceptions=(RequestError,),
    attempt_arg="attempt",
    on_final_exception=report_create_vm_final_failure,
)
async def create_vm(sdk: SDK, args: argparse.Namespace, attempt: int = 0):
    logger.info(
        "Trying to create VM at %s (attempt=%d)",
        time.ctime(time.time()),
        attempt,
    )
    validate_create_args(
        platform_id=args.platform_id,
        preset=args.preset,
        disk_type=args.disk_type,
        disk_size=args.disk_size,
    )

    GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]

    gh = Github(auth=GithubAuth.Token(GITHUB_TOKEN))

    runner_registration_token = get_runner_token(
        args.github_repo_owner, args.github_repo, GITHUB_TOKEN
    )

    ssh_keys = []
    if args.github_org and args.github_team_slug:
        ssh_keys = fetch_github_team_public_keys(
            gh, args.github_org, args.github_team_slug
        )
    else:
        logger.info(
            "No GitHub organization or team specified, skipping SSH key fetching"
        )

    args.preset = maybe_downgrade_preset(
        platform_id=args.platform_id,
        preset=args.preset,
        allow_downgrade=args.allow_downgrade,
        downgrade_after=args.downgrade_after,
        attempt=attempt,
    )

    runner_github_label = generate_github_label()
    (
        github_runner_version,
        github_runner_sha256_by_arch,
    ) = resolve_runner_release_for_update(
        args.github_runner_version,
        GITHUB_TOKEN,
        args.github_override_existing_runner,
    )

    user_data, ssh_keys = generate_cloud_init_script(
        args.user,
        ssh_keys,
        args.github_repo_owner,
        args.github_repo,
        runner_registration_token,
        github_runner_version,
        github_runner_sha256_by_arch,
        args.github_override_existing_runner,
        runner_github_label + ",runner_" + args.runner_flavor,
    )

    labels = build_vm_labels(args.labels, runner_github_label, args.runner_flavor)

    disk_id = await create_disk(sdk, args)
    service = InstanceServiceClient(sdk)
    instance_request = build_instance_request(
        parent_id=args.parent_id,
        name=args.name,
        platform_id=args.platform_id,
        preset=args.preset,
        subnet_id=args.subnet_id,
        no_public_ip=args.no_public_ip,
        disk_id=disk_id,
        user_data=user_data,
        labels=labels,
    )
    # Create the VM
    if not args.apply:
        logger.info("Would create VM with request: %s", instance_request)
        return
    else:
        logger.info("Creating VM with request: %s", instance_request)

    request = None
    try:
        request = await service.create(instance_request)
        await request.wait()
    except TypeError as e:
        logger.error("Failed to create VM, removing created disk", exc_info=True)
        await remove_disk_by_id(sdk, args, disk_id)
        raise e

    except RequestError as e:
        logger.error("Failed to create VM with request: %s", instance_request)
        logger.error("Response: %s", str(e), exc_info=True)
        if request is not None:
            logger.error("Removing created instance with ID %s", request.resource_id)
            await remove_vm_by_id(sdk, request.resource_id)
            await remove_disk_by_id(sdk, args, disk_id)
        else:
            await remove_disk_by_id(sdk, args, disk_id)
        raise e

    instance_id = request.resource_id
    logger.info("Operation status: %s", str(request.status()))
    if request.status().code != grpc.StatusCode.OK:
        logger.error("Failed to create VM with request: %s", instance_request)
        await remove_vm_by_id(sdk, instance_id)
        await remove_disk_by_id(sdk, args, disk_id)
        raise RequestError("Failed to create VM")

    instance = await service.get(GetInstanceRequest(id=instance_id))
    name = instance.metadata.name
    logger.info("Created VM %s", instance)

    local_ipv4, external_ipv4 = extract_instance_ips(instance, args.no_public_ip)
    if instance_id:
        logger.info(
            "Created VM %s with ID %s and label %s",
            name,
            instance_id,
            runner_github_label,
        )
        github_output(logger, "instance-id", instance_id)
        github_output(logger, "label", runner_github_label)
        github_output(logger, "local-ipv4", local_ipv4)
        github_output(logger, "vm-preset", args.preset)
        if external_ipv4:
            github_output(logger, "external-ipv4", external_ipv4)
        else:
            github_output(logger, "external-ipv4", "N/A")

        # Here we will wait until the VM registers as a GitHub Runner,
        # if it fails to do so within the timeout, we will delete the
        # VM to avoid orphaned resources
        try:
            runner_id = wait_runner_by_name(
                gh, args.github_repo_owner, args.github_repo, instance_id
            )
        except Exception:
            logger.error(
                "Failed to check runner registration; removing VM %s and disk %s",
                instance_id,
                disk_id,
                exc_info=True,
            )
            await remove_vm_by_id(sdk, instance_id)
            await remove_disk_by_id(sdk, args, disk_id)
            raise

        if runner_id is not None:
            logger.info("VM registered as Github Runner %s", runner_id)
        else:
            logger.error("Failed to register VM as Github Runner")
            await remove_vm_by_id(sdk, instance_id)
            await remove_disk_by_id(sdk, args, disk_id)
            raise ValueError("Failed to register VM as Github Runner")
    else:
        logger.error("Failed to create VM with request: %s", instance_request)
        logger.error("Response: %s", request.status, exc_info=True)


def remove_runner_from_github(
    client: Github, github_repo_owner: str, github_repo: str, vm_id: str, apply: bool
) -> str:
    runner_id = find_runner_by_name(client, github_repo_owner, github_repo, vm_id)
    repo = os.environ.get("GITHUB_REPOSITORY")

    if runner_id is None:
        # this is not critical error, just log it and be done with it,
        # removing the VM is more important
        logger.info("Runner with name %s not found, skipping", vm_id)
        return "not_found"

    runner = client.get_repo(repo).get_self_hosted_runner(runner_id)
    if runner is None:
        logger.info("Runner with name %s not found, skipping", vm_id)
        return "not_found"
    logger.info(
        "Runner with name %s found: id:%s status:%s busy: %s ",
        runner.name,
        runner.id,
        runner.status,
        runner.busy,
    )
    if runner.busy:
        logger.info("Runner with name %s is busy, skipping", vm_id)
        return "busy"

    if apply:
        result = client.get_repo(repo).remove_self_hosted_runner(runner_id)

        if not result:
            # removed throwing exception here, because removing VM is more important
            # added additional logging to see what went wrong
            logger.info("Failed to remove runner with name %s", vm_id)
            return "failed"

        logger.info("Removed runner with name %s and id %s", vm_id, runner_id)
        return "removed"
    else:
        logger.info("Would remove runner with name %s and id %s", vm_id, runner_id)
        return "would_remove"


async def remove_disk_by_name(sdk: SDK, args: argparse.Namespace, instance_name: str):
    if not args.apply:
        logger.info("Would delete disk with name %s", DISK_NAME_PREFIX + instance_name)
        return

    disk_id = None
    service = DiskServiceClient(sdk)
    try:
        request = GetByNameRequest(
            parent_id=args.parent_id, name=DISK_NAME_PREFIX + instance_name
        )
        response = await service.get_by_name(request)
        disk_id = response.metadata.id
        if disk_id is None:
            logger.info("ListDisksRequest result: %s", response)
            logger.error(
                "Failed to find disk with name %s", DISK_NAME_PREFIX + instance_name
            )
            return

    except Exception as e:
        logger.exception(
            "Failed to get Disk with name %s, response: %s",
            instance_name,
            response,
            exc_info=True,
        )
        raise e

    await remove_disk_by_id(sdk, args, disk_id)


async def find_disk_by_instance_name(
    sdk: SDK, args: argparse.Namespace, instance_name: str
):
    service = DiskServiceClient(sdk)
    disk_name = DISK_NAME_PREFIX + instance_name
    try:
        request = GetByNameRequest(parent_id=args.parent_id, name=disk_name)
        response = await service.get_by_name(request)
    except Exception as e:
        logger.exception(
            "Failed to get Disk with name %s while searching fallback cleanup candidates",
            disk_name,
            exc_info=True,
        )
        logger.error("Disk lookup error: %s", e)
        return None

    disk_id = response.metadata.id
    if disk_id is None:
        logger.info("Disk with name %s was not found", disk_name)
        return None

    logger.info("Disk with name %s found: id=%s", disk_name, disk_id)
    return response


async def remove_disk_by_id(sdk: SDK, args: argparse.Namespace, disk_id: int = None):
    if not args.apply:
        logger.info("Would delete disk with ID %s", disk_id)
        return
    service = DiskServiceClient(sdk)
    request = None
    try:
        request = await service.delete(DeleteDiskRequest(id=disk_id))
        await request.wait()
        logger.info("Deleted Disk with ID %s", disk_id)
    except Exception as e:
        logger.exception("Failed to delete Disk with ID %s", disk_id, exc_info=True)
        if request is not None:
            logger.error("Response: %s", request.status)
        raise e


async def remove_vm_by_id(sdk: SDK, instance_id: int = None) -> str:
    instance_name = None
    service = InstanceServiceClient(sdk)
    request = None
    try:
        instance_name = await service.get(GetInstanceRequest(id=instance_id))
        instance_name = instance_name.metadata.name
        request = await service.delete(DeleteInstanceRequest(id=instance_id))
        await request.wait()
        logger.info("Deleted VM with ID %s", instance_id)

    except Exception as e:
        logger.exception("Failed to delete VM with ID %s", instance_id, exc_info=True)
        if request is not None:
            logger.error("Response: %s", request.status)
        raise e

    return instance_name


def labels_match(
    candidate_labels: dict, expected_labels: dict
) -> tuple[bool, list[str]]:
    mismatches = []
    for key, expected_value in expected_labels.items():
        actual_value = candidate_labels.get(key)
        if actual_value != expected_value:
            mismatches.append(
                f"{key}: expected {expected_value!r}, actual {actual_value!r}"
            )
    return not mismatches, mismatches


async def search_vm_cleanup_candidates_by_labels(sdk: SDK, args: argparse.Namespace):
    expected_labels = args.labels or {}
    if not expected_labels:
        logger.error(
            "Cannot search for VM cleanup candidates: --id is empty and --labels is missing"
        )
        return []

    logger.warning(
        "VM id is empty. Searching for cleanup candidates by labels only. "
        "This fallback is dry-run only and will not remove resources."
    )
    logger.info("Expected VM labels: %s", expected_labels)

    service = InstanceServiceClient(sdk)
    request = ListInstancesRequest(parent_id=args.parent_id)
    candidates = []
    page = 0

    while True:
        page += 1
        logger.info(
            "Listing VMs for parent_id=%s (page=%d, page_token=%s)",
            args.parent_id,
            page,
            request.page_token or "<empty>",
        )
        response = await service.list(request)
        instances = response.items
        logger.info("Received %d VMs on page %d", len(instances), page)

        for instance in instances:
            metadata = instance.metadata
            labels = dict(metadata.labels)
            logger.info(
                "Inspecting VM id=%s name=%s labels=%s",
                metadata.id,
                metadata.name,
                labels,
            )
            matched, mismatches = labels_match(labels, expected_labels)
            if not matched:
                logger.info(
                    "VM id=%s does not match expected labels: %s",
                    metadata.id,
                    "; ".join(mismatches),
                )
                continue

            logger.warning(
                "VM id=%s name=%s matches cleanup labels. Would remove this VM.",
                metadata.id,
                metadata.name,
            )
            disk = await find_disk_by_instance_name(sdk, args, metadata.name)
            if disk is not None:
                logger.warning(
                    "Would remove disk id=%s name=%s for VM id=%s",
                    disk.metadata.id,
                    disk.metadata.name,
                    metadata.id,
                )
            else:
                logger.warning(
                    "Would try to remove disk named %s%s for VM id=%s, but lookup did not find it",
                    DISK_NAME_PREFIX,
                    metadata.name,
                    metadata.id,
                )

            candidates.append(instance)

        if not response.next_page_token:
            break
        request.page_token = response.next_page_token

    logger.warning(
        "Found %d VM cleanup candidate(s) by labels. Dry-run fallback did not remove anything.",
        len(candidates),
    )
    return candidates


async def remove_vm(sdk: SDK, args: argparse.Namespace):
    if not args.id:
        await search_vm_cleanup_candidates_by_labels(sdk, args)
        return

    GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]

    gh = Github(auth=GithubAuth.Token(GITHUB_TOKEN))

    result = remove_runner_from_github(
        gh, args.github_repo_owner, args.github_repo, args.id, args.apply
    )
    if result == "not_found":
        logger.info("Runner with name %s not found in github, we can continue", args.id)
    elif result == "busy":
        logger.info("Runner with name %s is busy, skipping", args.id)
        return
    elif result == "failed":
        logger.error("Failed to remove runner with name %s, we can ignore it", args.id)
    elif result == "removed" or result == "would_remove":
        logger.info("Runner with name %s removed from github", args.id)

    if not args.apply:
        logger.info("Would delete VM with ID %s", args.id)
        return

    instance_name = await remove_vm_by_id(sdk, args.id)

    if instance_name is None:
        logger.error(
            "Failed to get instance name for ID %s, therefore we can't find disk",
            args.id,
        )
        return

    await remove_disk_by_name(sdk, args, instance_name)


async def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")

    create = subparsers.add_parser("create", help="Create a new VM")
    create.add_argument(
        "--github-org",
        required=False,
        default="",
        help="GitHub organization name (we will fetch SSH keys from this org to allow access to the VM)",
    )

    create.add_argument(
        "--github-team-slug",
        required=False,
        default="",
        help="Slug of the GitHub team within the organization",
    )

    create.add_argument(
        "--github-repo-owner",
        required=True,
        default="ydb-platform",
        help="GitHub repository owner name",
    )
    create.add_argument(
        "--github-repo",
        required=True,
        default="nbs",
        help="GitHub repository name",
    )

    create.add_argument(
        "--github-runner-version",
        default=GITHUB_RUNNER_LATEST_VERSION,
        help="GitHub Runner version, or 'latest' to resolve it from GitHub",
    )
    create.add_argument(
        "--github-override-existing-runner",
        default="false",
        help="Whether to update GitHub Runner if it's already installed",
    )

    create.add_argument(
        "--parent-id",
        required=True,
        default="project-e00gg2f58gw75edn7x",
        help="Parent ID where the VM will be created",
    )
    create.add_argument("--name", default="", help="VM name")
    create.add_argument("--platform-id", default="cpu-e2", help="Platform ID")
    create.add_argument(
        "--no-public-ip", action="store_true", help="Do not assign public IP"
    )
    choices = []
    for key in PRESETS.keys():
        for preset in PRESETS[key]:
            choices.append(preset)
    choices = list(set(choices))
    choices = sorted(choices, key=lambda x: int(x.split("vcpu")[0]))
    create.add_argument(
        "--preset",
        type=str,
        choices=choices,
        default="2vcpu-8gb",
        required=True,
        help="Instance preset (some presets are available only for specific platforms)",
    )
    create.add_argument(
        "--disk-type",
        default="network-ssd-nonreplicated",
        choices=[
            "network-ssd",
            "network-hdd",
            "network-ssd-nonreplicated",
            "network-ssd-io-m3",
        ],
        help="Type of disk",
    )
    create.add_argument(
        "--disk-size", type=int, required=True, default=930, help="Disk size in GB"
    )
    create.add_argument(
        "--image-id", type=str, required=True, help="Image ID to use for the VM"
    )
    create.add_argument("--subnet-id", required=True, help="Subnet ID for the VM")
    create.add_argument("--user", default="github", help="Username for the VM")
    create.add_argument(
        "--labels",
        action=KeyValueAction,
        default="",
        help="Label for the VM (k=v,k2=v2)",
    )
    create.add_argument(
        "--runner-flavor",
        default="none",
        help="Additional label for the Github Runner",
    )

    create.add_argument("--retry-time", default=10, help="How often to retry (seconds)")
    create.add_argument(
        "--timeout", default=1200, help="How long to wait for creation (seconds)"
    )
    create.add_argument(
        "--allow-downgrade",
        action="store_true",
        help="Allow downgrade to lower presets",
    )
    create.add_argument(
        "--downgrade-after",
        type=int,
        default=2,
        help="Downgrade to lower preset after N failed attempts",
    )
    create.add_argument("--apply", action="store_true", help="Apply the changes")

    remove = subparsers.add_parser("remove", help="Remove an existing VM")
    remove.add_argument(
        "--parent-id",
        required=True,
        default="project-e00gg2f58gw75edn7x",
        help="Parent ID where the VM will be created",
    )
    remove.add_argument("--id", required=True, help="ID of the VM to remove")

    remove.add_argument(
        "--github-repo-owner",
        required=True,
        default="ydb-platform",
        help="GitHub repository owner name",
    )
    remove.add_argument(
        "--github-repo",
        required=True,
        default="nbs",
        help="GitHub repository name",
    )
    remove.add_argument(
        "--labels",
        action=KeyValueAction,
        default={},
        help="Labels used to find VM cleanup candidates when --id is empty (k=v,k2=v2)",
    )

    remove.add_argument("--retry-time", default=10, help="How often to retry (seconds)")
    remove.add_argument(
        "--timeout", default=600, help="How long to wait for removal (seconds)"
    )
    remove.add_argument("--apply", action="store_true", help="Apply the changes")

    create.set_defaults(func=create_vm)
    remove.set_defaults(func=remove_vm)

    args = parser.parse_args()

    sdk = SDK(config_reader=Config())

    if hasattr(args, "func"):
        await args.func(sdk, args)
    else:
        parser.print_help()


if __name__ == "__main__":

    asyncio.run(main())
