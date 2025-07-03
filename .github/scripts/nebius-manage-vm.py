import argparse
import logging
import math
import os
import random
import string
import time
import grpc
from typing import Optional
import asyncio
import requests
import yaml
import functools
from github import Auth as GithubAuth
from github import Github

from nebius.sdk import SDK
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
    DeleteInstanceRequest,
    DeleteDiskRequest,
    ExistingDisk,
)

SENSITIVE_DATA_VALUES = {}
if os.environ.get("GITHUB_TOKEN"):
    SENSITIVE_DATA_VALUES["github_token"] = os.environ.get("GITHUB_TOKEN")
if os.environ.get("VM_USER_PASSWD"):
    SENSITIVE_DATA_VALUES["passwd"] = os.environ.get("VM_USER_PASSWD")


class MaskingFormatter(logging.Formatter):
    @staticmethod
    def mask_sensitive_data(msg):
        # Iterate over the patterns and replace sensitive data with '***'
        for pattern_name, pattern in SENSITIVE_DATA_VALUES.items():
            msg = msg.replace(pattern, f"[{pattern_name}=***]")
        return msg

    def format(self, record):
        original = logging.Formatter.format(self, record)
        return self.mask_sensitive_data(original)


formatter = MaskingFormatter("%(asctime)s: %(levelname)s: %(message)s")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


DISK_NAME_PREFIX = "disk-"
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


def github_output(key: str, value: str, is_secret: bool = False):
    GITHUB_OUTPUT = os.environ.get("GITHUB_OUTPUT")

    if GITHUB_OUTPUT:
        with open(GITHUB_OUTPUT, "a") as fp:
            fp.write(f"{key}={value}\n")

    logger.info('echo "%s=%s" >> $GITHUB_OUTPUT', key, "******" if is_secret else value)


def generate_github_label():
    generated_string = "".join(
        random.choices(string.ascii_lowercase + string.digits, k=8)
    )
    logger.info("Generated label: %s", generated_string)
    return generated_string


class KeyValueAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):  # noqa: U100
        kv_dict = {}
        for item in values.split(","):
            key, value = item.split("=")
            kv_dict[key] = value
        setattr(namespace, self.dest, kv_dict)


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


def generate_cloud_init_script(
    user: str,
    ssh_keys: list[str],
    owner: str,
    repo: str,
    token: str,
    version: str,
    label: str,
):
    if os.environ.get("GITHUB_REPOSITORY"):
        label += (
            f",GITHUB_REPOSITORY_{os.environ['GITHUB_REPOSITORY'].replace('/', '_')}"
        )

    for item in ["GITHUB_SHA", "GITHUB_REF", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT"]:
        if os.environ.get(item):
            label += f",{item}_{os.environ[item]}"

    script = f"""
set -x

echo "fixing /etc/hosts"
echo "::1 localhost" | tee -a /etc/hosts
grep localhost /etc/hosts

[ -d /actions-runner ] && {{
    echo "Runner already installed"
    cd /actions-runner
}} || {{
    mkdir -p /actions-runner && cd /actions-runner
    case $(uname -m) in
        aarch64) ARCH="arm64" ;;
        amd64|x86_64) ARCH="x64";;
    esac
    export FILENAME=runner.tar.gz
    # https://github.com/actions/runner/releases/download/v2.314.1/actions-runner-linux-x64-2.314.1.tar.gz
    exit_code=1
    i=0
    url="https://github.com/actions/runner/releases/download/v{version}/actions-runner-linux-${{ARCH}}-{version}.tar.gz"
    until [ $exit_code -eq 0 ] || [ $i -gt 3 ]; do
        [ -f "$FILENAME" ] || curl --connect-timeout 5 -L "$url" -o "$FILENAME"
        exit_code=$?
        i=$((i+1))
        [ $exit_code -eq 0 ] || rm -f "$FILENAME"
        echo "$((date)) [$i] curl exited (or timed-out) with code $exit_code"
    done
    tar xzf "./$FILENAME" || exit 0
}}
export RUNNER_ALLOW_RUNASROOT=1

# trying to catch registration error
exit_code=1
i=0
until [ $exit_code -eq 0 ] || [ $i -gt 3 ]; do
    echo ./config.sh --labels {label} --url https://github.com/{owner}/{repo} --token XXX --unattended
    set +x
    timeout 60 ./config.sh --labels {label} --url https://github.com/{owner}/{repo} --token {token} --unattended
    set -x
    exit_code=$?
    i=$((i+1))
    echo "$((date)) [$i] config.sh exited (or timed-out) with code $exit_code"
    [ $exit_code -eq 0 ] || find /actions-runner -name *.log -print -exec cat {{}} \; # noqa: W605
done
# true to skip the error and to boot vm correctly
./svc.sh install || true
./svc.sh start || true
"""

    cloud_init = {
        "runcmd": [script],
        "manage_etc_hosts": "true",
        "users": [
            {
                "name": user,
                "sudo": "ALL=(ALL) NOPASSWD:ALL",
                "passwd": os.environ["VM_USER_PASSWD"],
                "lock_passwd": False,
                "shell": "/bin/bash",
            }
        ],
    }
    if ssh_keys:
        logger.info("Adding SSH keys to cloud-init")
        cloud_init["users"][0]["ssh_authorized_keys"] = ssh_keys

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


def get_runner_token(
    github_repo_owner: str, github_repo: str, github_token: str
) -> str:
    result = requests.post(
        f"https://api.github.com/repos/{github_repo_owner}/{github_repo}/actions/runners/registration-token",
        headers={
            "Authorization": f"Bearer {github_token}",
            "Accept": "application/vnd.github+json",
            "X-Github-Api-Version": "2022-11-28",
        },
    ).json()

    token = result.get("token")
    expires_at = result.get("expires_at")
    if token:
        # Mask the token in the logs
        print(f"::add-mask::{token}")
        SENSITIVE_DATA_VALUES["runner_token"] = token
        logger.debug(
            "Got runner registration token: %s (valid till: %s)", (token, expires_at)
        )
        return token
    else:
        raise ValueError(f"Failed to get runner registration token: {result}")


def wait_for_runner_registration(
    client: Github,
    vm_id: str,
    github_repo_owner: str,
    github_repo: str,
    timeout_sec: int = 10,
    retries: int = 60,
) -> Optional[str]:
    logger.info(
        "Waiting for runner registration every %d seconds x %d times",
        timeout_sec,
        retries,
    )

    for i in range(retries):
        runner_id = find_runner_by_name(client, github_repo_owner, github_repo, vm_id)
        if runner_id is not None:
            return runner_id
        else:
            time.sleep(timeout_sec)

    return None


def find_runner_by_name(
    client: Github, github_repo_owner: str, github_repo: str, vm_id: str
) -> Optional[str]:
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


async def create_disk(sdk: SDK, args: argparse.Namespace) -> str:
    service = DiskServiceClient(sdk)
    disk_request = CreateDiskRequest(
        metadata=ResourceMetadata(
            parent_id=args.parent_id,
            name=DISK_NAME_PREFIX + args.name,
        ),
        spec=DiskSpec(
            type=DiskSpec.DiskType.NETWORK_SSD_NON_REPLICATED,
            size_gibibytes=args.disk_size,
            source_image_id=args.image_id,
        ),
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
            logger.error("Removing created instance with ID %s", request.resource_id)
            remove_vm_by_id(sdk, request.resource_id)
        raise

    logger.info(
        "Created disk with ID %s status:%s",
        request.resource_id,
        request.status(),
    )

    return request.resource_id


def retry_create_vm(func: callable) -> callable:
    @functools.wraps(func)
    async def wrapper(sdk: SDK, args: argparse.Namespace) -> callable:
        total_time_limit = 30 * 60
        retry_interval = 5
        start_time = time.time()
        attempt = 0
        while time.time() - start_time < total_time_limit:
            try:
                logger.info(
                    "Trying to create VM at %s (attempt=%d)",
                    time.ctime(time.time()),
                    attempt,
                )
                result = await func(sdk, args, attempt)
                logger.info("VM created successfully at %s", time.ctime(time.time()))
                return result
            except RequestError as e:
                attempt += 1
                if time.time() - start_time >= total_time_limit:
                    if os.environ.get("GITHUB_EVENT_NAME") == "pull_request":
                        pr_number = int(os.environ.get("GITHUB_REF").split("/")[-1])
                        repo_name = os.environ.get("GITHUB_REPOSITORY")
                        github_token = os.environ.get("GITHUB_TOKEN")
                        comment = f"VM creation failed after 30 minutes: {e}"
                        gh = Github(auth=GithubAuth.Token(github_token))
                        repo = gh.get_repo(repo_name)
                        pr = repo.get_pull(pr_number)
                        pr.create_issue_comment(comment)
                    raise
                next_run_time = time.time() + retry_interval
                logger.error("Failed to create VM", exc_info=True)
                logger.info("Next run will be at %s", time.ctime(next_run_time))
                while (
                    time.time() < next_run_time
                    and time.time() - start_time < total_time_limit  # noqa: W503
                ):
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error("Failed to create VM", exc_info=True)
                raise e
        raise Exception("Time limit exceeded while retrying function")

    return wrapper


@retry_create_vm
async def create_vm(sdk: SDK, args: argparse.Namespace, attempt: int = 0):
    # validate preset
    if args.preset not in PRESETS.get(args.platform_id, []):
        raise Exception(
            f"Preset {args.preset} is not available for platform {args.platform_id}"
        )

    if os.environ.get("VM_USER_PASSWD") is None:
        raise Exception("VM_USER_PASSWD environment variable is not set")

    if os.environ.get("GITHUB_TOKEN") is None:
        raise Exception("GITHUB_TOKEN environment variable is not set")

    if (
        args.disk_type in ["network-ssd-nonreplicated", "network-ssd-io-m3"]
        and args.disk_size % 93 != 0  # noqa: W503
    ):
        raise ValueError(
            "Disk size must be a multiple of 93GB for the selected disk type"
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

    # We will downgrade every N failed attempts
    # if our preset is 80cpu and downgrade_after is 2 on third
    # attempt it will be downgraded to 64cpu
    # And on 4th attempt it will be downgraded to 48cpu
    logger.info("Attempt %d", attempt)
    logger.info("Current preset %s", args.preset)
    logger.info("Downgrade after %d", args.downgrade_after)
    logger.info("Allow downgrade %s", args.allow_downgrade)
    logger.info("attempt mod args.downgrade_after = %d", attempt % args.downgrade_after)
    logger.info("attempt > 0 = %s", attempt > 0)
    if args.allow_downgrade and attempt % args.downgrade_after == 0 and attempt > 0:
        current_preset_index = PRESETS[args.platform_id].index(args.preset)
        if current_preset_index > 0:
            args.preset = PRESETS[args.platform_id][current_preset_index - 1]
            logger.info("Downgrading to %s preset", args.preset)

    runner_github_label = generate_github_label()

    user_data, ssh_keys = generate_cloud_init_script(
        args.user,
        ssh_keys,
        args.github_repo_owner,
        args.github_repo,
        runner_registration_token,
        args.github_runner_version,
        runner_github_label,
    )

    labels = args.labels
    labels["runner-label"] = runner_github_label

    disk_id = await create_disk(sdk, args)
    service = InstanceServiceClient(sdk)
    instance_request = CreateInstanceRequest(
        metadata=ResourceMetadata(
            parent_id=args.parent_id,
            name=args.name,
            labels=labels,
        ),
        spec=InstanceSpec(
            stopped=False,
            cloud_init_user_data=user_data,
            resources=ResourcesSpec(platform=args.platform_id, preset=args.preset),
            boot_disk=AttachedDiskSpec(
                attach_mode=AttachedDiskSpec.AttachMode.READ_WRITE,
                existing_disk=ExistingDisk(id=disk_id),
                device_id="boot",
            ),
            network_interfaces=[
                NetworkInterfaceSpec(
                    name="eth0",
                    subnet_id=args.subnet_id,
                    ip_address=IPAddress(),
                    public_ip_address=PublicIPAddress(),
                ),
            ],
        ),
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

    network_interface = instance.status.network_interfaces[0]
    external_ipv4 = network_interface.public_ip_address.address.replace("/32", "")
    local_ipv4 = network_interface.ip_address.address.replace("/32", "")
    if instance_id:
        logger.info(
            "Created VM %s with ID %s and label %s",
            name,
            instance_id,
            runner_github_label,
        )
        github_output("instance-id", instance_id)
        github_output("label", runner_github_label)
        github_output("local-ipv4", local_ipv4)
        github_output("vm-preset", args.preset)
        if external_ipv4:
            github_output("external-ipv4", external_ipv4)

        logger.info("Waiting for VM to be registered as Github Runner")
        runner_id = wait_for_runner_registration(
            gh, instance_id, args.github_repo_owner, args.github_repo, 10, 60
        )

        if runner_id is not None:
            logger.info("VM registered as Github Runner %s", runner_id)
        else:
            logger.error("Failed to register VM as Github Runner")
            raise ValueError("Failed to register VM as Github Runner")
    else:
        logger.error("Failed to create VM with request: %s", instance_request)
        logger.error("Response: %s", request.status, exc_info=True)


def remove_runner_from_github(
    github_repo_owner: str, github_repo: str, vm_id: str, apply: bool
):
    github_token = os.environ["GITHUB_TOKEN"]

    gh = Github(auth=GithubAuth.Token(github_token))

    runner_id = find_runner_by_name(gh, github_repo_owner, github_repo, vm_id)

    if runner_id is None:
        # this is not critical error, just log it and be done with it,
        # removing the VM is more important
        logger.info("Runner with name %s not found, skipping", vm_id)
        return

    if apply:
        delete_status = requests.delete(
            f"https://api.github.com/repos/{github_repo_owner}/{github_repo}/actions/runners/{runner_id}",
            headers={
                "Authorization": f"Bearer {github_token}",
                "Accept": "application/vnd.github+json",
                "X-Github-Api-Version": "2022-11-28",
            },
        )

        if delete_status.status_code != 204:
            # removed throwing exception here, because removing VM is more important
            # added additional logging to see what went wrong
            logger.info(
                "Failed to remove runner with name %s, status_code: %d",
                vm_id,
                delete_status.status_code,
            )
            logger.info("Response: %s", delete_status.text)
            return

        logger.info("Removed runner with name %s and id %s", vm_id, runner_id)
    else:
        logger.info("Would remove runner with name %s and id %s", vm_id, runner_id)


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


async def remove_disk_by_id(sdk: SDK, args: argparse.Namespace, disk_id: int = None):
    if not args.apply:
        logger.info("Would delete disk with ID %s", disk_id)
        return
    service = DiskServiceClient(sdk)
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


async def remove_vm(sdk: SDK, args: argparse.Namespace):
    remove_runner_from_github(
        args.github_repo_owner, args.github_repo, args.id, args.apply
    )

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
    parser.add_argument(
        "--api-endpoint",
        default="api.ai.nebius.cloud",
        help="Cloud API Endpoint",
    )

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
        default="2.320.0",
        help="GitHub Runner version",
    )

    create.add_argument(
        "--service-account-key",
        required=True,
        help="Path to the service account key file",
    )
    create.add_argument(
        "--parent-id",
        required=True,
        default="project-e00gg2f58gw75edn7x",
        help="Parent ID where the VM will be created",
    )
    create.add_argument("--name", default="", help="VM name")
    create.add_argument("--platform-id", default="cpu-e2", help="Platform ID")
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
        "--service-account-key",
        required=True,
        help="Path to the service account key file",
    )
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

    remove.add_argument("--retry-time", default=10, help="How often to retry (seconds)")
    remove.add_argument(
        "--timeout", default=600, help="How long to wait for removal (seconds)"
    )
    remove.add_argument("--apply", action="store_true", help="Apply the changes")

    create.set_defaults(func=create_vm)
    remove.set_defaults(func=remove_vm)

    args = parser.parse_args()

    sdk = SDK(credentials_file_name=args.service_account_key)

    if hasattr(args, "func"):
        await args.func(sdk, args)
    else:
        parser.print_help()


if __name__ == "__main__":

    asyncio.run(main())
