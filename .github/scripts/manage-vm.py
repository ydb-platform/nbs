import os
import grpc
import json
import requests
import math
import logging
import argparse
import yaml
import random
import time
import string
from typing import Optional
from github import Github, Auth as GithubAuth
from yandexcloud import SDK, RetryInterceptor, backoff_linear_with_jitter
from yandex.cloud.compute.v1.instance_service_pb2_grpc import InstanceServiceStub
from yandex.cloud.compute.v1.instance_pb2 import Instance
from yandex.cloud.compute.v1.instance_service_pb2 import (
    CreateInstanceRequest,
    ResourcesSpec,
    AttachedDiskSpec,
    NetworkInterfaceSpec,
    PrimaryAddressSpec,
    OneToOneNatSpec,
    DeleteInstanceRequest,
    CreateInstanceMetadata,
    DeleteInstanceMetadata,
    GetInstanceSerialPortOutputRequest,
)
from yandex.cloud.compute.v1.instance_pb2 import IpVersion

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s"
)

logger = logging.getLogger(__name__)


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
# exit code 0 to skip the error and to boot vm correctly
./run.sh || exit 0
"""

    cloud_init = {"runcmd": [script]}
    # cloud_init["ssh_pwauth"] = False
    cloud_init["users"] = [
        {
            "name": user,
            "sudo": "ALL=(ALL) NOPASSWD:ALL",
            "passwd": os.environ["VM_USER_PASSWD"],
            "lock_passwd": False,
            "shell": "/bin/bash",
        }
    ]
    if ssh_keys:
        logger.info("Adding SSH keys to cloud-init")
        cloud_init["users"][0]["ssh_authorized_keys"] = ssh_keys

    logger.info(
        f"Cloud-init: \n{yaml.safe_dump(cloud_init, default_flow_style=False, width=math.inf)}".replace(
            token, "****"
        )
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


def create_vm(sdk: SDK, args: argparse.Namespace):
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

    # fmt: off
    standard_v2_cpu = [
        2, 4, 6, 8, 10, 12, 14, 16, 20, 24,
        28, 32, 36, 40, 44, 48, 52, 56, 60
    ]
    # fmt: on
    if args.platform_id == "standard-v2" and args.cpu not in standard_v2_cpu:
        raise ValueError(
            f"CPU cores must be {', '.join(standard_v2_cpu)} for standard-v2 platform"
        )

    if args.ram % args.cpu != 0:
        raise ValueError("RAM must be a multiple of CPU cores (1,2,4 etc)")

    if (
        args.disk_type in ["network-ssd-nonreplicated", "network-ssd-io-m3"]
        and args.disk_size % 93 != 0  # noqa: W503
    ):
        raise ValueError(
            "Disk size must be a multiple of 93GB for the selected disk type"
        )

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

    metadata = {
        "user-data": user_data,
        "serial-port-enable": "1",
    }
    if ssh_keys:
        logger.info("Adding SSH keys to metadata")
        metadata["ssh-keys"] = "\n".join([f"{args.user}:{key}" for key in ssh_keys])

    request = CreateInstanceRequest(
        name=args.name,
        folder_id=args.folder_id,
        zone_id=args.zone_id,
        platform_id=args.platform_id,
        resources_spec=ResourcesSpec(
            memory=args.ram * 1024 * 1024 * 1024, cores=args.cpu, core_fraction=100
        ),
        boot_disk_spec=AttachedDiskSpec(
            auto_delete=True,
            mode=AttachedDiskSpec.Mode.READ_WRITE,
            disk_spec=AttachedDiskSpec.DiskSpec(
                type_id=args.disk_type,
                size=args.disk_size * 1024 * 1024 * 1024,
                image_id=args.image_id,
            ),
        ),
        labels=labels,
        network_interface_specs=[
            NetworkInterfaceSpec(
                subnet_id=args.subnet_id,
                primary_v4_address_spec=PrimaryAddressSpec(
                    one_to_one_nat_spec=OneToOneNatSpec(ip_version=IpVersion.IPV4)
                ),
            )
        ],
        metadata=metadata,
    )

    # Create the VM
    if args.apply:
        result = sdk.create_operation_and_get_result(
            request=request,
            service=InstanceServiceStub,
            method_name="Create",
            response_type=Instance,
            meta_type=CreateInstanceMetadata,
            timeout=args.timeout,
            logger=logger,
        )
        instance_id = result.response.id
        name = result.response.name
        primary_ipv4 = result.response.network_interfaces[0].primary_v4_address
        external_ipv4 = primary_ipv4.one_to_one_nat.address
        local_ipv4 = primary_ipv4.address
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
            if external_ipv4:
                github_output("external-ipv4", external_ipv4)

            logger.info("Waiting for VM to be registered as Github Runner")
            runner_id = wait_for_runner_registration(
                gh, instance_id, args.github_repo_owner, args.github_repo, 10, 60
            )
            # Saving console output
            # it is not using create_operation_and_get_result() because it is not an operation like
            # https://github.com/yandex-cloud/cloudapi/blob/master/yandex/cloud/compute/v1/instance_service.proto#L35
            # and do not have necessary attributes for it
            # https://github.com/yandex-cloud/cloudapi/blob/master/yandex/cloud/compute/v1/instance_service.proto#L80
            instance_service = sdk.client(InstanceServiceStub)

            result_serial = instance_service.GetSerialPortOutput(
                GetInstanceSerialPortOutputRequest(instance_id=instance_id)
            )

            if not result_serial.contents:
                logger.error(
                    "Failed to get console output for VM with ID %s", instance_id
                )
                github_output("console-output", "false")
            else:
                with open("console_output.txt", "w") as f:
                    f.write(result_serial.contents)
                github_output("console-output", "console_output.txt")

            if runner_id is not None:
                logger.info("VM registered as Github Runner %s", runner_id)
            else:
                logger.error("Failed to register VM as Github Runner")
                raise ValueError("Failed to register VM as Github Runner")
        else:
            logger.error("Failed to create VM with request: %s", request)
            logger.error("Response: %s", result)
    else:
        logger.info("Would create VM with request: %s", request)


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


def remove_vm(sdk: SDK, args: argparse.Namespace):
    remove_runner_from_github(
        args.github_repo_owner, args.github_repo, args.id, args.apply
    )

    if args.apply:

        try:
            result = sdk.create_operation_and_get_result(
                request=DeleteInstanceRequest(instance_id=args.id),
                service=InstanceServiceStub,
                method_name="Delete",
                response_type=Instance,
                meta_type=DeleteInstanceMetadata,
                timeout=args.timeout,
                logger=logger,
            )

            logger.info("Deleted VM with ID %s", args.id)
        except Exception as e:
            logger.exception("Failed to delete VM with ID %s", args.id, exc_info=True)
            logger.error("Response: %s", result)
            raise e
    else:
        logger.info("Would delete VM with ID %s", args.id)


if __name__ == "__main__":
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
        default="2.316.1",
        help="GitHub Runner version",
    )

    create.add_argument(
        "--service-account-key",
        required=True,
        help="Path to the service account key file",
    )
    create.add_argument(
        "--folder-id",
        required=True,
        default="bjeuq5o166dq4ukv3eec",
        help="Folder ID where the VM will be created",
    )
    create.add_argument("--name", default="", help="VM name")
    create.add_argument(
        "--zone-id", default="eu-north1-c", required=True, help="Zone ID for the VM"
    )
    create.add_argument("--platform-id", default="standard-v2", help="Platform ID")
    create.add_argument(
        "--cpu", type=int, default=60, required=True, help="Number of CPU cores"
    )
    create.add_argument(
        "--ram", type=int, default=240, required=True, help="Amount of RAM in GB"
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
    create.add_argument("--apply", action="store_true", help="Apply the changes")

    remove = subparsers.add_parser("remove", help="Remove an existing VM")
    remove.add_argument(
        "--service-account-key",
        required=True,
        help="Path to the service account key file",
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

    interceptor = RetryInterceptor(
        max_retry_count=args.timeout / args.retry_time,
        retriable_codes=[grpc.StatusCode.UNAVAILABLE],
        back_off_func=backoff_linear_with_jitter(args.retry_time, 0),
    )

    with open(args.service_account_key, "r") as fp:
        sdk = SDK(
            service_account_key=json.load(fp),
            endpoint=args.api_endpoint,
            interceptor=interceptor,
        )

    if hasattr(args, "func"):
        args.func(sdk, args)
    else:
        parser.print_help()
