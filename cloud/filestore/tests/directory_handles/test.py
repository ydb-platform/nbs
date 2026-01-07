import logging
import os
from time import sleep

import retrying
from retrying import retry

import yatest.common as common

import cloud.filestore.public.sdk.python.protos as protos

from cloud.filestore.public.sdk.python.client.grpc_client import (
    CreateGrpcEndpointClient,
    CreateGrpcClient
)
from cloud.filestore.tests.python.lib.common import is_grpc_error

from cloud.storage.core.tools.testing.qemu.lib.common import (
    env_with_guest_index,
    SshToGuest,
)

RETRY_COUNT = 3
WAIT_TIMEOUT_MS = 1000
MAX_DIRS = 25000
WAIT_FILE_TIMEOUT_MS = 15000


@retrying.retry(
    stop_max_delay=60000,
    wait_fixed=1000,
    retry_on_exception=is_grpc_error
)
def wait_for_filestore_vhost(port, port_type="endpoint"):
    if port_type == "endpoint":
        with CreateGrpcEndpointClient(
            str("localhost:%d" % port)
        ) as grpc_client:
            grpc_client.ping(protos.TPingRequest())
    elif port_type == "filestore":
        with CreateGrpcClient(str("localhost:%d" % port)) as grpc_client:
            grpc_client.ping(protos.TPingRequest())
    else:
        raise Exception(f"Invalid port type {port_type}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT_MS)
def create_dirs_bulk(ssh: SshToGuest, parent_dir: str):
    command = (
        f"for i in {{1..{MAX_DIRS}}}; do sudo mkdir -p "
        f"{parent_dir}/dirname$(printf \"%03d\" $i); done"
    )
    return ssh(command)


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT_MS)
def create_dir(ssh: SshToGuest, dir: str, dir_name: str):
    return ssh(f"sudo mkdir -p {dir}/{dir_name}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT_MS)
def create_file(ssh: SshToGuest, dir: str, file_name: str):
    return ssh(f"sudo touch {dir}/{file_name}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT_MS)
def wait_for_file(ssh: SshToGuest, file_path: str):
    return ssh(f"test -f {file_path}")


def test():
    logger = logging.getLogger("test")
    vhost_port = os.getenv("NFS_VHOST_PORT")

    port = int(os.getenv(env_with_guest_index("QEMU_FORWARDING_PORT", 0)))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")

    ssh = SshToGuest(user="qemu", port=port, key=ssh_key)

    directory_handles_test_bin = common.binary_path(
        "cloud/filestore/tools/testing/directory_handles_test/"
        "directory_handles_test"
    )

    working_dir = "test_dir"
    continuation_file = "test_file_continue"
    completion_file = "test_file_completion"

    full_working_dir = f"{mount_dir}/{working_dir}"
    full_continuation_file = f"{mount_dir}/{continuation_file}"
    full_completion_file = f"{mount_dir}/{completion_file}"

    res = create_dir(ssh, mount_dir, working_dir)
    res = create_dirs_bulk(ssh, full_working_dir)
    assert 0 == res.returncode

    ssh(
        f"{directory_handles_test_bin} {full_working_dir} "
        f"{full_continuation_file} {WAIT_FILE_TIMEOUT_MS} "
        f"{MAX_DIRS} {full_completion_file} > "
        f"/tmp/working_dir.log 2>&1 &"
    )

    restart_flag_on_demand = os.getenv("VHOST_RESTART_FLAG_ON_DEMAND")

    if restart_flag_on_demand is None:
        exit(1)

    logger.info("creating flag file")
    if restart_flag_on_demand:
        ssh(f"sudo touch {restart_flag_on_demand}")
        sleep(int(os.getenv("VHOST_RESTART_INTERVAL")) * 0.5)
        ssh(f"sudo rm -f {restart_flag_on_demand}")

    logger.info("wait_for_filestore_vhost")
    wait_for_filestore_vhost(int(vhost_port))
    logger.info("wait_for_filestore_vhost finished")

    res = create_file(ssh, mount_dir, continuation_file)
    assert 0 == res.returncode

    logger.info("waiting for completion file")
    res = wait_for_file(ssh, full_completion_file)
    assert 0 == res.returncode
    logger.info("completion file appeared")

    ret = ssh("sudo cat /tmp/working_dir.log")

    results_path = common.output_path() + "/results.txt"
    with open(results_path, 'w') as results:
        results.write(ret.stdout.decode('utf8'))

    ret = common.canonical_file(results_path, local=True)
    return ret
