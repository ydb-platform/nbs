import json
import logging
import os
import time
from time import sleep

import retrying
from retrying import retry

import yatest.common as common

import cloud.filestore.public.sdk.python.protos as protos

from cloud.filestore.public.sdk.python.client.grpc_client import CreateGrpcEndpointClient, CreateGrpcClient
from cloud.filestore.tests.python.lib.common import daemon_log_files, is_grpc_error

import cloud.filestore.public.sdk.python.client as client
from cloud.storage.core.tools.testing.qemu.lib.common import (
    env_with_guest_index,
    SshToGuest,
)

RETRY_COUNT = 3
WAIT_TIMEOUT_MS = 1000
MAX_DIRS = 25000
WAIT_FILE_TIMEOUT_MS = 15000

@retrying.retry(stop_max_delay=60000, wait_fixed=1000, retry_on_exception=is_grpc_error)
def wait_for_filestore_vhost(port, port_type="endpoint"):
    if port_type == "endpoint":
        with CreateGrpcEndpointClient(str("localhost:%d" % port)) as grpc_client:
            grpc_client.ping(protos.TPingRequest())
    elif port_type == "filestore":
        with CreateGrpcClient(str("localhost:%d" % port)) as grpc_client:
            grpc_client.ping(protos.TPingRequest())
    else:
        raise Exception(f"Invalid port type {port_type}")

@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT_MS)
def create_dirs_bulk(ssh: SshToGuest, parent_dir: str):
    command = f"for i in {{1..{MAX_DIRS}}}; do sudo mkdir -p {parent_dir}/dirname$(printf \"%03d\" $i); done"
    return ssh(command)

@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT_MS)
def create_dir(ssh: SshToGuest, dir: str, dir_name: str):
    return ssh(f"sudo mkdir -p {dir}/{dir_name}")

@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT_MS)
def create_file(ssh: SshToGuest, dir: str, file_name: str):
    return ssh(f"sudo touch {dir}/{file_name}")


def test():
    logger = logging.getLogger("test")
    vhost_port = os.getenv("NFS_VHOST_PORT")

    port = int(os.getenv(env_with_guest_index("QEMU_FORWARDING_PORT", 0)))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")

    ssh = SshToGuest(user="qemu", port=port, key=ssh_key)

    readdir_bin_path = common.binary_path("cloud/filestore/tools/testing/directory_handles_state_test/directory_handles_state_test")

    dir_for_open_restart = f"test_dir"
    file_for_script_continue = f"test_file_continue"

    full_dir_for_open_restart = f"{mount_dir}/{dir_for_open_restart}"
    full_file_for_script_continue = f"{mount_dir}/{file_for_script_continue}"
        
    res1 = create_dir(ssh, mount_dir, dir_for_open_restart)
    res1 = create_dirs_bulk(ssh, full_dir_for_open_restart)
    assert 0 == res1.returncode


    res = ssh(
        f"{readdir_bin_path} {full_dir_for_open_restart} {full_file_for_script_continue} {WAIT_FILE_TIMEOUT_MS} {MAX_DIRS} > /tmp/dir_for_open_restart.log 2>&1 &")

    restart_flag_on_demand = os.getenv("VHOST_RESTART_FLAG_ON_DEMAND")

    if restart_flag_on_demand is None:
        exit(1)

    logger.info(f"creating flag file time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}.{int(time.time() * 1000) % 1000:03d}")
    if restart_flag_on_demand:
        ssh(f"sudo touch {restart_flag_on_demand}")
        sleep(int(os.getenv("VHOST_RESTART_INTERVAL")) * 0.5)
        ssh(f"sudo rm -f {restart_flag_on_demand}")

    logger.info(f"wait_for_filestore_vhost time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}.{int(time.time() * 1000) % 1000:03d}")
    wait_for_filestore_vhost(int(vhost_port))
    logger.info(f"wait_for_filestore_vhost finished time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}.{int(time.time() * 1000) % 1000:03d}")

    res1 = create_file(ssh, mount_dir, file_for_script_continue)
    assert 0 == res1.returncode

    ret = ssh(f"sudo cat /tmp/dir_for_open_restart.log")
    
    results_path = common.output_path() + "/results.txt"
    with open(results_path, 'w') as results:
        results.write(ret.stdout.decode('utf8'))

    ret = common.canonical_file(results_path, local=True)
    return ret
