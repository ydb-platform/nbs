import json
import logging
import os
from time import monotonic, sleep

import yatest.common as common

import cloud.filestore.public.sdk.python.protos as protos

import cloud.filestore.public.sdk.python.client as client
from cloud.filestore.public.sdk.python.client.grpc_client import (
    CreateGrpcEndpointClient,
)
from cloud.storage.core.tools.testing.qemu.lib.common import (
    env_with_guest_index,
    SshToGuest,
)


OPEN_HANDLE_COUNT = 1000
SHARD_COUNT = int(os.getenv("FILESTORE_SHARD_COUNT", 0))
FILE_SYSTEM_ID = "nfs_test"
MAX_WAIT_SECONDS = 600
READY_PATH = "/tmp/async_open_ready"
RELEASE_PATH = "/tmp/async_open_release"
VERIFY_PATH = "/tmp/async_open_verify"
VERIFIED_PATH = "/tmp/async_open_verified"
UNLINK_PATH = "/tmp/async_open_unlink"
UNLINKED_PATH = "/tmp/async_open_unlinked"
LOG_PATH = common.output_path("async_open.log")

logger = logging.getLogger("test")


def wait_for(description, get_value, done):
    deadline = monotonic() + MAX_WAIT_SECONDS
    last = None

    while monotonic() < deadline:
        try:
            last = get_value()
            logger.info("%s: %s", description, last)
            if done(last):
                return last
        except Exception as error:
            last = error
            logger.warning("Waiting for %s: %s", description, error)
        sleep(1)

    raise AssertionError(f"Timed out waiting for {description}; last: {last}")


def get_storage_stats(filestore_client, file_system_id):
    response = filestore_client.execute_action(
        action="getstoragestats",
        input=json.dumps({"FileSystemId": file_system_id}).encode())
    return json.loads(response.Output)["Stats"]


def get_handles_count(filestore_client, file_system_id):
    return int(get_storage_stats(
        filestore_client, file_system_id).get("UsedHandlesCount", 0))


def get_confirm_create_handle_count():
    profile_tool = common.binary_path(
        "cloud/filestore/tools/analytics/profile_tool/filestore-profile-tool")
    result = common.execute([
        "bash",
        "-c",
        (
            '"$1" dumpevents --profile-log "$2" --fs-id nfs_test '
            "--request-name ConfirmCreateHandle | "
            "awk '$5 == \"S_OK\" { count++ } END { print count + 0 }'"
        ),
        "--",
        profile_tool,
        common.output_path("vhost-profile.log"),
    ])
    return int(result.stdout)


def guest_file_exists(ssh, path):
    return common.execute(
        ssh.get_command(f"sudo test -f {path}"),
        check_exit_code=False).returncode == 0


def restart_tablets(filestore_client, file_system_ids):
    for file_system_id in file_system_ids:
        filestore_client.execute_action(
            action="restarttablet",
            input=json.dumps({"FileSystemId": file_system_id}).encode())


def is_filestore_vhost_alive(port):
    try:
        with CreateGrpcEndpointClient(
                f"localhost:{port}", timeout=1) as grpc_client:
            grpc_client.ping(protos.TPingRequest())
        return True
    except Exception:
        return False


def restart_vhost(ssh, port):
    restart_flag = os.getenv("VHOST_RESTART_FLAG_ON_DEMAND")
    assert restart_flag, "VHOST_RESTART_FLAG_ON_DEMAND is not set"

    ssh(f"sudo touch {restart_flag}")
    try:
        wait_for(
            "vhost to stop",
            lambda: not is_filestore_vhost_alive(port),
            bool)
    finally:
        ssh(f"sudo rm -f {restart_flag}")

    wait_for("vhost to restart", lambda: is_filestore_vhost_alive(port), bool)


def async_test():
    server_port = os.getenv("NFS_SERVER_PORT")
    vhost_port = int(os.getenv("NFS_VHOST_PORT"))
    port = int(os.getenv(env_with_guest_index("QEMU_FORWARDING_PORT", 0)))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")
    script = common.source_path(
        "cloud/filestore/tests/async_open_test/lib/script.py")

    ssh = SshToGuest(user="qemu", port=port, key=ssh_key)
    ssh(
        f"sudo rm -f {READY_PATH} {RELEASE_PATH} {VERIFY_PATH} "
        f"{VERIFIED_PATH} {UNLINK_PATH} {UNLINKED_PATH} {LOG_PATH}")
    ssh(
        f"sudo bash -c 'cd {mount_dir} && "
        f"python3 {script} prepare {OPEN_HANDLE_COUNT}'")

    with client.CreateClient(
            f"localhost:{server_port}", log=logger) as filestore_client:
        file_system_ids = [FILE_SYSTEM_ID]
        if SHARD_COUNT:
            file_system_ids = [
                shard["ShardId"]
                for shard in get_storage_stats(filestore_client, FILE_SYSTEM_ID).get(
                    "ShardStats", []
                )
            ]
            assert len(file_system_ids) == SHARD_COUNT, (
                f"Unexpected number of shards {file_system_ids}"
            )

        def handles():
            return sum(
                get_handles_count(filestore_client, file_system_id)
                for file_system_id in file_system_ids)

        def file_system_handles():
            return {
                file_system_id: get_handles_count(
                    filestore_client, file_system_id)
                for file_system_id in file_system_ids
            }

        wait_for("handles to drain", handles, lambda count: count == 0)
        initial_confirms = get_confirm_create_handle_count()

        workload = common.execute(
            ssh.get_command(
                f"cd {mount_dir} && ulimit -n 65535 && "
                f"exec sudo bash -c 'python3 {script} "
                f"hold {OPEN_HANDLE_COUNT} {READY_PATH} {RELEASE_PATH} "
                f"{VERIFY_PATH} {VERIFIED_PATH} {UNLINK_PATH} {UNLINKED_PATH} "
                f">{LOG_PATH} 2>&1'"),
            wait=False)
        wait_for(
            "guest to open files",
            lambda: guest_file_exists(ssh, READY_PATH) or
            not workload.running,
            bool)
        assert workload.running, (
            f"Guest workload exited with code {workload.returncode}")

        wait_for(
            "handles to appear",
            handles,
            lambda count: count >= OPEN_HANDLE_COUNT)

        wait_for(
            "handles to appear in each filesystem",
            file_system_handles,
            lambda counts: all(count > 0 for count in counts.values()))

        wait_for(
            "half of create handle confirmations",
            get_confirm_create_handle_count,
            lambda count: count >= initial_confirms + OPEN_HANDLE_COUNT // 2)

        ssh(f"sudo touch {UNLINK_PATH}")
        wait_for(
            "guest to unlink open files",
            lambda: guest_file_exists(ssh, UNLINKED_PATH) or
            not workload.running,
            bool)
        assert workload.running, (
            f"Guest workload exited with code {workload.returncode}")

        restart_tablets(filestore_client, file_system_ids)
        restart_vhost(ssh, vhost_port)

        wait_for(
            "90% of create handle confirmations after vhost restart",
            get_confirm_create_handle_count,
            lambda count: count >= initial_confirms + OPEN_HANDLE_COUNT * 9 // 10)

        # These descriptors were opened by the old vhost process. Verify that
        # they remain usable after the restarted process restores the queue.
        ssh(f"sudo touch {VERIFY_PATH}")
        wait_for(
            "guest to verify open files",
            lambda: guest_file_exists(ssh, VERIFIED_PATH) or
            not workload.running,
            bool)
        assert workload.running, (
            f"Guest workload exited with code {workload.returncode}")

        ssh(f"sudo touch {RELEASE_PATH}")
        workload.wait(timeout=MAX_WAIT_SECONDS)
        wait_for("handles to drain", handles, lambda count: count == 0)
