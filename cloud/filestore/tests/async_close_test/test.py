import json
import logging
import os
from time import monotonic, sleep

import requests
from retrying import retry

import yatest.common as common

import cloud.filestore.public.sdk.python.client as client
from cloud.storage.core.tools.testing.qemu.lib.common import (
    env_with_guest_index,
    SshToGuest,
)

RETRY_COUNT = 3
WAIT_TIMEOUT_MS = 1000  # 1sec
OPEN_HANDLE_COUNT = 10000
MAX_WAIT_SECONDS = 900
MAX_NO_PROGRESS_SECONDS = 30


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT_MS)
def get_handles_count(filestore_client: client.Client, logger) -> int:
    res = filestore_client.execute_action(
        action="getstoragestats",
        input=str.encode('{"FileSystemId": "nfs_test"}'))

    try:
        stats = json.loads(res.Output)
        handles_count = int(
            stats.get("Stats", {}).get("UsedHandlesCount", 0))
    except (json.JSONDecodeError, AttributeError, TypeError, ValueError) as e:
        logger.error(f"Failed to parse getstoragestats answer: {e}")
        raise

    return handles_count


def get_queue_size(mon_port, logger):
    url = f"http://localhost:{mon_port}/counters/counters=filestore/json"
    try:
        response = requests.get(url, timeout=2)
        response.raise_for_status()
        counters = response.json()
    except requests.RequestException as error:
        logger.info("Vhost monitoring is unavailable during restart: %s", error)
        return None

    sizes = [
        int(sensor["value"])
        for sensor in counters["sensors"]
        if all(sensor["labels"].get(key) == value for key, value in {
            "component": "client_fs",
            "filesystem": "nfs_test",
            "module": "HandleOpsQueue",
            "sensor": "EntryCount",
        }.items())
    ]
    # Counters disappear while vhost restarts. Missing is not an empty queue.
    return sum(sizes) if sizes else None


def test():
    logger = logging.getLogger("test")
    server_port = os.getenv("NFS_SERVER_PORT")
    mon_port = os.getenv("NFS_VHOST_MON_PORT")

    port = int(os.getenv(env_with_guest_index("QEMU_FORWARDING_PORT", 0)))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")

    script_path = common.source_path(
        "cloud/filestore/tests/async_close_test/script.py")

    # Observe async destroys while the guest opens and closes its files.
    ssh = SshToGuest(user="qemu", port=port, key=ssh_key)
    workload = common.execute(
        ssh.get_command(
            f"sudo bash -c 'cd {mount_dir} && ulimit -n 65535 && "
            f"python3 {script_path} {OPEN_HANDLE_COUNT}'",
            timeout=MAX_WAIT_SECONDS),
        wait=False)

    saw_pending = False
    started_at = monotonic()
    try:
        while True:
            queue_size = get_queue_size(mon_port, logger)
            logger.info("HandleOpsQueue size: %s", queue_size)
            saw_pending |= queue_size is not None and queue_size > 0
            if not workload.running:
                break
            assert monotonic() - started_at < MAX_WAIT_SECONDS, (
                f"Guest workload did not finish within {MAX_WAIT_SECONDS} seconds")
            sleep(1)

        workload.wait(timeout=10)
        assert workload.returncode == 0, (
            f"Guest workload failed with code {workload.returncode}")
    finally:
        if workload.running:
            workload.kill()

    with client.CreateClient(
            f"localhost:{server_port}", log=logger) as filestore_client:

        # The queue may already be drained when SSH returns. Keep observing it
        # here too, in case monitoring was unavailable at the end of the script.
        handles_count = get_handles_count(filestore_client, logger)
        queue_size = get_queue_size(mon_port, logger)
        saw_pending |= queue_size is not None and queue_size > 0
        last_queue_size = queue_size

        # Check that after file is closed, handles are eventually freed
        started_at = monotonic()
        last_progress_at = started_at

        while handles_count != 0 or queue_size != 0:
            now = monotonic()
            if now - started_at >= MAX_WAIT_SECONDS:
                raise AssertionError(
                    f"Handles were not destroyed within {MAX_WAIT_SECONDS} "
                    f"seconds; {handles_count} handles remain, "
                    f"queue size: {queue_size}")
            if now - last_progress_at >= MAX_NO_PROGRESS_SECONDS:
                raise AssertionError(
                    f"Handles count and queue size did not decrease for "
                    f"{MAX_NO_PROGRESS_SECONDS} seconds; "
                    f"{handles_count} handles remain, queue size: {queue_size}")

            sleep(1)

            prev_handles_count = handles_count
            handles_count = get_handles_count(filestore_client, logger)
            queue_size = get_queue_size(mon_port, logger)
            saw_pending |= queue_size is not None and queue_size > 0
            logger.info("Handles count: %s, queue size: %s",
                        handles_count, queue_size)
            if handles_count < prev_handles_count:
                last_progress_at = monotonic()
            if queue_size is not None:
                if last_queue_size is not None and queue_size < last_queue_size:
                    last_progress_at = monotonic()
                last_queue_size = queue_size

        # Synchronous closes bypass HandleOpsQueue and must not pass this test.
        assert saw_pending, "No pending async handle operations were observed"
        logger.info("Async handle operations observed; all handles are destroyed")
