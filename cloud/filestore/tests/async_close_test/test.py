import json
import logging
import os
from time import monotonic, sleep

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


def test():
    logger = logging.getLogger("test")
    server_port = os.getenv("NFS_SERVER_PORT")

    port = int(os.getenv(env_with_guest_index("QEMU_FORWARDING_PORT", 0)))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")

    script_path = common.source_path(
        "cloud/filestore/tests/async_close_test/script.py")

    # Run test script. It will open HANDLE_OPEN_COUNT files
    # and after that close them
    ssh = SshToGuest(user="qemu", port=port, key=ssh_key)
    res = ssh(
        f"sudo bash -c 'cd {mount_dir} && ulimit -n 65535 && "
        f"python3 {script_path} {OPEN_HANDLE_COUNT}'")

    # Check that test script successfully finished.
    assert 0 == res.returncode

    with client.CreateClient(
            f"localhost:{server_port}", log=logger) as filestore_client:

        # Check that after test script finishes,
        # handles count in server is not zero.
        # It means that async handle destroying is working
        handles_count = get_handles_count(filestore_client, logger)
        assert 0 != handles_count, (
            "Expected non-zero handles count after script run, got 0")

        # Check that after file is closed, handles are eventually freed
        started_at = monotonic()
        last_progress_at = started_at

        while handles_count != 0:
            now = monotonic()
            if now - started_at >= MAX_WAIT_SECONDS:
                raise AssertionError(
                    f"Handles were not destroyed within {MAX_WAIT_SECONDS} "
                    f"seconds; {handles_count} handles remain")
            if now - last_progress_at >= MAX_NO_PROGRESS_SECONDS:
                raise AssertionError(
                    f"Handles count did not decrease for "
                    f"{MAX_NO_PROGRESS_SECONDS} seconds; "
                    f"{handles_count} handles remain")

            sleep(1)

            prev_handles_count = handles_count
            handles_count = get_handles_count(filestore_client, logger)
            logger.info(f"Handles count: {handles_count}")
            if handles_count < prev_handles_count:
                last_progress_at = monotonic()

        logger.info("All handles are destroyed")
