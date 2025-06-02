import json
import logging
import os
from time import sleep

from retrying import retry

import yatest.common as common

import cloud.filestore.public.sdk.python.client as client
from cloud.storage.core.tools.testing.qemu.lib.common import (
    env_with_guest_index,
    SshToGuest,
)

RETRY_COUNT = 3
WAIT_TIMEOUT = 1000  # 1sec
HANDLE_OPEN_COUNT = 10000
MAX_WAIT_SECONDS = 600


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def get_handles_count(filestore_client: client.Client, logger) -> int:
    res = filestore_client.execute_action(
        action="getstoragestats",
        input=str.encode('{"FileSystemId": "nfs_test"}'))

    try:
        stats = json.loads(res.Output)
        handles_count = stats.get("Stats", {}).get("UsedHandlesCount", 0)
    except (json.JSONDecodeError, AttributeError, TypeError) as e:
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
        f"python3 {script_path} {HANDLE_OPEN_COUNT}'")

    # Check that test script successfully finished.
    assert 0 == res.returncode

    prev = -1
    with client.CreateClient(
            f"localhost:{server_port}", log=logger) as filestore_client:

        # Check that after test script finished,
        # handles count in server not zero.
        # It means that async handle destroying is working
        assert 0 != get_handles_count(filestore_client, logger), (
            "Expected non-zero handles count after script run, got 0")

        # Check that after file closing,
        # handles will be asynchronously freed eventually.
        for _ in range(MAX_WAIT_SECONDS):
            handles_count = get_handles_count(filestore_client, logger)
            logger.info(f"Handles count: {handles_count}")
            if handles_count == 0:
                logger.info("All handles are destroyed")
                break
            if handles_count == prev:
                raise AssertionError(f"Handles count stuck at {handles_count}")
            prev = handles_count
            sleep(1)

        assert 0 == handles_count
