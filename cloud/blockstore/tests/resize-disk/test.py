import os
import subprocess
import time

import yatest.common as common

from cloud.blockstore.public.sdk.python import client


BLOCK_SIZE = 4*1024
OLD_BLOCKS_COUNT = 32*1024
NEW_BLOCKS_COUNT = 64*1024


def _get_nbs_port():
    for env_name in (
            "LOCAL_KIKIMR_INSECURE_NBS_SERVER_PORT",
            "SERVICE_LOCAL_INSECURE_NBS_SERVER_PORT",
            "LOCAL_NULL_INSECURE_NBS_SERVER_PORT"):
        port = os.getenv(env_name)
        if port:
            return port

    raise RuntimeError("Cannot determine NBS port")


def _get_device_size(device_path):
    ex = common.execute(
        ["sudo", "blockdev", "--getsize64", device_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)

    return int(ex.stdout)


def _wait_for_device_size(device_path, expected_size, timeout=60):
    deadline = time.monotonic() + timeout
    last_size = None

    while time.monotonic() < deadline:
        last_size = _get_device_size(device_path)
        if last_size == expected_size:
            return
        time.sleep(1)

    raise AssertionError(
        f"{device_path} size is {last_size}, expected {expected_size}")


def test_resize_disk():
    device_path = os.getenv("NBS_DEVICE_PATH", "/dev/vdb")
    disk_id = os.getenv("NBS_DISK_ID")
    socket = os.getenv("NBS_VHOST_SOCKET")

    assert disk_id
    assert socket

    common.execute(["lsblk"])

    old_size = OLD_BLOCKS_COUNT * BLOCK_SIZE
    new_size = NEW_BLOCKS_COUNT * BLOCK_SIZE

    assert _get_device_size(device_path) == old_size

    with client.CreateClient("localhost:" + _get_nbs_port()) as nbs_client:
        volume = nbs_client.describe_volume(disk_id)
        assert volume.BlocksCount == OLD_BLOCKS_COUNT

        nbs_client.resize_volume(
            disk_id=disk_id,
            blocks_count=NEW_BLOCKS_COUNT,
            channels_count=0,
            config_version=None)

        volume = nbs_client.describe_volume(disk_id)
        assert volume.BlocksCount == NEW_BLOCKS_COUNT

        nbs_client.refresh_endpoint(unix_socket_path=socket)

    _wait_for_device_size(device_path, new_size)
