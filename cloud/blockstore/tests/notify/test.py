import logging
import os
import pytest
import requests
import signal
import subprocess
import time

from cloud.blockstore.config.notify_pb2 import TNotifyConfig
from cloud.blockstore.public.sdk.python.protos import STORAGE_MEDIA_SSD_NONREPLICATED
from cloud.blockstore.tests.python.lib.config import NbsConfigurator, generate_disk_agent_txt
from cloud.blockstore.tests.python.lib.test_client import CreateTestClient

import cloud.blockstore.tests.python.lib.daemon as daemon

import contrib.ydb.tests.library.harness.kikimr_runner as kikimr_runner

from yatest.common.network import PortManager

import yatest.common as yatest_common


DEFAULT_BLOCK_SIZE = 4096
DEVICE_HEADER = 4096
DEVICE_PADDING = 4096
DEVICE_SIZE = 1024 ** 3  # 1 GiB


class NotifyMock:
    def __init__(self, port, canonical_file):
        self.port = port
        self.__canonical_file = canonical_file

    def wait_for_notifications(self, count, timeout=30):
        deadline = time.monotonic() + timeout

        while True:
            try:
                with open(self.__canonical_file) as f:
                    lines = f.readlines()
            except FileNotFoundError:
                lines = []

            if sum(line.endswith("\n") for line in lines) >= count:
                return

            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Expected {count} notifications, received "
                    f"{sum(line.endswith(chr(10)) for line in lines)}"
                )

            time.sleep(0.1)

    @property
    def canonical_file(self):
        return yatest_common.canonical_file(
            str(self.__canonical_file), local=True)


@pytest.fixture(name='notify_mock')
def start_notify_mock(tmp_path):

    canonical_file = tmp_path / "canonical_file.txt"

    pm = PortManager()
    port = pm.get_port()

    notify_bin_path = yatest_common.binary_path(
        "cloud/blockstore/tools/testing/notify-mock/notify-mock")

    certs_dir = yatest_common.source_path('cloud/blockstore/tests/certs')

    p = subprocess.Popen(
        [
            notify_bin_path,
            '--port', str(port),
            '--ssl-cert-file', os.path.join(certs_dir, 'server.crt'),
            '--ssl-key-file', os.path.join(certs_dir, 'server.key'),
            '--output-path', canonical_file
        ],
        stdin=None,
        stdout=None,
        stderr=subprocess.PIPE)

    while True:
        try:
            r = requests.get(f"https://localhost:{port}/ping", verify=False)
            r.raise_for_status()
            logging.info(f"Notify service: {r.text}")
            break
        except Exception as e:
            logging.warning(f"Failed to connect to Notify service ({e}). Retry")
            time.sleep(1)
            continue

    yield NotifyMock(port, canonical_file)

    p.send_signal(signal.SIGTERM)
    p.communicate()

    assert p.returncode == 0


@pytest.fixture(name='ydb')
def start_ydb_cluster():

    p = daemon.start_ydb()

    yield p

    p.stop()


@pytest.fixture(name='nbs')
def start_nbs_daemon(request, ydb):

    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()
    cfg.files['storage'].AllocationUnitNonReplicatedSSD = 1  # 1 GiB

    if getattr(request, "param", True):
        notify = request.getfixturevalue("notify_mock")
        cfg.files["notify"] = TNotifyConfig(
            Endpoint=f"https://localhost:{notify.port}/notify/v1/send")

    p = daemon.start_nbs(cfg)
    cli = CreateTestClient(f"localhost:{p.port}")
    cli.execute_DiskRegistrySetWritableState(State=True)

    yield p

    p.stop()


@pytest.fixture(name='data_path')
def create_data_path(tmp_path):

    p = kikimr_runner.get_unique_path_for_current_test(
        output_path=tmp_path,
        sub_folder="data")

    p = os.path.join(p, "dev", "disk", "by-partlabel")
    kikimr_runner.ensure_path_exists(p)

    with open(os.path.join(p, 'NVMENBS01'), 'wb') as f:
        os.truncate(f.fileno(), DEVICE_HEADER + DEVICE_SIZE)

    return p


@pytest.fixture(name='disk_agent')
def start_disk_agent(ydb, nbs, data_path):

    agent_id = 'DiskAgent'

    cfg = NbsConfigurator(ydb, 'disk-agent')
    cfg.generate_default_nbs_configs()
    cfg.files["disk-agent"] = generate_disk_agent_txt(
        agent_id=agent_id,
        device_erase_method='DEVICE_ERASE_METHOD_NONE',  # speed up tests
        storage_discovery_config={
            "PathConfigs": [{
                "PathRegExp": f"{data_path}/NVMENBS([0-9]+)",
                "PoolConfigs": [{
                    "Layout": {
                        "DeviceSize": DEVICE_SIZE,
                        "DevicePadding": DEVICE_PADDING,
                        "HeaderSize": DEVICE_HEADER
                    }
                }]}
            ]})

    p = daemon.start_disk_agent(cfg)
    p.wait_for_registration()

    cli = CreateTestClient(f"localhost:{nbs.port}")

    cli.add_host(agent_id)
    cli.wait_for_devices_to_be_cleared()

    yield p

    p.stop()


@pytest.fixture(name='volume')
def create_volume(nbs, disk_agent):
    disk_id = "vol0"

    cli = CreateTestClient(f"localhost:{nbs.port}")
    cli.create_volume(
        disk_id=disk_id,
        block_size=DEFAULT_BLOCK_SIZE,
        blocks_count=DEVICE_SIZE // DEFAULT_BLOCK_SIZE,
        storage_media_kind=STORAGE_MEDIA_SSD_NONREPLICATED,
        cloud_id="yc-nbs",
        folder_id="tests")

    return cli.describe_volume(disk_id)


@pytest.mark.parametrize("nbs", [False], indirect=True, ids=["null"])
def test_notify_null(nbs, volume):

    cli = CreateTestClient(f"localhost:{nbs.port}")

    cli.execute_DiskRegistryChangeState(
        Message="test",
        ChangeDeviceState={
            "DeviceUUID": volume.Devices[0].DeviceUUID,
            "State": 2,     # DEVICE_STATE_ERROR
        },
    )
    cli.describe_volume(volume.DiskId)


def test_notify(nbs, volume, notify_mock):

    device_id = volume.Devices[0].DeviceUUID

    cli = CreateTestClient(f"localhost:{nbs.port}")

    cli.execute_DiskRegistryChangeState(
        Message="test",
        ChangeDeviceState={
            "DeviceUUID": device_id,
            "State": 2,    # DEVICE_STATE_ERROR
        }
    )

    notify_mock.wait_for_notifications(1)

    return notify_mock.canonical_file


def test_notify_back_online(nbs, volume, notify_mock):

    device_id = volume.Devices[0].DeviceUUID

    cli = CreateTestClient(f"localhost:{nbs.port}")

    cli.execute_DiskRegistryChangeState(
        Message="test",
        ChangeDeviceState={
            "DeviceUUID": device_id,
            "State": 2,    # DEVICE_STATE_ERROR
        }
    )

    notify_mock.wait_for_notifications(1)

    cli.execute_DiskRegistryChangeState(
        Message="test",
        ChangeDeviceState={
            "DeviceUUID": device_id,
            "State": 0,    # DEVICE_STATE_ONLINE
        }
    )

    notify_mock.wait_for_notifications(2)

    return notify_mock.canonical_file


def test_notify_by_user_id(nbs, volume, notify_mock):
    user_id = "vasya"
    device_id = volume.Devices[0].DeviceUUID

    cli = CreateTestClient(f"localhost:{nbs.port}")

    cli.execute_SetUserID(
        DiskId=volume.DiskId,
        UserId=user_id)

    cli.execute_DiskRegistryChangeState(
        Message="test",
        ChangeDeviceState={
            "DeviceUUID": device_id,
            "State": 2,    # DEVICE_STATE_ERROR
        }
    )

    notify_mock.wait_for_notifications(1)

    return notify_mock.canonical_file
