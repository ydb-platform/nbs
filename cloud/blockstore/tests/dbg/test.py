import json
import os
import pytest
import time
import sys

import yatest.common as yatest_common
import cloud.blockstore.tests.python.lib.daemon as daemon

from cloud.blockstore.public.sdk.python.client.error import ClientError
from cloud.blockstore.public.sdk.python.client import CreateClient, Session
from cloud.blockstore.public.sdk.python.protos import TCmsActionRequest, \
    TAction, STORAGE_MEDIA_SSD_NONREPLICATED
from cloud.blockstore.config.root_kms_pb2 import TRootKmsConfig
from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt
from cloud.storage.core.config.features_pb2 import TFeaturesConfig

from contrib.ydb.tests.library.harness.kikimr_runner import \
    get_unique_path_for_current_test, ensure_path_exists


DEVICE_SIZE = 1024**3   # 1 GiB
DEVICE_COUNT = 6
DEVICE_PADDING = 4096
DEVICE_HEADER = 4096


@pytest.fixture(name='agent_id')
def get_agent_id():
    return daemon.get_fqdn()


@pytest.fixture(name='ydb')
def start_ydb_cluster():

    ydb_cluster = daemon.start_ydb()

    yield ydb_cluster

    ydb_cluster.stop()


@pytest.fixture(name='nbs')
def start_nbs_control(ydb):
    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()
    cfg.files['storage'].AllocationUnitNonReplicatedSSD = 1  # 1 GiB
    cfg.files['storage'].DisableLocalService = False

    server = cfg.files['server'].ServerConfig
    server.NbdEnabled = True
    server.NbdThreadsCount = 1

    nbs = daemon.start_nbs(cfg, name='nbs-control')

    client = CreateClient(f"localhost:{nbs.port}")
    client.execute_action(
        action="DiskRegistrySetWritableState",
        input_bytes=str.encode('{"State": true}'))
    client.update_disk_registry_config({
        "KnownDevicePools":
            [{"Kind": "DEVICE_POOL_KIND_DEFAULT", "AllocationUnit": DEVICE_SIZE}]
        })

    yield nbs

    nbs.kill()


def _add_host(client, agent_id):
    request = TCmsActionRequest()
    action = request.Actions.add()
    action.Type = TAction.ADD_HOST
    action.Host = agent_id

    return client.cms_action(request)


def _wait_for_devices_to_be_cleared(client, expected_dirty_count=0):
    while True:
        response = client.execute_action(
            action="BackupDiskRegistryState",
            input_bytes=str.encode('{"BackupLocalDB": true}'))
        bkp = json.loads(response)["Backup"]
        agents = bkp.get("Agents", [])
        dirty_devices = bkp.get("DirtyDevices", [])
        dirty_count = len(dirty_devices)
        if len(agents) != 0 and dirty_count == expected_dirty_count:
            break
        time.sleep(1)


@pytest.fixture(name='disk_agent')
def start_disk_agent(ydb, nbs, agent_id):

    data_path = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder="data")

    data_path = os.path.join(data_path, "dev", "disk", "by-partlabel")
    ensure_path_exists(data_path)

    with open(os.path.join(data_path, 'NVMENBS01'), 'wb') as f:
        os.truncate(
            f.fileno(),
            DEVICE_HEADER + DEVICE_SIZE * DEVICE_COUNT + (DEVICE_COUNT - 1) *
            DEVICE_PADDING)

    cfg = NbsConfigurator(ydb, 'disk-agent')
    cfg.generate_default_nbs_configs()
    cfg.files["disk-agent"] = generate_disk_agent_txt(
        agent_id='',
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

    disk_agent = daemon.start_disk_agent(cfg)
    disk_agent.wait_for_registration()

    client = CreateClient(f"localhost:{nbs.port}")

    _add_host(client, agent_id)
    _wait_for_devices_to_be_cleared(client)

    yield disk_agent

    disk_agent.stop()


def test_generic(ydb, nbs, disk_agent):

    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()
    storage = cfg.files['storage']
    storage.AllocationUnitNonReplicatedSSD = 1  # 1 GiB
    storage.DisableLocalService = True
    storage.AcquireNonReplicatedDevices = True

    server = cfg.files['server'].ServerConfig
    server.NbdEnabled = True
    server.NbdThreadsCount = 1

    volume_host = daemon.start_nbs(cfg, name='volume-host')

    client = CreateClient(f"localhost:{volume_host.port}")

    print(f"======= volume_host.port: {volume_host.port}", file=sys.stderr)

    client.create_volume(
        disk_id="vol0",
        block_size=4096,
        blocks_count=2*DEVICE_SIZE//4096,
        storage_media_kind=STORAGE_MEDIA_SSD_NONREPLICATED)

    session = Session(client, "vol0", "")
    vol0 = session.mount_volume()['Volume']

    for i in range(1000):
        if i == 10:
            print(f"======= kill DR", file=sys.stderr)
            nbs.kill()

        if i == 100:
            print(f"======= restart NBS", file=sys.stderr)
            volume_host.kill()
            volume_host = daemon.start_nbs(cfg, name='volume-host-2')

        try:
            r = session.write_blocks(0, [b'\1' * 4096])
            print(f"write_blocks: {r}", file=sys.stderr)
        except ClientError as e:
            print(f"==== ERROR: {e}", file=sys.stderr)

        time.sleep(1)

    volume_host.kill()
