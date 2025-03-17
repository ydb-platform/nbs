import json
import os
import pytest
import time

import yatest.common as yatest_common
import cloud.blockstore.tests.python.lib.daemon as daemon

from cloud.blockstore.public.sdk.python.client import CreateClient, Session
from cloud.blockstore.public.sdk.python.protos import TCmsActionRequest, \
    TAction, STORAGE_MEDIA_SSD_MIRROR3
from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt

from contrib.ydb.tests.library.harness.kikimr_runner import \
    get_unique_path_for_current_test, ensure_path_exists


DEVICE_COUNT = 2
DEVICE_SIZE = 1024**3   # 1 GiB
DEVICE_PADDING = 4096
DEVICE_HEADER = 4096
DEFAULT_BLOCK_SIZE = 4096


@pytest.fixture(name='ydb')
def start_ydb_cluster():

    ydb_cluster = daemon.start_ydb()

    yield ydb_cluster

    ydb_cluster.stop()


@pytest.fixture(name='nbs')
def start_nbs_daemon(ydb):

    allocation_unit = DEVICE_SIZE // 1024**3
    assert allocation_unit != 0

    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()

    storage = cfg.files['storage']
    storage.AllocationUnitNonReplicatedSSD = allocation_unit
    storage.AllocationUnitMirror3SSD = allocation_unit
    storage.UseNonreplicatedRdmaActor = True
    storage.UseRdma = True

    server = cfg.files['server'].ServerConfig
    server.UseFakeRdmaClient = True
    server.RdmaClientEnabled = True

    nbs = daemon.start_nbs(cfg)

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


def _wait_for_devices_to_be_cleared(client, expected_agent_count, expected_dirty_count=0):

    while True:
        response = client.execute_action(
            action="BackupDiskRegistryState",
            input_bytes=str.encode('{"BackupLocalDB": true}'))
        bkp = json.loads(response)["Backup"]
        agents = bkp.get("Agents", [])
        dirty_devices = bkp.get("DirtyDevices", [])
        dirty_count = len(dirty_devices)
        if len(agents) == expected_agent_count and dirty_count == expected_dirty_count:
            break
        time.sleep(5)


def _get_agent_id(i):
    return f'node-{i:04}.nbs-dev.hwaas.man.nbhost.net'


def _create_disk_agent_configurator(ydb, i):

    agent_id = _get_agent_id(i)

    data_path = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder=f"{agent_id}_data")

    data_path = os.path.join(data_path, "dev", "disk", "by-partlabel")
    ensure_path_exists(data_path)

    file_size = DEVICE_HEADER + DEVICE_SIZE * DEVICE_COUNT + (DEVICE_COUNT - 1) * DEVICE_PADDING

    with open(os.path.join(data_path, 'NVMENBS01'), 'wb') as f:
        os.truncate(f.fileno(), file_size)

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
    cfg.files["location"].Rack = f'rack-{i}'

    return cfg


def test_fake_rdma_client(ydb, nbs):

    disk_agent_configs = [
        _create_disk_agent_configurator(ydb, i) for i in range(3)]

    disk_agents = [daemon.start_disk_agent(cfg) for cfg in disk_agent_configs]

    client = CreateClient(f"localhost:{nbs.port}")

    for i, disk_agent in enumerate(disk_agents):
        disk_agent.wait_for_registration()
        _add_host(client, _get_agent_id(i))

    _wait_for_devices_to_be_cleared(client, len(disk_agents))

    client.create_volume(
        disk_id="vol0",
        block_size=DEFAULT_BLOCK_SIZE,
        blocks_count=DEVICE_SIZE//DEFAULT_BLOCK_SIZE,
        storage_media_kind=STORAGE_MEDIA_SSD_MIRROR3)

    session = Session(client, "vol0", "")
    _ = session.mount_volume()['Volume']

    expected_data = os.urandom(DEFAULT_BLOCK_SIZE)

    session.write_blocks(0, [expected_data])

    blocks = session.read_blocks(0, 1, checkpoint_id="")
    assert len(blocks) == 1
    assert expected_data == blocks[0], "data corruption!"

    session.unmount_volume()

    for disk_agent in disk_agents:
        disk_agent.kill()
