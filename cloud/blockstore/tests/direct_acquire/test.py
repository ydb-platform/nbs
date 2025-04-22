import json
import logging
import os
import pytest
import time

from cloud.blockstore.public.sdk.python.client import CreateClient, Session
from cloud.blockstore.public.sdk.python.protos import TCmsActionRequest, \
    TAction, STORAGE_MEDIA_SSD_NONREPLICATED

from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs, \
    start_disk_agent

import yatest.common as yatest_common

from contrib.ydb.tests.library.harness.kikimr_runner import \
    get_unique_path_for_current_test, ensure_path_exists


DEVICE_SIZE = 1024 ** 3  # 1 GiB
DEVICES_PER_PATH = 6

KNOWN_DEVICE_POOLS = {
    "KnownDevicePools": [
        {"Kind": "DEVICE_POOL_KIND_DEFAULT", "AllocationUnit": DEVICE_SIZE},
    ]}


@pytest.fixture(name='ydb')
def start_ydb_cluster():

    ydb_cluster = start_ydb()

    yield ydb_cluster

    ydb_cluster.stop()


@pytest.fixture(name='nbs')
def start_nbs_daemon(ydb):

    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()

    cfg.files["storage"].DisableLocalService = 0
    cfg.files["storage"].NonReplicatedDontSuspendDevices = True
    cfg.files["storage"].AllocationUnitNonReplicatedSSD = 1
    cfg.files["storage"].NonReplicatedAgentMinTimeout = 600000  # 10min
    cfg.files["storage"].NonReplicatedAgentMaxTimeout = 600000  # 10min
    cfg.files["storage"].NonReplicatedVolumeDirectAcquireEnabled = True
    cfg.files["storage"].AcquireNonReplicatedDevices = True

    daemon = start_nbs(cfg)

    client = CreateClient(f"localhost:{daemon.port}")
    client.execute_action(
        action="DiskRegistrySetWritableState",
        input_bytes=str.encode('{"State": true}'))
    client.update_disk_registry_config(KNOWN_DEVICE_POOLS)

    yield daemon

    daemon.kill()


@pytest.fixture(name='agent_ids')
def create_agent_ids():
    return ['agent-1', 'agent-2', 'agent-3']


def _get_agent_data_path(agent_id, data_path):
    return os.path.join(data_path, agent_id, "dev", "disk", "by-partlabel")


@pytest.fixture(name='data_path')
def create_data_path(agent_ids):

    data = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder="data")

    for agent_id in agent_ids:
        ensure_path_exists(_get_agent_data_path(agent_id, data))

    return data


@pytest.fixture(autouse=True)
def create_device_files(data_path, agent_ids):

    for agent_id in agent_ids:
        p = _get_agent_data_path(agent_id, data_path)
        with open(os.path.join(p, 'NVMENBS01'), 'wb') as f:
            os.truncate(f.fileno(), DEVICES_PER_PATH * (DEVICE_SIZE + 4096))


def _create_disk_agent_configurator(ydb, agent_id, data_path):
    assert agent_id is not None

    configurator = NbsConfigurator(ydb, node_type='disk-agent')
    configurator.generate_default_nbs_configs()

    disk_agent_config = generate_disk_agent_txt(
        agent_id=agent_id,
        device_erase_method='DEVICE_ERASE_METHOD_NONE',  # speed up tests
        storage_discovery_config={
            "PathConfigs": [{
                "PathRegExp": f"{data_path}/NVMENBS([0-9]+)",
                "PoolConfigs": [{
                    "MinSize": 4096 + DEVICE_SIZE,
                    "Layout": {
                        "DeviceSize": DEVICE_SIZE,
                        "DevicePadding": 4096,
                        "HeaderSize": 4096
                    }
                }]}
            ]})

    caches = os.path.join(
        get_unique_path_for_current_test(
            output_path=yatest_common.output_path(),
            sub_folder="caches"),
        agent_id)
    ensure_path_exists(caches)

    disk_agent_config.CachedConfigPath = os.path.join(caches, 'config.txt')
    disk_agent_config.DisableBrokenDevices = True

    configurator.files["disk-agent"] = disk_agent_config
    configurator.files["location"].Rack = 'c:RACK'

    return configurator


@pytest.fixture(name='disk_agent_configurators')
def create_disk_agent_configurators(ydb, agent_ids, data_path):
    configurators = []

    for agent_id in agent_ids:
        p = _get_agent_data_path(agent_id, data_path)
        configurators.append(_create_disk_agent_configurator(ydb, agent_id, p))

    return configurators


def _backup(client):
    response = client.execute_action(
        action="BackupDiskRegistryState",
        input_bytes=str.encode('{"BackupLocalDB": true}'))

    return json.loads(response)["Backup"]


def _add_host(client, agent_id):
    request = TCmsActionRequest()
    action = request.Actions.add()
    action.Type = TAction.ADD_HOST
    action.Host = agent_id

    return client.cms_action(request)


# wait for devices to be cleared
def _wait_devices(client):
    while True:
        bkp = _backup(client)
        if bkp.get("DirtyDevices", 0) == 0:
            break
        time.sleep(1)


def test_should_mount_volume_with_unknown_devices(
        nbs,
        data_path,
        agent_ids,
        disk_agent_configurators):

    logger = logging.getLogger("client")
    logger.setLevel(logging.DEBUG)

    client = CreateClient(f"localhost:{nbs.port}", log=logger)
    agent_id = agent_ids[0]
    configurator = disk_agent_configurators[0]

    data_path_for_agent = _get_agent_data_path(agent_id, data_path)

    with open(os.path.join(data_path_for_agent, 'NVMENBS02'), 'wb') as f:
        os.truncate(f.fileno(), DEVICES_PER_PATH * (DEVICE_SIZE + 4096))

    # run an agent
    agent = start_disk_agent(configurator, name=agent_id)

    agent.wait_for_registration()
    r = _add_host(client, agent_id)
    assert len(r.ActionResults) == 1
    assert r.ActionResults[0].Result.Code == 0

    _wait_devices(client)

    # create a volume
    client.create_volume(
        disk_id="vol1",
        block_size=4096,
        blocks_count=2*DEVICES_PER_PATH*DEVICE_SIZE//4096,
        storage_media_kind=STORAGE_MEDIA_SSD_NONREPLICATED,
        cloud_id="test")

    bkp = _backup(client)
    assert len(bkp['Disks']) == 1
    assert len(bkp['Disks'][0]['DeviceUUIDs']) == 12
    assert len(bkp['Agents']) == 1

    session = Session(client, "vol1", "")
    session.mount_volume()

    # IO should work
    session.write_blocks(0, [b'\1' * 4096 * 2])
    blocks = session.read_blocks(0, 1, checkpoint_id="")
    assert len(blocks) == 1

    session.unmount_volume()

    # stop the agent
    agent.kill()

    time.sleep(5)

    configurator.files["disk-agent"].\
        StorageDiscoveryConfig.PathConfigs[0].PathRegExp = f"{data_path_for_agent}/NVMENBS([0-1]+)"
    configurator.files["disk-agent"].CachedConfigPath = ""

    agent = start_disk_agent(configurator, name=agent_id)
    # assert false
    agent.wait_for_registration()

    r = _add_host(client, agent_id)
    assert len(r.ActionResults) == 1
    assert r.ActionResults[0].Result.Code == 0

    session.mount_volume()
    except_count = 0
    for i in range(2*DEVICES_PER_PATH):
        try:
            blocks = session.read_blocks(i*DEVICE_SIZE//4096, 1, checkpoint_id="")
        except Exception:
            except_count += 1
    assert except_count == DEVICES_PER_PATH
    session.unmount_volume()
