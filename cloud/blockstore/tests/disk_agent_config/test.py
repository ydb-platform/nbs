import logging
import os
import pytest
import requests
import time

from cloud.blockstore.tests.python.lib.test_client import CreateTestClient

from cloud.blockstore.public.sdk.python.client import Session
from cloud.blockstore.public.sdk.python.client.error import ClientError
from cloud.blockstore.public.sdk.python.client.error_codes import EResult
from cloud.blockstore.public.sdk.python.protos import STORAGE_MEDIA_SSD_NONREPLICATED
from cloud.blockstore.config.disk_pb2 import DISK_AGENT_BACKEND_NULL

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

    daemon = start_nbs(cfg)

    client = CreateTestClient(f"localhost:{daemon.port}")
    client.execute_DiskRegistrySetWritableState(State=True)
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


def test_change_rack(nbs, agent_ids, disk_agent_configurators):

    client = CreateTestClient(f"localhost:{nbs.port}")

    # run agents

    agents = []
    for agent_id, configurator in zip(agent_ids, disk_agent_configurators):
        agents.append(start_disk_agent(configurator, name=agent_id))

    for agent_id, agent in zip(agent_ids, agents):
        agent.wait_for_registration()
        r = client.add_host(agent_id)
        assert len(r.ActionResults) == 1
        assert r.ActionResults[0].Result.Code == 0

    bkp = client.backup_disk_registry_state()

    assert len(bkp['Agents']) == len(agent_ids)
    for agent in bkp['Agents']:
        assert len(agent['Devices']) == DEVICES_PER_PATH
        assert agent.get('State') is None  # online
        for d in agent['Devices']:
            assert d.get('State') is None  # online
            assert d['Rack'] == 'c:RACK'   # default rack

    client.wait_for_devices_to_be_cleared()

    # create volumes

    client.create_volume(
        disk_id="vol1",
        block_size=4096,
        blocks_count=2*DEVICE_SIZE//4096,
        storage_media_kind=STORAGE_MEDIA_SSD_NONREPLICATED)

    client.create_volume(
        disk_id="vol2",
        block_size=4096,
        blocks_count=3*DEVICE_SIZE//4096,
        storage_media_kind=STORAGE_MEDIA_SSD_NONREPLICATED)

    client.create_volume(
        disk_id="vol3",
        block_size=4096,
        blocks_count=4*DEVICE_SIZE//4096,
        storage_media_kind=STORAGE_MEDIA_SSD_NONREPLICATED)

    # change racks
    for agent_id, configurator in zip(agent_ids, disk_agent_configurators):
        configurator.files["location"].Rack = f'{agent_id}-RACK'

    # restart agents
    for i, agent in enumerate(agents):
        agent.kill()
        agent = start_disk_agent(
            disk_agent_configurators[i], name=f'{agent_ids[i]}.new')
        agent.wait_for_registration()
        agents[i] = agent

    # check that all disks and devices are online, and each disk_agent has its
    # own rack

    bkp = client.backup_disk_registry_state()

    bkp['Agents'].sort(key=lambda x: x['AgentId'])
    bkp['Disks'].sort(key=lambda x: x['DiskId'])

    for agent_id, agent in zip(agent_ids, bkp['Agents']):
        assert agent_id == agent['AgentId']
        assert len(agent['Devices']) == DEVICES_PER_PATH
        assert agent.get('State') is None  # online

        for d in agent['Devices']:
            assert d.get('State') is None  # online
            assert d['Rack'] == f'{agent_id}-RACK'

    for i, disk in enumerate(bkp['Disks']):
        disk_id = f'vol{i+1}'
        assert disk_id == disk['DiskId']
        assert disk.get('State') is None  # online
        assert len(disk['DeviceUUIDs']) == i + 2

    for agent in agents:
        agent.kill()


def test_null_backend(nbs, agent_ids, disk_agent_configurators):

    client = CreateTestClient(f"localhost:{nbs.port}")

    agent_id = agent_ids[0]
    configurator = disk_agent_configurators[0]
    configurator.files["disk-agent"].Backend = DISK_AGENT_BACKEND_NULL

    agent = start_disk_agent(configurator, name=agent_id)

    agent.wait_for_registration()
    r = client.add_host(agent_id)
    assert len(r.ActionResults) == 1
    assert r.ActionResults[0].Result.Code == 0

    client.wait_for_devices_to_be_cleared()

    client.create_volume(
        disk_id="vol1",
        block_size=4096,
        blocks_count=DEVICE_SIZE//4096,
        storage_media_kind=STORAGE_MEDIA_SSD_NONREPLICATED)

    session = Session(client, "vol1", "")
    session.mount_volume()
    session.write_blocks(0, [b'\1' * 4096])
    blocks = session.read_blocks(0, 1, checkpoint_id="")
    assert len(blocks) == 1
    session.unmount_volume()


def test_disable_node_broker_registration(nbs, agent_ids, disk_agent_configurators):
    assert len(disk_agent_configurators) >= 2

    # The first agent will not register in the node broker.
    disk_agent_configurators[0].files["disk-agent"]\
        .StorageDiscoveryConfig.PathConfigs[0].PathRegExp = "unknown_path"
    disk_agent_configurators[0].files["disk-agent"]\
        .DisableNodeBrokerRegistrationOnDevicelessAgent = True

    # The second agent should register, even without devices.
    disk_agent_configurators[1].files["disk-agent"]\
        .StorageDiscoveryConfig.PathConfigs[0].PathRegExp = "unknown_path"
    disk_agent_configurators[1].files["disk-agent"]\
        .DisableNodeBrokerRegistrationOnDevicelessAgent = False

    agents = []
    for agent_id, configurator in zip(agent_ids, disk_agent_configurators):
        agents.append(start_disk_agent(configurator, name=agent_id))

    for idx, agent in enumerate(agents):
        with open(agent.stderr_file_name) as log_file:
            deep_idle_agent = \
                "Devices were not found. Skipping the node broker registration." in log_file.read()
            assert deep_idle_agent == disk_agent_configurators[idx].files[
                "disk-agent"].DisableNodeBrokerRegistrationOnDevicelessAgent

    response = requests.get(f"http://localhost:{agents[0].mon_port}")
    assert response.status_code == 200
    assert "This node is not registered in the NodeBroker" in response.text

    for agent in agents:
        agent.kill()


def test_disable_io_for_broken_devices(
        nbs,
        data_path,
        agent_ids,
        disk_agent_configurators):

    agent_id = agent_ids[0]
    configurator = disk_agent_configurators[0]

    # setup a serial number for NVMENBS01
    m = configurator.files["disk-agent"].PathToSerialNumberMapping.add()
    m.Path = os.path.join(_get_agent_data_path(
        agent_id, data_path), 'NVMENBS01')
    m.SerialNumber = 'SN'

    logger = logging.getLogger("client")
    logger.setLevel(logging.DEBUG)

    client = CreateTestClient(f"localhost:{nbs.port}", log=logger)

    # run an agent
    agent = start_disk_agent(configurator, name=agent_id)

    agent.wait_for_registration()
    r = client.add_host(agent_id)
    assert len(r.ActionResults) == 1
    assert r.ActionResults[0].Result.Code == 0

    client.wait_for_devices_to_be_cleared()

    # create a volume
    client.create_volume(
        disk_id="vol1",
        block_size=4096,
        blocks_count=DEVICE_SIZE//4096,
        storage_media_kind=STORAGE_MEDIA_SSD_NONREPLICATED,
        cloud_id="test")

    bkp = client.backup_disk_registry_state()
    assert len(bkp['Disks']) == 1
    assert len(bkp['Disks'][0]['DeviceUUIDs']) == 1
    assert len(bkp['Agents']) == 1
    for d in bkp['Agents'][0]['Devices']:
        assert d.get('SerialNumber') == 'SN'

    session = Session(client, "vol1", "")
    session.mount_volume()

    # IO should work
    session.write_blocks(0, [b'\1' * 4096])
    blocks = session.read_blocks(0, 1, checkpoint_id="")
    assert len(blocks) == 1

    # stop the agent
    agent.kill()

    # start an IO operation
    future = session.read_blocks_async(0, 1, checkpoint_id="")

    assert not future.done()
    time.sleep(5)
    assert not future.done()

    # change NVMENBS01's serial number
    m.SerialNumber = 'XXX'

    # restart the agent
    agent = start_disk_agent(configurator, name=agent_id+'.new')
    agent.wait_for_registration()

    # IO should result in E_IO_SILENT now
    try:
        _ = future.result()
        assert False
    except ClientError as e:
        assert e.code == EResult.E_IO_SILENT.value

    session.unmount_volume()

    bkp = client.backup_disk_registry_state()
    for d in bkp['Agents'][0]['Devices']:
        assert d.get('SerialNumber') == 'XXX'
