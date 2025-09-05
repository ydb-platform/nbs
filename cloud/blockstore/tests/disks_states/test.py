import logging
import os
import pytest
import time

from cloud.blockstore.tests.python.lib.test_client import CreateTestClient

from cloud.blockstore.public.sdk.python.client.error_codes import EResult
from cloud.blockstore.public.sdk.python.protos import \
    STORAGE_MEDIA_SSD_NONREPLICATED, \
    STORAGE_MEDIA_SSD_MIRROR3, \
    DISK_STATE_ONLINE, \
    DISK_STATE_ERROR

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

DISK_STATE_MIGRATION_MESSAGE = "data migration in progress, slight performance decrease may be experienced"


@pytest.fixture(name='ydb')
def start_ydb_cluster():

    ydb_cluster = start_ydb()

    yield ydb_cluster

    ydb_cluster.stop()


@pytest.fixture(name='nbs')
def start_nbs_daemon(ydb):

    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()

    cfg.files["storage"].DisableLocalService = False
    cfg.files["storage"].NonReplicatedDontSuspendDevices = True
    cfg.files["storage"].AllocationUnitNonReplicatedSSD = 1
    cfg.files["storage"].AllocationUnitMirror3SSD = 1
    cfg.files["storage"].NonReplicatedAgentMinTimeout = 10000  # 10s
    cfg.files["storage"].NonReplicatedAgentMaxTimeout = 10000  # 10s

    daemon = start_nbs(cfg)

    client = CreateTestClient(f"localhost:{daemon.port}")
    client.execute_DiskRegistrySetWritableState(State=True)
    client.update_disk_registry_config(KNOWN_DEVICE_POOLS)

    yield daemon

    daemon.stop()


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
    configurator.files["location"].Rack = 'RACK:' + agent_id

    return configurator


@pytest.fixture(name='disk_agent_configurators')
def create_disk_agent_configurators(ydb, agent_ids, data_path):
    configurators = []

    for agent_id in agent_ids:
        p = _get_agent_data_path(agent_id, data_path)
        configurators.append(_create_disk_agent_configurator(ydb, agent_id, p))

    return configurators


def test_statuses(nbs, agent_ids, disk_agent_configurators):

    logger = logging.getLogger("client")
    logger.setLevel(logging.DEBUG)

    client = CreateTestClient(f"localhost:{nbs.port}", log=logger)

    # run agents

    agents = []
    for agent_id, configurator in zip(agent_ids, disk_agent_configurators):
        agents.append(start_disk_agent(configurator, name=agent_id))

    for agent_id, agent in zip(agent_ids, agents):
        agent.wait_for_registration()
        r = client.add_host(agent_id)
        assert len(r.ActionResults) == 1
        assert r.ActionResults[0].Result.Code == 0

    client.wait_for_devices_to_be_cleared()

    states = client.list_disks_states()
    assert len(states) == 0

    # create volumes

    client.create_volume(
        disk_id="m3",
        block_size=4096,
        blocks_count=3*DEVICE_SIZE//4096,
        storage_media_kind=STORAGE_MEDIA_SSD_MIRROR3)

    states = client.list_disks_states()
    assert len(states) == 1

    m3 = states[0]

    assert m3.DiskId == "m3"
    assert m3.State == DISK_STATE_ONLINE
    assert m3.StateMessage == ''

    for i, agent_id in enumerate(agent_ids):
        client.create_volume(
            disk_id=f"vol{i + 1}",
            block_size=4096,
            blocks_count=1*DEVICE_SIZE//4096,
            storage_media_kind=STORAGE_MEDIA_SSD_NONREPLICATED,
            agent_ids=[agent_id])

    def get_states():
        states = client.list_disks_states()
        assert len(states) == (len(agent_ids) + 1)
        states.sort(key=lambda s: s.DiskId)
        return states

    m3, vol1, vol2, vol3 = get_states()

    assert m3.DiskId == "m3"
    assert m3.State == DISK_STATE_ONLINE
    assert m3.StateMessage == ''

    assert vol1.DiskId == "vol1"
    assert vol1.State == DISK_STATE_ONLINE
    assert vol1.StateMessage == ''

    assert vol2.DiskId == "vol2"
    assert vol2.State == DISK_STATE_ONLINE
    assert vol2.StateMessage == ''

    assert vol3.DiskId == "vol3"
    assert vol3.State == DISK_STATE_ONLINE
    assert vol3.StateMessage == ''

    # remove agent-2
    r = client.remove_host(agent_ids[1])
    assert len(r.ActionResults) == 1
    assert r.ActionResults[0].Result.Code == EResult.E_TRY_AGAIN.value
    assert r.ActionResults[0].Timeout != 0

    m3, vol1, vol2, vol3 = get_states()

    # m3 is migrating
    assert m3.DiskId == "m3"
    assert m3.State == DISK_STATE_ONLINE
    assert m3.StateMessage == DISK_STATE_MIGRATION_MESSAGE

    assert vol1.DiskId == "vol1"
    assert vol1.State == DISK_STATE_ONLINE
    assert vol1.StateMessage == ''

    # vol2 is migrating
    assert vol2.DiskId == "vol2"
    assert vol2.State == DISK_STATE_ONLINE
    assert vol2.StateMessage == DISK_STATE_MIGRATION_MESSAGE

    assert vol3.DiskId == "vol3"
    assert vol3.State == DISK_STATE_ONLINE
    assert vol3.StateMessage == ''

    vol3_device = client.stat_volume("vol3")["Volume"].Devices[0]

    r = client.remove_device(vol3_device.AgentId, vol3_device.DeviceName)

    assert len(r.ActionResults) == 1
    assert r.ActionResults[0].Result.Code == EResult.E_TRY_AGAIN.value
    assert r.ActionResults[0].Timeout != 0

    m3, vol1, vol2, vol3 = get_states()

    # m3 is migrating
    assert m3.DiskId == "m3"
    assert m3.State == DISK_STATE_ONLINE
    assert m3.StateMessage == DISK_STATE_MIGRATION_MESSAGE

    assert vol1.DiskId == "vol1"
    assert vol1.State == DISK_STATE_ONLINE
    assert vol1.StateMessage == ''

    # vol2 is migrating
    assert vol2.DiskId == "vol2"
    assert vol2.State == DISK_STATE_ONLINE
    assert vol2.StateMessage == DISK_STATE_MIGRATION_MESSAGE

    # vol3 is migrating
    assert vol3.DiskId == "vol3"
    assert vol3.State == DISK_STATE_ONLINE
    assert vol3.StateMessage == DISK_STATE_MIGRATION_MESSAGE

    client.execute_DiskRegistryChangeState(
        Message="test",
        ChangeDeviceState={
            "DeviceUUID": vol3_device.DeviceUUID,
            "State": 2,    # DEVICE_STATE_ERROR
        })

    m3, vol1, vol2, vol3 = get_states()

    # m3 is migrating
    assert m3.DiskId == "m3"
    assert m3.State == DISK_STATE_ONLINE
    assert m3.StateMessage == DISK_STATE_MIGRATION_MESSAGE

    assert vol1.DiskId == "vol1"
    assert vol1.State == DISK_STATE_ONLINE
    assert vol1.StateMessage == ''

    # vol2 is migrating
    assert vol2.DiskId == "vol2"
    assert vol2.State == DISK_STATE_ONLINE
    assert vol2.StateMessage == DISK_STATE_MIGRATION_MESSAGE

    # vol3 is broken
    assert vol3.DiskId == "vol3"
    assert vol3.State == DISK_STATE_ERROR
    assert vol3.StateMessage == ''

    # kill agent-1
    agents[0].kill()

    def wait_agent1_state(desired_state):
        for _ in range(120):
            bkp = client.backup_disk_registry_state()
            bkp['Agents'].sort(key=lambda a: a['AgentId'])
            assert bkp['Agents'][0]['AgentId'] == agent_ids[0]
            if bkp['Agents'][0].get('State') == desired_state:
                return
            time.sleep(1)
        pytest.fail(f"{agent_ids[0]} is not in {desired_state} state")

    # wait until agent-1 becomes unavailable
    wait_agent1_state('AGENT_STATE_UNAVAILABLE')

    m3, vol1, vol2, vol3 = get_states()

    # m3 is migrating (despite the fact that one of the replicas is unavailable)
    assert m3.DiskId == "m3"
    assert m3.State == DISK_STATE_ONLINE
    assert m3.StateMessage == DISK_STATE_MIGRATION_MESSAGE

    # vol1 is broken
    assert vol1.DiskId == "vol1"
    assert vol1.State == DISK_STATE_ERROR
    assert vol1.StateMessage == ''

    # vol2 is migrating
    assert vol2.DiskId == "vol2"
    assert vol2.State == DISK_STATE_ONLINE
    assert vol2.StateMessage == DISK_STATE_MIGRATION_MESSAGE

    # vol3 is broken
    assert vol3.DiskId == "vol3"
    assert vol3.State == DISK_STATE_ERROR
    assert vol3.StateMessage == ''

    # restart agent-1

    agents[0] = start_disk_agent(disk_agent_configurators[0], name=agent_ids[0])
    agents[0].wait_for_registration()

    # wait until agent-1 returns from the unavailable state
    wait_agent1_state('AGENT_STATE_WARNING')

    m3, vol1, vol2, vol3 = get_states()

    # m3 is migrating
    assert m3.DiskId == "m3"
    assert m3.State == DISK_STATE_ONLINE
    assert m3.StateMessage == DISK_STATE_MIGRATION_MESSAGE

    # vol1 is migrating
    assert vol1.DiskId == "vol1"
    assert vol1.State == DISK_STATE_ONLINE
    assert vol1.StateMessage == DISK_STATE_MIGRATION_MESSAGE

    # vol2 is migrating
    assert vol2.DiskId == "vol2"
    assert vol2.State == DISK_STATE_ONLINE
    assert vol2.StateMessage == DISK_STATE_MIGRATION_MESSAGE

    # vol3 is broken
    assert vol3.DiskId == "vol3"
    assert vol3.State == DISK_STATE_ERROR
    assert vol3.StateMessage == ''

    # fix agent-1
    client.execute_DiskRegistryChangeState(
        Message="test",
        ChangeAgentState={
            "AgentId": agent_ids[0],
            "State": 0,    # AGENT_STATE_ONLINE
        })

    m3, vol1, vol2, vol3 = get_states()

    # m3 is migrating
    assert m3.DiskId == "m3"
    assert m3.State == DISK_STATE_ONLINE
    assert m3.StateMessage == DISK_STATE_MIGRATION_MESSAGE

    # vol1 is online
    assert vol1.DiskId == "vol1"
    assert vol1.State == DISK_STATE_ONLINE
    assert vol1.StateMessage == ''

    # vol2 is migrating
    assert vol2.DiskId == "vol2"
    assert vol2.State == DISK_STATE_ONLINE
    assert vol2.StateMessage == DISK_STATE_MIGRATION_MESSAGE

    # vol3 is broken
    assert vol3.DiskId == "vol3"
    assert vol3.State == DISK_STATE_ERROR
    assert vol3.StateMessage == ''

    for agent in agents:
        agent.stop()
