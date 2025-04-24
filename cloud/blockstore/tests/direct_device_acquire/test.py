import logging
import os
import pytest

from cloud.blockstore.tests.python.lib.test_client import CreateTestClient
from cloud.blockstore.public.sdk.python.client import Session
from cloud.blockstore.public.sdk.python.protos import STORAGE_MEDIA_SSD_NONREPLICATED

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
def start_nbs_daemon(request, ydb):
    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()

    cfg.files["storage"].DisableLocalService = False
    cfg.files["storage"].NonReplicatedDontSuspendDevices = True
    cfg.files["storage"].AllocationUnitNonReplicatedSSD = 1
    cfg.files["storage"].NonReplicatedAgentMinTimeout = request.param
    cfg.files["storage"].NonReplicatedAgentMaxTimeout = request.param
    cfg.files["storage"].NonReplicatedVolumeDirectAcquireEnabled = True
    cfg.files["storage"].AcquireNonReplicatedDevices = True

    daemon = start_nbs(cfg)

    client = CreateTestClient(f"localhost:{daemon.port}")
    client.execute_action(
        action="DiskRegistrySetWritableState",
        input_bytes=str.encode('{"State": true}'))
    client.update_disk_registry_config(KNOWN_DEVICE_POOLS)

    yield daemon

    daemon.kill()


@pytest.fixture(name='agent_ids')
def create_agent_ids():
    return ['agent-1', 'agent-2', 'agent-3']


def _get_device_path(agent_id, device_path):
    return os.path.join(device_path, agent_id, "dev", "disk", "by-partlabel")


@pytest.fixture(name='device_path')
def create_device_path(agent_ids):
    data = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder="data")

    for agent_id in agent_ids:
        ensure_path_exists(_get_device_path(agent_id, data))

    return data


@pytest.fixture(autouse=True)
def create_device_files(device_path, agent_ids):
    for agent_id in agent_ids:
        p = _get_device_path(agent_id, device_path)
        with open(os.path.join(p, 'NVMENBS01'), 'wb') as f:
            os.truncate(f.fileno(), DEVICES_PER_PATH * (DEVICE_SIZE + 4096))


def _create_disk_agent_configurator(ydb, agent_id, device_path):
    assert agent_id is not None

    configurator = NbsConfigurator(ydb, node_type='disk-agent')
    configurator.generate_default_nbs_configs()

    disk_agent_config = generate_disk_agent_txt(
        agent_id=agent_id,
        device_erase_method='DEVICE_ERASE_METHOD_NONE',  # speed up tests
        storage_discovery_config={
            "PathConfigs": [{
                "PathRegExp": f"{device_path}/NVMENBS([0-9]+)",
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
def create_disk_agent_configurators(ydb, agent_ids, device_path):
    configurators = []

    for agent_id in agent_ids:
        p = _get_device_path(agent_id, device_path)
        configurators.append(_create_disk_agent_configurator(ydb, agent_id, p))

    return configurators


@pytest.mark.parametrize("nbs", [(6000)], indirect=["nbs"])
def test_should_mount_volume_with_unavailable_agents(
        nbs,
        device_path,
        agent_ids,
        disk_agent_configurators):

    logger = logging.getLogger("client")
    logger.setLevel(logging.DEBUG)

    client = CreateTestClient(f"localhost:{nbs.port}", log=logger)

    agents = []

    # run agents
    for i in range(2):
        agent_id = agent_ids[i]
        configurator = disk_agent_configurators[i]
        agent = start_disk_agent(configurator, name=agent_id)
        agent.wait_for_registration()
        r = client.add_host(agent_id)
        assert len(r.ActionResults) == 1
        assert r.ActionResults[0].Result.Code == 0

        agents.append(agent)

    client.wait_for_devices_to_be_cleared()

    # create a volume
    client.create_volume(
        disk_id="vol1",
        block_size=4096,
        blocks_count=2*DEVICES_PER_PATH*DEVICE_SIZE//4096,
        storage_media_kind=STORAGE_MEDIA_SSD_NONREPLICATED,
        cloud_id="test")

    bkp = client.backup_disk_registry_state()
    assert len(bkp['Disks']) == 1
    assert len(bkp['Disks'][0]['DeviceUUIDs']) == 12
    assert len(bkp['Agents']) == 2

    session = Session(client, "vol1", "")
    session.mount_volume()

    # IO should work
    session.write_blocks(0, [b'\1' * 4096 * 2])
    blocks = session.read_blocks(0, 1, checkpoint_id="")
    assert len(blocks) == 1

    session.unmount_volume()

    # stop the agent
    agents[0].kill()

    client.wait_agent_state(agent_ids[0], "AGENT_STATE_UNAVAILABLE")

    unavailable_agent = bkp['Agents'][0]
    unavailable_devices = [x["DeviceUUID"]
                           for x in unavailable_agent["Devices"]]
    devices = bkp['Disks'][0]['DeviceUUIDs']

    session.mount_volume()

    for i in range(2*DEVICES_PER_PATH):
        if devices[i] in unavailable_devices:
            continue
        blocks = session.read_blocks(i*DEVICE_SIZE//4096, 1, checkpoint_id="")

    session.unmount_volume()
