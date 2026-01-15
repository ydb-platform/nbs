import json
import logging
import os
import tempfile

from pathlib import Path

import pytest
import time


from cloud.blockstore.tests.python.lib.test_client import CreateTestClient

from cloud.blockstore.public.sdk.python.client import Session
from cloud.blockstore.public.sdk.python.protos import STORAGE_MEDIA_SSD_NONREPLICATED, \
    IPC_VHOST, VOLUME_ACCESS_READ_WRITE

from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs, \
    start_disk_agent

import yatest.common as yatest_common

from contrib.ydb.tests.library.harness.kikimr_runner import \
    get_unique_path_for_current_test, ensure_path_exists


DEVICE_SIZE = 1024 ** 3  # 1 GiB
DEVICES_PER_PATH = 6
INACTIVE_CLIENTS_TIMEOUT = 1  # in seconds

KNOWN_DEVICE_POOLS = {
    "KnownDevicePools": [
        {"Kind": "DEVICE_POOL_KIND_DEFAULT", "AllocationUnit": DEVICE_SIZE},
    ]}


@pytest.fixture(name='ydb')
def start_ydb_cluster():
    ydb_cluster = start_ydb()
    yield ydb_cluster

    ydb_cluster.stop()


def apply_common_params_to_config(cfg, params):
    cfg.files["storage"].NonReplicatedDontSuspendDevices = True
    cfg.files["storage"].AllocationUnitNonReplicatedSSD = 1
    cfg.files["storage"].NonReplicatedVolumeDirectAcquireEnabled = True
    cfg.files["storage"].AcquireNonReplicatedDevices = True
    cfg.files["storage"].InactiveClientsTimeout = INACTIVE_CLIENTS_TIMEOUT * 1000

    timeout = params.get("NonReplicatedAgentTimeout", 60000)
    cfg.files["storage"].NonReplicatedAgentMinTimeout = timeout
    cfg.files["storage"].NonReplicatedAgentMaxTimeout = timeout

    cfg.files["storage"].NonReplicatedVolumeAcquireDiskAfterAddClientEnabled = params.get(
        "NonReplicatedVolumeAcquireDiskAfterAddClientEnabled", False)

    cfg.files["server"].ServerConfig.VhostEnabled = True
    cfg.files["server"].ServerConfig.VhostServerPath = yatest_common.binary_path(
        "cloud/blockstore/vhost-server/blockstore-vhost-server")


@pytest.fixture(name='nbs_with_dr')
def start_nbs_daemon_with_dr(request, ydb):
    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()

    params = {}
    if hasattr(request, 'param'):
        params = request.param

    apply_common_params_to_config(cfg, params)
    cfg.files["storage"].DisableLocalService = False

    daemon = start_nbs(cfg)

    client = CreateTestClient(f"localhost:{daemon.port}")
    client.execute_DiskRegistrySetWritableState(State=True)
    client.update_disk_registry_config(KNOWN_DEVICE_POOLS)

    yield daemon

    daemon.kill()


@pytest.fixture(name='nbs')
def start_nbs_daemon(request, ydb):

    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()

    params = {}
    if hasattr(request, 'param'):
        params = request.param

    apply_common_params_to_config(cfg, params)
    cfg.files["storage"].DisableLocalService = True

    daemon = start_nbs(cfg)

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


def restart_tablet(client, tabletId):
    client.execute_KillTablet(TabletId=tabletId)


def get_volume_tablet_id(client, disk_id):
    response = client.execute_DescribeVolume(DiskId=disk_id)

    return json.loads(response)["VolumeTabletId"]


def restart_volume(client, disk_id):
    tablet_id = get_volume_tablet_id(client, disk_id)
    restart_tablet(client, tablet_id)


def wait_for_all_volumes_to_be_notified(client, timeout=60):
    start_time = time.time()
    while True:
        bkp = client.backup_disk_registry_state()
        if not ("DisksToNotify" in bkp):
            break
        if time.time() - start_time > timeout:
            raise TimeoutError(
                f"Timeout waiting for all volumes to be notified (waited {timeout} seconds). Last state: {bkp}")
        time.sleep(1)


@pytest.mark.parametrize("nbs", [
    ({"NonReplicatedVolumeAcquireDiskAfterAddClientEnabled": True}),
    ({})
], indirect=["nbs"])
def test_should_mount_volume_with_unknown_devices(
        nbs_with_dr,
        nbs,
        data_path,
        agent_ids,
        disk_agent_configurators):

    logger = logging.getLogger("client")
    logger.setLevel(logging.DEBUG)

    client = CreateTestClient(f"localhost:{nbs.port}", log=logger)
    agent_id = agent_ids[0]
    configurator = disk_agent_configurators[0]

    data_path_for_agent = _get_agent_data_path(agent_id, data_path)

    with open(os.path.join(data_path_for_agent, 'NVMENBS02'), 'wb') as f:
        os.truncate(f.fileno(), DEVICES_PER_PATH * (DEVICE_SIZE + 4096))

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
        blocks_count=2*DEVICES_PER_PATH*DEVICE_SIZE//4096,
        storage_media_kind=STORAGE_MEDIA_SSD_NONREPLICATED,
        cloud_id="test")

    bkp = client.backup_disk_registry_state()
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

    time.sleep(1)

    configurator.files["disk-agent"].\
        StorageDiscoveryConfig.PathConfigs[
            0].PathRegExp = f"{data_path_for_agent}/NVMENBS([0-1]+)"
    configurator.files["disk-agent"].CachedConfigPath = ""

    agent = start_disk_agent(configurator, name=agent_id)
    agent.wait_for_registration()

    r = client.add_host(agent_id)
    assert len(r.ActionResults) == 1
    assert r.ActionResults[0].Result.Code == 0

    wait_for_all_volumes_to_be_notified(client)

    nbs_with_dr.kill()
    restart_volume(client, "vol1")

    session.mount_volume()
    except_count = 0
    for i in range(2*DEVICES_PER_PATH):
        try:
            blocks = session.read_blocks(
                i*DEVICE_SIZE//4096, 1, checkpoint_id="")
        except Exception:
            except_count += 1
    assert except_count == DEVICES_PER_PATH
    session.unmount_volume()


@pytest.mark.parametrize("nbs", [
    ({"NonReplicatedVolumeAcquireDiskAfterAddClientEnabled": True}),
    ({})
], indirect=["nbs"])
def test_should_mount_volume_without_dr(nbs_with_dr, nbs, agent_ids, disk_agent_configurators):

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

    nbs_with_dr.kill()
    restart_volume(client, "vol1")

    session.mount_volume()

    for i in range(2*DEVICES_PER_PATH):
        blocks = session.read_blocks(i*DEVICE_SIZE//4096, 1, checkpoint_id="")

    session.unmount_volume()


@pytest.mark.parametrize("should_break_device", [True, False])
@pytest.mark.parametrize("nbs_with_dr", [{"NonReplicatedAgentTimeout": 6000}], indirect=["nbs_with_dr"])
@pytest.mark.parametrize("nbs", [
    ({"NonReplicatedVolumeAcquireDiskAfterAddClientEnabled": True}),
    ({})
], indirect=["nbs"])
def test_should_mount_volume_with_unavailable_agents(
        nbs_with_dr,
        nbs,
        agent_ids,
        disk_agent_configurators,
        should_break_device):

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

    if should_break_device:
        client.execute_DiskRegistryChangeState(
            Message="test",
            ChangeDeviceState={
                "DeviceUUID": bkp['Agents'][0]["Devices"][0]["DeviceUUID"],
                "State": 2,    # DEVICE_STATE_ERROR
            })

        wait_for_all_volumes_to_be_notified(client)

    # stop the agent
    agents[0].kill()

    client.wait_agent_state(agent_ids[0], "AGENT_STATE_UNAVAILABLE")

    wait_for_all_volumes_to_be_notified(client)

    nbs_with_dr.kill()
    restart_volume(client, "vol1")

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


@pytest.mark.parametrize("nbs, pass_client_id", [({"NonReplicatedVolumeAcquireDiskAfterAddClientEnabled": True}, False), ({}, True)], indirect=["nbs"])
def test_should_stop_not_restored_endpoint(nbs_with_dr,
                                           nbs,
                                           agent_ids,
                                           disk_agent_configurators,
                                           pass_client_id):

    client = CreateTestClient(f"localhost:{nbs.port}")

    test_disk_id = "vol0"

    agent_id = agent_ids[0]
    configurator = disk_agent_configurators[0]
    agent = start_disk_agent(configurator, name=agent_id)
    agent.wait_for_registration()
    r = client.add_host(agent_id)
    assert len(r.ActionResults) == 1
    assert r.ActionResults[0].Result.Code == 0

    client.create_volume(
        disk_id=test_disk_id,
        block_size=4096,
        blocks_count=DEVICE_SIZE//4096,
        storage_media_kind=STORAGE_MEDIA_SSD_NONREPLICATED,
        cloud_id="test")

    socket = tempfile.NamedTemporaryFile()
    client.start_endpoint(
        unix_socket_path=socket.name,
        disk_id=test_disk_id,
        ipc_type=IPC_VHOST,
        access_mode=VOLUME_ACCESS_READ_WRITE,
        client_id=f"{socket.name}-id",
        seq_number=0
    )

    nbs.kill()
    if not pass_client_id:
        time.sleep(INACTIVE_CLIENTS_TIMEOUT + 1)
    nbs.start()

    client.stop_endpoint(
        unix_socket_path=socket.name,
    )

    if pass_client_id:
        client.stop_endpoint(
            unix_socket_path=socket.name,
            client_id=f"{socket.name}-id",
            disk_id=test_disk_id,
        )

    assert not Path(socket.name).exists()

    another_socket = tempfile.NamedTemporaryFile()
    client.start_endpoint(
        unix_socket_path=another_socket.name,
        disk_id=test_disk_id,
        ipc_type=IPC_VHOST,
        access_mode=VOLUME_ACCESS_READ_WRITE,
        client_id=f"{another_socket.name}-id-1",
        seq_number=0
    )

    client.stop_endpoint(
        unix_socket_path=another_socket.name,
    )

    assert not Path(another_socket.name).exists()


@pytest.mark.parametrize("nbs", [
    ({"NonReplicatedVolumeAcquireDiskAfterAddClientEnabled": True}),
    ({})
], indirect=["nbs"])
def test_should_stop_not_restored_endpoint_when_volume_was_deleted(nbs_with_dr,
                                                                   nbs,
                                                                   agent_ids,
                                                                   disk_agent_configurators):

    client = CreateTestClient(f"localhost:{nbs.port}")

    test_disk_id = "vol0"

    agent_id = agent_ids[0]
    configurator = disk_agent_configurators[0]
    agent = start_disk_agent(configurator, name=agent_id)
    agent.wait_for_registration()
    r = client.add_host(agent_id)
    assert len(r.ActionResults) == 1
    assert r.ActionResults[0].Result.Code == 0

    client.create_volume(
        disk_id=test_disk_id,
        block_size=4096,
        blocks_count=DEVICE_SIZE//4096,
        storage_media_kind=STORAGE_MEDIA_SSD_NONREPLICATED,
        cloud_id="test")

    socket = tempfile.NamedTemporaryFile()
    client.start_endpoint(
        unix_socket_path=socket.name,
        disk_id=test_disk_id,
        ipc_type=IPC_VHOST,
        access_mode=VOLUME_ACCESS_READ_WRITE,
        client_id=f"{socket.name}-id",
        seq_number=0
    )

    nbs.kill()
    nbs.start()

    client.destroy_volume(
        disk_id=test_disk_id,
        sync=True
    )

    client.stop_endpoint(
        unix_socket_path=socket.name,
        client_id=f"{socket.name}-id",
        disk_id=test_disk_id,
    )

    assert not Path(socket.name).exists()
