import json
import os
import pytest
import time

from cloud.blockstore.tests.python.lib.test_client import CreateTestClient

from cloud.blockstore.public.sdk.python.client.error_codes import EResult
from cloud.blockstore.public.sdk.python.protos import TCmsActionRequest, \
    TAction, STORAGE_POOL_KIND_GLOBAL, STORAGE_MEDIA_SSD_LOCAL

from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs, \
    start_disk_agent, get_fqdn

import yatest.common as yatest_common

from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, \
    ensure_path_exists


DEFAULT_DEVICE_SIZE = 1024**2             # 1 MiB
DEFAULT_DEVICE_RAW_SIZE = 1024**2 + 4096  # ~ 1 MiB

LOCAL_DEVICE_SIZE = 9437184      # 9 MiB
LOCAL_DEVICE_RAW_SIZE = 9999872  # 19531*512 B ~ 9.5 MiB

KNOWN_DEVICE_POOLS = {
    "KnownDevicePools": [
        {"Name": "9Mb", "Kind": "DEVICE_POOL_KIND_LOCAL",
            "AllocationUnit": LOCAL_DEVICE_SIZE},
        {"Name": "1Mb", "Kind": "DEVICE_POOL_KIND_GLOBAL",
            "AllocationUnit": DEFAULT_DEVICE_SIZE},
    ]}


def _remove_host(client, agent_id):
    request = TCmsActionRequest()
    action = request.Actions.add()
    action.Type = TAction.REMOVE_HOST
    action.Host = agent_id

    return client.cms_action(request).ActionResults


def _purge_host(client, agent_id):
    request = TCmsActionRequest()
    action = request.Actions.add()
    action.Type = TAction.PURGE_HOST
    action.Host = agent_id

    return client.cms_action(request).ActionResults


def _wait_agent_state(client, agent_id, desired_state):
    while True:
        bkp = client.backup_disk_registry_state()
        agent = [x for x in bkp["Agents"] if x['AgentId'] == agent_id]
        assert len(agent) == 1

        if agent[0].get("State") == desired_state:
            break
        time.sleep(1)


@pytest.fixture(name='ydb')
def start_ydb_cluster():

    ydb_cluster = start_ydb()

    yield ydb_cluster

    ydb_cluster.stop()


@pytest.fixture(name='agent_id')
def get_agent_id():
    return get_fqdn()


@pytest.fixture(name='data_path')
def create_data_path():

    p = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder="data")

    p = os.path.join(p, "dev", "disk", "by-partlabel")
    ensure_path_exists(p)

    return p


@pytest.fixture(name='disk_agent_config')
def create_disk_agent_config(ydb, data_path):
    cfg = NbsConfigurator(ydb, 'disk-agent')
    cfg.generate_default_nbs_configs()
    cfg.files["disk-agent"] = generate_disk_agent_txt(
        agent_id='',
        device_erase_method='DEVICE_ERASE_METHOD_NONE',  # speed up tests
        storage_discovery_config={
            "PathConfigs": [{
                "PathRegExp": f"{data_path}/NVMENBS([0-9]+)",
                "PoolConfigs": [{
                    "PoolName": "1Mb",
                    "MinSize": DEFAULT_DEVICE_RAW_SIZE,
                    "MaxSize": DEFAULT_DEVICE_RAW_SIZE
                }]}, {
                "PathRegExp": f"{data_path}/NVMECOMPUTE([0-9]+)",
                "BlockSize": 512,
                "PoolConfigs": [{
                    "PoolName": "9Mb",
                    "HashSuffix": "-local",
                    "MinSize": LOCAL_DEVICE_RAW_SIZE,
                    "MaxSize": LOCAL_DEVICE_RAW_SIZE
                }]}
            ]})

    return cfg


@pytest.fixture(autouse=True)
def create_device_files(data_path):

    def create_file(name, size):
        with open(os.path.join(data_path, name), 'wb') as f:
            f.seek(size-1)
            f.write(b'\0')
            f.flush()

    for i in range(6):
        create_file(f"NVMENBS{i + 1:02}", DEFAULT_DEVICE_RAW_SIZE)

    for i in range(4):
        create_file(f"NVMECOMPUTE{i + 1:02}", LOCAL_DEVICE_RAW_SIZE)


def test_add_host_with_legacy_local_ssd(
        ydb,
        agent_id,
        data_path,
        disk_agent_config):

    nbs_config = NbsConfigurator(ydb)
    nbs_config.generate_default_nbs_configs()

    # do not resume local devices on ADD_HOST action
    nbs_config.files["storage"].NonReplicatedDontSuspendDevices = False
    nbs_config.files["storage"].DiskRegistryAlwaysAllocatesLocalDisks = True
    nbs_config.files["storage"].NonReplicatedInfraTimeout = 0

    nbs = start_nbs(nbs_config)

    client = CreateTestClient(f"localhost:{nbs.port}")
    client.execute_DiskRegistrySetWritableState(State=True)
    client.update_disk_registry_config(KNOWN_DEVICE_POOLS)

    disk_agent = start_disk_agent(disk_agent_config, name='disk-agent.1')
    assert disk_agent.wait_for_registration()

    client.add_host(agent_id)

    # 6 devices were added; 4 local devices were added and suspended

    storage = client.query_available_storage(
        [agent_id],
        storage_pool_name="1Mb",
        storage_pool_kind=STORAGE_POOL_KIND_GLOBAL)

    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    assert storage[0].ChunkCount == 6
    assert storage[0].ChunkSize == DEFAULT_DEVICE_SIZE

    # we can't see the local devices
    storage = client.query_available_storage([agent_id])
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    assert storage[0].ChunkCount == 0
    assert storage[0].ChunkSize == 0

    client.wait_for_devices_to_be_cleared(4)

    bkp = client.backup_disk_registry_state()
    assert len(bkp["DirtyDevices"]) == 4
    assert len(bkp["SuspendedDevices"]) == 4

    client.create_volume_from_device(
        "vol0",
        agent_id,
        os.path.join(data_path, "NVMECOMPUTE01"))

    client.create_volume_from_device(
        "vol1",
        agent_id,
        os.path.join(data_path, "NVMECOMPUTE02"))

    # now we can see 2 local devices
    storage = client.query_available_storage([agent_id])
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    assert storage[0].ChunkCount == 2
    assert storage[0].ChunkSize == LOCAL_DEVICE_SIZE

    bkp = client.backup_disk_registry_state()
    assert len(bkp["DirtyDevices"]) == 2
    assert len(bkp["SuspendedDevices"]) == 2

    client.resume_device(agent_id, os.path.join(data_path, "NVMECOMPUTE03"))
    client.resume_device(agent_id, os.path.join(data_path, "NVMECOMPUTE04"))

    client.wait_for_devices_to_be_cleared()

    bkp = client.backup_disk_registry_state()
    assert len(bkp.get("DirtyDevices", [])) == 0
    assert len(bkp.get("SuspendedDevices", [])) == 0

    # and now we can see all the local devices
    storage = client.query_available_storage([agent_id])
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    assert storage[0].ChunkCount == 4
    assert storage[0].ChunkSize == LOCAL_DEVICE_SIZE

    vol0 = client.stat_volume("vol0")["Volume"]

    assert vol0.DiskId == "vol0"
    assert vol0.BlockSize == 512
    assert vol0.BlockSize * vol0.BlocksCount == LOCAL_DEVICE_RAW_SIZE
    assert len(vol0.Devices) == 1
    assert vol0.Devices[0].BlockCount == LOCAL_DEVICE_RAW_SIZE // 512
    assert vol0.Devices[0].AgentId == agent_id
    assert vol0.Devices[0].DeviceName == os.path.join(
        data_path, "NVMECOMPUTE01")

    vol1 = client.stat_volume("vol1")["Volume"]

    assert vol1.DiskId == "vol1"
    assert vol1.BlockSize == 512
    assert vol1.BlockSize * vol0.BlocksCount == LOCAL_DEVICE_RAW_SIZE
    assert len(vol1.Devices) == 1
    assert vol1.Devices[0].BlockCount == LOCAL_DEVICE_RAW_SIZE // 512
    assert vol1.Devices[0].AgentId == agent_id
    assert vol1.Devices[0].DeviceName == os.path.join(
        data_path, "NVMECOMPUTE02")

    # let's try to remove agent
    action_results = _remove_host(client, agent_id)
    assert len(action_results) == 1
    assert action_results[0].Result.Code == EResult.E_TRY_AGAIN.value
    assert sorted(action_results[0].DependentDisks) == ["vol0", "vol1"]

    bkp = client.backup_disk_registry_state()
    assert len(bkp.get("DirtyDevices", [])) == 0
    assert len(bkp.get("SuspendedDevices", [])) == 0

    # Local disks still allowed for creation
    storage = client.query_available_storage([agent_id])
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    assert storage[0].ChunkCount == 4
    assert storage[0].ChunkSize == LOCAL_DEVICE_SIZE

    # REMOVE_HOST disallows creation of all disk types except locals.
    storage = client.query_available_storage(
        [agent_id],
        storage_pool_name="1Mb",
        storage_pool_kind=STORAGE_POOL_KIND_GLOBAL)
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    assert storage[0].ChunkCount == 0
    assert storage[0].ChunkSize == 0

    # Purge the host. This is abnormal situation. Normally infra should invoke
    # this only when all clients and disks are gone.
    action_results = _purge_host(client, agent_id)
    assert len(action_results) == 1
    assert action_results[0].Result.Code == EResult.S_OK.value

    # All local devices should now be suspended.
    bkp = client.backup_disk_registry_state()
    assert len(bkp.get("DirtyDevices", [])) == 0
    assert len(bkp["SuspendedDevices"]) == 4

    # After purge we can't create any local disks.
    storage = client.query_available_storage([agent_id])
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    assert storage[0].ChunkCount == 0
    assert storage[0].ChunkSize == 0

    disk_agent.kill()
    assert not disk_agent.is_alive()

    # wait for DR to mark the agent as unavailable
    _wait_agent_state(client, agent_id, 'AGENT_STATE_UNAVAILABLE')

    disk_agent = start_disk_agent(disk_agent_config, name='disk-agent.2')
    assert disk_agent.wait_for_registration()

    # wait for DR to mark the agent as warning (back from unavailable)
    _wait_agent_state(client, agent_id, 'AGENT_STATE_WARNING')

    bkp = client.backup_disk_registry_state()
    assert bkp["Agents"][0]["State"] == 'AGENT_STATE_WARNING', json.dumps(bkp)
    assert len(bkp.get("SuspendedDevices", [])) == 4

    client.destroy_volume("vol0", sync=True)
    client.destroy_volume("vol1", sync=True)

    bkp = client.backup_disk_registry_state()
    assert len(bkp.get("DirtyDevices", [])) == 2
    assert len(bkp.get("SuspendedDevices", [])) == 4

    # put the agent back
    client.add_host(agent_id)

    # Devices are still suspended
    bkp = client.backup_disk_registry_state()
    assert len(bkp.get("DirtyDevices", [])) == 2
    assert len(bkp.get("SuspendedDevices", [])) == 4

    storage = client.query_available_storage([agent_id])
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    assert storage[0].ChunkCount == 0

    storage = client.query_available_storage(
        [agent_id],
        storage_pool_name="1Mb",
        storage_pool_kind=STORAGE_POOL_KIND_GLOBAL)
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    assert storage[0].ChunkCount == 6
    assert storage[0].ChunkSize == DEFAULT_DEVICE_SIZE

    # Resume dirty devices
    client.resume_device(agent_id, os.path.join(data_path, "NVMECOMPUTE01"))
    client.resume_device(agent_id, os.path.join(data_path, "NVMECOMPUTE02"))
    client.wait_for_devices_to_be_cleared()

    bkp = client.backup_disk_registry_state()
    assert len(bkp.get("DirtyDevices", [])) == 0
    assert len(bkp.get("SuspendedDevices", [])) == 2

    storage = client.query_available_storage([agent_id])
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    assert storage[0].ChunkCount == 2
    assert storage[0].ChunkSize == LOCAL_DEVICE_SIZE

    disk_agent.kill()
    nbs.kill()


def test_add_host(
        ydb,
        agent_id,
        data_path,
        disk_agent_config):

    nbs_config = NbsConfigurator(ydb)
    nbs_config.generate_default_nbs_configs()

    # resume local devices on ADD_HOST action
    nbs_config.files["storage"].NonReplicatedDontSuspendDevices = True
    nbs_config.files["storage"].DiskRegistryAlwaysAllocatesLocalDisks = True
    nbs_config.files["storage"].NonReplicatedInfraTimeout = 0

    nbs = start_nbs(nbs_config)

    client = CreateTestClient(f"localhost:{nbs.port}")
    client.execute_DiskRegistrySetWritableState(State=True)
    client.update_disk_registry_config(KNOWN_DEVICE_POOLS)

    disk_agent = start_disk_agent(disk_agent_config, name='disk-agent.1')
    assert disk_agent.wait_for_registration()

    client.add_host(agent_id)

    client.wait_for_devices_to_be_cleared()

    # 10 devices were added

    # we can see the local devices immediately
    storage = client.query_available_storage([agent_id])
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    assert storage[0].ChunkCount == 4
    assert storage[0].ChunkSize == LOCAL_DEVICE_SIZE

    storage = client.query_available_storage(
        [agent_id],
        storage_pool_name="1Mb",
        storage_pool_kind=STORAGE_POOL_KIND_GLOBAL)

    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    assert storage[0].ChunkCount == 6
    assert storage[0].ChunkSize == DEFAULT_DEVICE_SIZE

    bkp = client.backup_disk_registry_state()
    assert len(bkp.get("DirtyDevices", [])) == 0
    assert len(bkp.get("SuspendedDevices", [])) == 0

    client.create_volume(
        "vol0",
        block_size=512,
        blocks_count=4*LOCAL_DEVICE_SIZE//512,
        storage_media_kind=STORAGE_MEDIA_SSD_LOCAL)

    vol0 = client.stat_volume("vol0")["Volume"]

    assert vol0.DiskId == "vol0"
    assert vol0.BlockSize == 512
    assert vol0.BlockSize * vol0.BlocksCount == 4 * LOCAL_DEVICE_SIZE
    assert len(vol0.Devices) == 4

    for i, d in enumerate(sorted(vol0.Devices, key=lambda d: d.DeviceName)):
        assert d.BlockCount == LOCAL_DEVICE_SIZE // 512
        assert d.AgentId == agent_id
        assert d.DeviceName == os.path.join(data_path, f"NVMECOMPUTE0{i+1}")

    # let's try to remove agent
    action_results = _remove_host(client, agent_id)
    assert len(action_results) == 1
    assert action_results[0].Result.Code == EResult.E_TRY_AGAIN.value
    assert action_results[0].DependentDisks == ["vol0"]

    bkp = client.backup_disk_registry_state()
    assert len(bkp.get("DirtyDevices", [])) == 0
    assert len(bkp.get("SuspendedDevices", [])) == 0

    # Local disks still allowed for creation
    storage = client.query_available_storage([agent_id])
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    assert storage[0].ChunkCount == 4
    assert storage[0].ChunkSize == LOCAL_DEVICE_SIZE

    # REMOVE_HOST disallows creation of all disk types except locals.
    storage = client.query_available_storage(
        [agent_id],
        storage_pool_name="1Mb",
        storage_pool_kind=STORAGE_POOL_KIND_GLOBAL)
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    assert storage[0].ChunkCount == 0
    assert storage[0].ChunkSize == 0

    # Purge the host. This is abnormal situation. Normally infra should invoke
    # this only when all clients and disks are gone.
    action_results = _purge_host(client, agent_id)
    assert len(action_results) == 1
    assert action_results[0].Result.Code == EResult.S_OK.value

    # After purge we can't create any local disks.
    storage = client.query_available_storage([agent_id])
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    assert storage[0].ChunkCount == 0
    assert storage[0].ChunkSize == 0

    bkp = client.backup_disk_registry_state()
    assert len(bkp.get("DirtyDevices", [])) == 0
    assert len(bkp["SuspendedDevices"]) == 4

    disk_agent.kill()
    assert not disk_agent.is_alive()

    # wait for DR to mark the agent as unavailable
    _wait_agent_state(client, agent_id, 'AGENT_STATE_UNAVAILABLE')

    disk_agent = start_disk_agent(disk_agent_config, name='disk-agent.2')
    assert disk_agent.wait_for_registration()

    # wait for DR to mark the agent as warning (back from unavailable)
    _wait_agent_state(client, agent_id, 'AGENT_STATE_WARNING')

    client.destroy_volume("vol0", sync=True)

    bkp = client.backup_disk_registry_state()
    assert bkp["Agents"][0]["State"] == 'AGENT_STATE_WARNING', json.dumps(bkp)
    assert len(bkp.get("SuspendedDevices", [])) == 4

    # put the agent back
    client.add_host(agent_id)

    client.wait_for_devices_to_be_cleared()

    # all devices are available
    bkp = client.backup_disk_registry_state()
    assert len(bkp.get("DirtyDevices", [])) == 0
    assert len(bkp.get("SuspendedDevices", [])) == 0

    storage = client.query_available_storage([agent_id])
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    assert storage[0].ChunkCount == 4
    assert storage[0].ChunkSize == LOCAL_DEVICE_SIZE

    storage = client.query_available_storage(
        [agent_id],
        storage_pool_name="1Mb",
        storage_pool_kind=STORAGE_POOL_KIND_GLOBAL)

    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    assert storage[0].ChunkCount == 6
    assert storage[0].ChunkSize == DEFAULT_DEVICE_SIZE

    disk_agent.kill()
    nbs.kill()
