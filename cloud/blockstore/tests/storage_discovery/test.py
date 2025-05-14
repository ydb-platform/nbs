import hashlib
import os
import pytest
import time

from copy import deepcopy

from cloud.blockstore.public.sdk.python.client import CreateClient
from cloud.blockstore.public.sdk.python.protos import TCmsActionRequest, \
    TAction, STORAGE_POOL_KIND_LOCAL, STORAGE_POOL_KIND_DEFAULT, \
    STORAGE_POOL_KIND_GLOBAL

from cloud.blockstore.tests.python.lib.client import NbsClient
from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs, \
    start_disk_agent, get_fqdn

import yatest.common as yatest_common

from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, \
    ensure_path_exists


DEVICE_SIZE = 1024**2

KNOWN_DEVICE_POOLS = {
    "KnownDevicePools": [
        {"Kind": "DEVICE_POOL_KIND_DEFAULT", "AllocationUnit": DEVICE_SIZE},
        {"Name": "1Mb", "Kind": "DEVICE_POOL_KIND_LOCAL", "AllocationUnit": DEVICE_SIZE},
        {"Name": "rot", "Kind": "DEVICE_POOL_KIND_GLOBAL", "AllocationUnit": DEVICE_SIZE},
        {"Name": "local", "Kind": "DEVICE_POOL_KIND_LOCAL", "AllocationUnit": DEVICE_SIZE},
    ]}


def _md5(s):
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def _add_devices(client, agent_id, paths):
    request = TCmsActionRequest()
    for path in paths:
        action = request.Actions.add()
        action.Type = TAction.ADD_DEVICE
        action.Host = agent_id
        action.Device = path
    return client.cms_action(request)


def _remove_devices(client, agent_id, paths):
    request = TCmsActionRequest()
    for path in paths:
        action = request.Actions.add()
        action.Type = TAction.REMOVE_DEVICE
        action.Host = agent_id
        action.Device = path
    return client.cms_action(request)


def _setup_disk_registry_config(nbs, agent_id):

    client = NbsClient(nbs.port)
    client.update_disk_registry_config({
        "KnownAgents": [{
            "AgentId": agent_id,
            "KnownDevices":
                [{"DeviceUUID": _md5(f"{agent_id}-{i + 1:02}")} for i in range(6)] +
                [{"DeviceUUID": _md5(f"{agent_id}-01-{i + 1:03}-rot")} for i in range(8)] +
                [{"DeviceUUID": _md5(f"{agent_id}-{i + 1:02}-local")} for i in range(4)]
            }],
        } | KNOWN_DEVICE_POOLS)


def _wait_for_devices_to_be_cleared(client, expected_dirty_count=0):
    while True:
        bkp = client.backup_disk_registry_state()["Backup"]
        dirty_devices = bkp.get("DirtyDevices", [])
        dirty_count = len(dirty_devices)
        if dirty_count == expected_dirty_count:
            break
        time.sleep(1)


@pytest.fixture(name='ydb')
def start_ydb_cluster():

    ydb_cluster = start_ydb()

    yield ydb_cluster

    ydb_cluster.stop()


@pytest.fixture(name='nbs')
def start_nbs_daemon(ydb):

    nbs_configurator = NbsConfigurator(ydb)
    nbs_configurator.generate_default_nbs_configs()

    daemon = start_nbs(nbs_configurator)

    yield daemon

    daemon.kill()


@pytest.fixture(name='agent_id')
def get_agent_id():
    return get_fqdn()


@pytest.fixture(autouse=True)
def disk_registry_set_writable_state(nbs):

    client = NbsClient(nbs.port)
    client.disk_registry_set_writable_state()


@pytest.fixture(name='data_path')
def create_data_path():

    p = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder="data")

    p = os.path.join(p, "dev", "disk", "by-partlabel")
    ensure_path_exists(p)

    return p


@pytest.fixture(autouse=True)
def create_device_files(data_path):

    def create_file(name, size):
        with open(os.path.join(data_path, name), 'wb') as f:
            f.seek(size-1)
            f.write(b'\0')
            f.flush()

    for i in range(6):
        create_file(f"NVMENBS{i + 1:02}", DEVICE_SIZE + i * 4096)

    for i in range(4):
        create_file(f"NVMECOMPUTE{i + 1:02}", DEVICE_SIZE + i * 512)

    create_file("ROTNBS01", DEVICE_SIZE * 10)


@pytest.fixture(name='disk_agent_configurator')
def create_disk_agent_configurator(ydb):

    configurator = NbsConfigurator(ydb, 'disk-agent')
    configurator.generate_default_nbs_configs()

    return configurator


@pytest.fixture(name='disk_agent_static_config')
def create_disk_agent_static_config(data_path, agent_id):

    def create_nvme_device(i):
        return {
            "Path": os.path.join(data_path, f"NVMENBS{i + 1:02}"),
            "BlockSize": 4096,
            "DeviceId": _md5(f"{agent_id}-{i + 1:02}")
        }

    def create_local_ssd_device(i):
        return {
            "Path": os.path.join(data_path, f"NVMECOMPUTE{i + 1:02}"),
            "BlockSize": 512,
            "PoolName": "local",
            "DeviceId": _md5(f"{agent_id}-{i + 1:02}-local")
        }

    def create_rot_device(i):
        return {
            "Path": os.path.join(data_path, "ROTNBS01"),
            "BlockSize": 4096,
            "PoolName": "rot",
            "Offset": DEVICE_SIZE / 2 + i * (DEVICE_SIZE + 4096),
            "FileSize": DEVICE_SIZE,
            "DeviceId": _md5(f"{agent_id}-01-{i + 1:03}-rot")
        }

    devices = [create_nvme_device(i) for i in range(6)]
    devices += [create_local_ssd_device(i) for i in range(4)]
    devices += [create_rot_device(i) for i in range(8)]

    return generate_disk_agent_txt(agent_id=agent_id, file_devices=devices)


@pytest.fixture(name='disk_agent_dynamic_config')
def create_disk_agent_dynamic_config(data_path):
    return generate_disk_agent_txt(
        agent_id='',
        device_erase_method='DEVICE_ERASE_METHOD_NONE',  # speed up tests
        storage_discovery_config={
            "PathConfigs": [{
                "PathRegExp": f"{data_path}/ROTNBS([0-9]+)",
                "MaxDeviceCount": 8,
                "PoolConfigs": [{
                    "PoolName": "rot",
                    "MinSize": 0,
                    "MaxSize": DEVICE_SIZE * 10,
                    "HashSuffix": "-rot",
                    "Layout": {
                        "DeviceSize": DEVICE_SIZE,
                        "DevicePadding": 4096,
                        "HeaderSize": DEVICE_SIZE / 2
                    }
                }]
            }, {
                "PathRegExp": f"{data_path}/NVMENBS([0-9]+)",
                "PoolConfigs": [{
                    "MinSize": DEVICE_SIZE,
                    "MaxSize": DEVICE_SIZE + 5 * 4096
                }]}, {
                "PathRegExp": f"{data_path}/NVMECOMPUTE([0-9]+)",
                "BlockSize": 512,
                "PoolConfigs": [{
                    "PoolName": "local",
                    "HashSuffix": "-local",
                    "MinSize": DEVICE_SIZE,
                    "MaxSize": DEVICE_SIZE + 3 * 512
                }]}
            ]})


def _check_disk_agent_config(config, agent_id, data_path):
    assert config["AgentId"] == agent_id

    devices = config.get("Devices")

    assert devices is not None
    assert len(devices) == 18

    for d in devices:
        assert d.get("State") is None
        assert d["AgentId"] == agent_id
        assert d.get("PoolName", "") in ["rot", "local", ""]

    nvme = sorted(
        [d for d in devices if d.get("PoolName") is None],
        key=lambda d: d["DeviceName"])
    assert len(nvme) == 6

    for i, d in enumerate(nvme):
        assert d["DeviceName"] == f"{data_path}/NVMENBS{i + 1:02}"
        assert int(d["BlockSize"]) == 4096
        assert int(d["BlocksCount"]) == DEVICE_SIZE / 4096
        assert int(d["UnadjustedBlockCount"]) == DEVICE_SIZE / 4096 + i

    local = sorted(
        [d for d in devices if d.get("PoolName") == "local"],
        key=lambda d: d["DeviceName"])
    assert len(local) == 4

    for i, d in enumerate(local):
        assert d["DeviceName"] == f"{data_path}/NVMECOMPUTE{i + 1:02}"
        assert int(d["BlockSize"]) == 512
        assert int(d["BlocksCount"]) == DEVICE_SIZE / 512
        assert int(d["UnadjustedBlockCount"]) == DEVICE_SIZE / 512 + i

    rot = sorted(
        [d for d in devices if d.get("PoolName") == "rot"],
        key=lambda d: int(d["PhysicalOffset"]))
    assert len(rot) == 8

    offset = DEVICE_SIZE / 2
    for d in rot:
        assert d["DeviceName"] == f"{data_path}/ROTNBS01"
        assert int(d["BlockSize"]) == 4096
        assert int(d["BlocksCount"]) == DEVICE_SIZE / 4096
        assert int(d["UnadjustedBlockCount"]) == DEVICE_SIZE / 4096
        assert int(d["PhysicalOffset"]) == offset
        offset += DEVICE_SIZE + 4096


@pytest.mark.parametrize("source", ['file', 'cms', 'mix'])
def test_storage_discovery(
        nbs,
        agent_id,
        data_path,
        disk_agent_configurator,
        disk_agent_dynamic_config,
        source):

    _setup_disk_registry_config(nbs, agent_id)

    if source == "cms":
        disk_agent_configurator.cms["DiskAgentConfig"] = disk_agent_dynamic_config

    if source in ["file", "mix"]:
        disk_agent_configurator.files["disk-agent"] = disk_agent_dynamic_config

    if source == "mix":
        disk_agent_configurator.cms["DiskAgentConfig"] = generate_disk_agent_txt(
            agent_id=agent_id)

    disk_agent = start_disk_agent(disk_agent_configurator)
    disk_agent.wait_for_registration()

    client = NbsClient(nbs.port)

    _check_disk_agent_config(
        client.backup_disk_registry_state()["Backup"]["Agents"][0],
        agent_id,
        data_path)

    disk_agent.kill()


@pytest.mark.parametrize("cmp", ['mismatch', 'ok'])
def test_config_comparison(
        cmp,
        nbs,
        agent_id,
        data_path,
        disk_agent_configurator,
        disk_agent_static_config,
        disk_agent_dynamic_config):

    if cmp == 'mismatch':
        # change the device padding of the 'rot' pool
        disk_agent_dynamic_config.StorageDiscoveryConfig.PathConfigs[0]. \
            PoolConfigs[0].Layout.DevicePadding = 8192

    _setup_disk_registry_config(nbs, agent_id)

    disk_agent_configurator.files["disk-agent"] = disk_agent_dynamic_config
    disk_agent_configurator.cms["DiskAgentConfig"] = disk_agent_static_config

    disk_agent = start_disk_agent(disk_agent_configurator)
    disk_agent.wait_for_registration()

    client = NbsClient(nbs.port)

    _check_disk_agent_config(
        client.backup_disk_registry_state()["Backup"]["Agents"][0],
        agent_id,
        data_path)

    crit = disk_agent.counters.find({'sensor': 'AppDiskAgentCriticalEvents/DiskAgentConfigMismatch'})
    assert crit is not None
    assert crit['value'] == (1 if cmp == 'mismatch' else 0)

    if cmp == 'mismatch':
        # Wait for duplicate event.
        time.sleep(30)
        crit = disk_agent.counters.find(
            {'sensor': 'AppDiskAgentCriticalEvents/DiskAgentConfigMismatch'})
        assert crit is not None
        assert crit['value'] == 2

    disk_agent.kill()


def test_add_devices(
        nbs,
        agent_id,
        data_path,
        disk_agent_configurator,
        disk_agent_dynamic_config):

    # start DA with a dynamically generated config
    disk_agent_configurator.files["disk-agent"] = disk_agent_dynamic_config

    disk_agent = start_disk_agent(disk_agent_configurator)
    disk_agent.wait_for_registration()

    client = NbsClient(nbs.port)

    # just pools in the DR config
    client.update_disk_registry_config(KNOWN_DEVICE_POOLS)

    bkp = client.backup_disk_registry_state()["Backup"]
    assert bkp["Agents"][0]["AgentId"] == agent_id
    assert bkp["Agents"][0].get("Devices") is None

    grpc_client = CreateClient(f"localhost:{nbs.port}")

    default_storage = grpc_client.query_available_storage(
        [agent_id], "", STORAGE_POOL_KIND_DEFAULT)
    local_storage = grpc_client.query_available_storage(
        [agent_id], "local", STORAGE_POOL_KIND_LOCAL)
    assert len(default_storage) == 1
    assert len(local_storage) == 1
    assert default_storage[0].AgentId == agent_id
    assert local_storage[0].AgentId == agent_id
    # no devices have been added yet
    assert default_storage[0].ChunkCount == 0
    assert local_storage[0].ChunkCount == 0

    # adding one device
    r = _add_devices(grpc_client, agent_id, [os.path.join(data_path, 'NVMENBS01')])
    assert len(r.ActionResults) == 1
    assert r.ActionResults[0].Result.Code == 0, r

    # wait until it is cleared
    _wait_for_devices_to_be_cleared(client)

    default_storage = grpc_client.query_available_storage(
        [agent_id], "", STORAGE_POOL_KIND_DEFAULT)
    assert len(default_storage) == 1
    assert default_storage[0].AgentId == agent_id
    # now we see one chunk
    assert default_storage[0].ChunkCount == 1
    assert default_storage[0].ChunkSize == DEVICE_SIZE

    bkp = client.backup_disk_registry_state()["Backup"]
    assert bkp.get("DirtyDevices") is None
    assert bkp["Agents"][0]["AgentId"] == agent_id
    assert len(bkp["Agents"][0]["Devices"]) == 1
    assert bkp["Agents"][0]["Devices"][0].get("State") is None

    # adding all default and local devices
    r = _add_devices(grpc_client, agent_id, [
        os.path.join(data_path, f'NVMENBS{i + 1:02}') for i in range(6)
    ] + [
        os.path.join(data_path, f'NVMECOMPUTE{i + 1:02}') for i in range(4)
    ])
    assert len(r.ActionResults) == 10
    assert all(x.Result.Code == 0 for x in r.ActionResults)

    # wait until the local devices are cleared
    _wait_for_devices_to_be_cleared(client, 4)

    default_storage = grpc_client.query_available_storage(
        [agent_id], "", STORAGE_POOL_KIND_DEFAULT)
    local_storage = grpc_client.query_available_storage(
        [agent_id], "local", STORAGE_POOL_KIND_LOCAL)
    assert len(default_storage) == 1
    assert len(local_storage) == 1
    assert default_storage[0].AgentId == agent_id
    assert local_storage[0].AgentId == agent_id
    # now we see all the default devices, but local are still suspended
    assert local_storage[0].ChunkCount == 0
    assert default_storage[0].ChunkCount == 6
    assert default_storage[0].ChunkSize == DEVICE_SIZE

    # resume all local devices
    for i in range(4):
        grpc_client.resume_device(
            agent_id,
            os.path.join(data_path, f'NVMECOMPUTE{i + 1:02}'),
            dry_run=False)
    # wait until the local devices are cleared
    _wait_for_devices_to_be_cleared(client)

    local_storage = grpc_client.query_available_storage(
        [agent_id], "local", STORAGE_POOL_KIND_LOCAL)
    assert len(local_storage) == 1
    assert local_storage[0].AgentId == agent_id
    # local devices are ready
    assert local_storage[0].ChunkCount == 4
    assert local_storage[0].ChunkSize == DEVICE_SIZE

    bkp = client.backup_disk_registry_state()["Backup"]
    assert bkp.get("DirtyDevices") is None
    assert len(bkp["Agents"][0]["Devices"]) == 10
    assert all([d.get("State") is None for d in bkp["Agents"][0]["Devices"]])

    # Check that there is no rot devices
    rot_storage = grpc_client.query_available_storage(
        [agent_id], "rot", STORAGE_POOL_KIND_GLOBAL)
    assert len(rot_storage) == 1
    assert rot_storage[0].AgentId == agent_id
    assert rot_storage[0].ChunkCount == 0

    # adding a rot device
    r = _add_devices(grpc_client, agent_id, [os.path.join(data_path, 'ROTNBS01')])
    assert len(r.ActionResults) == 1
    assert r.ActionResults[0].Result.Code == 0, r

    # we see all the devices in the backup
    bkp = client.backup_disk_registry_state()["Backup"]
    assert bkp.get("DirtyDevices") is not None

    # wait until rot devices to be cleared.
    _wait_for_devices_to_be_cleared(client)

    _check_disk_agent_config(bkp["Agents"][0], agent_id, data_path)

    local_storage = grpc_client.query_available_storage(
        [agent_id], "local", STORAGE_POOL_KIND_LOCAL)
    assert len(local_storage) == 1
    assert local_storage[0].AgentId == agent_id
    # we still see all the local devices
    assert local_storage[0].ChunkCount == 4
    assert local_storage[0].ChunkSize == DEVICE_SIZE

    rot_storage = grpc_client.query_available_storage(
        [agent_id], "rot", STORAGE_POOL_KIND_GLOBAL)
    assert len(rot_storage) == 1
    assert rot_storage[0].AgentId == agent_id
    # we see the rot devices
    assert rot_storage[0].ChunkCount == 8
    assert rot_storage[0].ChunkSize == DEVICE_SIZE

    disk_agent.kill()


@pytest.mark.parametrize("backup_from", ['local_db', 'state'])
def test_remove_devices(
        nbs,
        agent_id,
        data_path,
        disk_agent_configurator,
        disk_agent_dynamic_config,
        backup_from):

    _setup_disk_registry_config(nbs, agent_id)

    default_device_count = 18

    def get_bkp():
        r = client.backup_disk_registry_state(
            backup_local_db=backup_from == 'local_db')
        return r["Backup"]

    client = NbsClient(nbs.port)
    bkp = get_bkp()
    assert len(bkp["Config"]["KnownAgents"]) == 1
    assert bkp["Config"]["KnownAgents"][0]["AgentId"] == agent_id
    assert len(bkp["Config"]["KnownAgents"][0]["Devices"]) == default_device_count
    assert bkp.get("Agents") is None

    disk_agent_configurator.files["disk-agent"] = disk_agent_dynamic_config

    disk_agent = start_disk_agent(disk_agent_configurator)
    disk_agent.wait_for_registration()

    bkp = get_bkp()

    assert bkp["Agents"][0]["AgentId"] == agent_id
    assert len(bkp["Agents"][0]["Devices"]) == default_device_count

    assert len(bkp["Config"]["KnownAgents"]) == 1
    assert bkp["Config"]["KnownAgents"][0]["AgentId"] == agent_id
    assert len(bkp["Config"]["KnownAgents"][0]["Devices"]) == default_device_count

    grpc_client = CreateClient(f"localhost:{nbs.port}")

    r = _remove_devices(grpc_client, agent_id, [os.path.join(data_path, 'NVMENBS01')])
    assert r.ActionResults[0].Result.Code == 0  # S_OK
    assert r.ActionResults[0].Timeout == 0

    bkp = get_bkp()
    assert bkp["Agents"][0]["AgentId"] == agent_id

    assert len(bkp["Agents"][0]["Devices"]) == default_device_count
    assert len(bkp["Config"]["KnownAgents"][0]["Devices"]) == default_device_count

    r = _remove_devices(grpc_client, agent_id, [os.path.join(data_path, 'NVMENBS01')])
    assert r.ActionResults[0].Result.Code == 0  # S_OK
    assert r.ActionResults[0].Timeout == 0

    bkp = get_bkp()
    assert bkp["Agents"][0]["AgentId"] == agent_id

    assert len(bkp["Agents"][0]["Devices"]) == default_device_count
    assert len(bkp["Config"]["KnownAgents"][0]["Devices"]) == default_device_count

    _remove_devices(grpc_client, agent_id, [
        os.path.join(data_path, f'NVMENBS{i + 1:02}') for i in range(6)
    ])

    bkp = get_bkp()
    assert len(bkp["Agents"][0]["Devices"]) == default_device_count
    assert len(bkp["Config"]["KnownAgents"][0]["Devices"]) == default_device_count

    _remove_devices(grpc_client, agent_id, [os.path.join(data_path, 'ROTNBS01')])

    bkp = get_bkp()

    assert len(bkp["Agents"][0]["Devices"]) == default_device_count

    _add_devices(grpc_client, agent_id, [
        os.path.join(data_path, f'NVMENBS{i + 1:02}') for i in range(6)
    ] + [os.path.join(data_path, 'ROTNBS01')])

    bkp = get_bkp()
    assert len(bkp["Config"]["KnownAgents"]) == 1
    assert bkp["Config"]["KnownAgents"][0]["AgentId"] == agent_id
    assert len(bkp["Config"]["KnownAgents"][0]["Devices"]) == default_device_count

    assert len(bkp["Agents"][0]["Devices"]) == default_device_count

    disk_agent.kill()


def test_change_layout(
        nbs,
        agent_id,
        data_path,
        disk_agent_configurator,
        disk_agent_dynamic_config):

    client = NbsClient(nbs.port)
    client.update_disk_registry_config(KNOWN_DEVICE_POOLS)

    disk_agent_configurator.files["disk-agent"] = disk_agent_dynamic_config

    disk_agent = start_disk_agent(disk_agent_configurator)
    disk_agent.wait_for_registration()

    bkp = client.backup_disk_registry_state()["Backup"]

    grpc_client = CreateClient(f"localhost:{nbs.port}")

    _add_devices(grpc_client, agent_id, [os.path.join(data_path, 'ROTNBS01')])

    bkp = client.backup_disk_registry_state()["Backup"]

    assert len(bkp["Config"]["KnownAgents"]) == 1
    assert bkp["Config"]["KnownAgents"][0]["AgentId"] == agent_id
    assert len(bkp["Config"]["KnownAgents"][0]["Devices"]) == 8

    assert bkp["Agents"][0]["AgentId"] == agent_id
    assert len(bkp["Agents"][0]["Devices"]) == 8
    for d in bkp["Agents"][0]["Devices"]:
        assert d.get('State') is None

    new_disk_agent_config = deepcopy(disk_agent_dynamic_config)
    new_disk_agent_config.StorageDiscoveryConfig.PathConfigs[0].MaxDeviceCount = 9

    disk_agent_configurator.files["disk-agent"] = new_disk_agent_config

    # restart DA to apply new configs
    disk_agent.kill()
    disk_agent = start_disk_agent(disk_agent_configurator)
    disk_agent.wait_for_registration()

    # nothing has changed
    bkp = client.backup_disk_registry_state()["Backup"]

    assert len(bkp["Config"]["KnownAgents"]) == 1
    assert bkp["Config"]["KnownAgents"][0]["AgentId"] == agent_id
    assert len(bkp["Config"]["KnownAgents"][0]["Devices"]) == 8

    assert bkp["Agents"][0]["AgentId"] == agent_id
    assert len(bkp["Agents"][0]["Devices"]) == 8

    _add_devices(grpc_client, agent_id, [os.path.join(data_path, 'ROTNBS01')])

    # and again nothing has changed
    bkp = client.backup_disk_registry_state()["Backup"]

    assert len(bkp["Config"]["KnownAgents"]) == 1
    assert bkp["Config"]["KnownAgents"][0]["AgentId"] == agent_id
    assert len(bkp["Config"]["KnownAgents"][0]["Devices"]) == 8

    assert bkp["Agents"][0]["AgentId"] == agent_id
    assert len(bkp["Agents"][0]["Devices"]) == 8

    # remove all devices from config
    client.update_disk_registry_config({"Version": 2} | KNOWN_DEVICE_POOLS)

    bkp = client.backup_disk_registry_state()["Backup"]

    assert bkp["Config"].get("KnownAgents") is None
    assert bkp["Agents"][0]["AgentId"] == agent_id
    assert bkp["Agents"][0].get("Devices") is None

    _add_devices(grpc_client, agent_id, [os.path.join(data_path, 'ROTNBS01')])

    # now we have a new device
    bkp = client.backup_disk_registry_state()["Backup"]

    assert len(bkp["Config"]["KnownAgents"]) == 1
    assert bkp["Config"]["KnownAgents"][0]["AgentId"] == agent_id
    assert len(bkp["Config"]["KnownAgents"][0]["Devices"]) == 9

    assert bkp["Agents"][0]["AgentId"] == agent_id
    assert len(bkp["Agents"][0]["Devices"]) == 9

    disk_agent.kill()


def test_override_storage_discovery_config(
        ydb,
        nbs,
        data_path,
        agent_id,
        disk_agent_dynamic_config):

    client = NbsClient(nbs.port)
    client.update_disk_registry_config(KNOWN_DEVICE_POOLS)

    custom_agent_id = "agent_with_custom_config"

    # create a custom device
    device_path = os.path.join(data_path, "DEVNBS01")
    device_uuid = _md5(f"{custom_agent_id}-01")

    with open(device_path, 'wb') as f:
        f.seek(DEVICE_SIZE-1)
        f.write(b'\0')
        f.flush()

    # default gobal config in the file
    default_configurator = NbsConfigurator(ydb, 'disk-agent')
    default_configurator.generate_default_nbs_configs()
    default_configurator.files["disk-agent"] = disk_agent_dynamic_config

    regular_disk_agent = start_disk_agent(default_configurator)
    regular_disk_agent.wait_for_registration()

    # setup a custom config for the DA: override the storage discovery config

    overridden_config = deepcopy(disk_agent_dynamic_config)
    overridden_config.AgentId = custom_agent_id

    del overridden_config.StorageDiscoveryConfig.PathConfigs[:]

    path_config = overridden_config.StorageDiscoveryConfig.PathConfigs.add()
    path_config.PathRegExp = f"{data_path}/DEVNBS0(1)"

    pool_config = path_config.PoolConfigs.add()
    pool_config.MinSize = DEVICE_SIZE
    pool_config.MaxSize = DEVICE_SIZE
    pool_config.PoolName = "1Mb"
    pool_config.BlockSize = 512

    custom_configurator = NbsConfigurator(ydb, 'disk-agent')
    custom_configurator.generate_default_nbs_configs()
    custom_configurator.files["disk-agent"] = disk_agent_dynamic_config
    # store overridden config in the CMS
    custom_configurator.cms["DiskAgentConfig"] = overridden_config

    disk_agent_with_overridden_config = start_disk_agent(custom_configurator, name='custom-da')
    disk_agent_with_overridden_config.wait_for_registration()

    crit = disk_agent_with_overridden_config.counters.find(
        {'sensor': 'AppDiskAgentCriticalEvents/DiskAgentConfigMismatch'})

    assert crit is not None
    assert crit['value'] == 0

    grpc_client = CreateClient(f"localhost:{nbs.port}")

    _add_devices(grpc_client, agent_id, [
        os.path.join(data_path, f'NVMENBS{i + 1:02}') for i in range(6)] + [
        os.path.join(data_path, f'NVMECOMPUTE{i + 1:02}') for i in range(4)] + [
        os.path.join(data_path, 'ROTNBS01')])

    _add_devices(grpc_client, custom_agent_id, [device_path])

    bkp = client.backup_disk_registry_state()["Backup"]

    assert len(bkp["Agents"]) == 2

    for config in bkp["Agents"]:
        if config["AgentId"] == custom_agent_id:
            # check the custom agent

            assert len(config["Devices"]) == 1

            d = config["Devices"][0]

            assert d["DeviceName"] == device_path
            assert d["DeviceUUID"] == device_uuid
            assert d.get("State") is None
            assert d["PoolName"] == "1Mb"
            assert d["BlockSize"] == 512
        else:
            # check the regular agent
            _check_disk_agent_config(config, agent_id, data_path)

    disk_agent_with_overridden_config.kill()
    regular_disk_agent.kill()
