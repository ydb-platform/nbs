import hashlib
import os
import pytest

from copy import deepcopy

from cloud.blockstore.public.sdk.python.client import CreateClient
from cloud.blockstore.public.sdk.python.protos import TCmsActionRequest, TAction

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
        {"Name": "1Mb", "Kind": "DEVICE_POOL_KIND_LOCAL", "AllocationUnit": DEVICE_SIZE},
        {"Name": "rot", "Kind": "DEVICE_POOL_KIND_GLOBAL", "AllocationUnit": DEVICE_SIZE},
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
                [{"DeviceUUID": _md5(f"{agent_id}-01-{i + 1:03}-rot")} for i in range(8)]
            }],
        } | KNOWN_DEVICE_POOLS)


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
            "PoolName": "1Mb",
            "DeviceId": _md5(f"{agent_id}-{i + 1:02}")
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
    devices += [create_rot_device(i) for i in range(8)]

    return generate_disk_agent_txt(agent_id=agent_id, file_devices=devices)


@pytest.fixture(name='disk_agent_dynamic_config')
def create_disk_agent_dynamic_config(data_path):
    return generate_disk_agent_txt(agent_id='', storage_discovery_config={
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
                "PoolName": "1Mb",
                "MinSize": DEVICE_SIZE,
                "MaxSize": DEVICE_SIZE + 5 * 4096
            }]}]
        })


def _check_disk_agent_config(nbs, agent_id, data_path):
    client = NbsClient(nbs.port)

    bkp = client.backup_disk_registry_state()["Backup"]
    assert bkp["Agents"][0]["AgentId"] == agent_id

    devices = bkp["Agents"][0].get("Devices")

    assert devices is not None
    assert len(devices) == 14

    for d in devices:
        assert d.get("State") is None
        assert d.get("StateMessage") is None
        assert d["BlockSize"] == 4096
        assert d["AgentId"] == agent_id
        assert d["PoolName"] in ["rot", "1Mb"]

    nvme = sorted(
        [d for d in devices if d["PoolName"] == "1Mb"],
        key=lambda d: d["DeviceName"])
    assert len(nvme) == 6

    for i, d in enumerate(nvme):
        assert d["DeviceName"] == f"{data_path}/NVMENBS{i + 1:02}"
        assert int(d["BlocksCount"]) == DEVICE_SIZE / 4096
        assert int(d["UnadjustedBlockCount"]) == DEVICE_SIZE / 4096 + i

    rot = sorted(
        [d for d in devices if d["PoolName"] == "rot"],
        key=lambda d: int(d["PhysicalOffset"]))
    assert len(rot) == 8

    offset = DEVICE_SIZE / 2
    for d in rot:
        assert d["DeviceName"] == f"{data_path}/ROTNBS01"
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

    _check_disk_agent_config(nbs, agent_id, data_path)

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

    _check_disk_agent_config(nbs, agent_id, data_path)

    crit = disk_agent.counters.find({'sensor': 'AppCriticalEvents/DiskAgentConfigMismatch'})
    assert crit is not None
    assert crit['value'] == (1 if cmp == 'mismatch' else 0)

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

    storage = grpc_client.query_available_storage([agent_id])
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    # no devices have been added yet
    assert storage[0].ChunkCount == 0

    # adding one local device: DR must wait for this device to be cleaned.
    r = _add_devices(grpc_client, agent_id, [os.path.join(data_path, 'NVMENBS01')])
    assert len(r.ActionResults) == 1
    assert r.ActionResults[0].Result.Code == 0, r

    storage = grpc_client.query_available_storage([agent_id])
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    # now we see one chunk
    assert storage[0].ChunkCount == 1
    assert storage[0].ChunkSize == DEVICE_SIZE

    bkp = client.backup_disk_registry_state()["Backup"]
    assert bkp.get("DirtyDevices") is None
    assert bkp["Agents"][0]["AgentId"] == agent_id

    assert len(bkp["Agents"][0]["Devices"]) == 1
    assert bkp["Agents"][0]["Devices"][0].get("State") is None

    # adding all the local devices
    r = _add_devices(grpc_client, agent_id, [
        os.path.join(data_path, f'NVMENBS{i + 1:02}') for i in range(6)
    ])
    assert len(r.ActionResults) == 6
    assert all(x.Result.Code == 0 for x in r.ActionResults)

    storage = grpc_client.query_available_storage([agent_id])
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    # now we see all the local devices
    assert storage[0].ChunkCount == 6
    assert storage[0].ChunkSize == DEVICE_SIZE

    bkp = client.backup_disk_registry_state()["Backup"]
    assert bkp.get("DirtyDevices") is None
    assert len(bkp["Agents"][0]["Devices"]) == 6
    assert all([d.get("State") is None for d in bkp["Agents"][0]["Devices"]])

    # adding a non local device: DR will not wait for this device to clear.
    r = _add_devices(grpc_client, agent_id, [os.path.join(data_path, 'ROTNBS01')])
    assert len(r.ActionResults) == 1
    assert r.ActionResults[0].Result.Code == 0, r

    # we see all the devices in the backup
    bkp = client.backup_disk_registry_state()["Backup"]
    assert bkp.get("DirtyDevices") is not None
    assert len(bkp["Agents"][0]["Devices"]) == 14

    storage = grpc_client.query_available_storage([agent_id])
    assert len(storage) == 1
    assert storage[0].AgentId == agent_id
    # we still see all the local devices
    assert storage[0].ChunkCount == 6
    assert storage[0].ChunkSize == DEVICE_SIZE

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

    def get_bkp():
        r = client.backup_disk_registry_state(
            backup_local_db=backup_from == 'local_db')
        return r["Backup"]

    client = NbsClient(nbs.port)
    bkp = get_bkp()
    assert len(bkp["Config"]["KnownAgents"]) == 1
    assert bkp["Config"]["KnownAgents"][0]["AgentId"] == agent_id
    assert len(bkp["Config"]["KnownAgents"][0]["Devices"]) == 14
    assert bkp.get("Agents") is None

    disk_agent_configurator.files["disk-agent"] = disk_agent_dynamic_config

    disk_agent = start_disk_agent(disk_agent_configurator)
    disk_agent.wait_for_registration()

    bkp = get_bkp()

    assert bkp["Agents"][0]["AgentId"] == agent_id
    assert len(bkp["Agents"][0]["Devices"]) == 14

    assert len(bkp["Config"]["KnownAgents"]) == 1
    assert bkp["Config"]["KnownAgents"][0]["AgentId"] == agent_id
    assert len(bkp["Config"]["KnownAgents"][0]["Devices"]) == 14

    grpc_client = CreateClient(f"localhost:{nbs.port}")

    r = _remove_devices(grpc_client, agent_id, [os.path.join(data_path, 'NVMENBS01')])
    assert r.ActionResults[0].Result.Code == 0  # S_OK
    assert r.ActionResults[0].Timeout == 0

    bkp = get_bkp()
    assert bkp["Agents"][0]["AgentId"] == agent_id

    assert len(bkp["Agents"][0]["Devices"]) == 14
    assert len(bkp["Config"]["KnownAgents"][0]["Devices"]) == 14

    r = _remove_devices(grpc_client, agent_id, [os.path.join(data_path, 'NVMENBS01')])
    assert r.ActionResults[0].Result.Code == 0  # S_OK
    assert r.ActionResults[0].Timeout == 0

    bkp = get_bkp()
    assert bkp["Agents"][0]["AgentId"] == agent_id

    assert len(bkp["Agents"][0]["Devices"]) == 14
    assert len(bkp["Config"]["KnownAgents"][0]["Devices"]) == 14

    _remove_devices(grpc_client, agent_id, [
        os.path.join(data_path, f'NVMENBS{i + 1:02}') for i in range(6)
    ])

    bkp = get_bkp()
    assert len(bkp["Agents"][0]["Devices"]) == 14
    assert len(bkp["Config"]["KnownAgents"][0]["Devices"]) == 14

    _remove_devices(grpc_client, agent_id, [os.path.join(data_path, 'ROTNBS01')])

    bkp = get_bkp()

    assert len(bkp["Agents"][0]["Devices"]) == 14

    _add_devices(grpc_client, agent_id, [
        os.path.join(data_path, f'NVMENBS{i + 1:02}') for i in range(6)
    ] + [os.path.join(data_path, 'ROTNBS01')])

    bkp = get_bkp()
    assert len(bkp["Config"]["KnownAgents"]) == 1
    assert bkp["Config"]["KnownAgents"][0]["AgentId"] == agent_id
    assert len(bkp["Config"]["KnownAgents"][0]["Devices"]) == 14

    assert len(bkp["Agents"][0]["Devices"]) == 14

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
    assert len(bkp["DirtyDevices"]) == 9

    disk_agent.kill()
