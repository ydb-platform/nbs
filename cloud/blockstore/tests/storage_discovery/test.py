import hashlib
import os
import pytest
import socket
import time

from cloud.blockstore.tests.python.lib.client import NbsClient
from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs, \
    start_disk_agent

import yatest.common as yatest_common

from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, \
    ensure_path_exists


def _md5(s):
    return hashlib.md5(s.encode("utf-8")).hexdigest()


@pytest.fixture
def ydb():

    ydb_cluster = start_ydb()

    yield ydb_cluster

    ydb_cluster.stop()


@pytest.fixture
def nbs(ydb):

    nbs_configurator = NbsConfigurator(ydb)
    nbs_configurator.generate_default_nbs_configs()

    daemon = start_nbs(nbs_configurator)

    yield daemon

    daemon.kill()


@pytest.fixture
def agent_id():
    return socket.getfqdn()


@pytest.fixture(autouse=True)
def setup_disk_registry(nbs, agent_id):

    client = NbsClient(nbs.port)
    client.disk_registry_set_writable_state()
    client.update_disk_registry_config({
        "KnownAgents": [{
            "AgentId": agent_id,
            "KnownDevices":
                [{"DeviceUUID": _md5(f"{agent_id}-{i + 1:02}")} for i in range(6)] +
                [{"DeviceUUID": _md5(f"{agent_id}-01-{i + 1:03}-rot")} for i in range(8)]
            }],
        "KnownDevicePools": [
            {"Name": "1Mb", "Kind": "DEVICE_POOL_KIND_GLOBAL", "AllocationUnit": 1024**2},
            {"Name": "rot", "Kind": "DEVICE_POOL_KIND_GLOBAL", "AllocationUnit": 1024**2},
        ]})


@pytest.fixture
def data_path():

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
        create_file(f"NVMENBS{i + 1:02}", 1024**2 + i * 4096)

    create_file("ROTNBS01", 1024**2 * 10)


@pytest.fixture
def disk_agent_configurator(ydb):

    configurator = NbsConfigurator(ydb, 'disk-agent')
    configurator.generate_default_nbs_configs()

    return configurator


@pytest.fixture
def disk_agent_static_config(data_path, agent_id):

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
            "Offset": 1024**2 / 2 + i * (1024**2 + 4096),
            "FileSize": 1024**2,
            "DeviceId": _md5(f"{agent_id}-01-{i + 1:03}-rot")
        }

    devices = [create_nvme_device(i) for i in range(6)]
    devices += [create_rot_device(i) for i in range(8)]

    return generate_disk_agent_txt(file_devices=devices)


@pytest.fixture
def disk_agent_dynamic_config(data_path):
    return generate_disk_agent_txt(agent_id='', storage_discovery_config={
        "PathConfigs": [{
            "PathRegExp": f"{data_path}/ROTNBS([0-9]+)",
            "MaxDeviceCount": 8,
            "PoolConfigs": [{
                "PoolName": "rot",
                "MinSize": 0,
                "MaxSize": 1024**2 * 10,
                "HashSuffix": "-rot",
                "Layout": {
                    "DeviceSize": 1024**2,
                    "DevicePadding": 4096,
                    "HeaderSize": 1024**2 / 2
                }
            }]
        }, {
            "PathRegExp": f"{data_path}/NVMENBS([0-9]+)",
            "PoolConfigs": [{
                "PoolName": "1Mb",
                "MinSize": 1024**2,
                "MaxSize": 1024**2 + 5 * 4096
            }]}]
        })


def _check_disk_agent_config(nbs, agent_id, data_path):
    client = NbsClient(nbs.port)

    while True:
        bkp = client.backup_disk_registry_state()["Backup"]
        if bkp.get("Agents") is not None:
            break
        time.sleep(1)

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
        assert int(d["BlocksCount"]) == 1024**2 / 4096
        assert int(d["UnadjustedBlockCount"]) == 1024**2 / 4096 + i

    rot = sorted(
        [d for d in devices if d["PoolName"] == "rot"],
        key=lambda d: int(d["PhysicalOffset"]))
    assert len(rot) == 8

    offset = 1024**2 / 2
    for d in rot:
        assert d["DeviceName"] == f"{data_path}/ROTNBS01"
        assert int(d["BlocksCount"]) == 1024**2 / 4096
        assert int(d["UnadjustedBlockCount"]) == 1024**2 / 4096
        assert int(d["PhysicalOffset"]) == offset
        offset += 1024**2 + 4096


@pytest.mark.parametrize("source", ['file', 'cms', 'mix'])
def test_storage_discovery(
        nbs,
        agent_id,
        data_path,
        disk_agent_configurator,
        disk_agent_dynamic_config,
        source):

    if source == "cms":
        disk_agent_configurator.cms["DiskAgentConfig"] = disk_agent_dynamic_config

    if source in ["file", "mix"]:
        disk_agent_configurator.files["disk-agent"] = disk_agent_dynamic_config

    if source == "mix":
        disk_agent_configurator.cms["DiskAgentConfig"] = generate_disk_agent_txt()

    disk_agent = start_disk_agent(disk_agent_configurator)

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
        # create an additional file to make the dynamic configuration different
        # from the static one.
        with open(os.path.join(data_path, "NVMENBS42"), 'wb') as f:
            f.seek(1024**2 - 1)
            f.write(b'\0')
            f.flush()

    disk_agent_configurator.files["disk-agent"] = disk_agent_dynamic_config
    disk_agent_configurator.cms["DiskAgentConfig"] = disk_agent_static_config

    disk_agent = start_disk_agent(disk_agent_configurator)

    _check_disk_agent_config(nbs, agent_id, data_path)

    crit = disk_agent.counters.find(sensor='AppCriticalEvents/DiskAgentConfigMismatch')
    assert crit is not None
    assert crit['value'] == (1 if cmp == 'mismatch' else 0)

    disk_agent.kill()
