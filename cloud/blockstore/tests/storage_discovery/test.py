import hashlib
import os
import pytest
import socket
import time

from google.protobuf.json_format import ParseDict

from cloud.blockstore.tests.python.lib.client import NbsClient
from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs, \
    start_disk_agent

import yatest.common as yatest_common

from contrib.ydb.tests.library.common.yatest_common import PortManager
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, \
    ensure_path_exists

from cloud.blockstore.config.disk_pb2 import TStorageDiscoveryConfig


def __create_data_path():
    data_path = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder="data")

    data_path = os.path.join(data_path, "dev", "disk", "by-partlabel")
    ensure_path_exists(data_path)

    return data_path


def __create_storage(path):

    def create_file(name, size):
        with open(os.path.join(path, name), 'wb') as f:
            f.seek(size-1)
            f.write(b'\0')
            f.flush()

    for i in range(6):
        create_file(f"NVMENBS0{i + 1}", 1024**2 + i * 4096)

    create_file("ROTNBS01", 1024**2 * 10)

    return ParseDict({
        "PathConfigs": [{
            "PathRegExp": f"{path}/ROTNBS([0-9]+)",
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
            "PathRegExp": f"{path}/NVMENBS([0-9]+)",
            "PoolConfigs": [{
                "PoolName": "1Mb",
                "MinSize": 1024**2,
                "MaxSize": 1024**2 + 5 * 4096
            }]}]
        },
        TStorageDiscoveryConfig())


def __md5(s):
    return hashlib.md5(s.encode("utf-8")).hexdigest()


@pytest.mark.parametrize("source", ['file', 'cms', 'mix'])
def test_storage_discovery(source):
    pm = PortManager()

    ydb = start_ydb()

    nbs_configurator = NbsConfigurator(pm, ydb)
    nbs_configurator.generate_default_nbs_configs()

    nbs = start_nbs(nbs_configurator)

    agent_id = socket.gethostname()

    client = NbsClient(nbs.port)
    client.disk_registry_set_writable_state()
    client.update_disk_registry_config({
        "KnownAgents": [{
            "AgentId": agent_id,
            "KnownDevices":
                [{"DeviceUUID": __md5(f"{agent_id}-{i + 1:02}")} for i in range(6)] +
                [{"DeviceUUID": __md5(f"{agent_id}-01-{i + 1:03}-rot")} for i in range(8)]
            }],
        "KnownDevicePools": [
            {"Name": "1Mb", "Kind": "DEVICE_POOL_KIND_GLOBAL", "AllocationUnit": 1024**2},
            {"Name": "rot", "Kind": "DEVICE_POOL_KIND_GLOBAL", "AllocationUnit": 1024**2},
        ]})

    data_path = __create_data_path()

    disk_agent_configurator = NbsConfigurator(pm, ydb, 'disk-agent')
    disk_agent_configurator.generate_default_nbs_configs()

    disk_agent_config = generate_disk_agent_txt()
    disk_agent_config.StorageDiscoveryConfig.CopyFrom(__create_storage(data_path))

    if source == "cms":
        disk_agent_configurator.cms["DiskAgentConfig"] = disk_agent_config

    if source in ["file", "mix"]:
        disk_agent_configurator.files["disk-agent"] = disk_agent_config

    if source == "mix":
        disk_agent_configurator.cms["DiskAgentConfig"] = generate_disk_agent_txt()

    disk_agent = start_disk_agent(disk_agent_configurator)

    while True:
        bkp = client.backup_disk_registry_state()["Backup"]
        if bkp.get("Agents") is not None:
            break
        time.sleep(1)

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

    disk_agent.kill()
    nbs.kill()

    ydb.stop()
