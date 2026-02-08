import json
import os
import pytest

from google.protobuf.text_format import MessageToString

from cloud.blockstore.config.disk_pb2 import TDiskAgentConfig
from cloud.blockstore.config.local_nvme_pb2 import TLocalNVMeConfig
from cloud.blockstore.libs.storage.protos.local_nvme_pb2 import \
    TNVMeDevice, TNVMeDeviceList, TLocalNVMeServiceState
from cloud.blockstore.tests.python.lib.config import NbsConfigurator
from cloud.blockstore.tests.python.lib.test_client import CreateTestClient

from contrib.ydb.tests.library.harness.kikimr_runner import \
    get_unique_path_for_current_test, ensure_path_exists

import yatest.common as yatest_common
import cloud.blockstore.tests.python.lib.daemon as daemon


LOCAL_NVME_DEVICES = [
    TNVMeDevice(
        SerialNumber="NVME_0",
        PCIAddress="f1:00.0",
        IOMMUGroup=10,
        VendorId=0x100,
        DeviceId=0x200,
        Model="Test NVMe 1",
    ),
    TNVMeDevice(
        SerialNumber="NVME_1",
        PCIAddress="f2:00.0",
        IOMMUGroup=20,
        VendorId=0x100,
        DeviceId=0x300,
        Model="Test NVMe 2",
    ),
    TNVMeDevice(
        SerialNumber="NVME_2",
        PCIAddress="32:00.0",
        IOMMUGroup=30,
        VendorId=0x100,
        DeviceId=0x300,
        Model="Test NVMe 2",
    ),
    TNVMeDevice(
        SerialNumber="NVME_3",
        PCIAddress="32:00.0",
        IOMMUGroup=40,
        VendorId=0x100,
        DeviceId=0x200,
        Model="Test NVMe 1",
    ),
]


@pytest.fixture(name='ydb')
def start_ydb_cluster():

    ydb_cluster = daemon.start_ydb()

    yield ydb_cluster

    ydb_cluster.stop()


@pytest.fixture(name='local_nvme_config')
def create_local_nvme_config():

    path = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder='local_nvme')

    ensure_path_exists(path)

    devicesPath = os.path.join(path, "devices.txt")

    with open(devicesPath, 'w') as f:
        f.write(MessageToString(TNVMeDeviceList(Devices=LOCAL_NVME_DEVICES)))

    cacheFilePath = os.path.join(path, "cache.txt")

    with open(cacheFilePath, 'w') as f:
        f.write(MessageToString(TLocalNVMeServiceState(Devices=LOCAL_NVME_DEVICES)))

    return TLocalNVMeConfig(
        DevicesSourceUri=f"file://{devicesPath}",
        StateCacheFilePath=cacheFilePath)


@pytest.fixture(name='nbs')
def start_nbs_daemon(ydb, local_nvme_config):
    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()
    cfg.files["local-nvme"] = local_nvme_config
    cfg.files["disk-agent"] = TDiskAgentConfig(Enabled=True)

    nbs = daemon.start_nbs(cfg)

    yield nbs

    nbs.stop()


def test_local_nvme(ydb, nbs):
    client = CreateTestClient(f"localhost:{nbs.port}")

    response = json.loads(client.execute_ListNVMeDevices())

    devices = response['Devices']
    assert len(LOCAL_NVME_DEVICES) == len(devices)

    devices.sort(key=lambda d: d['SerialNumber'])

    for lhs, rhs in zip(LOCAL_NVME_DEVICES, devices):
        assert lhs.SerialNumber == rhs['SerialNumber']
        assert lhs.PCIAddress == rhs['PCIAddress']
        assert lhs.IOMMUGroup == rhs['IOMMUGroup']
        assert lhs.VendorId == rhs['VendorId']
        assert lhs.DeviceId == rhs['DeviceId']
        assert lhs.Model == rhs['Model']
