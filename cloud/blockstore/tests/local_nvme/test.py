import logging
import os
import pytest

from cloud.blockstore.libs.storage.protos import disk_pb2
from cloud.blockstore.config.local_nvme_pb2 import TLocalNVMeConfig

from cloud.blockstore.tests.python.lib.test_base import wait_for_nbs_server

from cloud.blockstore.tests.python.lib.test_client import CreateTestClient

from cloud.blockstore.public.sdk.python.client.error_codes import EResult
from cloud.blockstore.public.sdk.python.client.error import ClientError

from contrib.ydb.tests.library.harness.kikimr_runner import \
    get_unique_path_for_current_test, ensure_path_exists

from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs

import yatest.common as yatest_common

from google.protobuf.text_format import MessageToString
from google.protobuf.json_format import Parse


NVME_DEVICES = disk_pb2.TNVMeDeviceList(Devices=[
    disk_pb2.TNVMeDevice(
        SerialNumber="S6EYNA0R104150",
        Model="SAMSUNG MZWLR3T8HBLS-00007",
        Capacity=3200631791616,
        PCIVendorId=0x144D,
        PCIDeviceId=0xA824,
        PCIAddress="0000:34:00.0",
        IOMMUGroup=48,
    ),
    disk_pb2.TNVMeDevice(
        SerialNumber="S4YPNC0R501553",
        Model="SAMSUNG MZWLJ3T8HBLS-00007",
        Capacity=3200631791616,
        PCIVendorId=0x144D,
        PCIDeviceId=0xA824,
        PCIAddress="0000:f1:00.0",
        IOMMUGroup=83,
    ),
    disk_pb2.TNVMeDevice(
        SerialNumber="S4YPNC0R500432",
        Model="SAMSUNG MZWLJ3T8HBLS-00007",
        Capacity=3200631791616,
        PCIVendorId=0x144D,
        PCIDeviceId=0xA824,
        PCIAddress="0000:f2:00.0",
        IOMMUGroup=84,
    ),
])


@pytest.fixture(name='ydb')
def start_ydb_cluster():

    ydb_cluster = start_ydb()

    yield ydb_cluster

    ydb_cluster.stop()


@pytest.fixture(name='cached_nvme_devices_path')
def create_cached_nvme_devices_file():

    p = get_unique_path_for_current_test(
        yatest_common.output_path(),
        sub_folder='cache')
    ensure_path_exists(p)

    p = os.path.join(p, 'nvme-devices.txt')

    with open(p, 'w') as file:
        file.write(MessageToString(NVME_DEVICES))
        file.write('\n')

    return p


@pytest.fixture(name='nbs')
def start_nbs_daemon(ydb, cached_nvme_devices_path):

    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()

    disk_agent_config = generate_disk_agent_txt(agent_id='')
    disk_agent_config.DedicatedDiskAgent = False

    cfg.files["disk-agent"] = disk_agent_config
    cfg.files["local-nvme"] = TLocalNVMeConfig(
        NVMeDevicesCacheFile=cached_nvme_devices_path)

    daemon = start_nbs(cfg)

    logging.info("Wait for nbs (port %d) ...", daemon.port)
    wait_for_nbs_server(daemon.port)

    yield daemon

    daemon.kill()


def test_nvme_devices(nbs):

    client = CreateTestClient(f"localhost:{nbs.port}")

    response = client.execute_ListNVMeDevices()

    devices = Parse(response, disk_pb2.TNVMeDeviceList()).Devices
    assert len(NVME_DEVICES.Devices) == len(devices)

    for lhs, rhs in zip(NVME_DEVICES.Devices, devices):
        assert lhs.SerialNumber == rhs.SerialNumber
        assert lhs.Model == rhs.Model
        assert lhs.Capacity == rhs.Capacity
        assert lhs.PCIVendorId == rhs.PCIVendorId
        assert lhs.PCIDeviceId == rhs.PCIDeviceId
        assert lhs.PCIAddress == rhs.PCIAddress
        assert lhs.IOMMUGroup == rhs.IOMMUGroup

    client.execute_AcquireNVMeDevice(SerialNumber=devices[0].SerialNumber)

    try:
        client.execute_AcquireNVMeDevice(SerialNumber="unknown")
        assert False
    except ClientError as e:
        assert e.code == EResult.E_NOT_FOUND.value

    try:
        client.execute_ReleaseNVMeDevice(SerialNumber="unknown")
        assert False
    except ClientError as e:
        assert e.code == EResult.E_NOT_FOUND.value

    client.execute_ReleaseNVMeDevice(SerialNumber=devices[0].SerialNumber)
