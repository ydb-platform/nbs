import json
import os
import pytest
import time

import yatest.common as yatest_common
import cloud.blockstore.tests.python.lib.daemon as daemon

from cloud.blockstore.public.sdk.python.client import CreateClient
from cloud.blockstore.public.sdk.python.protos import TCmsActionRequest, \
    TAction, STORAGE_MEDIA_SSD_NONREPLICATED
from cloud.blockstore.config.root_kms_pb2 import TRootKmsConfig
from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt

from contrib.ydb.tests.library.harness.kikimr_runner import \
    get_unique_path_for_current_test, ensure_path_exists


DEVICE_SIZE = 1024**3   # 1 GiB
DEVICE_COUNT = 6
DEVICE_PADDING = 4096
DEVICE_HEADER = 4096


@pytest.fixture(name='agent_id')
def get_agent_id():
    return daemon.get_fqdn()


@pytest.fixture(name='data_path')
def create_data_path():

    p = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder="data")

    p = os.path.join(p, "dev", "disk", "by-partlabel")
    ensure_path_exists(p)

    return p


@pytest.fixture(autouse=True)
def create_device_storage(data_path):
    with open(os.path.join(data_path, 'NVMENBS01'), 'wb') as f:
        os.truncate(f.fileno(), DEVICE_HEADER + DEVICE_SIZE * DEVICE_COUNT + (DEVICE_COUNT - 1) * DEVICE_PADDING)


@pytest.fixture(name='ydb')
def start_ydb_cluster():

    ydb_cluster = daemon.start_ydb()

    yield ydb_cluster

    ydb_cluster.stop()


@pytest.fixture(name='nbs')
def start_nbs_daemon(ydb):

    nbs_configurator = NbsConfigurator(ydb)
    nbs_configurator.generate_default_nbs_configs()
    nbs_configurator.files["storage"].AllocationUnitNonReplicatedSSD = 1  # 1 GiB

    root_kms = TRootKmsConfig()
    root_kms.Address = f'localhost:{os.environ.get("FAKE_ROOT_KMS_PORT")}'
    root_kms.KeyId = 'nbs'
    root_kms.RootCertsFile = os.environ.get("FAKE_ROOT_KMS_CA")
    root_kms.CertChainFile = os.environ.get("FAKE_ROOT_KMS_CLIENT_CRT")
    root_kms.PrivateKeyFile = os.environ.get("FAKE_ROOT_KMS_CLIENT_KEY")

    nbs_configurator.files['root-kms'] = root_kms

    nbs = daemon.start_nbs(nbs_configurator)

    client = CreateClient(f"localhost:{nbs.port}")
    client.execute_action(
        action="DiskRegistrySetWritableState",
        input_bytes=str.encode('{"State": true}'))
    client.update_disk_registry_config({
        "KnownDevicePools":
            [{"Kind": "DEVICE_POOL_KIND_DEFAULT", "AllocationUnit": DEVICE_SIZE}]
        })

    yield nbs

    nbs.kill()


def _add_host(client, agent_id):
    request = TCmsActionRequest()
    action = request.Actions.add()
    action.Type = TAction.ADD_HOST
    action.Host = agent_id

    return client.cms_action(request)


def _wait_for_devices_to_be_cleared(client, expected_dirty_count=0):
    while True:
        response = client.execute_action(
            action="BackupDiskRegistryState",
            input_bytes=str.encode('{"BackupLocalDB": true}'))
        bkp = json.loads(response)["Backup"]
        agents = bkp.get("Agents", [])
        dirty_devices = bkp.get("DirtyDevices", [])
        dirty_count = len(dirty_devices)
        if len(agents) != 0 and dirty_count == expected_dirty_count:
            break
        time.sleep(1)


@pytest.fixture(name='disk_agent')
def start_disk_agent(ydb, nbs, agent_id, data_path):

    configurator = NbsConfigurator(ydb, 'disk-agent')
    configurator.generate_default_nbs_configs()
    configurator.files["disk-agent"] = generate_disk_agent_txt(
        agent_id='',
        device_erase_method='DEVICE_ERASE_METHOD_NONE',  # speed up tests
        storage_discovery_config={
            "PathConfigs": [{
                "PathRegExp": f"{data_path}/NVMENBS([0-9]+)",
                "PoolConfigs": [{
                    "Layout": {
                        "DeviceSize": DEVICE_SIZE,
                        "DevicePadding": DEVICE_PADDING,
                        "HeaderSize": DEVICE_HEADER
                    }
                }]}
            ]})

    disk_agent = daemon.start_disk_agent(configurator)
    disk_agent.wait_for_registration()

    client = CreateClient(f"localhost:{nbs.port}")

    _add_host(client, agent_id)
    _wait_for_devices_to_be_cleared(client)

    yield disk_agent

    disk_agent.stop()


def test_create_encrypted_volume(nbs, disk_agent):

    client = CreateClient(f"localhost:{nbs.port}")

    client.create_volume(
        disk_id="vol0",
        block_size=4096,
        blocks_count=2*DEVICE_SIZE//4096,
        storage_media_kind=STORAGE_MEDIA_SSD_NONREPLICATED)

    vol0 = client.stat_volume("vol0")["Volume"]

    assert len(vol0.Devices) == 2
