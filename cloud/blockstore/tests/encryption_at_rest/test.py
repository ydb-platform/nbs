import json
import os
import pytest
import time

import yatest.common as yatest_common
import cloud.blockstore.tests.python.lib.daemon as daemon

from cloud.blockstore.public.api.protos.encryption_pb2 import \
    ENCRYPTION_DEFAULT_AES_XTS

from cloud.blockstore.public.sdk.python.client import CreateClient, Session
from cloud.blockstore.public.sdk.python.protos import TCmsActionRequest, \
    TAction, STORAGE_MEDIA_SSD_NONREPLICATED
from cloud.blockstore.config.root_kms_pb2 import TRootKmsConfig
from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt
from cloud.storage.core.config.features_pb2 import TFeaturesConfig

from contrib.ydb.tests.library.harness.kikimr_runner import \
    get_unique_path_for_current_test, ensure_path_exists


DEVICE_SIZE = 1024**3   # 1 GiB
DEVICE_COUNT = 6
DEVICE_PADDING = 4096
DEVICE_HEADER = 4096
KEK_ID = 'nbs'


@pytest.fixture(name='agent_id')
def get_agent_id():
    return daemon.get_fqdn()


@pytest.fixture(name='ydb')
def start_ydb_cluster():

    ydb_cluster = daemon.start_ydb()

    yield ydb_cluster

    ydb_cluster.stop()


@pytest.fixture(name='nbs')
def start_nbs_daemon(ydb):

    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()
    cfg.files['storage'].AllocationUnitNonReplicatedSSD = 1  # 1 GiB

    features = TFeaturesConfig()
    feature = features.Features.add()
    feature.Name = 'EncryptionAtRestForDiskRegistryBasedDisks'
    feature.Whitelist.EntityIds.append("vol0")

    cfg.files['features'] = features

    root_kms = TRootKmsConfig()
    root_kms.Address = f'localhost:{os.environ.get("FAKE_ROOT_KMS_PORT")}'
    root_kms.KeyId = KEK_ID
    root_kms.RootCertsFile = os.environ.get("FAKE_ROOT_KMS_CA")
    root_kms.CertChainFile = os.environ.get("FAKE_ROOT_KMS_CLIENT_CRT")
    root_kms.PrivateKeyFile = os.environ.get("FAKE_ROOT_KMS_CLIENT_KEY")

    cfg.files['root-kms'] = root_kms

    nbs = daemon.start_nbs(cfg)

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
def start_disk_agent(ydb, nbs, agent_id):

    data_path = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder="data")

    data_path = os.path.join(data_path, "dev", "disk", "by-partlabel")
    ensure_path_exists(data_path)

    with open(os.path.join(data_path, 'NVMENBS01'), 'wb') as f:
        os.truncate(
            f.fileno(),
            DEVICE_HEADER + DEVICE_SIZE * DEVICE_COUNT + (DEVICE_COUNT - 1) *
            DEVICE_PADDING)

    cfg = NbsConfigurator(ydb, 'disk-agent')
    cfg.generate_default_nbs_configs()
    cfg.files["disk-agent"] = generate_disk_agent_txt(
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

    disk_agent = daemon.start_disk_agent(cfg)
    disk_agent.wait_for_registration()

    client = CreateClient(f"localhost:{nbs.port}")

    _add_host(client, agent_id)
    _wait_for_devices_to_be_cleared(client)

    yield disk_agent

    disk_agent.stop()


def test_create_volume_with_default_ecnryption(nbs, disk_agent):

    client = CreateClient(f"localhost:{nbs.port}")

    client.create_volume(
        disk_id="vol0",
        block_size=4096,
        blocks_count=2*DEVICE_SIZE//4096,
        storage_media_kind=STORAGE_MEDIA_SSD_NONREPLICATED)

    session = Session(client, "vol0", "")
    vol0 = session.mount_volume()['Volume']

    def read_first_block():
        with open(vol0.Devices[0].DeviceName, 'rb') as f:
            f.seek(vol0.Devices[0].PhysicalOffset)
            return f.read(4096)

    # check that the first block is clean
    raw_data = read_first_block()
    assert raw_data.count(0) == 4096

    assert len(vol0.Devices) == 2
    assert vol0.EncryptionDesc.Mode == ENCRYPTION_DEFAULT_AES_XTS

    assert vol0.EncryptionDesc.EncryptionKey.KekId == KEK_ID
    assert len(vol0.EncryptionDesc.EncryptionKey.EncryptedDEK) == 512

    expected_data = os.urandom(4096)

    session.write_blocks(0, [expected_data])
    blocks = session.read_blocks(0, 1, checkpoint_id="")
    assert len(blocks) == 1
    assert expected_data == blocks[0]

    # check that the first block is encrypted
    raw_data = read_first_block()
    assert raw_data.count(0) != 4096
    assert raw_data != expected_data

    session.unmount_volume()

    # check that the volume doesn't tack used blocks

    response = json.loads(client.execute_action(
        action="UpdateUsedBlocks",
        input_bytes=json.dumps({
            "DiskId": "vol0",
            "StartIndices": [0],
            "BlockCounts": [1],
            "Used": True
        }).encode()))

    error = response.get("Error", {})

    assert error.get("Code") == 1   # S_FALSE
    assert error.get("Message") == "Used block tracking not set up"
