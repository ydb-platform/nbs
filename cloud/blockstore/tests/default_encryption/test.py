import logging
import os
import pytest
import time

# from cloud.blockstore.public.api.protos.encryption_pb2 import TEncryptionSpec,\
#    ENCRYPTION_AES_XTS, TKeyPath
from cloud.blockstore.public.sdk.python.client.session import Session
from cloud.blockstore.public.sdk.python.client import CreateClient, \
    ClientError
from cloud.blockstore.public.sdk.python.protos import TCmsActionRequest, \
    TAction, STORAGE_MEDIA_SSD_NONREPLICATED

from cloud.blockstore.tests.python.lib.client import NbsClient
from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs, \
    start_disk_agent, get_fqdn

import yatest.common as yatest_common

from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, \
    ensure_path_exists

from library.python.retry import retry


DEFAULT_BLOCK_SIZE = 4096
DEVICE_SIZE = 1024**3   # 1 GiB
DEVICE_COUNT = 2


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("test_logger")


@pytest.fixture(name='data_path')
def create_storage():

    p = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder="data")

    p = os.path.join(p, "dev", "disk", "by-partlabel")
    ensure_path_exists(p)

    for i in range(DEVICE_COUNT):
        with open(os.path.join(p, f"NVMENBS{i + 1:02}"), 'wb') as f:
            f.seek(DEVICE_SIZE - 1)
            f.write(b'\0')
            f.flush()

    return p


@pytest.fixture(name='disk_agent_configurator')
def create_disk_agent_configurator(ydb, data_path):

    configurator = NbsConfigurator(ydb, 'disk-agent')
    configurator.generate_default_nbs_configs()

    disk_agent_config = generate_disk_agent_txt(
        agent_id='',
        device_erase_method='DEVICE_ERASE_METHOD_NONE',  # speed up tests
        storage_discovery_config={
            "PathConfigs": [{
                "PathRegExp": f"{data_path}/NVMENBS([0-9]+)",
                "PoolConfigs": [{
                    "MinSize": DEVICE_SIZE,
                }]}]
            })

    configurator.files["disk-agent"] = disk_agent_config

    return configurator


@pytest.fixture(name='ydb')
def start_ydb_cluster():

    ydb_cluster = start_ydb()

    yield ydb_cluster

    ydb_cluster.stop()


@pytest.fixture(name='nbs')
def start_nbs_daemon(ydb):

    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()

    cfg.files["storage"].AllocationUnitNonReplicatedSSD = 1
    cfg.files["storage"].DefaultEncryptionForNonReplicatedDisksEnabled = True

    daemon = start_nbs(cfg)

    yield daemon

    daemon.kill()


@pytest.fixture(name='disk_agent')
def start_disk_agent_daemon(ydb, disk_agent_configurator):

    daemon = start_disk_agent(disk_agent_configurator)

    yield daemon

    daemon.kill()


@pytest.fixture(autouse=True)
def setup_env(nbs, disk_agent, data_path):

    client = CreateClient(f"localhost:{nbs.port}")
    client.execute_action(
        action="DiskRegistrySetWritableState",
        input_bytes=str.encode('{"State": true}'))

    disk_agent.wait_for_registration()

    request = TCmsActionRequest()

    action = request.Actions.add()
    action.Type = TAction.ADD_HOST
    action.Host = get_fqdn()

    response = client.cms_action(request)

    assert len(response.ActionResults) == 1
    for r in response.ActionResults:
        assert r.Result.Code == 0, r

    # wait for devices to be cleared
    nbs_client = NbsClient(nbs.port)
    while True:
        bkp = nbs_client.backup_disk_registry_state()["Backup"]
        if bkp.get("DirtyDevices", 0) == 0:
            break
        time.sleep(1)


def __read_raw_blocks(device, start_index, blocks_count):
    offset = start_index * DEFAULT_BLOCK_SIZE

    blocks = []

    with open(device.DeviceName, 'rb') as f:
        f.seek(offset)
        for i in range(blocks_count):
            blocks.append(f.read(DEFAULT_BLOCK_SIZE))

    return blocks


def test_encryption(nbs):

    client = CreateClient(f"localhost:{nbs.port}")

    @retry(max_times=10, exception=ClientError)
    def create_vol0():
        client.create_volume(
            disk_id="vol0",
            block_size=DEFAULT_BLOCK_SIZE,
            blocks_count=2*DEVICE_SIZE//DEFAULT_BLOCK_SIZE,
            storage_media_kind=STORAGE_MEDIA_SSD_NONREPLICATED,
            storage_pool_name="")

    create_vol0()

    session = Session(
        client,
        "vol0",
        mount_token='',
        log=logger)

    volume = session.mount_volume()["Volume"]

    assert len(volume.Devices) == 2
    device = volume.Devices[0]

    test_data = os.urandom(DEFAULT_BLOCK_SIZE)

    # read zeroes
    raw = __read_raw_blocks(device, start_index=0, blocks_count=2)
    assert len(raw) == 2
    assert all(len(b) == DEFAULT_BLOCK_SIZE for b in raw)
    for b in raw:
        assert all(x == 0 for x in b)

    # write single block
    session.write_blocks(start_index=0, blocks=[test_data])

    # check data
    blocks = session.read_blocks(
        start_index=0,
        blocks_count=2,
        checkpoint_id='')

    assert len(blocks) == 2
    assert len(blocks[0]) == DEFAULT_BLOCK_SIZE
    assert len(blocks[1]) == DEFAULT_BLOCK_SIZE

    # first block must be with data
    assert test_data == blocks[0]

    # second block must be zeroed
    assert all(x == 0 for x in blocks[1])

    # read data bypass the volume

    raw = __read_raw_blocks(device, start_index=0, blocks_count=2)
    assert len(raw) == 2
    assert all(len(b) == DEFAULT_BLOCK_SIZE for b in raw)
    # first block must be encoded
    assert raw[0] != test_data
    # end non zeroed
    assert any(x != 0 for x in raw[0])

    # second block must be zeroed
    assert all(x == 0 for x in raw[1])

    session.unmount_volume()
