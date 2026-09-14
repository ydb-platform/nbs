import os
import pytest
import time

from cloud.blockstore.tests.python.lib.test_client import CreateTestClient

from cloud.blockstore.public.sdk.python.client.error import ClientError
from cloud.blockstore.public.sdk.python.client.error_codes import EResult
from cloud.blockstore.public.sdk.python.protos import \
    STORAGE_MEDIA_SSD_LOCAL, STORAGE_MEDIA_SSD_NONREPLICATED

from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs, \
    start_disk_agent, get_fqdn

from contrib.ydb.tests.library.harness.kikimr_runner import \
    get_unique_path_for_current_test, ensure_path_exists


BLOCK_SIZE = 4096

# A nonreplicated disk is allocated in units of AllocationUnitNonReplicatedSSD
# gigabytes, so its devices can't be as small as the local ones.
NRD_DEVICE_SIZE = 1024**3                   # 1 GiB
NRD_DEVICE_RAW_SIZE = NRD_DEVICE_SIZE + 4096
NRD_DEVICE_COUNT = 2

# A local disk has no allocation unit of its own, its pool can be any size.
LOCAL_POOL_NAME = "local-ssd"
LOCAL_DEVICE_SIZE = 9 * 1024**2             # 9 MiB
LOCAL_DEVICE_RAW_SIZE = LOCAL_DEVICE_SIZE + 4096
LOCAL_DEVICE_COUNT = 2

# Longer than this test can possibly run: the devices reported by the agent are
# dirty, and postponing their secure erase keeps them that way, so that every
# allocation below deterministically fails to find a clean device.
SECURE_ERASE_COOL_DOWN_MS = 10 * 60 * 1000

BROKEN_DISK_DESTRUCTION_DELAY_MS = 2000

NRD_DISK_ID = "nrd0"
LOCAL_DISK_ID = "local0"

KNOWN_DEVICE_POOLS = {
    "KnownDevicePools": [
        {"Kind": "DEVICE_POOL_KIND_DEFAULT",
            "AllocationUnit": NRD_DEVICE_SIZE},
        {"Name": LOCAL_POOL_NAME, "Kind": "DEVICE_POOL_KIND_LOCAL",
            "AllocationUnit": LOCAL_DEVICE_SIZE},
    ]}


@pytest.fixture(name='ydb')
def start_ydb_cluster():

    ydb_cluster = start_ydb()

    yield ydb_cluster

    ydb_cluster.stop()


@pytest.fixture(name='agent_id')
def get_agent_id():
    return get_fqdn()


@pytest.fixture(name='data_path')
def create_data_path(tmp_path):

    p = get_unique_path_for_current_test(
        output_path=tmp_path,
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
                "BlockSize": BLOCK_SIZE,
                "PoolConfigs": [{
                    "MinSize": NRD_DEVICE_RAW_SIZE,
                    "MaxSize": NRD_DEVICE_RAW_SIZE
                }]}, {
                "PathRegExp": f"{data_path}/NVMECOMPUTE([0-9]+)",
                "BlockSize": BLOCK_SIZE,
                "PoolConfigs": [{
                    "PoolName": LOCAL_POOL_NAME,
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
            os.truncate(f.fileno(), size)

    for i in range(NRD_DEVICE_COUNT):
        create_file(f"NVMENBS{i + 1:02}", NRD_DEVICE_RAW_SIZE)

    for i in range(LOCAL_DEVICE_COUNT):
        create_file(f"NVMECOMPUTE{i + 1:02}", LOCAL_DEVICE_RAW_SIZE)


def _wait_for(predicate, description, timeout_sec=60, poll_interval=1):
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(poll_interval)

    assert False, f"timed out waiting for {description}"


def _create_nrd_volume(client):
    client.create_volume(
        disk_id=NRD_DISK_ID,
        block_size=BLOCK_SIZE,
        blocks_count=NRD_DEVICE_SIZE // BLOCK_SIZE,
        storage_media_kind=STORAGE_MEDIA_SSD_NONREPLICATED)


def _create_local_volume(client, agent_id):
    client.create_volume(
        disk_id=LOCAL_DISK_ID,
        block_size=BLOCK_SIZE,
        blocks_count=LOCAL_DEVICE_COUNT * LOCAL_DEVICE_SIZE // BLOCK_SIZE,
        storage_media_kind=STORAGE_MEDIA_SSD_LOCAL,
        storage_pool_name=LOCAL_POOL_NAME,
        agent_ids=[agent_id])


def test_local_disk_allocation_is_retriable(
        ydb,
        agent_id,
        data_path,
        disk_agent_config):

    nbs_config = NbsConfigurator(ydb)
    nbs_config.generate_default_nbs_configs()

    nbs_config.files["storage"].NonReplicatedDontSuspendDevices = True
    nbs_config.files["storage"].AllocationUnitNonReplicatedSSD = \
        NRD_DEVICE_SIZE // 1024**3
    nbs_config.files["storage"].LocalDiskAsyncDeallocationEnabled = True
    nbs_config.files["storage"].CoolDownTimeoutBeforeSecureErase = \
        SECURE_ERASE_COOL_DOWN_MS
    nbs_config.files["storage"].BrokenDiskDestructionDelay = \
        BROKEN_DISK_DESTRUCTION_DELAY_MS

    nbs = start_nbs(nbs_config)

    client = CreateTestClient(f"localhost:{nbs.port}")
    client.execute_DiskRegistrySetWritableState(State=True)
    client.update_disk_registry_config(KNOWN_DEVICE_POOLS)

    disk_agent = start_disk_agent(disk_agent_config)
    assert disk_agent.wait_for_registration()

    client.add_host(agent_id)

    # Every device is dirty and its secure erase won't happen while this test
    # is running, so nothing can be allocated - neither now nor after a retry
    # that DR is willing to wait for.
    client.wait_for_devices_to_be_cleared(
        expected_dirty_count=NRD_DEVICE_COUNT + LOCAL_DEVICE_COUNT)

    # A nonreplicated disk gets no second chance: DR reports the allocation
    # failure as is and marks the volume as broken.
    with pytest.raises(ClientError) as e:
        _create_nrd_volume(client)

    assert e.value.code == EResult.E_DISK_ALLOCATION_FAILED.value, \
        str(e.value)

    # ... and the broken volume is destroyed shortly after.
    _wait_for(
        lambda: NRD_DISK_ID not in client.list_volumes(),
        f"{NRD_DISK_ID} to be destroyed")

    # A local disk whose devices are merely waiting for a secure erase is a
    # different story: the allocation will succeed once the erase is done, so
    # the client is told to try again instead of being told that the disk is
    # dead. Repeating the request must not change that.
    for _ in range(2):
        with pytest.raises(ClientError) as e:
            _create_local_volume(client, agent_id)

        assert e.value.code == EResult.E_TRY_AGAIN.value, str(e.value)

        bkp = client.backup_disk_registry_state()
        assert bkp.get("BrokenDisks", []) == []

    # The volume must survive the broken disk destruction that DR would have
    # scheduled had it marked the disk as broken.
    time.sleep(3 * BROKEN_DISK_DESTRUCTION_DELAY_MS / 1000)

    bkp = client.backup_disk_registry_state()
    assert bkp.get("BrokenDisks", []) == []
    assert LOCAL_DISK_ID in client.list_volumes()

    disk_agent.kill()
    nbs.kill()
