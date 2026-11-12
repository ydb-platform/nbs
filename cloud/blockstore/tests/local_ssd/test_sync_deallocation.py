import os
import pytest
import time

from cloud.blockstore.tests.python.lib.test_client import CreateTestClient

from cloud.blockstore.public.sdk.python.client.error_codes import EResult
from cloud.blockstore.public.sdk.python.protos import TCmsActionRequest, \
    TAction, STORAGE_MEDIA_SSD_LOCAL

from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs, \
    start_disk_agent, get_fqdn

from contrib.ydb.tests.library.harness.kikimr_runner import \
    get_unique_path_for_current_test, ensure_path_exists


BLOCK_SIZE = 512

LOCAL_POOL_NAME = "local-ssd"
LOCAL_DEVICE_SIZE = 9 * 1024**2             # 9 MiB
LOCAL_DEVICE_RAW_SIZE = LOCAL_DEVICE_SIZE + 4096
LOCAL_DEVICE_COUNT = 2

KNOWN_DEVICE_POOLS = {
    "KnownDevicePools": [
        {"Name": LOCAL_POOL_NAME, "Kind": "DEVICE_POOL_KIND_LOCAL",
            "AllocationUnit": LOCAL_DEVICE_SIZE},
    ]}

DISK_ID = "vol0"

# A sync deallocation is answered once the devices of the disk are secure
# erased. Without the erase it must be answered as soon as there is nothing to
# wait for anymore; this is much shorter than the time the client would keep
# retrying it.
DEALLOCATION_TIMEOUT_SEC = 60


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

    for i in range(LOCAL_DEVICE_COUNT):
        path = os.path.join(data_path, f"NVMECOMPUTE{i + 1:02}")
        with open(path, 'wb') as f:
            os.truncate(f.fileno(), LOCAL_DEVICE_RAW_SIZE)


@pytest.fixture(name='env')
def start_env(ydb, agent_id, disk_agent_config):

    nbs_config = NbsConfigurator(ydb)
    nbs_config.generate_default_nbs_configs()
    nbs_config.files["storage"].NonReplicatedDontSuspendDevices = True

    nbs = start_nbs(nbs_config)

    client = CreateTestClient(f"localhost:{nbs.port}")
    client.execute_DiskRegistrySetWritableState(State=True)
    client.update_disk_registry_config(KNOWN_DEVICE_POOLS)

    disk_agent = start_disk_agent(disk_agent_config, name='disk-agent.1')
    assert disk_agent.wait_for_registration()

    client.add_host(agent_id)
    client.wait_for_devices_to_be_cleared()

    env = _Env(nbs, disk_agent, client)

    yield env

    env.disk_agent.kill()
    nbs.kill()


class _Env:

    def __init__(self, nbs, disk_agent, client):
        self.nbs = nbs
        self.disk_agent = disk_agent
        self.client = client


def _wait_for(predicate, description, timeout_sec=60, poll_interval=1):
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(poll_interval)

    assert False, f"timed out waiting for {description}"


def _find_dirty_device(client, device_id):
    bkp = client.backup_disk_registry_state()
    for device in bkp.get("DirtyDevices", []):
        if device["Id"] == device_id:
            return device

    return None


def _find_device(client, device_id):
    bkp = client.backup_disk_registry_state()
    for agent in bkp.get("Agents", []):
        for device in agent.get("Devices", []):
            if device["DeviceUUID"] == device_id:
                return device

    return None


def _cms_action(client, action_type, host, device=None):
    request = TCmsActionRequest()
    action = request.Actions.add()
    action.Type = action_type
    action.Host = host
    if device is not None:
        action.Device = device

    action_results = client.cms_action(request).ActionResults
    assert len(action_results) == 1

    return action_results[0]


def _start_stuck_sync_deallocation(client):
    """
    Creates a local disk and starts its sync destruction while the secure
    erase is suspended, so the destruction waits for the disk's device to
    leave the pending cleanup. Returns the device and the destruction future.
    """

    client.create_volume(
        disk_id=DISK_ID,
        block_size=BLOCK_SIZE,
        blocks_count=LOCAL_DEVICE_SIZE // BLOCK_SIZE,
        storage_media_kind=STORAGE_MEDIA_SSD_LOCAL,
        storage_pool_name=LOCAL_POOL_NAME)

    devices = client.stat_volume(DISK_ID)["Volume"].Devices
    assert len(devices) == 1
    device = devices[0]

    client.execute_DiskRegistrySuspendSecureErase(Suspend=True)

    future = client.destroy_volume_async(DISK_ID, sync=True)

    # The device waits for the secure erase on behalf of the deallocated disk.
    def is_waiting_for_secure_erase():
        dirty_device = _find_dirty_device(client, device.DeviceUUID)
        return dirty_device is not None and \
            dirty_device.get("DiskId") == DISK_ID

    _wait_for(
        is_waiting_for_secure_erase,
        f"{device.DeviceUUID} to wait for the secure erase")

    time.sleep(2)
    assert not future.done()

    return device, future


def _check_deallocation_completed(client, device_id, future):
    # Raises if the destruction fails or doesn't complete in time.
    future.result(timeout=DEALLOCATION_TIMEOUT_SEC)

    assert DISK_ID not in client.list_volumes()

    # The device isn't tied to the destroyed disk anymore.
    dirty_device = _find_dirty_device(client, device_id)
    assert dirty_device is None or dirty_device.get("DiskId", "") == ""


def test_should_complete_sync_deallocation_when_device_is_lost(
        env,
        data_path,
        disk_agent_config):

    client = env.client

    device, future = _start_stuck_sync_deallocation(client)

    # The device disappears while the agent is down. The agent reports it as
    # lost on registration and DR puts the device into the error state, so it
    # will never be secure erased.
    env.disk_agent.kill()
    os.remove(device.DeviceName)

    env.disk_agent = start_disk_agent(disk_agent_config, name='disk-agent.2')
    assert env.disk_agent.wait_for_registration()

    _check_deallocation_completed(client, device.DeviceUUID, future)

    assert _find_device(client, device.DeviceUUID)["State"] == \
        "DEVICE_STATE_ERROR"


def test_should_complete_sync_deallocation_when_host_is_purged(
        env,
        agent_id):

    client = env.client

    device, future = _start_stuck_sync_deallocation(client)

    # The local device waits for the secure erase, so the host can't be
    # removed yet.
    result = _cms_action(client, TAction.REMOVE_HOST, agent_id)
    assert result.Result.Code == EResult.E_TRY_AGAIN.value, \
        result.Result.Message

    # Infra gives up and purges the host. DR forgets its devices, so there is
    # nothing to wait for anymore.
    result = _cms_action(client, TAction.PURGE_HOST, agent_id)
    assert result.Result.Code == EResult.S_OK.value, result.Result.Message
    assert list(result.DependentDisks) == []

    _check_deallocation_completed(client, device.DeviceUUID, future)

    # The device is forgotten.
    assert _find_dirty_device(client, device.DeviceUUID) is None


def test_should_complete_sync_deallocation_when_device_is_purged(
        env,
        agent_id):

    client = env.client

    device, future = _start_stuck_sync_deallocation(client)

    result = _cms_action(
        client,
        TAction.PURGE_DEVICE,
        agent_id,
        device.DeviceName)
    assert result.Result.Code == EResult.S_OK.value, result.Result.Message
    assert list(result.DependentDisks) == []

    _check_deallocation_completed(client, device.DeviceUUID, future)

    # The device is forgotten.
    assert _find_dirty_device(client, device.DeviceUUID) is None
