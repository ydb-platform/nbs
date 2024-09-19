import os
import psutil
import pytest
import requests
import tempfile
import time

from cloud.blockstore.public.sdk.python.client import CreateClient, \
    ClientError
from cloud.blockstore.public.sdk.python.protos import TCmsActionRequest, \
    TAction, STORAGE_MEDIA_SSD_LOCAL, IPC_VHOST, VOLUME_ACCESS_READ_ONLY

from cloud.blockstore.tests.python.lib.client import NbsClient
from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs, \
    start_disk_agent, get_fqdn

import yatest.common as yatest_common

from contrib.ydb.tests.library.harness.kikimr_runner import \
    get_unique_path_for_current_test, \
    ensure_path_exists


from library.python.retry import retry


DEVICE_SIZE = 1024**2
KNOWN_DEVICE_POOLS = {
    "KnownDevicePools": [
        {"Name": "1Mb", "Kind": "DEVICE_POOL_KIND_LOCAL",
            "AllocationUnit": DEVICE_SIZE},
    ]}


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

    for i in range(6):
        with open(os.path.join(data_path, f"NVMELOCAL{i + 1:02}"), 'wb') as f:
            f.seek(DEVICE_SIZE-1)
            f.write(b'\0')
            f.flush()


@pytest.fixture(name='disk_agent_configurator')
def create_disk_agent_configurator(ydb, data_path):

    configurator = NbsConfigurator(ydb, 'disk-agent')
    configurator.generate_default_nbs_configs()

    disk_agent_config = generate_disk_agent_txt(
        agent_id='',
        device_erase_method='DEVICE_ERASE_METHOD_NONE',  # speed up tests
        storage_discovery_config={
            "PathConfigs": [{
                "PathRegExp": f"{data_path}/NVMELOCAL([0-9]+)",
                "PoolConfigs": [{
                    "PoolName": "1Mb",
                    "MinSize": DEVICE_SIZE,
                    "MaxSize": DEVICE_SIZE
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

    server = cfg.files["server"].ServerConfig
    server.VhostEnabled = True
    server.VhostServerPath = yatest_common.binary_path(
        "cloud/blockstore/vhost-server/blockstore-vhost-server")
    server.VhostServerTimeoutAfterParentExit = 3000  # 3 seconds
    cfg.files["storage"].NonReplicatedDontSuspendDevices = True

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

    client.update_disk_registry_config(KNOWN_DEVICE_POOLS)

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


def test_multiple_endpoints(nbs):
    client = CreateClient(f"localhost:{nbs.port}")

    @retry(max_times=10, exception=ClientError)
    def create_vol0():
        client.create_volume(
            disk_id="vol0",
            block_size=4096,
            blocks_count=2 * DEVICE_SIZE//4096,
            storage_media_kind=STORAGE_MEDIA_SSD_LOCAL,
            storage_pool_name="1Mb")

    @retry(max_times=10, exception=requests.ConnectionError)
    def wait_for_vhost_servers(nbs, expected_count):
        count = 0
        for process in psutil.process_iter():
            try:
                process_name = os.path.basename(process.exe())
                process_parent = process.parent()
            except psutil.AccessDenied:
                continue

            if process_parent is None:
                continue

            if process_name == "blockstore-vhost-server" and process_parent.pid == nbs.pid:
                count += 1
        if count != expected_count:
            raise RuntimeError(
                f"vhost count expected {expected_count}, actual {count}")

    create_vol0()

    # Start a lot of blockstore-vhost-server processes.
    SOCKET_COUNT = 15
    for i in range(0, SOCKET_COUNT):
        socket = tempfile.NamedTemporaryFile()
        client.start_endpoint_async(
            unix_socket_path=socket.name,
            disk_id="vol0",
            ipc_type=IPC_VHOST,
            access_mode=VOLUME_ACCESS_READ_ONLY,
            client_id=f"{socket.name}-id",
            seq_number=i+1
        )

    wait_for_vhost_servers(nbs, SOCKET_COUNT)

    # Wait for the counters to be updated
    time.sleep(15)

    crit = nbs.counters.find({
        'sensor': 'AppCriticalEvents/ExternalEndpointUnexpectedExit'
    })
    assert crit is not None
    assert crit['value'] == 0  # Vhost servers should not have restarted.


def test_switch_multiple_endpoints(nbs):
    client = CreateClient(f"localhost:{nbs.port}")

    @retry(max_times=10, exception=ClientError)
    def create_vol(disk_id: str):
        client.create_volume(
            disk_id=disk_id,
            block_size=4096,
            blocks_count=DEVICE_SIZE//4096,
            storage_media_kind=STORAGE_MEDIA_SSD_LOCAL,
            storage_pool_name="1Mb")

    @retry(max_times=10, exception=requests.ConnectionError)
    def wait_for_vhost_servers(nbs, expected_count):
        count = 0
        for process in psutil.process_iter():
            try:
                process_name = os.path.basename(process.exe())
                process_parent = process.parent()
            except psutil.AccessDenied:
                continue

            if process_parent is None:
                continue

            if process_name == "blockstore-vhost-server" and process_parent.pid == nbs.pid:
                count += 1
        if count != expected_count:
            raise RuntimeError(
                f"vhost count expected {expected_count}, actual {count}")

    # Create disks and start an endpoint for each one.
    DISK_COUNT = 6
    disks = []
    for i in range(DISK_COUNT):
        disks.append(f"local-{i+1}")
    sockets = []
    for disk in disks:
        create_vol(disk)
        socket = tempfile.NamedTemporaryFile()
        sockets.append(socket)
        client.start_endpoint(
            unix_socket_path=socket.name,
            disk_id=disk,
            ipc_type=IPC_VHOST,
            client_id=f"{socket.name}-id",
            seq_number=1
        )

    wait_for_vhost_servers(nbs, DISK_COUNT)

    # Switch the endpoints. This will restart all vhost servers.
    for i in range(0, DISK_COUNT):
        idx = i % DISK_COUNT
        disk = disks[idx]
        socket = sockets[idx]

        client.start_endpoint_async(
            unix_socket_path=socket.name,
            disk_id=disk,
            ipc_type=IPC_VHOST,
            client_id=f"{socket.name}-id",
            seq_number=2
        )

    wait_for_vhost_servers(nbs, len(disks))

    # Wait for the counters to be updated
    time.sleep(15)

    crit = nbs.counters.find({
        'sensor': 'AppCriticalEvents/ExternalEndpointUnexpectedExit'
    })
    assert crit is not None
    # Vhost servers should not have restarted unexpectedly.
    assert crit['value'] == 0
