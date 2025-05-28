import os
import pytest
import random
import requests
import stat
import string
import subprocess
import tempfile
import time

from cloud.blockstore.public.sdk.python.client import CreateClient, \
    ClientError
from cloud.blockstore.public.sdk.python.protos import TCmsActionRequest, \
    TAction, STORAGE_MEDIA_SSD_LOCAL, IPC_VHOST

from cloud.blockstore.tests.python.lib.client import NbsClient
from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs, \
    start_disk_agent, get_fqdn

import yatest.common as yatest_common

from contrib.ydb.tests.library.common.yatest_common import PortManager
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, \
    ensure_path_exists

from library.python.retry import retry


DEVICE_SIZE = 1024**2
KNOWN_DEVICE_POOLS = {
    "KnownDevicePools": [
        {"Name": "1Mb", "Kind": "DEVICE_POOL_KIND_LOCAL", "AllocationUnit": DEVICE_SIZE},
    ]}

FAKE_VHOST_SERVER = yatest_common.binary_path(
    "cloud/blockstore/tools/testing/fake-vhost-server/fake-vhost-server")


def run_ownerless_vhost_server(disk_id):
    out_r_fd, out_w_fd = os.pipe()

    with os.fdopen(out_r_fd, 'r') as out_r, \
            os.fdopen(out_w_fd, 'w') as out_w:
        proc = subprocess.Popen(
            [
                FAKE_VHOST_SERVER,
                "--disk-id", disk_id,
                "--port", str(PortManager().get_port()),
                "-s", "/tmp/dummy",
                "-i", "dummy",
                "--device", "/dev/vda:1000000:0"
            ],
            stdout=out_w,
            stderr=out_w,
            bufsize=0,
            universal_newlines=True)

        # wait for server start
        while True:
            line = out_r.readline().strip()
            if line.find("start...") != -1:
                break

    return proc


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
        with open(os.path.join(data_path, f"NVMENBS{i + 1:02}"), 'wb') as f:
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
                "PathRegExp": f"{data_path}/NVMENBS([0-9]+)",
                "BlockSize": 512,
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


@pytest.fixture(name='fake_vhost_server')
def setup_fake_vhost_server_script():

    class Script:

        def __init__(self, port, path):
            self.port = port
            self.path = path

    port = PortManager().get_port()

    with tempfile.NamedTemporaryFile(mode='w') as script:
        script.write("\n".join([
            "#!/bin/bash",
            f"exec {FAKE_VHOST_SERVER} --port {port} $@"
        ]))

        os.chmod(script.name, os.stat(script.name).st_mode | stat.S_IXUSR)

        script.file.close()

        yield Script(port, script.name)


def __generate_test_disk_id(prefix):
    return prefix + "-" + ''.join(random.choices(string.ascii_letters, k=6))


@pytest.fixture(name='vol0_id')
def generate_vol0_id():
    return __generate_test_disk_id("vol0")


@pytest.fixture(name='vol1_id')
def generate_vol1_id():
    return __generate_test_disk_id("vol1")


@pytest.fixture(name='nbs')
def start_nbs_daemon(vol0_id, vol1_id, ydb, fake_vhost_server):
    # run "old" external vhost-servers that serve the disks "vol0" and "vol1"
    old_vhost_server_vol0 = run_ownerless_vhost_server(vol0_id)
    old_vhost_server_vol1 = run_ownerless_vhost_server(vol1_id)

    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()

    server = cfg.files["server"].ServerConfig
    server.VhostEnabled = True
    server.VhostServerPath = fake_vhost_server.path

    cfg.files["storage"].NonReplicatedDontSuspendDevices = True

    daemon = start_nbs(cfg)
    daemon.old_vhost_server_vol0 = old_vhost_server_vol0
    daemon.old_vhost_server_vol1 = old_vhost_server_vol1

    yield daemon

    daemon.kill()


@pytest.fixture(name='disk_agent')
def start_disk_agent_daemon(ydb, disk_agent_configurator):

    daemon = start_disk_agent(disk_agent_configurator)

    yield daemon

    daemon.kill()


@pytest.fixture(autouse=True)
def setup_env(nbs, vol0_id, vol1_id, disk_agent, data_path):

    client = CreateClient(f"localhost:{nbs.port}")
    client.execute_DiskRegistrySetWritableState(State=True)

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


def test_external_endpoint(nbs, vol0_id, fake_vhost_server):

    client = CreateClient(f"localhost:{nbs.port}")

    @retry(max_times=10, exception=ClientError)
    def create_vol0():
        client.create_volume(
            disk_id=vol0_id,
            block_size=512,
            blocks_count=2 * DEVICE_SIZE//512,
            storage_media_kind=STORAGE_MEDIA_SSD_LOCAL,
            storage_pool_name="1Mb")

    vhost_server_url = f"http://localhost:{fake_vhost_server.port}"

    @retry(max_times=10, exception=requests.ConnectionError)
    def wait_for_vhost_server():
        r = requests.get(f"{vhost_server_url}/ping").text
        assert r == "pong"

    create_vol0()

    with tempfile.NamedTemporaryFile() as unix_socket:
        r = client.start_endpoint(
            unix_socket_path=unix_socket.name,
            disk_id=vol0_id,
            ipc_type=IPC_VHOST,
            client_id="test"
        )

        assert r["Volume"].DiskId == vol0_id

        wait_for_vhost_server()

        # expect the old server servicing the "vol0" will terminate when the new one starts.
        nbs.old_vhost_server_vol0.communicate(timeout=1)
        assert nbs.old_vhost_server_vol0.returncode == -9

        # expect old vhost-server for "vol1" still running.
        try:
            nbs.old_vhost_server_vol1.communicate(timeout=1)
            assert False
        except subprocess.TimeoutExpired:
            nbs.old_vhost_server_vol1.kill()

        r = requests.get(f"{vhost_server_url}/describe").json()
        devices = r["devices"]
        assert len(devices) == 2

        for d in devices:
            assert d[0].find("NVMENBS") != -1
            assert d[1] == DEVICE_SIZE
            assert d[2] == 0

        crit = nbs.counters.find({
            'sensor': 'AppCriticalEvents/ExternalEndpointUnexpectedExit'
        })
        assert crit is not None
        assert crit['value'] == 0

        try:
            _ = requests.post(f"{vhost_server_url}/terminate", data='{"exit_code": 42}')
        except requests.ConnectionError:
            pass

        # wait for the counters to be updated
        time.sleep(15)

        crit = nbs.counters.find({
            'sensor': 'AppCriticalEvents/ExternalEndpointUnexpectedExit'
        })
        assert crit is not None
        assert crit['value'] == 1

        wait_for_vhost_server()

        client.stop_endpoint(unix_socket.name)

    # wait for the counters to be updated
    time.sleep(15)

    crit = nbs.counters.find({
        'sensor': 'AppCriticalEvents/ExternalEndpointUnexpectedExit'
    })
    assert crit is not None
    assert crit['value'] == 1   # the same value
