import hashlib
import os
import pytest

import cloud.blockstore.public.sdk.python.protos as protos

from cloud.blockstore.public.sdk.python.client import CreateClient, \
    ClientError, Session
from cloud.blockstore.tests.python.lib.client import NbsClient
from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs, \
    start_disk_agent, get_fqdn

import yatest.common as yatest_common

from cloud.blockstore.pylibs.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, \
    ensure_path_exists

from library.python.retry import retry


DEVICE_SIZE = 1024**2
KNOWN_DEVICE_POOLS = {
    "KnownDevicePools": [
        {"Name": "1Mb", "Kind": "DEVICE_POOL_KIND_LOCAL", "AllocationUnit": DEVICE_SIZE},
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
        device_erase_method='DEVICE_ERASE_METHOD_NONE',  # speed up test
        storage_discovery_config={
            "PathConfigs": [{
                "PathRegExp": f"{data_path}/NVMENBS([0-9]+)",
                "PoolConfigs": [{
                    "PoolName": "1Mb",
                    "MinSize": DEVICE_SIZE,
                    "MaxSize": DEVICE_SIZE
                }]}]
            })

    disk_agent_config.CachedSessionsPath = os.path.join(
        get_unique_path_for_current_test(yatest_common.output_path(), ''),
        "nbs-disk-agent-sessions.txt")

    configurator.files["disk-agent"] = disk_agent_config

    return configurator


@pytest.fixture(name='nbs_volume_host')
def start_nbs_volume_host(ydb):

    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()
    cfg.files["storage"].DisableLocalService = 1

    daemon = start_nbs(cfg, 'volume-host')

    yield daemon

    daemon.kill()


def _md5(s):
    return hashlib.md5(s.encode("utf-8")).hexdigest()


@pytest.fixture(name='nbs_dr_host')
def start_nbs_dr_host(ydb, agent_id):

    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()

    cfg.files["storage"].DisableLocalService = 0
    cfg.files["storage"].NonReplicatedDontSuspendDevices = True

    daemon = start_nbs(cfg, name='dr-host')

    client = NbsClient(daemon.port)
    client.disk_registry_set_writable_state()
    client.update_disk_registry_config({
        "KnownAgents": [{
            "AgentId": agent_id,
            "KnownDevices":
                [{"DeviceUUID": _md5(f"{agent_id}-{i + 1:02}")} for i in range(6)]
            }],
        } | KNOWN_DEVICE_POOLS)

    yield daemon

    daemon.kill()


def test_session_cache(
        nbs_volume_host,
        nbs_dr_host,
        disk_agent_configurator):

    disk_agent = start_disk_agent(disk_agent_configurator)
    disk_agent.wait_for_registration()

    grpc_client = CreateClient(f"localhost:{nbs_volume_host.port}")

    @retry(max_times=10, exception=ClientError)
    def create_vol0():
        grpc_client.create_volume(
            disk_id="vol0",
            block_size=4096,
            blocks_count=6*DEVICE_SIZE//4096,
            storage_media_kind=protos.STORAGE_MEDIA_SSD_LOCAL,
            storage_pool_name="1Mb")

    create_vol0()

    session = Session(grpc_client, "vol0", "")
    session.mount_volume()
    session.write_blocks(0, [b'\1' * 4096])

    # kill DR
    nbs_dr_host.kill()

    # restart DA
    disk_agent.kill()
    disk_agent = start_disk_agent(disk_agent_configurator)

    assert not disk_agent.is_registered()

    # I/O should work
    session.write_blocks(1, [b'\1' * 4096])
    session.unmount_volume()

    assert not disk_agent.is_registered()

    disk_agent.kill()
