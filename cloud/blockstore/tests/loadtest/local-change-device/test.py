from cloud.blockstore.config.disk_pb2 import TDiskAgentConfig, DEVICE_ERASE_METHOD_NONE
from cloud.blockstore.config.server_pb2 import \
    TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import \
    TStorageServiceConfig

from cloud.blockstore.tests.python.lib.nbs_runner import \
    LocalNbs
from cloud.blockstore.tests.python.lib.test_base import \
    thread_count, wait_for_nbs_server, wait_for_disk_agent, \
    wait_for_secure_erase

from cloud.blockstore.tests.python.lib.disk_agent_runner import LocalDiskAgent

from contrib.ydb.tests.library.harness.kikimr_cluster import \
    kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import \
    KikimrConfigGenerator

from cloud.blockstore.tests.python.lib.nonreplicated_setup import \
    setup_nonreplicated, create_devices, setup_disk_registry_config, \
    enable_writable_state, make_agent_id, AgentInfo, DeviceInfo

import yatest.common as yatest_common

from subprocess import call, check_output, run

PDISK_SIZE = 32 * 1024 * 1024 * 1024
STORAGE_POOL = [
    dict(name="dynamic_storage_pool:1", kind="rot", pdisk_user_kind=0),
    dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0),
]

DEFAULT_BLOCK_SIZE = 4096
DEFAULT_DEVICE_COUNT = 3
DEFAULT_BLOCK_COUNT_PER_DEVICE = 262144


def kikimr_start():
    kikimr_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")

    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=kikimr_binary_path,
        use_in_memory_pdisks=True,
        static_pdisk_size=PDISK_SIZE,
        dynamic_pdisk_size=PDISK_SIZE,
        dynamic_pdisks=[],
        dynamic_storage_pools=STORAGE_POOL)

    kikimr_cluster = kikimr_cluster_factory(configurator=configurator)
    kikimr_cluster.start()

    return kikimr_cluster, configurator


def nbs_server_start(kikimr_cluster, configurator):
    server_app_config = TServerAppConfig()
    server_app_config.ServerConfig.CopyFrom(TServerConfig())
    server_app_config.ServerConfig.ThreadsCount = thread_count()
    server_app_config.ServerConfig.StrictContractValidation = False
    server_app_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    storage = TStorageServiceConfig()
    storage.SchemeShardDir = "/Root/nbs"
    storage.StatsUploadInterval = 10000
    storage.AllocationUnitNonReplicatedSSD = 1
    storage.AcquireNonReplicatedDevices = True
    storage.ClientRemountPeriod = 1000
    storage.NonReplicatedMigrationStartAllowed = True
    storage.NonReplicatedSecureEraseTimeout = 2000  # 2 sec
    storage.DisableLocalService = False
    storage.InactiveClientsTimeout = 60000  # 1 min
    storage.AgentRequestTimeout = 5000      # 5 sec

    nbs = LocalNbs(
        kikimr_port,
        configurator.domains_txt,
        server_app_config=server_app_config,
        contract_validation=False,
        storage_config_patches=[storage],
        tracking_enabled=False,
        enable_access_service=False,
        enable_tls=False,
        dynamic_storage_pools=STORAGE_POOL)

    nbs.start()

    wait_for_nbs_server(nbs.nbs_port)

    return nbs, server_app_config, storage


def test_change_device():
    nbs_client_binary_path = \
        yatest_common.binary_path(
            "cloud/blockstore/apps/client/blockstore-client")
    disk_agent_binary_path = \
        yatest_common.binary_path(
            "cloud/blockstore/apps/disk_agent/diskagentd")

    kikimr_cluster, configurator = \
        kikimr_start()

    nbs, server_app_config, storage = \
        nbs_server_start(kikimr_cluster, configurator)

    devices = create_devices(
        True,
        DEFAULT_DEVICE_COUNT,
        DEFAULT_BLOCK_SIZE,
        DEFAULT_BLOCK_COUNT_PER_DEVICE,
        yatest_common.ram_drive_path())

    setup_nonreplicated(
        kikimr_cluster.client,
        [devices],
        disk_agent_config_patch=TDiskAgentConfig(
            DedicatedDiskAgent=True,
            DeviceEraseMethod=DEVICE_ERASE_METHOD_NONE))

    enable_writable_state(nbs.nbs_port, nbs_client_binary_path)

    setup_disk_registry_config(
        [AgentInfo(make_agent_id(0), [DeviceInfo(d.uuid) for d in devices])],
        nbs.nbs_port,
        nbs_client_binary_path)

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    disk_agent = LocalDiskAgent(
        kikimr_port,
        configurator.domains_txt,
        server_app_config=server_app_config,
        storage_config_patches=[storage],
        enable_tls=True,
        kikimr_binary_path=yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd"),
        disk_agent_binary_path=yatest_common.binary_path(
            disk_agent_binary_path))

    disk_agent.start()
    wait_for_disk_agent(disk_agent.mon_port)
    wait_for_secure_erase(nbs.mon_port)

    result = call([
        nbs_client_binary_path, "ExecuteAction",
        "--action", "CreateDiskFromDevices",
        "--input-bytes", '{"DiskId":"vol0","BlockSize":4096,"DeviceUUIDs":'
                         '["MemoryDevice-1","MemoryDevice-2"]}',
        "--port", str(nbs.nbs_port),
    ])
    assert result == 0

    raw_device_data_size = DEFAULT_BLOCK_COUNT_PER_DEVICE * DEFAULT_BLOCK_SIZE
    raw_data_size = raw_device_data_size * 2
    write_data = bytes(b'A' * raw_data_size)

    p_result = run([
        nbs_client_binary_path, "WriteBlocks",
        "--disk-id", "vol0",
        "--host", "localhost",
        "--port", str(nbs.nbs_port),
    ], input=write_data)
    assert p_result.returncode == 0

    read_data = check_output([
        nbs_client_binary_path, "ReadBlocks",
        "--disk-id", "vol0",
        "--blocks-count", "{}".format(DEFAULT_BLOCK_COUNT_PER_DEVICE * 2),
        "--host", "localhost",
        "--port", str(nbs.nbs_port),
    ])
    assert read_data == write_data

    # after change device MemoryDevice-3 should be in Error state
    result = call([
        nbs_client_binary_path, "ExecuteAction",
        "--action", "ChangeDiskDevice",
        "--input-bytes", '{"DiskId":"vol0",'
                         '"SourceDeviceId":"MemoryDevice-1",'
                         '"TargetDeviceId":"MemoryDevice-3"}',
        "--port", str(nbs.nbs_port),
    ])
    assert result == 0

    # reading from MemoryDevice-3 is not allowed because it is in Error state
    read_md3_result = run([
        nbs_client_binary_path, "ReadBlocks",
        "--disk-id", "vol0",
        "--blocks-count", "{}".format(DEFAULT_BLOCK_COUNT_PER_DEVICE),
        "--host", "localhost",
        "--port", str(nbs.nbs_port),
    ])
    assert read_md3_result.returncode != 0

    # with reading from MemoryDevice-2 is ok
    read_data = check_output([
        nbs_client_binary_path, "ReadBlocks",
        "--disk-id", "vol0",
        "--blocks-count", "{}".format(DEFAULT_BLOCK_COUNT_PER_DEVICE),
        "--host", "localhost",
        "--port", str(nbs.nbs_port),
        "--start-index", "{}".format(DEFAULT_BLOCK_COUNT_PER_DEVICE),
    ])
    assert read_data == bytes(b'A' * raw_device_data_size)

    # after change device MemoryDevice-1 should be in Error state
    result = call([
        nbs_client_binary_path, "ExecuteAction",
        "--action", "ChangeDiskDevice",
        "--input-bytes", '{"DiskId":"vol0",'
                         '"SourceDeviceId":"MemoryDevice-3",'
                         '"TargetDeviceId":"MemoryDevice-1"}',
        "--port", str(nbs.nbs_port),
    ])
    assert result == 0

    # reading from MemoryDevice-1 is not allowed because it is in Error state
    read_md1_result = run([
        nbs_client_binary_path, "ReadBlocks",
        "--disk-id", "vol0",
        "--blocks-count", "{}".format(DEFAULT_BLOCK_COUNT_PER_DEVICE),
        "--host", "localhost",
        "--port", str(nbs.nbs_port),
    ])
    assert read_md1_result.returncode != 0

    # with reading from MemoryDevice-2 is ok
    read_data = check_output([
        nbs_client_binary_path, "ReadBlocks",
        "--disk-id", "vol0",
        "--blocks-count", "{}".format(DEFAULT_BLOCK_COUNT_PER_DEVICE),
        "--host", "localhost",
        "--port", str(nbs.nbs_port),
        "--start-index", "{}".format(DEFAULT_BLOCK_COUNT_PER_DEVICE),
    ])
    assert read_data == bytes(b'A' * raw_device_data_size)
