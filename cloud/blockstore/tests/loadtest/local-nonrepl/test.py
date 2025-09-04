import logging
import os
import pytest

from cloud.blockstore.config.client_pb2 import TClientConfig
from cloud.blockstore.config.disk_pb2 import TDiskAgentConfig
from cloud.blockstore.config.server_pb2 import TServerConfig, TServerAppConfig, \
    TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig

from cloud.blockstore.config.diagnostics_pb2 import TDiagnosticsConfig
from cloud.blockstore.config.disk_pb2 import DEVICE_ERASE_METHOD_NONE,  \
    DISK_AGENT_BACKEND_AIO, \
    DISK_AGENT_BACKEND_IO_URING

from cloud.blockstore.tests.python.lib.disk_agent_runner import LocalDiskAgent
from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import thread_count, \
    wait_for_nbs_server, wait_for_disk_agent, run_test, wait_for_secure_erase, \
    get_restart_interval
from cloud.blockstore.tests.python.lib.nonreplicated_setup import setup_nonreplicated, \
    create_devices, setup_disk_registry_config, \
    update_cms_config, enable_writable_state, make_agent_id, AgentInfo, DeviceInfo

from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from contrib.ydb.tests.library.harness.kikimr_runner import ensure_path_exists, \
    get_unique_path_for_current_test

import yatest.common as yatest_common

DEFAULT_BLOCK_SIZE = 4096
DEFAULT_DEVICE_COUNT = 8
DEFAULT_BLOCK_COUNT_PER_DEVICE = 262186  # 262144 + 42


def enable_lwtrace(kikimr, query_file):
    config = TDiagnosticsConfig()
    config.UnsafeLWTrace = True
    config.LWTraceDebugInitializationQuery = query_file

    update_cms_config(kikimr, 'DiagnosticsConfig', config, 'disk-agent')


class _TestCase(object):

    def __init__(
            self,
            name,
            config_path,
            restart_interval=None,
            lwtrace_query_path=None,
            use_memory_devices=False,
            device_erase_method=None,
            device_count=DEFAULT_DEVICE_COUNT,
            block_count_per_device=DEFAULT_BLOCK_COUNT_PER_DEVICE,
            allocation_unit_size=1,
            agent_count=1,
            storage_pool_name=None,
            dump_block_digests=False,
            reject_late_requests_at_disk_agent=False,
            encryption_at_rest=False):
        self.name = name
        self.config_path = config_path
        self.restart_interval = restart_interval
        self.lwtrace_query_path = lwtrace_query_path
        self.use_memory_devices = use_memory_devices
        self.device_erase_method = device_erase_method
        self.device_count = device_count
        self.block_count_per_device = block_count_per_device
        self.allocation_unit_size = allocation_unit_size
        self.agent_count = agent_count
        self.storage_pool_name = storage_pool_name
        self.dump_block_digests = dump_block_digests
        self.reject_late_requests_at_disk_agent = reject_late_requests_at_disk_agent
        self.encryption_at_rest = encryption_at_rest


TESTS = [
    _TestCase(
        "load",
        "cloud/blockstore/tests/loadtest/local-nonrepl/local-smallreqs.txt",
    ),
    _TestCase(
        "restarts",
        "cloud/blockstore/tests/loadtest/local-nonrepl/local-nonrepl.txt",
        restart_interval=get_restart_interval(),
    ),
    _TestCase(
        "late_requests",
        "cloud/blockstore/tests/loadtest/local-nonrepl/local-nonrepl.txt",
        reject_late_requests_at_disk_agent=True,
    ),
    _TestCase(
        "io-errors",
        "cloud/blockstore/tests/loadtest/local-nonrepl/local-nonrepl.txt",
        lwtrace_query_path="cloud/blockstore/tests/loadtest/local-nonrepl/io-errors.tr",
    ),
    _TestCase(
        "large-disk",
        "cloud/blockstore/tests/loadtest/local-nonrepl/local-large-disk.txt",
        use_memory_devices=True,
        device_erase_method=DEVICE_ERASE_METHOD_NONE,
        device_count=256,
        block_count_per_device=256 * 1024 * 1024,  # 1TiB
        allocation_unit_size=1024,  # 1TiB
    ),
    _TestCase(
        "migration",
        "cloud/blockstore/tests/loadtest/local-nonrepl/local-migration.txt",
        # agent_count=2,
        dump_block_digests=True,
    ),
    _TestCase(
        "migration-nbd",
        "cloud/blockstore/tests/loadtest/local-nonrepl/local-migration-nbd.txt",
        # agent_count=2,
        dump_block_digests=True,
    ),
    _TestCase(
        "local-encryption",
        "cloud/blockstore/tests/loadtest/local-nonrepl/local-encryption.txt",
    ),
    _TestCase(
        "remote-encryption",
        "cloud/blockstore/tests/loadtest/local-nonrepl/remote-encryption.txt",
    ),
    _TestCase(
        "local-encryption-overlay",
        "cloud/blockstore/tests/loadtest/local-nonrepl/local-encryption-overlay.txt",
    ),
    _TestCase(
        "local-nonrepl-overlay",
        "cloud/blockstore/tests/loadtest/local-nonrepl/local-nonrepl-overlay.txt",
    ),
    _TestCase(
        "load-hdd",
        "cloud/blockstore/tests/loadtest/local-nonrepl/local-hdd.txt",
        storage_pool_name="rot",
    ),
    _TestCase(
        "load-encryption-at-rest",
        "cloud/blockstore/tests/loadtest/local-nonrepl/local-smallreqs.txt",
        encryption_at_rest=True,
    )
]


def cleanup_file_devices(devices):
    logging.info("Remove temporary device files")
    for d in devices:
        if d.path is not None:
            logging.info("unlink %s (%s)" % (d.uuid, d.path))
            assert d.handle is not None
            d.handle.close()
            os.unlink(d.path)


def __prepare_test_config(test_case):
    with open(test_case.config_path, 'r') as file:
        filedata = file.read()

    if '$' not in filedata:
        return test_case.config_path

    key_file_path = yatest_common.source_path(
        'cloud/blockstore/tests/loadtest/local-nonrepl/encryption-key.txt')
    filedata = filedata.replace('$KEY_FILE_PATH', key_file_path)

    config_folder = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder="test_configs")
    ensure_path_exists(config_folder)
    prepared_config_path = config_folder + "/" + test_case.name + ".txt"
    with open(prepared_config_path, 'w') as file:
        file.write(filedata)
    return prepared_config_path


def __run_test(test_case, backend, use_rdma):
    kikimr_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")

    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=kikimr_binary_path,
        use_in_memory_pdisks=True,
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1", kind="hdd", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0)
        ])

    dedicated_disk_agent = (os.getenv("DEDICATED_DISK_AGENT", "false") == "true")

    nbs_binary_path = "cloud/blockstore/apps/server/nbsd"
    disk_agent_binary_path = "cloud/blockstore/apps/disk_agent/diskagentd"

    kikimr_cluster = kikimr_cluster_factory(configurator=configurator)
    kikimr_cluster.start()

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    devices = create_devices(
        test_case.use_memory_devices,
        test_case.device_count,
        DEFAULT_BLOCK_SIZE,
        test_case.block_count_per_device,
        yatest_common.ram_drive_path())

    try:
        for device in devices:
            device.storage_pool_name = test_case.storage_pool_name

        # TODO: allocate devices for all agents to support agent_count > 1
        setup_nonreplicated(
            kikimr_cluster.client,
            [devices],
            disk_agent_config_patch=TDiskAgentConfig(
                DeviceEraseMethod=test_case.device_erase_method,
                DedicatedDiskAgent=dedicated_disk_agent,
                Backend=backend,
            ))

        if test_case.lwtrace_query_path:
            enable_lwtrace(kikimr_cluster.client, test_case.lwtrace_query_path)

        nbd_socket_suffix = "_nbd"

        server_app_config = TServerAppConfig()
        server_app_config.ServerConfig.CopyFrom(TServerConfig())
        server_app_config.ServerConfig.ThreadsCount = thread_count()
        server_app_config.ServerConfig.StrictContractValidation = False
        server_app_config.ServerConfig.NbdEnabled = True
        server_app_config.ServerConfig.NbdSocketSuffix = nbd_socket_suffix
        server_app_config.ServerConfig.UseFakeRdmaClient = use_rdma
        server_app_config.ServerConfig.RdmaClientEnabled = use_rdma
        server_app_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

        certs_dir = yatest_common.source_path('cloud/blockstore/tests/certs')

        server_app_config.ServerConfig.RootCertsFile = os.path.join(certs_dir, 'server.crt')
        cert = server_app_config.ServerConfig.Certs.add()
        cert.CertFile = os.path.join(certs_dir, 'server.crt')
        cert.CertPrivateKeyFile = os.path.join(certs_dir, 'server.key')

        storage = TStorageServiceConfig()
        storage.AllocationUnitNonReplicatedSSD = test_case.allocation_unit_size
        storage.AllocationUnitNonReplicatedHDD = test_case.allocation_unit_size
        storage.AcquireNonReplicatedDevices = True
        storage.ClientRemountPeriod = 1000
        storage.NonReplicatedMigrationStartAllowed = True
        storage.NonReplicatedSecureEraseTimeout = 2000  # 2 sec
        storage.DisableLocalService = False
        storage.InactiveClientsTimeout = 60000  # 1 min
        storage.AgentRequestTimeout = 5000      # 5 sec
        storage.RejectLateRequestsAtDiskAgentEnabled = test_case.reject_late_requests_at_disk_agent
        storage.AssignIdToWriteAndZeroRequestsEnabled = test_case.reject_late_requests_at_disk_agent
        storage.NodeType = 'main'
        storage.UseNonreplicatedRdmaActor = use_rdma
        storage.UseRdma = use_rdma
        storage.EncryptionAtRestForDiskRegistryBasedDisksEnabled = test_case.encryption_at_rest

        if test_case.dump_block_digests:
            storage.BlockDigestsEnabled = True
            storage.UseTestBlockDigestGenerator = True

        nbs = LocalNbs(
            kikimr_port,
            configurator.domains_txt,
            server_app_config=server_app_config,
            storage_config_patches=[storage],
            enable_tls=True,
            kikimr_binary_path=kikimr_binary_path,
            nbs_binary_path=yatest_common.binary_path(nbs_binary_path))

        nbs.start()
        wait_for_nbs_server(nbs.nbs_port)

        nbs_client_binary_path = yatest_common.binary_path("cloud/blockstore/apps/client/blockstore-client")
        # TODO: support agent_count > 1
        enable_writable_state(nbs.nbs_port, nbs_client_binary_path)
        setup_disk_registry_config(
            [AgentInfo(make_agent_id(0), [DeviceInfo(d.uuid) for d in devices])],
            nbs.nbs_port,
            nbs_client_binary_path,
        )

        # node with DiskAgent

        storage.DisableLocalService = True

        disk_agents = []
        for i in range(test_case.agent_count):
            disk_agent = None
            if dedicated_disk_agent:
                disk_agent = LocalDiskAgent(
                    kikimr_port,
                    configurator.domains_txt,
                    server_app_config=server_app_config,
                    storage_config_patches=[storage],
                    config_sub_folder="disk_agent_configs_%s" % i,
                    log_sub_folder="disk_agent_logs_%s" % i,
                    enable_tls=True,
                    kikimr_binary_path=kikimr_binary_path,
                    disk_agent_binary_path=yatest_common.binary_path(disk_agent_binary_path),
                    restart_interval=test_case.restart_interval,
                    suspend_restarts=True)

                disk_agent.start()
                wait_for_disk_agent(disk_agent.mon_port)
            else:
                storage.NodeType = "disk-agent"
                disk_agent = LocalNbs(
                    kikimr_port,
                    configurator.domains_txt,
                    server_app_config=server_app_config,
                    storage_config_patches=[storage],
                    enable_tls=True,
                    kikimr_binary_path=kikimr_binary_path,
                    nbs_binary_path=yatest_common.binary_path(nbs_binary_path),
                    restart_interval=test_case.restart_interval,
                    suspend_restarts=True,
                    ping_path="/blockstore/disk_agent")

                disk_agent.start()
                wait_for_nbs_server(disk_agent.nbs_port)

            disk_agents.append(disk_agent)

        wait_for_secure_erase(
            nbs.mon_port,
            test_case.storage_pool_name or "default")

        if test_case.restart_interval:
            for agent in disk_agents:
                agent.allow_restart()

        config_path = __prepare_test_config(test_case)

        client = TClientConfig()
        client.NbdSocketSuffix = nbd_socket_suffix

        try:
            ret = run_test(
                test_case.name,
                config_path,
                nbs.nbs_port,
                nbs.mon_port,
                nbs_log_path=nbs.stderr_file_name,
                client_config=client,
                env_processes=disk_agents + [nbs])
        finally:
            for disk_agent in disk_agents:
                disk_agent.stop()

            nbs.stop()
            kikimr_cluster.stop()
    finally:
        if not test_case.use_memory_devices:
            cleanup_file_devices(devices)

    return ret


BACKENDS = {
    'aio': DISK_AGENT_BACKEND_AIO,
    'io_uring': DISK_AGENT_BACKEND_IO_URING,
}


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
@pytest.mark.parametrize("backend", BACKENDS.values(), ids=BACKENDS.keys())
def test_load(test_case, backend):
    test_case.config_path = yatest_common.source_path(test_case.config_path)
    if test_case.lwtrace_query_path:
        test_case.lwtrace_query_path = yatest_common.source_path(test_case.lwtrace_query_path)

    return __run_test(test_case, backend, use_rdma=False)


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_load_rdma(test_case):
    test_case.config_path = yatest_common.source_path(test_case.config_path)
    if test_case.lwtrace_query_path:
        test_case.lwtrace_query_path = yatest_common.source_path(test_case.lwtrace_query_path)

    return __run_test(test_case, backend=DISK_AGENT_BACKEND_AIO, use_rdma=True)
