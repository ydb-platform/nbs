import logging
import os
import pytest

from cloud.blockstore.config.client_pb2 import TClientAppConfig, TClientConfig
from cloud.blockstore.config.disk_pb2 import TDiskAgentConfig
from cloud.blockstore.config.server_pb2 import TServerConfig, TServerAppConfig, \
    TKikimrServiceConfig, TChecksumFlags
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig

from cloud.blockstore.tests.python.lib.disk_agent_runner import LocalDiskAgent
from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import thread_count, \
    wait_for_nbs_server, wait_for_disk_agent, run_test, wait_for_secure_erase, \
    get_nbs_counters, get_sensor
from cloud.blockstore.tests.python.lib.nonreplicated_setup import setup_nonreplicated, \
    create_devices, setup_disk_registry_config, \
    enable_writable_state, make_agent_node_type, make_agent_id, AgentInfo, DeviceInfo

from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists

import yatest.common as yatest_common

DEFAULT_BLOCK_SIZE = 4096
DEFAULT_DEVICE_COUNT = 3
DEFAULT_BLOCK_COUNT_PER_DEVICE = 262144


class TestCase(object):
    __test__ = False

    def __init__(
            self,
            name,
            config_path,
            agent_count=1,
            use_rdma=False):
        self.name = name
        self.config_path = config_path
        self.agent_count = agent_count
        self.use_rdma = use_rdma


TESTS = [
    TestCase(
        "mirror2",
        "cloud/blockstore/tests/loadtest/local-data-integrity/local-mirror2.txt",
        agent_count=2,
    ),
    TestCase(
        "mirror2-rdma",
        "cloud/blockstore/tests/loadtest/local-data-integrity/local-mirror2.txt",
        agent_count=2,
        use_rdma=True,
    ),
    TestCase(
        "ssd",
        "cloud/blockstore/tests/loadtest/local-data-integrity/local-ssd.txt",
        agent_count=0,
    ),
]


def __remove_file_devices(devices):
    logging.info("Remove temporary device files")
    for d in devices:
        if d.path is not None:
            logging.info("unlink %s (%s)" % (d.uuid, d.path))
            assert d.handle is not None
            d.handle.close()
            os.unlink(d.path)


def __process_config(config_path, devices_per_agent):
    with open(config_path) as c:
        config_data = c.read()
    device_tag = "\"$DEVICE:"
    prev_index = 0
    new_config_data = ""
    has_replacements = False
    while True:
        next_device_tag_index = config_data.find(device_tag, prev_index)
        if next_device_tag_index == -1:
            new_config_data += config_data[prev_index:]
            break
        new_config_data += config_data[prev_index:next_device_tag_index]
        prev_index = next_device_tag_index + len(device_tag)
        next_index = config_data.find("\"", prev_index)
        assert next_index != -1
        agent_id, device_id = config_data[prev_index:next_index].split("/")
        new_config_data += "\"%s\"" % devices_per_agent[int(agent_id)][int(device_id)].path
        has_replacements = True

        prev_index = next_index + 1

    if has_replacements:
        config_folder = get_unique_path_for_current_test(
            output_path=yatest_common.output_path(),
            sub_folder="test_configs")
        ensure_path_exists(config_folder)
        config_path = os.path.join(
            config_folder,
            os.path.basename(config_path) + ".patched")
        with open(config_path, "w") as new_c:
            new_c.write(new_config_data)

    return config_path


def __get_sensors(mon_port):
    sensors = get_nbs_counters(mon_port)['sensors']
    write_requests = get_sensor(sensors, default_value=0, component='service',
                                subcomponent='data_integrity', sensor="WriteRequests")
    read_requests = get_sensor(sensors, default_value=0, component='service',
                               subcomponent='data_integrity', sensor="ReadRequests")
    read_checksum_mismatch = get_sensor(
        sensors, default_value=0, component='service', subcomponent='data_integrity', sensor="ReadChecksumMismatch")
    write_checksum_mismatch = get_sensor(
        sensors, default_value=0, component='service', subcomponent='data_integrity', sensor="WriteChecksumMismatch")
    return write_requests, read_requests, read_checksum_mismatch, write_checksum_mismatch


def __run_test(test_case):
    kikimr_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")

    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=kikimr_binary_path,
        use_in_memory_pdisks=True,
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1", kind="hdd", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0)
        ])

    nbs_binary_path = "cloud/blockstore/apps/server/nbsd"
    disk_agent_binary_path = "cloud/blockstore/apps/disk_agent/diskagentd"

    kikimr_cluster = kikimr_cluster_factory(configurator=configurator)
    kikimr_cluster.start()

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    devices_per_agent = []
    agent_infos = []

    if test_case.agent_count > 0:
        devices = create_devices(
            False,  # use_memory_devices
            DEFAULT_DEVICE_COUNT * test_case.agent_count,
            DEFAULT_BLOCK_SIZE,
            DEFAULT_BLOCK_COUNT_PER_DEVICE,
            yatest_common.ram_drive_path())
        device_idx = 0
        for i in range(test_case.agent_count):
            device_infos = []
            agent_devices = []
            for _ in range(DEFAULT_DEVICE_COUNT):
                device_infos.append(DeviceInfo(devices[device_idx].uuid))
                agent_devices.append(devices[device_idx])
                device_idx += 1
            agent_infos.append(AgentInfo(make_agent_id(i), device_infos))
            devices_per_agent.append(agent_devices)

    try:
        if test_case.agent_count > 0:
            setup_nonreplicated(
                kikimr_cluster.client,
                devices_per_agent,
                disk_agent_config_patch=TDiskAgentConfig(
                    DedicatedDiskAgent=True,
                    EnableDataIntegrityValidationForDrBasedDisks=True),
                agent_count=test_case.agent_count,
            )

        nbd_socket_suffix = "_nbd"

        server_app_config = TServerAppConfig()
        server_app_config.ServerConfig.CopyFrom(TServerConfig())
        server_app_config.ServerConfig.ThreadsCount = thread_count()
        server_app_config.ServerConfig.StrictContractValidation = False
        server_app_config.ServerConfig.NbdEnabled = True
        server_app_config.ServerConfig.NbdSocketSuffix = nbd_socket_suffix
        server_app_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
        server_app_config.ServerConfig.ChecksumFlags.CopyFrom(TChecksumFlags())
        server_app_config.ServerConfig.ChecksumFlags.EnableDataIntegrityClient = True
        server_app_config.ServerConfig.UseFakeRdmaClient = test_case.use_rdma
        server_app_config.ServerConfig.RdmaClientEnabled = test_case.use_rdma

        storage = TStorageServiceConfig()
        storage.AllocationUnitNonReplicatedSSD = 1
        storage.AllocationUnitMirror2SSD = 1
        storage.AllocationUnitMirror3SSD = 1
        storage.AcquireNonReplicatedDevices = True
        storage.ClientRemountPeriod = 1000
        storage.NonReplicatedMigrationStartAllowed = True
        storage.MirroredMigrationStartAllowed = True
        storage.DisableLocalService = False
        storage.InactiveClientsTimeout = 60000  # 1 min
        storage.AgentRequestTimeout = 5000      # 5 sec
        storage.UseMirrorResync = False
        storage.NodeType = 'main'
        storage.NonReplicatedMinRequestTimeoutSSD = 500   # 500 msec
        storage.NonReplicatedMaxRequestTimeoutSSD = 1000  # 1 sec
        storage.MaxTimedOutDeviceStateDuration = 60000    # 1 min
        storage.NonReplicatedAgentMinTimeout = 60000      # 1 min
        storage.NonReplicatedAgentMaxTimeout = 60000      # 1 min
        storage.NonReplicatedSecureEraseTimeout = 60000    # 1 min
        storage.EnableToChangeStatesFromDiskRegistryMonpage = True
        storage.EnableToChangeErrorStatesFromDiskRegistryMonpage = True
        storage.EnableDataIntegrityValidationForYdbBasedDisks = True
        storage.MultiAgentWriteEnabled = True
        storage.DirectWriteBandwidthQuota = 100 * 1024  # 100 KiB
        storage.UseNonreplicatedRdmaActor = test_case.use_rdma
        storage.UseRdma = test_case.use_rdma

        client_config = TClientConfig()
        client_config.RetryTimeout = 20000  # 20 sec
        client_config.RetryTimeoutIncrement = 100  # 100 msec
        client_app_config = TClientAppConfig()
        client_app_config.ClientConfig.CopyFrom(client_config)

        nbs = LocalNbs(
            kikimr_port,
            configurator.domains_txt,
            server_app_config=server_app_config,
            storage_config_patches=[storage],
            client_config_patch=client_app_config,
            kikimr_binary_path=kikimr_binary_path,
            nbs_binary_path=yatest_common.binary_path(nbs_binary_path))

        nbs.start()
        wait_for_nbs_server(nbs.nbs_port)

        if test_case.agent_count > 0:
            nbs_client_binary_path = yatest_common.binary_path("cloud/blockstore/apps/client/blockstore-client")
            enable_writable_state(nbs.nbs_port, nbs_client_binary_path)
            setup_disk_registry_config(
                agent_infos,
                nbs.nbs_port,
                nbs_client_binary_path
            )

        storage.DisableLocalService = True

        disk_agents = []
        for i in range(test_case.agent_count):
            disk_agent = None

            disk_agent = LocalDiskAgent(
                kikimr_port,
                configurator.domains_txt,
                server_app_config=server_app_config,
                storage_config_patches=[storage],
                config_sub_folder="disk_agent_configs_%s" % i,
                log_sub_folder="disk_agent_logs_%s" % i,
                kikimr_binary_path=kikimr_binary_path,
                disk_agent_binary_path=yatest_common.binary_path(disk_agent_binary_path),
                rack="rack-%s" % i,
                node_type=make_agent_node_type(i))

            disk_agent.start()
            wait_for_disk_agent(disk_agent.mon_port)

            disk_agents.append(disk_agent)

        config_path = test_case.config_path
        if test_case.agent_count > 0:
            wait_for_secure_erase(nbs.mon_port, expectedAgents=test_case.agent_count)
            config_path = __process_config(test_case.config_path, devices_per_agent)

        client_config.NbdSocketSuffix = nbd_socket_suffix
        ret = run_test(
            test_case.name,
            config_path,
            nbs.nbs_port,
            nbs.mon_port,
            nbs_log_path=nbs.stderr_file_name,
            client_config=client_config,
            env_processes=disk_agents + [nbs],
        )

        write_requests, read_requests, read_mismatches, write_mismatches = __get_sensors(
            nbs.mon_port)
        if read_requests == 0 or write_requests == 0:
            raise Exception("Read or write requests are 0: read_requests={}, write_requests={}".format(
                read_requests, write_requests))
        if read_mismatches != 0 and write_mismatches != 0:
            raise Exception("Checksum mismatches: read_checksum_mismatch={}, write_checksum_mismatch={}".format(
                read_mismatches, write_mismatches))

    finally:
        for disk_agent in disk_agents:
            disk_agent.stop()
        if nbs is not None:
            nbs.stop()
        if kikimr_cluster is not None:
            kikimr_cluster.stop()
        if test_case.agent_count > 0:
            __remove_file_devices(devices)

    return ret


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_load(test_case: TestCase):
    test_case.config_path = yatest_common.source_path(test_case.config_path)
    return __run_test(test_case)
