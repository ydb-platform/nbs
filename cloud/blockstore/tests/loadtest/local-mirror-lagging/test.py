import logging
import os
import pytest

from cloud.blockstore.config.client_pb2 import TClientConfig
from cloud.blockstore.config.server_pb2 import TServerConfig, TServerAppConfig, \
    TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig

from cloud.blockstore.tests.python.lib.disk_agent_runner import LocalDiskAgent
from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import thread_count, \
    wait_for_nbs_server, wait_for_disk_agent, run_test, wait_for_secure_erase
from cloud.blockstore.tests.python.lib.nonreplicated_setup import setup_nonreplicated, \
    create_devices, setup_disk_registry_config, \
    enable_writable_state, make_agent_node_type, make_agent_id, AgentInfo, DeviceInfo

from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists

import yatest.common as yatest_common

DEFAULT_BLOCK_SIZE = 4096
DEFAULT_DEVICE_COUNT = 1
DEFAULT_BLOCK_COUNT_PER_DEVICE = 262144


class TestCase(object):
    __test__ = False

    def __init__(
            self,
            name,
            config_path,
            nbs_restart_interval=None,
            restart_interval=None,
            downtime_after_restart=None,
            agent_indexes_to_restart=[0],
            device_count=DEFAULT_DEVICE_COUNT,
            block_count_per_device=DEFAULT_BLOCK_COUNT_PER_DEVICE,
            agent_count=1,
            dump_block_digests=False,
            max_migration_bandwidth=1024 * 1024 * 1024):
        self.name = name
        self.config_path = config_path
        self.nbs_restart_interval = nbs_restart_interval
        self.restart_interval = restart_interval
        self.downtime_after_restart = downtime_after_restart
        self.agent_indexes_to_restart = agent_indexes_to_restart
        self.device_count = device_count
        self.block_count_per_device = block_count_per_device
        self.agent_count = agent_count
        self.dump_block_digests = dump_block_digests
        self.max_migration_bandwidth = max_migration_bandwidth


TESTS = [
    TestCase(
        "mirror2-basic",
        "cloud/blockstore/tests/loadtest/local-mirror-lagging/local-mirror2-basic.txt",
        agent_count=2,
        device_count=3,
        restart_interval=30,
        downtime_after_restart=5,
        dump_block_digests=True,
    ),
    TestCase(
        "mirror2-device-per-agent",
        "cloud/blockstore/tests/loadtest/local-mirror-lagging/local-mirror2-device-per-agent.txt",
        agent_count=6,
        device_count=1,
        restart_interval=30,
        agent_indexes_to_restart=[0, 2],
        downtime_after_restart=5,
        dump_block_digests=True,
    ),
    TestCase(
        "mirror2-small-interval",
        "cloud/blockstore/tests/loadtest/local-mirror-lagging/local-mirror2-small-interval.txt",
        agent_count=2,
        device_count=3,
        restart_interval=5,
        downtime_after_restart=5,
        dump_block_digests=True,
    ),
    TestCase(
        "mirror2-small-downtime",
        "cloud/blockstore/tests/loadtest/local-mirror-lagging/local-mirror2-small-downtime.txt",
        agent_count=2,
        device_count=3,
        restart_interval=30,
        downtime_after_restart=1,
        dump_block_digests=True,
    ),
    TestCase(
        "mirror3-basic",
        "cloud/blockstore/tests/loadtest/local-mirror-lagging/local-mirror3-basic.txt",
        agent_count=3,
        device_count=3,
        restart_interval=30,
        downtime_after_restart=5,
        dump_block_digests=True,
    ),
    TestCase(
        "mirror2-restart-nbs",
        "cloud/blockstore/tests/loadtest/local-mirror-lagging/local-mirror2-restart-nbs.txt",
        agent_count=2,
        device_count=3,
        restart_interval=30,
        downtime_after_restart=5,
        nbs_restart_interval=40,
        dump_block_digests=True,
    ),
]


def __cleanup_file_devices(devices):
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
    have_replacements = False
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
        have_replacements = True

        prev_index = next_index + 1

    if have_replacements:
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

    devices = create_devices(
        False,  # use_memory_devices
        test_case.device_count * test_case.agent_count,
        DEFAULT_BLOCK_SIZE,
        test_case.block_count_per_device,
        yatest_common.ram_drive_path())
    devices_per_agent = []
    agent_infos = []
    device_idx = 0
    for i in range(test_case.agent_count):
        device_infos = []
        agent_devices = []
        for _ in range(test_case.device_count):
            device_infos.append(DeviceInfo(devices[device_idx].uuid))
            agent_devices.append(devices[device_idx])
            device_idx += 1
        agent_infos.append(AgentInfo(make_agent_id(i), device_infos))
        devices_per_agent.append(agent_devices)

    try:
        setup_nonreplicated(
            kikimr_cluster.client,
            devices_per_agent,
            dedicated_disk_agent=True,
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

        storage = TStorageServiceConfig()
        storage.AllocationUnitNonReplicatedSSD = 1
        storage.AllocationUnitMirror2SSD = 1
        storage.AllocationUnitMirror3SSD = 1
        storage.AcquireNonReplicatedDevices = True
        storage.ClientRemountPeriod = 1000
        storage.NonReplicatedMigrationStartAllowed = True
        storage.DisableLocalService = False
        storage.InactiveClientsTimeout = 60000  # 1 min
        storage.AgentRequestTimeout = 5000      # 5 sec
        storage.MaxMigrationBandwidth = test_case.max_migration_bandwidth
        storage.MaxMigrationIoDepth = 4
        storage.UseMirrorResync = False
        storage.MirroredMigrationStartAllowed = True
        storage.NodeType = 'main'
        storage.LaggingDevicesForMirror2DisksEnabled = True
        storage.LaggingDevicesForMirror3DisksEnabled = True
        storage.LaggingDeviceTimeoutThreshold = 3000      # 3 sec
        storage.NonReplicatedMinRequestTimeoutSSD = 500   # 500 msec
        storage.NonReplicatedMaxRequestTimeoutSSD = 5000  # 5 sec
        storage.MaxTimedOutDeviceStateDuration = 60000    # 1 min
        storage.MaxTimedOutDeviceStateDuration = 60000    # 1 min
        storage.NonReplicatedAgentMinTimeout = 60000      # 1 min
        storage.NonReplicatedAgentMaxTimeout = 60000      # 1 min
        storage.EnableToChangeStatesFromDiskRegistryMonpage = True
        storage.EnableToChangeErrorStatesFromDiskRegistryMonpage = True

        if test_case.dump_block_digests:
            storage.BlockDigestsEnabled = True
            storage.UseTestBlockDigestGenerator = True

        nbs = LocalNbs(
            kikimr_port,
            configurator.domains_txt,
            server_app_config=server_app_config,
            storage_config_patches=[storage],
            kikimr_binary_path=kikimr_binary_path,
            restart_interval=test_case.nbs_restart_interval,
            nbs_binary_path=yatest_common.binary_path(nbs_binary_path))

        nbs.start()
        wait_for_nbs_server(nbs.nbs_port)

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

            restart_interval = test_case.restart_interval if i in test_case.agent_indexes_to_restart else None
            disk_agent = LocalDiskAgent(
                kikimr_port,
                configurator.domains_txt,
                server_app_config=server_app_config,
                storage_config_patches=[storage],
                config_sub_folder="disk_agent_configs_%s" % i,
                log_sub_folder="disk_agent_logs_%s" % i,
                kikimr_binary_path=kikimr_binary_path,
                disk_agent_binary_path=yatest_common.binary_path(disk_agent_binary_path),
                restart_interval=restart_interval,
                downtime_after_restart=test_case.downtime_after_restart,
                rack="rack-%s" % i,
                node_type=make_agent_node_type(i))

            disk_agent.start()
            wait_for_disk_agent(disk_agent.mon_port)

            disk_agents.append(disk_agent)

        wait_for_secure_erase(nbs.mon_port, expectedAgents=test_case.agent_count)

        client = TClientConfig()
        client.NbdSocketSuffix = nbd_socket_suffix

        config_path = __process_config(test_case.config_path, devices_per_agent)

        ret = run_test(
            test_case.name,
            config_path,
            nbs.nbs_port,
            nbs.mon_port,
            nbs_log_path=nbs.stderr_file_name,
            client_config=client,
            env_processes=disk_agents + [nbs],
        )
    finally:
        for disk_agent in disk_agents:
            disk_agent.stop()
        if nbs is not None:
            nbs.stop()
        if kikimr_cluster is not None:
            kikimr_cluster.stop()
        assert False, "this is ok :)"

    return ret


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_load(test_case: TestCase):
    test_case.config_path = yatest_common.source_path(test_case.config_path)
    return __run_test(test_case)
