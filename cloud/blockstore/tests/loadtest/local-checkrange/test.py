import logging
import os
import pytest
import json

from cloud.blockstore.config.client_pb2 import TClientAppConfig, TClientConfig
from cloud.blockstore.config.disk_pb2 import TDiskAgentConfig
from cloud.blockstore.config.server_pb2 import TServerConfig, \
    TServerAppConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig

from cloud.blockstore.tests.python.lib.disk_agent_runner import LocalDiskAgent
from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import thread_count, \
    wait_for_nbs_server, wait_for_disk_agent, run_test, wait_for_secure_erase
from cloud.blockstore.tests.python.lib.client import NbsClient
from cloud.blockstore.tests.python.lib.nonreplicated_setup import \
    setup_nonreplicated, create_devices, setup_disk_registry_config, \
    enable_writable_state, make_agent_node_type, make_agent_id, AgentInfo, \
    DeviceInfo

from contrib.ydb.tests.library.harness.kikimr_cluster import \
    kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import \
    KikimrConfigGenerator
from contrib.ydb.tests.library.harness.kikimr_runner import \
    get_unique_path_for_current_test, ensure_path_exists

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
            storage_media_kind,
            volume_name,  # должно совпадать с именем в .txt конфиге теста
            agent_count=1):
        self.name = name
        self.config_path = config_path
        self.storage_media_kind = storage_media_kind
        self.volume_name = volume_name
        self.agent_count = agent_count
        self.disk_blocks_count = 786432
        self.blocks_per_request = 1024


TESTS = [
    TestCase(
        "checkrange_mirror2",
        "cloud/blockstore/tests/loadtest/local-checkrange/local-mirror2.txt",
        storage_media_kind="mirror2",
        volume_name="test_volume_mrr2",
        agent_count=2,
    ),
    TestCase(
        "checkrange_mirror3",
        "cloud/blockstore/tests/loadtest/local-checkrange/local-mirror3.txt",
        storage_media_kind="mirror3",
        volume_name="test_volume_mrr3",
        agent_count=3,
    ),
    TestCase(
        "checkrange_ssd",
        "cloud/blockstore/tests/loadtest/local-checkrange/local-ssd.txt",
        storage_media_kind="ssd",
        volume_name="test_volume_vol0",
        agent_count=0,
    ),
    TestCase(
        "checkrange_nonreplicated",
        "cloud/blockstore/tests/loadtest/local-checkrange/"
        "local-nonreplicated.txt",
        storage_media_kind="nonreplicated",
        volume_name="test_volume_nrd0",
        agent_count=1,
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
        new_config_data += "\"%s\"" % devices_per_agent[int(
            agent_id)][int(device_id)].path
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


def __run_test(test_case):
    kikimr_binary_path = yatest_common.binary_path(
        "contrib/ydb/apps/ydbd/ydbd")
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
                    DedicatedDiskAgent=True),
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
        storage.EnableDataIntegrityValidationForYdbBasedDisks = False
        storage.MultiAgentWriteEnabled = True
        storage.DirectWriteBandwidthQuota = 100 * 1024  # 100 KiB

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
            nbs_client_binary_path = yatest_common.binary_path(
                "cloud/blockstore/apps/client/blockstore-client")
            enable_writable_state(nbs.nbs_port, nbs_client_binary_path)
            setup_disk_registry_config(
                agent_infos,
                nbs.nbs_port,
                nbs_client_binary_path
            )

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
                disk_agent_binary_path=yatest_common.binary_path(
                    disk_agent_binary_path),
                rack="rack-%s" % i,
                node_type=make_agent_node_type(i))

            disk_agent.start()
            wait_for_disk_agent(disk_agent.mon_port)
            disk_agents.append(disk_agent)

        config_path = test_case.config_path
        if test_case.agent_count > 0:
            wait_for_secure_erase(
                nbs.mon_port, expectedAgents=test_case.agent_count)
            config_path = __process_config(
                test_case.config_path, devices_per_agent)

        client = NbsClient(nbs.nbs_port)
        client.create_volume(
            test_case.volume_name, test_case.storage_media_kind,
            test_case.disk_blocks_count)

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

        checkrange_output_file = "checkrange_output.data"
        client.checkrange(
            test_case.volume_name,
            checkrange_output_file,
            test_case.disk_blocks_count,
            test_case.blocks_per_request)

        __validate_checkrange(test_case, checkrange_output_file)

    finally:
        client.destroy_volume(test_case.volume_name)
        for disk_agent in disk_agents:
            disk_agent.stop()
        if nbs is not None:
            nbs.stop()
        if kikimr_cluster is not None:
            kikimr_cluster.stop()
        if test_case.agent_count > 0:
            __remove_file_devices(devices)

    return ret


def __validate_checkrange(test_case, checkrange_result_file):
    try:
        with open(checkrange_result_file, 'r') as file:
            data = json.load(file)
    except json.JSONDecodeError as e:
        raise Exception(f"Checkrange parse result error {e}")

    test_mirrors = test_case.storage_media_kind == "mirror2" \
        or test_case.storage_media_kind == "mirror3"

    expected_requests_num = test_case.disk_blocks_count / \
        test_case.blocks_per_request
    if test_case.disk_blocks_count % test_case.blocks_per_request:
        expected_requests_num += 1

    if data['Summary']['RequestsNum'] != expected_requests_num:
        raise Exception(f"Expected RequestsNum {expected_requests_num}, "
                        f"actual {data['Summary']['RequestsNum']}")
    if test_mirrors and data['Summary']['ErrorsNum'] == 0:
        raise Exception("Expecting non zero 'Summary':'ErrorsNum'")

    expected_problem_ranges = []
    for r in data['Ranges']:
        start = r['Start']
        end = r['End']
        have_error = r.get('Error') is not None
        if not have_error:
            checksums = r['Checksums']
            if checksums is None or len(checksums) == 0:
                raise Exception(
                    f"Expected 'Checksums' is missing for correct range "
                    f"{start}-{end}")
        else:
            if not test_mirrors:
                continue
            mirror_checksums = r['MirrorChecksums']
            if mirror_checksums is None:
                raise Exception(
                    f"Expected 'MirrorChecksums' array for problem range "
                    f"{start}-{end}")
            if len(mirror_checksums) != test_case.agent_count:
                raise Exception(
                    f"Expected {test_case.agent_count} MirrorChecksums for"
                    f" {test_case.name}, actual {len(mirror_checksums)}")
            expected_problem_ranges.append((start, end))
            for mirrorCS in mirror_checksums:
                if mirrorCS.get('ReplicaName') is None:
                    raise Exception(
                        f"Expected 'ReplicaName' is missing for problem range "
                        f"{start}-{end}")
                checksums = mirrorCS['Checksums']
                if checksums is None or len(checksums) == 0:
                    raise Exception(
                        f"Expected 'Checksums' is missing for problem range "
                        f"{start}-{end}")

    # проверка алгоритма мержа problem_ranges
    actual_problem_ranges = data['Summary'].get('ProblemRanges')
    actual_problem_ranges_size = 0
    if actual_problem_ranges is not None:
        actual_problem_ranges_size = len(actual_problem_ranges)

    merged_expected_problem_ranges = __merge_ranges(expected_problem_ranges)
    if len(merged_expected_problem_ranges) != actual_problem_ranges_size:
        raise Exception(
            f"Expected length of merged ProblemRanges"
            " {len(merged_expected_problem_ranges)}"
            f" is not equal to actual {actual_problem_ranges_size}")
    if len(merged_expected_problem_ranges) != 0:
        i = 0
        for r1, r2 in zip(merged_expected_problem_ranges,
                          actual_problem_ranges):
            if r1[0] != r2['Start'] or r1[1] != r2['End']:
                raise Exception(
                    f"ProblemRanges merge algorithm problem. "
                    f"Expected {i} element '{r1[0]}-{r1[1]}', "
                    f"actual {i} element '{r2['Start']}-{r2['End']}'")
            i += 1


def __merge_ranges(ranges):
    if not ranges:
        return []

    merged_ranges = [ranges[0]]
    for current_start, current_end in ranges[1:]:
        last_start, last_end = merged_ranges[-1]
        if last_end == current_start - 1:
            merged_ranges[-1] = (last_start, current_end)
        else:
            merged_ranges.append((current_start, current_end))

    return merged_ranges


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_load(test_case: TestCase):
    test_case.config_path = yatest_common.source_path(test_case.config_path)
    return __run_test(test_case)
