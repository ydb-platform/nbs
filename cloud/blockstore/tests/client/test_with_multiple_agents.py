from enum import Enum
import json
import logging
import os
import pytest
import time
from subprocess import TimeoutExpired

from cloud.blockstore.config.disk_pb2 import TDiskAgentConfig
from cloud.blockstore.config.server_pb2 import TServerConfig, \
    TServerAppConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig

from cloud.blockstore.tests.python.lib.client import NbsClient
from cloud.blockstore.tests.python.lib.disk_agent_runner import LocalDiskAgent
from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import (
    thread_count,
    wait_for_nbs_server,
    wait_for_disk_agent,
    wait_for_secure_erase,
    files_equal
)
from cloud.blockstore.tests.python.lib.nonreplicated_setup import (
    setup_nonreplicated,
    create_devices,
    setup_disk_registry_config,
    enable_writable_state,
    make_agent_node_type,
    make_agent_id,
    AgentInfo,
    DeviceInfo
)

from contrib.ydb.tests.library.harness.kikimr_cluster import \
    kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import \
    KikimrConfigGenerator
from contrib.ydb.tests.library.harness.kikimr_runner import \
    get_unique_path_for_current_test, ensure_path_exists

import yatest.common as yatest_common

DEFAULT_BLOCK_SIZE = 4096
DEFAULT_DEVICE_COUNT = 3
DEFAULT_AGENT_COUNT = 1
DEFAULT_ALLOCATION_UNIT_SIZE = 1
DEFAULT_BLOCK_COUNT_PER_DEVICE = 262144
NBD_SOCKET_SUFFIX = "_nbd"
NRD_BLOCKS_COUNT = 1024**3
DATA_FILE = "random.bin"

################################################################################


def cache_dir_path():
    cache_dir_path = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder="disk_agents_cache"
    )
    ensure_path_exists(cache_dir_path)
    return cache_dir_path

################################################################################


class TestWithMultipleAgents(object):

    def __init__(
            self,
            device_count=DEFAULT_DEVICE_COUNT,
            block_count_per_device=DEFAULT_BLOCK_COUNT_PER_DEVICE,
            allocation_unit_size=DEFAULT_ALLOCATION_UNIT_SIZE,
            agent_count=DEFAULT_AGENT_COUNT):
        self.device_count = device_count
        self.block_count_per_device = block_count_per_device
        self.allocation_unit_size = allocation_unit_size
        self.agent_count = agent_count
        self.__disk_agents = []

        self.kikimr_binary_path = yatest_common.binary_path(
            "ydb/apps/ydbd/ydbd")
        self.nbs_binary_path = yatest_common.binary_path(
            "cloud/blockstore/apps/server/nbsd")
        self.disk_agent_binary_path = yatest_common.binary_path(
            "cloud/blockstore/apps/disk_agent/diskagentd")
        self.nbs_client_binary_path = yatest_common.binary_path(
            "cloud/blockstore/apps/client/blockstore-client")

        self.server_app_config = TServerAppConfig()
        self.server_app_config.ServerConfig.CopyFrom(TServerConfig())
        self.server_app_config.ServerConfig.ThreadsCount = thread_count()
        self.server_app_config.ServerConfig.StrictContractValidation = False
        self.server_app_config.ServerConfig.NbdEnabled = True
        self.server_app_config.ServerConfig.NbdSocketSuffix = NBD_SOCKET_SUFFIX
        self.server_app_config.KikimrServiceConfig.CopyFrom(
            TKikimrServiceConfig())

        self.storage_config = TStorageServiceConfig()
        self.storage_config.AllocationUnitNonReplicatedSSD = self.allocation_unit_size
        self.storage_config.AllocationUnitMirror2SSD = self.allocation_unit_size
        self.storage_config.AllocationUnitMirror3SSD = self.allocation_unit_size
        self.storage_config.AcquireNonReplicatedDevices = True
        self.storage_config.ClientRemountPeriod = 1000
        self.storage_config.NonReplicatedMigrationStartAllowed = True
        self.storage_config.DisableLocalService = False
        self.storage_config.InactiveClientsTimeout = 60000  # 1 min
        self.storage_config.AgentRequestTimeout = 5000      # 5 sec
        self.storage_config.MaxMigrationBandwidth = 1024 * 1024 * 1024
        self.storage_config.UseMirrorResync = True
        self.storage_config.MirroredMigrationStartAllowed = True
        self.storage_config.NodeType = 'main'

    def run_disk_agent(self, index, temporary=False):
        storage = TStorageServiceConfig()
        storage.CopyFrom(self.storage_config)
        storage.DisableLocalService = True

        disk_agent = LocalDiskAgent(
            self.__kikimr_port,
            self.__configurator.domains_txt,
            server_app_config=self.server_app_config,
            storage_config_patches=[storage],
            config_sub_folder="disk_agent_configs_%s" % index,
            log_sub_folder="disk_agent_logs_%s" % index,
            kikimr_binary_path=self.kikimr_binary_path,
            disk_agent_binary_path=self.disk_agent_binary_path,
            rack="rack-%s" % index,
            node_type=make_agent_node_type(index),
            temporary_agent=temporary)

        disk_agent.start()
        wait_for_disk_agent(disk_agent.mon_port)
        self.__disk_agents.append(disk_agent)
        return disk_agent

    def setup(self):
        self.__configurator = KikimrConfigGenerator(
            erasure=None,
            binary_path=self.kikimr_binary_path,
            use_in_memory_pdisks=True,
            dynamic_storage_pools=[
                dict(name="dynamic_storage_pool:1",
                     kind="hdd",
                     pdisk_user_kind=0),
                dict(name="dynamic_storage_pool:2",
                     kind="ssd",
                     pdisk_user_kind=0)
            ])

        self.__kikimr_cluster = kikimr_cluster_factory(
            configurator=self.__configurator)
        self.__kikimr_cluster.start()
        self.__kikimr_port = list(self.__kikimr_cluster.nodes.values())[0].port

        self.__devices = create_devices(
            False,  # use_memory_devices
            self.device_count * self.agent_count,
            DEFAULT_BLOCK_SIZE,
            self.block_count_per_device,
            yatest_common.ram_drive_path())
        devices_per_agent = []
        agent_infos = []
        device_idx = 0
        for i in range(self.agent_count):
            device_infos = []
            agent_devices = []
            for _ in range(self.device_count):
                device_infos.append(DeviceInfo(
                    self.__devices[device_idx].uuid))
                agent_devices.append(self.__devices[device_idx])
                device_idx += 1
            agent_infos.append(AgentInfo(make_agent_id(i), device_infos))
            devices_per_agent.append(agent_devices)

        setup_nonreplicated(
            self.__kikimr_cluster.client,
            devices_per_agent,
            disk_agent_config_patch=TDiskAgentConfig(DedicatedDiskAgent=True),
            agent_count=self.agent_count,
            cached_sessions_dir_path=cache_dir_path()
        )

        self.nbs = LocalNbs(
            self.__kikimr_port,
            self.__configurator.domains_txt,
            server_app_config=self.server_app_config,
            storage_config_patches=[self.storage_config],
            kikimr_binary_path=self.kikimr_binary_path,
            nbs_binary_path=self.nbs_binary_path)

        self.nbs.start()
        wait_for_nbs_server(self.nbs.nbs_port)

        enable_writable_state(self.nbs.nbs_port, self.nbs_client_binary_path)
        setup_disk_registry_config(
            agent_infos,
            self.nbs.nbs_port,
            self.nbs_client_binary_path
        )

    def cleanup_file_devices(self):
        logging.info("Remove temporary device files")
        for d in self.__devices:
            if d.path is not None:
                logging.info("unlink %s (%s)" % (d.uuid, d.path))
                assert d.handle is not None
                d.handle.close()
                os.unlink(d.path)

    @property
    def disk_agents(self):
        return self.__disk_agents


def test_wait_dependent_disks_to_switch_node_timeout():
    env = TestWithMultipleAgents()
    try:
        env.setup()
        env.run_disk_agent(0)
        wait_for_secure_erase(env.nbs.mon_port)

        client = NbsClient(env.nbs.nbs_port)
        client.create_volume("nrd0", "nonreplicated",
                             DEFAULT_BLOCK_COUNT_PER_DEVICE)

        agent_id = make_agent_id(0)
        node_id_response = json.loads(client.get_disk_agent_node_id(agent_id))
        assert node_id_response["NodeId"] > 50000
        assert node_id_response["AgentState"] == "AGENT_STATE_ONLINE"
        assert node_id_response["Connected"]

        # This should return immediately.
        wait_response = client.wait_dependent_disks_to_switch_node(
            agent_id, node_id_response["NodeId"] + 1)
        assert json.loads(wait_response) == {
            "DependentDiskStates": [
                {
                    "DiskId": "nrd0",
                    "DiskState": "DISK_STATE_READY"
                }
            ]
        }, f"wait_response = {wait_response}"

        # This should return after 40s timeout in the wait actor.
        process = client.wait_dependent_disks_to_switch_node_async(
            agent_id, node_id_response["NodeId"], timeout=40)
        try:
            out, err = process.communicate(timeout=60)
        except TimeoutExpired:
            process.kill()
            out, err = process.communicate()
            assert False, "blockstore-client should have exited after 30s."\
                " stdout: {}\nstderr: {}".format(out, err)

        # This should return after 25s communicate() timeout.
        process = client.wait_dependent_disks_to_switch_node_async(
            agent_id, node_id_response["NodeId"])
        try:
            out, err = process.communicate(timeout=25)
        except TimeoutExpired:
            process.kill()
            out, err = process.communicate()
        else:
            assert False, "blockstore-client shouldn't have exited."\
                " stdout: {}\nstderr: {}".format(out, err)

    finally:
        env.cleanup_file_devices()


class DiskType(Enum):
    NRD = {
        "agent_count": 1,
        "storage_media_kind": "nonreplicated",
    }
    MIRROR3 = {
        "agent_count": 3,
        "storage_media_kind": "mirror3",
    }


@pytest.mark.parametrize("disk_type", [dt for dt in DiskType], ids=[dt.name for dt in DiskType])
def test_disk_agent_partial_suspend_cancellation(disk_type: DiskType):
    env = TestWithMultipleAgents(agent_count=disk_type.value["agent_count"])
    try:
        env.setup()
        for i in range(disk_type.value["agent_count"]):
            env.run_disk_agent(i)
        wait_for_secure_erase(env.nbs.mon_port)

        client = NbsClient(env.nbs.nbs_port)
        DISK_ID = "diskid"
        client.create_volume(DISK_ID, disk_type.value["storage_media_kind"],
                             DEFAULT_BLOCK_COUNT_PER_DEVICE)

        agent_id = make_agent_id(0)
        da_node_id = json.loads(
            client.get_disk_agent_node_id(agent_id))["NodeId"]
        assert da_node_id > 50000

        BLOCK_COUNT = 100
        with open("/dev/urandom", "rb") as r:
            with open(DATA_FILE, "wb") as w:
                block = r.read(DEFAULT_BLOCK_SIZE * BLOCK_COUNT)
                w.write(block)

        client.write_blocks(DISK_ID, 0, DATA_FILE)
        client.read_blocks(DISK_ID, 0, BLOCK_COUNT, "read.data")
        assert files_equal(DATA_FILE, "read.data")

        SUSPENSION_DELAY_MS = 180000  # 3 min
        client.partially_suspend_disk_agent(da_node_id, SUSPENSION_DELAY_MS)

        # Client will try to create new session, but it's now blocked by
        # rejects from the suspended agent.
        read_data_process = client.read_blocks_async(
            DISK_ID, 0, BLOCK_COUNT, "read2.data")
        try:
            out, err = read_data_process.communicate(timeout=20)
        except TimeoutExpired:
            pass
        else:
            assert False, "blockstore-client shouldn't have exited."\
                " stdout: {}\nstderr: {}".format(out, err)

        env.run_disk_agent(0, True)

        # Now we wait for disk to switch to the temporary agent.
        logging.info("wait for switch")
        wait_response = client.wait_dependent_disks_to_switch_node(
            agent_id, da_node_id)
        assert json.loads(wait_response) == {
            "DependentDiskStates": [
                {
                    "DiskId": DISK_ID,
                    "DiskState": "DISK_STATE_READY"
                }
            ]
        }, f"wait_response = {wait_response}"

        # Temporary agent read the data.
        out, err = read_data_process.communicate()
        assert read_data_process.returncode == 0
        assert files_equal(DATA_FILE, "read2.data")

        # Node id of the temporary agent differs from the primary one.
        temp_da_node_id = json.loads(
            client.get_disk_agent_node_id(agent_id))["NodeId"]
        assert temp_da_node_id > 50000
        assert temp_da_node_id != da_node_id

        logging.info("da_node_id: {}".format(da_node_id))
        logging.info("temp_da_node_id: {}".format(temp_da_node_id))

        # We need to suspend the temporary agent to allow primary agent to
        # register in the DR.
        client.partially_suspend_disk_agent(
            temp_da_node_id, SUSPENSION_DELAY_MS * 2)

        # Partial suspend should be canceled after a delay.
        time.sleep(SUSPENSION_DELAY_MS / 1000)

        # Now we wait for disk to switch to the primary agent.
        wait_response = client.wait_dependent_disks_to_switch_node(
            agent_id, temp_da_node_id)
        assert json.loads(wait_response) == {
            "DependentDiskStates": [
                {
                    "DiskId": DISK_ID,
                    "DiskState": "DISK_STATE_READY"
                }
            ]
        }, f"wait_response = {wait_response}"

        node_id = json.loads(client.get_disk_agent_node_id(agent_id))["NodeId"]
        assert node_id == da_node_id

        # IO still works fine.
        client.read_blocks(DISK_ID, 0, BLOCK_COUNT, "read3.data")
        assert files_equal(DATA_FILE, "read3.data")

    finally:
        env.cleanup_file_devices()
