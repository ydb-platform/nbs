import logging
import os
import pytest
import subprocess
import time

import cloud.blockstore.public.sdk.python.protos as protos

from cloud.blockstore.public.sdk.python.client.error_codes import EResult

from cloud.blockstore.config.server_pb2 import TServerConfig, TServerAppConfig, \
    TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig

from cloud.blockstore.tests.python.lib.test_client import CreateTestClient


from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import thread_count, wait_for_nbs_server, \
    wait_for_secure_erase, get_sensor_by_name, get_nbs_counters
from cloud.blockstore.tests.python.lib.nonreplicated_setup import setup_nonreplicated, \
    create_file_devices, setup_disk_registry_config_simple, enable_writable_state

from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

from google.protobuf.json_format import MessageToDict

import yatest.common as yatest_common

DEFAULT_BLOCK_SIZE = 4096
DEFAULT_DEVICE_COUNT = 4
NRD_DEVICE_COUNT = 3
DEFAULT_BLOCK_COUNT_PER_DEVICE = 262144
DEVICE_SIZE = DEFAULT_BLOCK_COUNT_PER_DEVICE * DEFAULT_BLOCK_SIZE
DISK_SIZE = DEFAULT_BLOCK_COUNT_PER_DEVICE * NRD_DEVICE_COUNT
KNOWN_DEVICE_POOLS = {
    "KnownDevicePools": [
        {"Name": "local-ssd", "Kind": "DEVICE_POOL_KIND_LOCAL",
            "AllocationUnit": DEVICE_SIZE},
    ]}


class CMS:

    def __init__(self, nbs_client):
        self.__nbs_client = nbs_client

    def add_agent(self, host):
        logging.info('[CMS] Try to add agent "{}"'.format(host))

        request = protos.TCmsActionRequest()
        action = request.Actions.add()
        action.Type = protos.TAction.ADD_HOST
        action.Host = host
        r = self.__nbs_client.cms_action(request)
        return r

    def add_device(self, host, device):
        logging.info(
            '[CMS] Try to add device "{}" on agent"{}"'.format(device, host))

        request = protos.TCmsActionRequest()
        action = request.Actions.add()
        action.Type = protos.TAction.ADD_DEVICE
        action.Host = host
        action.Device = device
        r = self.__nbs_client.cms_action(request)
        return r

    def remove_agent(self, host):
        logging.info('[CMS] Try to remove agent "{}"'.format(host))

        request = protos.TCmsActionRequest()
        action = request.Actions.add()
        action.Type = protos.TAction.REMOVE_HOST
        action.Host = host
        r = self.__nbs_client.cms_action(request)
        return r

    def purge_agent(self, host):
        logging.info('[CMS] Try to purge agent "{}"'.format(host))

        request = protos.TCmsActionRequest()
        action = request.Actions.add()
        action.Type = protos.TAction.PURGE_HOST
        action.Host = host
        r = self.__nbs_client.cms_action(request)
        return r

    def remove_device(self, host, path):
        logging.info('[CMS] Try to remove device "{}":"{}"'.format(host, path))

        request = protos.TCmsActionRequest()
        action = request.Actions.add()
        action.Type = protos.TAction.REMOVE_DEVICE
        action.Host = host
        action.Device = path
        r = self.__nbs_client.cms_action(request)
        return r

    def wait_for_no_paths_to_attach_detach(self, expected_paths_count=0, poll_interval=1):
        while True:
            bkp = self.__nbs_client.backup_disk_registry_state()
            if len(bkp.get("PathsToAttachDetach", [])) == expected_paths_count:
                break
            time.sleep(poll_interval)


def file_descriptors_count(path):
    try:
        out = subprocess.check_output(["lsof", path]).decode()
        return len(out.split("\n")) - 2
    except subprocess.CalledProcessError:
        return 0


class _TestCmsRemoveAgentNoUserDisks:

    def __init__(self, name):
        self.name = name

    def run(self, storage, nbs, disk_agent, cms: CMS, devices, always_allocate_local_ssd, attach_detach_paths):

        fds_count = []
        for device in devices:
            fd_count = file_descriptors_count(device.path)
            assert fd_count != 0
            fds_count.append(fd_count)

        if attach_detach_paths:
            response = cms.remove_agent("localhost")

            assert response.ActionResults[0].Result.Code == EResult.E_TRY_AGAIN.value
            assert response.ActionResults[0].Timeout != 0

            cms.wait_for_no_paths_to_attach_detach()

        if attach_detach_paths:
            for i in range(len(devices)):
                device = devices[i]
                fd_count = file_descriptors_count(device.path)
                assert fd_count + 1 == fds_count[i]

        response = cms.remove_agent("localhost")

        assert response.ActionResults[0].Timeout == 0

        nbs.create_volume("vol0", return_code=1)

        time.sleep(storage.NonReplicatedInfraTimeout / 1000)

        response = cms.remove_agent("localhost")

        assert response.ActionResults[0].Timeout == 0

        disk_agent.stop()
        if not always_allocate_local_ssd:
            # The agent will be deleted after timeout.
            nbs.wait_for_stats(AgentsInWarningState=0)
            assert nbs.get_stats("AgentsInUnavailableState") == 0
            assert nbs.get_stats("AgentsInOnlineState") == 0
        else:
            nbs.wait_for_stats(AgentsInUnavailableState=1)

        disk_agent.start()
        wait_for_nbs_server(disk_agent.nbs_port)
        if not always_allocate_local_ssd:
            # The agent is online but all devices are unknown.
            nbs.wait_for_stats(AgentsInOnlineState=1)
            assert nbs.get_stats("UnknownDevices") == 4

            # Add agent and its devices.
            response = cms.add_agent("localhost")
            assert response.ActionResults[0].Timeout == 0
            nbs.wait_for_stats(UnknownDevices=0)
            wait_for_secure_erase(nbs.mon_port)
        else:
            nbs.wait_for_stats(AgentsInWarningState=1)
            assert nbs.get_stats("UnknownDevices") == 0

            response = cms.add_agent("localhost")
            assert response.ActionResults[0].Timeout == 0
            cms.wait_for_no_paths_to_attach_detach()
            wait_for_secure_erase(nbs.mon_port)

        for i in range(len(devices)):
            device = devices[i]
            fd_count = file_descriptors_count(device.path)
            assert fd_count == fds_count[i]

        nbs.create_volume("vol0")
        nbs.read_blocks("vol0", start_index=0, block_count=32)

        return True


class _TestCmsRemoveAgent:

    def __init__(self, name):
        self.name = name

    def run(self, storage, nbs, disk_agent, cms, devices, always_allocate_local_ssd, attach_detach_paths):
        nbs.create_volume("vol0")

        fds_count = []
        for device in devices:
            fd_count = file_descriptors_count(device.path)
            assert fd_count != 0
            fds_count.append(fd_count)

        response = cms.remove_agent("localhost")

        assert response.ActionResults[0].Result.Code == EResult.E_TRY_AGAIN.value
        assert response.ActionResults[0].Timeout != 0

        time.sleep(storage.NonReplicatedInfraTimeout / 1000)

        response = cms.remove_agent("localhost")

        assert response.ActionResults[0].Result.Code == EResult.E_TRY_AGAIN.value
        assert response.ActionResults[0].Timeout != 0

        nbs.destroy_volume("vol0", sync=True)

        cms.wait_for_no_paths_to_attach_detach()

        for i in range(len(devices)):
            device = devices[i]
            fd_count = file_descriptors_count(device.path)
            assert fd_count + 1 == fds_count[i]

        response = cms.remove_agent("localhost")

        assert response.ActionResults[0].Timeout == 0

        disk_agent.stop()
        if not always_allocate_local_ssd:
            # The agent will be deleted after timeout.
            nbs.wait_for_stats(AgentsInWarningState=0)
            assert nbs.get_stats("AgentsInUnavailableState") == 0
            assert nbs.get_stats("AgentsInOnlineState") == 0
        else:
            nbs.wait_for_stats(AgentsInUnavailableState=1)

        disk_agent.start()
        wait_for_nbs_server(disk_agent.nbs_port)
        if not always_allocate_local_ssd:
            # The agent is online but all devices are unknown.
            nbs.wait_for_stats(AgentsInOnlineState=1)
            assert nbs.get_stats("UnknownDevices") == 4

            # Add agent and its devices.
            response = cms.add_agent("localhost")
            assert response.ActionResults[0].Result.Code == 0
            assert response.ActionResults[0].Timeout == 0
            nbs.wait_for_stats(UnknownDevices=0)
            wait_for_secure_erase(nbs.mon_port)
        else:
            nbs.wait_for_stats(AgentsInWarningState=1)
            assert nbs.get_stats("UnknownDevices") == 0

            response = cms.add_agent("localhost")
            assert response.ActionResults[0].Timeout == 0
            cms.wait_for_no_paths_to_attach_detach()
            wait_for_secure_erase(nbs.mon_port)

        for i in range(len(devices)):
            device = devices[i]
            fd_count = file_descriptors_count(device.path)
            assert fd_count == fds_count[i]

        nbs.create_volume("vol1")
        nbs.read_blocks("vol1", start_index=0, block_count=32)

        return True


class _TestCmsPurgeAgentNoUserDisks:

    def __init__(self, name):
        self.name = name

    def run(self, storage, nbs, disk_agent, cms: CMS, devices, always_allocate_local_ssd, attach_detach_paths):
        nbs.change_agent_state("localhost", 1)

        fds_count = []
        for device in devices:
            fd_count = file_descriptors_count(device.path)
            assert fd_count != 0
            fds_count.append(fd_count)

        nbs.wait_for_stats(AgentsInWarningState=1)

        return_code = 0 if always_allocate_local_ssd else 1
        nbs.create_volume("local0", blocks_count=DEFAULT_BLOCK_COUNT_PER_DEVICE,
                          kind="ssd_local", return_code=return_code)
        nbs.create_volume("vol0", return_code=1)
        if always_allocate_local_ssd:
            nbs.destroy_volume("local0", sync=True)

        if attach_detach_paths:
            response = cms.remove_agent("localhost")
            assert response.ActionResults[0].Result.Code == EResult.E_TRY_AGAIN.value
            assert response.ActionResults[0].Timeout != 0

            cms.wait_for_no_paths_to_attach_detach()

            for i in range(len(devices)):
                device = devices[i]
                fd_count = file_descriptors_count(device.path)
                assert fd_count + 1 == fds_count[i]

        response = cms.remove_agent("localhost")
        assert response.ActionResults[0].Result.Code == 0
        assert response.ActionResults[0].Timeout == 0

        response = cms.purge_agent("localhost")
        nbs.wait_for_stats(UnknownDevices=4)
        assert nbs.get_stats("AgentsInWarningState") == 1

        disk_agent.stop()
        # The agent will be deleted after timeout.
        nbs.wait_for_stats(AgentsInWarningState=0)
        assert nbs.get_stats("AgentsInOnlineState") == 0
        assert nbs.get_stats("AgentsInUnavailableState") == 0

        # Should not create any disks from now.
        nbs.create_volume("local1", blocks_count=DEFAULT_BLOCK_COUNT_PER_DEVICE,
                          kind="ssd_local", return_code=1)
        nbs.create_volume("vol1", return_code=1)

        disk_agent.start()
        wait_for_nbs_server(disk_agent.nbs_port)
        # The agent is online but all devices are unknown.
        nbs.wait_for_stats(AgentsInOnlineState=1)
        assert nbs.get_stats("UnknownDevices") == 4

        # Add agent and its devices.
        response = cms.add_agent("localhost")
        assert response.ActionResults[0].Result.Code == 0
        assert response.ActionResults[0].Timeout == 0
        nbs.wait_for_stats(UnknownDevices=0)
        wait_for_secure_erase(nbs.mon_port, pool="local-ssd", kind="local")

        for i in range(len(devices)):
            device = devices[i]
            fd_count = file_descriptors_count(device.path)
            assert fd_count == fds_count[i]

        nbs.create_volume("local1", blocks_count=DEFAULT_BLOCK_COUNT_PER_DEVICE,
                          kind="ssd_local")
        nbs.create_volume("vol1")
        nbs.read_blocks("vol1", start_index=0, block_count=32)

        return True


class _TestCmsPurgeAgent:

    def __init__(self, name):
        self.name = name

    def run(self, storage, nbs, disk_agent, cms, devices, always_allocate_local_ssd, attach_detach_paths):
        nbs.create_volume("vol0")
        assert nbs.get_stats("AgentsInWarningState") == 0

        response = cms.purge_agent("localhost")

        # Purge doesn't return error even if there is DependentDisks.
        assert response.ActionResults[0].Result.Code == 0
        assert response.ActionResults[0].Timeout == 0
        assert "vol0" in response.ActionResults[0].DependentDisks

        # Agent is the warning state now.
        nbs.wait_for_stats(AgentsInWarningState=1)
        nbs.wait_for_stats(AgentsInWarningState=1)
        assert nbs.get_stats("UnknownDevices") == 0

        # Even though all devices are knonwn, local disks allocation is
        # forbidden since devices are in suspended state.
        nbs.create_volume("local0", blocks_count=DEFAULT_BLOCK_COUNT_PER_DEVICE,
                          kind="ssd_local", return_code=1)

        nbs.destroy_volume("vol0", sync=True)

        response = cms.purge_agent("localhost")
        assert response.ActionResults[0].Result.Code == 0
        assert response.ActionResults[0].Timeout == 0
        assert len(response.ActionResults[0].DependentDisks) == 0

        nbs.wait_for_stats(UnknownDevices=4)
        assert nbs.get_stats("AgentsInWarningState") == 1
        assert nbs.get_stats("AgentsInOnlineState") == 0

        disk_agent.stop()

        nbs.wait_for_stats(AgentsInWarningState=0)
        assert nbs.get_stats("AgentsInOnlineState") == 0
        assert nbs.get_stats("AgentsInUnavailableState") == 0

        return True


class _TestCmsRemoveDevice:

    def __init__(self, name):
        self.name = name

    def run(self, storage, nbs, disk_agent, cms: CMS, devices, always_allocate_local_ssd, attach_detach_paths):
        nbs.create_volume("vol0")

        device = devices[0]

        fd_count = file_descriptors_count(device.path)
        assert fd_count != 0

        response = cms.remove_device("localhost", device.path)

        assert response.ActionResults[0].Result.Code == EResult.E_TRY_AGAIN.value
        assert response.ActionResults[0].Timeout != 0

        time.sleep(storage.NonReplicatedInfraTimeout / 1000)

        response = cms.remove_device("localhost", device.path)

        assert response.ActionResults[0].Result.Code == EResult.E_TRY_AGAIN.value
        assert response.ActionResults[0].Timeout != 0

        nbs.change_device_state(device.uuid, 2)

        if attach_detach_paths:
            cms.wait_for_no_paths_to_attach_detach()

            assert file_descriptors_count(device.path) + 1 == fd_count

        response = cms.remove_device("localhost", device.path)

        assert response.ActionResults[0].Timeout == 0

        nbs.change_device_state(device.uuid, 0)

        if attach_detach_paths:
            response = cms.add_device("localhost", device.path)
            assert response.ActionResults[0].Timeout == 0
            cms.wait_for_no_paths_to_attach_detach()
            assert file_descriptors_count(device.path) == fd_count

        nbs.destroy_volume("vol0", sync=True)

        nbs.create_volume("vol1")
        nbs.read_blocks("vol1", start_index=0, block_count=32)

        return True


class _TestCmsRemoveDeviceNoUserDisks:

    def __init__(self, name):
        self.name = name

    def run(self, storage, nbs, disk_agent, cms: CMS, devices, always_allocate_local_ssd, attach_detach_paths):
        device = devices[0]

        fd_count = file_descriptors_count(device.path)
        assert fd_count != 0

        if attach_detach_paths:
            cms.remove_device("localhost", device.path)
            cms.wait_for_no_paths_to_attach_detach()
            assert file_descriptors_count(device.path) + 1 == fd_count

        response = cms.remove_device("localhost", device.path)

        assert response.ActionResults[0].Timeout == 0

        nbs.create_volume("vol0", return_code=1)
        nbs.destroy_volume("vol0")

        time.sleep(storage.NonReplicatedInfraTimeout / 1000)

        response = cms.remove_device("localhost", device.path)

        assert response.ActionResults[0].Timeout == 0

        nbs.change_device_state(device.uuid, 2)
        nbs.change_device_state(device.uuid, 0)

        response = cms.add_device("localhost", device.path)
        assert response.ActionResults[0].Timeout == 0

        if attach_detach_paths:
            cms.wait_for_no_paths_to_attach_detach()
            assert file_descriptors_count(device.path) == fd_count

        nbs.create_volume("vol1")
        nbs.read_blocks("vol1", start_index=0, block_count=32)

        return True


TESTS = [
    _TestCmsRemoveAgentNoUserDisks("removeagentnodisks"),
    _TestCmsRemoveAgent("removeagent"),
    _TestCmsPurgeAgentNoUserDisks("purgeagentnodisks"),
    _TestCmsPurgeAgent("purgeagent"),
    _TestCmsRemoveDevice("removedevice"),
    _TestCmsRemoveDeviceNoUserDisks("removedevicenodisks")
]


class Nbs(LocalNbs):
    BINARY_PATH = yatest_common.binary_path(
        "cloud/blockstore/apps/client/blockstore-client")

    def create_volume(self, disk_id, blocks_count=DISK_SIZE,
                      kind="nonreplicated", return_code=0):
        logging.info('[NBS] create volume "%s"' % disk_id)

        self.__rpc(
            "createvolume",
            "--disk-id", disk_id,
            "--blocks-count", str(blocks_count),
            "--storage-media-kind", kind,
            return_code=return_code)

    def destroy_volume(self, disk_id, sync=False):
        logging.info('[NBS] destroy volume "%s"' % disk_id)

        args = ["destroyvolume", "--disk-id", disk_id]

        if sync:
            args.append("--sync")

        self.__rpc(*args, input=disk_id, stdout=subprocess.PIPE)

    def read_blocks(self, disk_id, start_index, block_count):
        logging.info('[NBS] read blocks (%d:%d) from "%s"' %
                     (start_index, block_count, disk_id))

        self.__rpc(
            "readblocks",
            "--disk-id", disk_id,
            "--start-index", str(start_index),
            "--blocks-count", str(block_count))

    def change_agent_state(self, agent_id, state):
        logging.info('[NBS] change agent "%s" state to %d' % (agent_id, state))

        self.__rpc(
            "executeaction",
            "--action", "diskregistrychangestate",
            "--input-bytes", "{'Message': 'test', 'ChangeAgentState': {'AgentId': '%s', 'State': %d}}" % (
                agent_id, state),
            stdout=subprocess.PIPE)

    def change_device_state(self, device_id, state):
        logging.info('[NBS] change device "%s" state to %d' %
                     (device_id, state))

        self.__rpc(
            "executeaction",
            "--action", "diskregistrychangestate",
            "--input-bytes", "{'Message': 'test', 'ChangeDeviceState': {'DeviceUUID': '%s', 'State': %d}}" % (
                device_id, state),
            stdout=subprocess.PIPE)

    def __rpc(self, *args, **kwargs):
        input = kwargs.get("input")
        return_code = kwargs.get("return_code", 0)

        if input is not None:
            input = (input + "\n").encode("utf8")

        args = [self.BINARY_PATH] + list(args) + [
            "--host", "localhost",
            "--port", str(self.nbs_port),
            "--verbose", "error"]

        process = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=kwargs.get("stdout"),
            stderr=subprocess.PIPE)

        outs, errs = process.communicate(input=input)

        if outs:
            logging.info("blockstore-client's output: {}".format(outs))

        if errs:
            logging.error("blockstore-client's errors: {}".format(errs))

        assert process.returncode == return_code

    def wait_for_stats(self, **kwargs):
        logging.info('wait for stats: {} ...'.format(kwargs))

        while True:
            sensors = get_nbs_counters(self.mon_port)['sensors']
            satisfied = set()

            for name, expected in kwargs.items():
                val = get_sensor_by_name(sensors, 'disk_registry', name)
                if val != expected:
                    break
                satisfied.add(name)

            if len(kwargs) == len(satisfied):
                break
            time.sleep(1)

    def get_stats(self, name):
        sensors = get_nbs_counters(self.mon_port)['sensors']
        return get_sensor_by_name(sensors, 'disk_registry', name)


def __run_test(test_case, always_allocate_local_ssd, attach_detach_paths):
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

    nbs_binary_path = yatest_common.binary_path(
        "cloud/blockstore/apps/server/nbsd")

    kikimr_cluster = kikimr_cluster_factory(configurator=configurator)
    kikimr_cluster.start()

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    devices = create_file_devices(
        None,   # dir
        DEFAULT_DEVICE_COUNT,
        DEFAULT_BLOCK_SIZE,
        DEFAULT_BLOCK_COUNT_PER_DEVICE)
    devices[DEFAULT_DEVICE_COUNT - 1].storage_pool_name = "local-ssd"

    setup_nonreplicated(kikimr_cluster.client, [devices])

    server_app_config = TServerAppConfig()
    server_app_config.ServerConfig.CopyFrom(TServerConfig())
    server_app_config.ServerConfig.ThreadsCount = thread_count()
    server_app_config.ServerConfig.StrictContractValidation = False
    server_app_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    certs_dir = yatest_common.source_path('cloud/blockstore/tests/certs')

    server_app_config.ServerConfig.RootCertsFile = os.path.join(
        certs_dir, 'server.crt')
    cert = server_app_config.ServerConfig.Certs.add()
    cert.CertFile = os.path.join(certs_dir, 'server.crt')
    cert.CertPrivateKeyFile = os.path.join(certs_dir, 'server.key')

    storage = TStorageServiceConfig()
    storage.AllocationUnitNonReplicatedSSD = 1
    storage.AcquireNonReplicatedDevices = True
    storage.ClientRemountPeriod = 1000
    storage.NonReplicatedInfraTimeout = 30000
    storage.NonReplicatedAgentMinTimeout = 3000
    storage.NonReplicatedAgentMaxTimeout = 3000
    storage.NonReplicatedDiskRecyclingPeriod = 5000
    storage.DisableLocalService = False
    storage.NonReplicatedDontSuspendDevices = True
    storage.DiskRegistryAlwaysAllocatesLocalDisks = always_allocate_local_ssd
    storage.DiskRegistryCleanupConfigOnRemoveHost = not always_allocate_local_ssd
    storage.AttachDetachPathsEnabled = attach_detach_paths
    storage.NodeType = 'main'

    nbs = Nbs(
        kikimr_port,
        configurator.domains_txt,
        server_app_config=server_app_config,
        storage_config_patches=[storage],
        enable_tls=True,
        kikimr_binary_path=kikimr_binary_path,
        nbs_binary_path=nbs_binary_path)

    nbs.start()
    wait_for_nbs_server(nbs.nbs_port)

    nbs_client_binary_path = yatest_common.binary_path(
        "cloud/blockstore/apps/client/blockstore-client")
    enable_writable_state(nbs.nbs_port, nbs_client_binary_path)
    setup_disk_registry_config_simple(
        devices,
        nbs.nbs_port,
        nbs_client_binary_path)

    nbs_client = CreateTestClient("localhost:{}".format(nbs.nbs_port))

    config = nbs_client.describe_disk_registry_config()
    nbs_client.update_disk_registry_config(
        MessageToDict(config) | KNOWN_DEVICE_POOLS)

    # node with DiskAgent

    storage.NodeType = 'disk-agent'
    storage.DisableLocalService = True

    disk_agent = LocalNbs(
        kikimr_port,
        configurator.domains_txt,
        server_app_config=server_app_config,
        storage_config_patches=[storage],
        enable_tls=True,
        kikimr_binary_path=kikimr_binary_path,
        nbs_binary_path=nbs_binary_path)

    disk_agent.start()
    wait_for_nbs_server(disk_agent.nbs_port)

    # wait for DiskAgent registration & secure erase
    wait_for_secure_erase(nbs.mon_port)

    try:
        ret = test_case.run(storage, nbs, disk_agent, CMS(
            nbs_client), devices, always_allocate_local_ssd, attach_detach_paths)
    finally:
        nbs.stop()
        disk_agent.stop()
        kikimr_cluster.stop()
        logging.info("Remove temporary device files")
        for d in devices:
            d.handle.close()
            os.unlink(d.path)

    return ret


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
@pytest.mark.parametrize("always_allocate_local_ssd", [True, False], ids=["locals_in_warning", "locals_in_online"])
@pytest.mark.parametrize("attach_detach_paths", [True, False], ids=["attach-detach-paths", ""])
def test_cms(test_case, always_allocate_local_ssd, attach_detach_paths):
    assert __run_test(test_case, always_allocate_local_ssd,
                      attach_detach_paths) is True
