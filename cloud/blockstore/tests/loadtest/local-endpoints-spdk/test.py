import pytest

import yatest.common as common

from cloud.blockstore.config.client_pb2 import TClientConfig
from cloud.blockstore.config.server_pb2 import \
    TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.config.spdk_pb2 import TSpdkEnvConfig

from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import thread_count, run_test

from yatest.common.network import PortManager


CONFIG_DIR = "cloud/blockstore/tests/loadtest/local-endpoints-spdk/"


class TestCase(object):

    def __init__(
        self,
        name,
        config_path,
        nvme_enabled=False,
        scsi_enabled=False,
        rdma_enabled=False,
    ):
        self.name = name
        self.config_path = config_path
        self.nvme_enabled = nvme_enabled
        self.scsi_enabled = scsi_enabled
        self.rdma_enabled = rdma_enabled


TESTS = [
    TestCase(
        "version1-nvme-endpoint",
        "local-tablet-version-1-nvme-endpoint.txt",
        nvme_enabled=True,
    ),
    TestCase(
        "version1-scsi-endpoint",
        "local-tablet-version-1-scsi-endpoint.txt",
        scsi_enabled=True,
    ),
    # TODO: need libibverbs-compatible network adapter (mellanox)
    # TestCase(
    #     "version1-rdma-endpoint",
    #     "local-tablet-version-1-rdma-endpoint.txt",
    #     rdma_enabled=True,
    # ),
]


def __run_test(test_case):
    port_manager = PortManager()
    nvme_port = port_manager.get_port()
    scsi_port = port_manager.get_port()

    nvme_nqn = "nqn.2014-08.org.nvmexpress:"
    nvme_transport_id = "trtype:TCP adrfam:IPv4 traddr:127.0.0.1 trsvcid:" + str(nvme_port)

    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(TServerConfig())
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.StrictContractValidation = True
    server.ServerConfig.NVMeEndpointEnabled = test_case.nvme_enabled
    server.ServerConfig.NVMeEndpointNqn = nvme_nqn
    server.ServerConfig.NVMeEndpointTransportIDs.append(nvme_transport_id)
    server.ServerConfig.SCSIEndpointEnabled = test_case.scsi_enabled
    server.ServerConfig.SCSIEndpointListenAddress = "127.0.0.1"
    server.ServerConfig.SCSIEndpointListenPort = scsi_port
    server.ServerConfig.RdmaEndpointEnabled = test_case.rdma_enabled
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    env = LocalLoadTest(
        "",
        server_app_config=server,
        use_in_memory_pdisks=True,
    )

    client_config = TClientConfig()
    client_config.NvmeDeviceNqn = nvme_nqn
    client_config.NvmeDeviceTransportId = nvme_transport_id
    client_config.ScsiDeviceUrl = "iscsi://127.0.0.1:" + str(scsi_port) + "/iqn.2016-06.io.spdk:"

    spdk_config = None
    if test_case.nvme_enabled or test_case.scsi_enabled:
        # TODO: configure reactor threads and anything else
        spdk_config = TSpdkEnvConfig()

    ret = run_test(
        test_case.name,
        test_case.config_path,
        env.nbs_port,
        env.mon_port,
        nbs_log_path=env.nbs_log_path,
        client_config=client_config,
        spdk_config=spdk_config,
        env_processes=[env.nbs],
    )

    env.tear_down()

    return ret


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_load(test_case):
    common.execute(["sysctl", "-w", "vm.nr_hugepages=4096"])
    common.execute(["grep", "-i", "huge", "/proc/meminfo"])

    config = common.get_param("config")
    if config is None:
        test_case.config_path = common.source_path(
            CONFIG_DIR + test_case.config_path)
        return __run_test(test_case)
