import os

import contrib.ydb.tests.library.common.yatest_common as yatest_common

from cloud.blockstore.config.discovery_pb2 import TDiscoveryServiceConfig
from cloud.blockstore.config.grpc_client_pb2 import TGrpcClientConfig
from cloud.blockstore.config.server_pb2 import TServerConfig, TServerAppConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig
from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import thread_count, wait_for_nbs_server, \
    wait_for_secure_erase
from cloud.blockstore.tests.python.lib.nonreplicated_setup import enable_writable_state, \
    create_devices, setup_nonreplicated, setup_disk_registry_config, make_agent_id, AgentInfo, DeviceInfo
from cloud.tasks.test.common.processes import register_process, kill_processes

SERVICE_NAME = "nbs"
DEFAULT_BLOCK_SIZE = 4096
DEFAULT_BLOCK_COUNT_PER_DEVICE = 262186  # 262144 + 42


class NbsLauncher:

    def __init__(self,
                 ydb_port,
                 domains_txt,
                 dynamic_storage_pools,
                 root_certs_file,
                 cert_file,
                 cert_key_file,
                 ydb_binary_path,
                 nbs_binary_path,
                 ydb_client,
                 compute_port=0,
                 kms_port=0):
        self.__ydb_port = ydb_port
        self.__domains_txt = domains_txt
        self.__ydb_binary_path = ydb_binary_path
        self.__nbs_binary_path = nbs_binary_path

        self.__port_manager = yatest_common.PortManager()
        nbs_port = self.__port_manager.get_port()
        nbs_secure_port = self.__port_manager.get_port()

        server_app_config = TServerAppConfig()
        server_app_config.ServerConfig.CopyFrom(TServerConfig())
        server_app_config.ServerConfig.ThreadsCount = thread_count()
        server_app_config.ServerConfig.StrictContractValidation = False
        server_app_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
        server_app_config.ServerConfig.RootCertsFile = root_certs_file
        server_app_config.ServerConfig.NodeType = 'main'
        server_app_config.ServerConfig.NbdEnabled = True
        server_app_config.ServerConfig.NbdSocketSuffix = "_nbd"
        self.__server_app_config = server_app_config

        cert = server_app_config.ServerConfig.Certs.add()
        cert.CertFile = cert_file
        cert.CertPrivateKeyFile = cert_key_file

        storage_config_patch = TStorageServiceConfig()
        storage_config_patch.AllocationUnitNonReplicatedSSD = 1
        storage_config_patch.AllocationUnitNonReplicatedHDD = 1
        storage_config_patch.AcquireNonReplicatedDevices = True
        storage_config_patch.ClientRemountPeriod = 1000
        storage_config_patch.NonReplicatedMigrationStartAllowed = True
        storage_config_patch.NonReplicatedSecureEraseTimeout = 2000  # 2 sec
        storage_config_patch.DisableLocalService = False
        storage_config_patch.InactiveClientsTimeout = 60000  # 1 min
        storage_config_patch.AgentRequestTimeout = 5000      # 5 sec

        self.__storage_config_patch = storage_config_patch

        self.__devices = create_devices(
            use_memory_devices=True,
            device_count=3,
            block_size=DEFAULT_BLOCK_SIZE,
            block_count_per_device=DEFAULT_BLOCK_COUNT_PER_DEVICE,
            ram_drive_path=None
        )
        self.__devices[1].storage_pool_name = "rot"

        setup_nonreplicated(ydb_client, [self.__devices])

        instance_list_file = os.path.join(yatest_common.output_path(),
                                          "static_instances_%s.txt" % nbs_port)
        with open(instance_list_file, "w") as f:
            print("localhost\t%s\t%s" % (nbs_port, nbs_secure_port), file=f)
        discovery_config = TDiscoveryServiceConfig()
        discovery_config.InstanceListFile = instance_list_file

        compute_config = None
        if compute_port != 0:
            compute_config = TGrpcClientConfig()
            compute_config.Address = "localhost:{}".format(compute_port)
            compute_config.RequestTimeout = 5000
            compute_config.Insecure = True

        kms_config = None
        if kms_port != 0:
            kms_config = TGrpcClientConfig()
            kms_config.Address = "localhost:{}".format(kms_port)
            kms_config.RequestTimeout = 5000
            kms_config.Insecure = True

        self.__nbs = LocalNbs(
            ydb_port,
            domains_txt,
            server_app_config=server_app_config,
            storage_config_patches=[storage_config_patch],
            discovery_config=discovery_config,
            enable_tls=True,
            dynamic_storage_pools=dynamic_storage_pools,
            nbs_secure_port=nbs_secure_port,
            nbs_port=nbs_port,
            kikimr_binary_path=ydb_binary_path,
            nbs_binary_path=nbs_binary_path,
            grpc_trace=False,
            compute_config=compute_config,
            kms_config=kms_config)

    def start(self):
        self.__nbs.start()
        wait_for_nbs_server(self.__nbs.nbs_port)

        nbs_client_binary_path = yatest_common.binary_path("cloud/blockstore/apps/client/blockstore-client")
        enable_writable_state(self.__nbs.nbs_port, nbs_client_binary_path)

        register_process(SERVICE_NAME, self.__nbs.pid)

        # Start disk-agent
        setup_disk_registry_config(
            [AgentInfo(make_agent_id(0), [DeviceInfo(d.uuid) for d in self.__devices])],
            self.__nbs.nbs_port,
            nbs_client_binary_path,
        )

        self.__server_app_config.ServerConfig.NodeType = 'disk-agent'
        self.__storage_config_patch.DisableLocalService = True
        disk_agent = LocalNbs(
            self.__ydb_port,
            self.__domains_txt,
            server_app_config=self.__server_app_config,
            storage_config_patches=[self.__storage_config_patch],
            enable_tls=True,
            kikimr_binary_path=self.__ydb_binary_path,
            nbs_binary_path=self.__nbs_binary_path,
            ping_path='/blockstore/disk_agent')

        disk_agent.start()
        wait_for_nbs_server(disk_agent.nbs_port)
        wait_for_secure_erase(self.__nbs.mon_port)
        register_process(SERVICE_NAME, disk_agent.pid)

    @staticmethod
    def stop():
        kill_processes(SERVICE_NAME)

    @property
    def port(self):
        return self.__nbs.nbs_secure_port
