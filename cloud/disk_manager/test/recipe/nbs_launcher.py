import os

import contrib.ydb.tests.library.common.yatest_common as yatest_common

from cloud.blockstore.config.discovery_pb2 import TDiscoveryServiceConfig
from cloud.blockstore.config.grpc_client_pb2 import TGrpcClientConfig
from cloud.blockstore.config.server_pb2 import TServerConfig, TServerAppConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig
from cloud.blockstore.tests.python.lib.disk_agent_runner import LocalDiskAgent
from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import thread_count, wait_for_nbs_server, \
    wait_for_secure_erase, wait_for_disk_agent
from cloud.blockstore.tests.python.lib.nonreplicated_setup import enable_writable_state, \
    create_devices, setup_nonreplicated, setup_disk_registry_config, make_agent_id, AgentInfo, \
    DeviceInfo, make_agent_node_type
from cloud.storage.core.config.features_pb2 import TFeaturesConfig
from cloud.tasks.test.common.processes import register_process, kill_processes

NBS_SERVICE_NAME = "nbs"
DISK_AGENT_SERVICE_NAME = "disk_agent"
DEFAULT_BLOCK_SIZE = 4096
DEFAULT_BLOCK_COUNT_PER_DEVICE = 262144


class NbsLauncher:

    def __init__(
        self,
        ydb_port,
        domains_txt,
        dynamic_storage_pools,
        root_certs_file,
        cert_file,
        cert_key_file,
        ydb_binary_path,
        nbs_binary_path,
        disk_agent_binary_path,
        ydb_client,
        compute_port=0,
        kms_port=0,
        destruction_allowed_only_for_disks_with_id_prefixes=[],
        disk_agent_count=1
    ):
        self.__ydb_port = ydb_port
        self.__domains_txt = domains_txt
        self.__ydb_binary_path = ydb_binary_path
        self.__nbs_binary_path = nbs_binary_path
        self.__disk_agent_binary_path = disk_agent_binary_path

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
        storage_config_patch.UseShadowDisksForNonreplDiskCheckpoints = True

        # Needed for tests on blockstore client https://github.com/ydb-platform/nbs/pull/3067
        storage_config_patch.MaxDisksInPlacementGroup = 2

        if destruction_allowed_only_for_disks_with_id_prefixes:
            storage_config_patch.DestructionAllowedOnlyForDisksWithIdPrefixes.extend(destruction_allowed_only_for_disks_with_id_prefixes)

        self.__storage_config_patch = storage_config_patch

        self.__disk_agent_count = disk_agent_count
        self.__devices_per_agent = []
        device_count_per_agent = 3

        devices = create_devices(
            use_memory_devices=True,
            device_count=device_count_per_agent*self.__disk_agent_count,
            block_size=DEFAULT_BLOCK_SIZE,
            block_count_per_device=DEFAULT_BLOCK_COUNT_PER_DEVICE,
            ram_drive_path=None
        )
        for i in range(0, len(devices), device_count_per_agent):
            agent_devices = [devices[j] for j in range(i, i + device_count_per_agent)]
            agent_devices[1].storage_pool_name = "rot"
            self.__devices_per_agent.append(agent_devices)
        setup_nonreplicated(
            ydb_client,
            self.__devices_per_agent,
            dedicated_disk_agent=True,
            agent_count=self.__disk_agent_count)

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

        features_config_patch = TFeaturesConfig()
        encryption_at_rest = features_config_patch.Features.add()
        encryption_at_rest.Name = 'EncryptionAtRestForDiskRegistryBasedDisks'
        encryption_at_rest.Whitelist.FolderIds.append("encrypted-folder")

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
            kms_config=kms_config,
            features_config_patch=features_config_patch)

    @property
    def nbs(self):
        return self.__nbs

    def start(self):
        self.__nbs.start()
        wait_for_nbs_server(self.__nbs.nbs_port)

        nbs_client_binary_path = yatest_common.binary_path("cloud/blockstore/apps/client/blockstore-client")
        enable_writable_state(self.__nbs.nbs_port, nbs_client_binary_path)

        register_process(NBS_SERVICE_NAME, self.__nbs.pid)

        # Start disk agents
        agent_infos = []
        for i in range(self.__disk_agent_count):
            agent_infos.append(AgentInfo(
                make_agent_id(i),
                [DeviceInfo(d.uuid) for d in self.__devices_per_agent[i]]))

        setup_disk_registry_config(
            agent_infos,
            self.__nbs.nbs_port,
            nbs_client_binary_path,
        )

        for i in range(self.__disk_agent_count):
            self.__run_disk_agent(i)
        wait_for_secure_erase(self.__nbs.mon_port)

    def __run_disk_agent(self, index):
        server_app_config = TServerAppConfig()
        server_app_config.CopyFrom(self.__server_app_config)
        self.__server_app_config.ServerConfig.NodeType = 'disk-agent'

        storage_config_patch = TStorageServiceConfig()
        storage_config_patch.CopyFrom(self.__storage_config_patch)
        storage_config_patch.DisableLocalService = True

        disk_agent = LocalDiskAgent(
            self.__ydb_port,
            self.__domains_txt,
            server_app_config=self.__server_app_config,
            storage_config_patches=[storage_config_patch],
            config_sub_folder="disk_agent_configs_%s" % index,
            log_sub_folder="disk_agent_logs_%s" % index,
            kikimr_binary_path=self.__ydb_binary_path,
            disk_agent_binary_path=self.__disk_agent_binary_path,
            rack="rack-%s" % index,
            node_type=make_agent_node_type(index))

        disk_agent.start()
        wait_for_disk_agent(disk_agent.mon_port)
        register_process(DISK_AGENT_SERVICE_NAME, disk_agent.pid)
        return disk_agent

    @staticmethod
    def stop():
        kill_processes(NBS_SERVICE_NAME)
        kill_processes(DISK_AGENT_SERVICE_NAME)

    @property
    def port(self):
        return self.__nbs.nbs_secure_port
