import os

from ydb.tests.library.common.yatest_common import PortManager

from google.protobuf.text_format import MessageToString

from cloud.blockstore.config.diagnostics_pb2 import TDiagnosticsConfig
from cloud.blockstore.config.disk_pb2 import TDiskRegistryProxyConfig, \
    TDiskAgentConfig, DISK_AGENT_BACKEND_AIO

from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig
from cloud.blockstore.config.server_pb2 import TServerAppConfig, \
    TKikimrServiceConfig, TServerConfig, TLocation
from ydb.core.protos.config_pb2 import TActorSystemConfig, \
    TDynamicNameserviceConfig, TLogConfig


class NbsConfigurator:

    def __init__(self, pm: PortManager, ydb):
        assert ydb.config

        self.__pm = pm
        self.__ydb = ydb

        self.ic_port = None
        self.mon_port = None
        self.server_port = None
        self.data_port = None
        self.domain = None
        self.load_configs_from_cms = None

        self.files = dict()
        self.cms = dict()

        self.__params = []

    @property
    def ydb_port(self):
        return list(self.__ydb.nodes.values())[0].port

    @property
    def params(self):
        return self.__params

    def generate_default_nbs_configs(self):
        self.node_broker_addr = f"localhost:{self.ydb_port}"

        self.ic_port = self.__pm.get_port()
        self.mon_port = self.__pm.get_port()
        self.server_port = self.__pm.get_port()
        self.data_port = self.__pm.get_port()

        self.domain = "Root"

        self.load_configs_from_cms = True

        self.files["sys"] = self.__generate_sys_txt()
        self.files["log"] = self.__generate_log_txt()

        self.files["server"] = self.__generate_server_txt()

        self.files["storage"] = self.__generate_storage_txt()
        self.files["dynamic-naming"] = self.__generate_dyn_ns_txt()
        self.files["diag"] = self.__generate_diag_txt()
        self.files["dr-proxy"] = self.__generate_dr_proxy_txt()
        self.files["domains"] = self.__ydb.config.domains_txt
        self.files["location"] = self.__generate_location_txt()

        return self

    def generate_default_disk_agent_configs(self):
        self.generate_default_nbs_configs()

        self.files["disk-agent"] = self.__generate_disk_agent_txt()

        return self

    def install(self, config_folder):

        self.__install_cms_configs()

        self.__params += [
            "--domain", self.domain,
            "--ic-port", str(self.ic_port),
            "--mon-port", str(self.mon_port),
            "--node-broker", f"localhost:{self.ydb_port}",
            "--load-configs-from-cms",
            "--mon-address", "localhost",
            ]

        def filename(name):
            if name != "profile":
                return os.path.join(config_folder, f"nbs-{name}.txt")

            return os.path.join(config_folder, f"nbs-profile-{self.ic_port}.log")

        for name, config in self.files.items():
            path = filename(name)
            with open(path, 'w') as f:
                f.write(MessageToString(config))

            self.__params += [f"--{name}-file", path]

        return self.__params

    def __install_cms_configs(self):
        # TODO
        pass

    def __generate_location_txt(self):
        location = TLocation()
        location.Rack = "rack"

        return location

    def __generate_server_txt(self):
        app = TServerAppConfig()
        server = TServerConfig()
        server.Port = self.server_port
        server.DataPort = self.data_port
        app.ServerConfig.CopyFrom(server)
        app.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
        return app

    def __generate_sys_txt(self):
        sys_config = TActorSystemConfig()

        sys_config.SysExecutor = 0
        sys_config.Executor.add(
            Type=sys_config.TExecutor.EType.Value("BASIC"),
            Threads=4,
            SpinThreshold=20,
            Name="System"
        )

        sys_config.UserExecutor = 1
        sys_config.Executor.add(
            Type=sys_config.TExecutor.EType.Value("BASIC"),
            Threads=4,
            SpinThreshold=20,
            Name="User"
        )

        sys_config.BatchExecutor = 2
        sys_config.Executor.add(
            Type=sys_config.TExecutor.EType.Value("BASIC"),
            Threads=4,
            SpinThreshold=20,
            Name="Batch"
        )

        sys_config.IoExecutor = 3
        sys_config.Executor.add(
            Type=sys_config.TExecutor.EType.Value("IO"),
            Threads=1,
            Name="IO"
        )

        sys_config.Executor.add(
            Type=sys_config.TExecutor.EType.Value("BASIC"),
            Threads=1,
            SpinThreshold=10,
            Name="IC",
            TimePerMailboxMicroSecs=100
        )
        sys_config.ServiceExecutor.add(
            ServiceName="Interconnect",
            ExecutorId=4
        )

        sys_config.Scheduler.Resolution = 2048
        sys_config.Scheduler.SpinThreshold = 0
        sys_config.Scheduler.ProgressThreshold = 10000

        return sys_config

    def __generate_log_txt(self):
        services_info = [
            b"TABLET_EXECUTOR",
            b"INTERCONNECT",
            b"BLOCKSTORE_AUTH",
            b"BLOCKSTORE_BOOTSTRAPPER",
            b"BLOCKSTORE_CLIENT",
            b"BLOCKSTORE_NBD",
            b"BLOCKSTORE_SCHEDULER",
            b"BLOCKSTORE_SERVER",
            b"BLOCKSTORE_SERVICE",
            b"BLOCKSTORE_SERVICE_PROXY",
            b"BLOCKSTORE_DISCOVERY",
            b"BLOCKSTORE_DISK_REGISTRY_PROXY",
            b"BLOCKSTORE_VOLUME",
        ]

        services_dbg = [
            b"BLOCKSTORE_PARTITION",
            b"BLOCKSTORE_DISK_REGISTRY",
            b"BLOCKSTORE_DISK_REGISTRY_WORKER",
            b"BLOCKSTORE_DISK_AGENT",
            b"BLOCKSTORE_HIVE_PROXY",
            b"BLOCKSTORE_SS_PROXY",
        ]

        log_config = TLogConfig()
        for service_name in services_info:
            log_config.Entry.add(Component=service_name, Level=6)

        for service_name in services_dbg:
            log_config.Entry.add(Component=service_name, Level=7)

        log_config.AllowDropEntries = False

        return log_config

    def __generate_diag_txt(self):
        diag = TDiagnosticsConfig()
        diag.ProfileLogTimeThreshold = 100
        diag.SlowRequestSamplingRate = 0

        return diag

    def __generate_storage_txt(self):
        storage = TStorageServiceConfig()
        storage.SchemeShardDir = "/Root/nbs"
        storage.DiskSpaceScoreThrottlingEnabled = True
        return storage

    def __generate_dyn_ns_txt(self):
        configs = TDynamicNameserviceConfig()
        configs.MaxStaticNodeId = 1000
        configs.MaxDynamicNodeId = 1064
        return configs

    def __generate_dr_proxy_txt(self):
        config = TDiskRegistryProxyConfig()
        config.Owner = 16045690984503103501
        config.OwnerIdx = 2
        return config

    def __generate_disk_agent_txt(self):
        config = TDiskAgentConfig()
        config.Enabled = True
        config.DedicatedDiskAgent = True
        config.Backend = DISK_AGENT_BACKEND_AIO
        config.DirectIoFlagDisabled = True
        config.AgentId = "localhost"
        config.NvmeTarget.Nqn = "nqn.2018-09.io.spdk:cnode1"
        config.AcquireRequired = True
        config.RegisterRetryTimeout = 1000  # 1 second
        config.ShutdownTimeout = 0

        return config
