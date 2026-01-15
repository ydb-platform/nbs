import os

from contrib.ydb.tests.library.common.yatest_common import PortManager

from google.protobuf.json_format import ParseDict
from google.protobuf.text_format import MessageToString

from cloud.blockstore.config.diagnostics_pb2 import TDiagnosticsConfig
from cloud.blockstore.config.disk_pb2 import TDiskRegistryProxyConfig, \
    TDiskAgentConfig, TFileDeviceArgs, TStorageDiscoveryConfig, \
    DISK_AGENT_BACKEND_AIO, EDeviceEraseMethod

from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig
from cloud.blockstore.config.server_pb2 import TServerAppConfig, \
    TKikimrServiceConfig, TServerConfig, TLocation

from contrib.ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

from contrib.ydb.core.protos.msgbus_pb2 import TConsoleRequest
from contrib.ydb.core.protos.config_pb2 import TActorSystemConfig, \
    TDynamicNameserviceConfig, TLogConfig


class NbsConfigurator:

    def __init__(
            self,
            ydb,
            node_type=None,
            pm=None,
            ssl_registration=False,
            ic_port=None,
            use_location=True,
            location=None):
        assert ydb.config

        self.__pm = PortManager() if pm is None else pm
        self.__ydb = ydb
        self.__node_type = node_type

        self.ic_port = ic_port
        self.mon_port = None
        self.server_port = None
        self.data_port = None
        self.domain = None
        self.load_configs_from_cms = None

        self.files = dict()
        self.cms = dict()

        self.ssl_registration = ssl_registration
        self.location = location
        self.use_location = use_location

        self.__params = []

    @property
    def ydb_port(self):
        return list(self.__ydb.nodes.values())[0].port

    @property
    def ydb_ssl_port(self):
        return list(self.__ydb.nodes.values())[0].grpc_ssl_port

    @property
    def node_type(self):
        return self.__node_type

    @property
    def params(self):
        return self.__params

    def generate_default_nbs_configs(self):
        self.node_broker_addr = f"localhost:{self.ydb_port}"

        if self.ic_port is None:
            self.ic_port = self.__pm.get_port()
        self.mon_port = self.__pm.get_port()
        self.server_port = self.__pm.get_port()
        self.data_port = self.__pm.get_port()

        self.domain = "Root"

        self.load_configs_from_cms = True

        self.files["sys"] = generate_sys_txt()
        self.files["log"] = generate_log_txt()

        self.files["server"] = generate_server_txt(self.server_port, self.data_port)

        self.files["storage"] = generate_storage_txt()
        self.files["dynamic-naming"] = generate_dyn_ns_txt()
        self.files["diag"] = generate_diag_txt()
        self.files["dr-proxy"] = generate_dr_proxy_txt()
        self.files["domains"] = self.__ydb.config.domains_txt
        if self.use_location:
            self.files["location"] = self.location
            if self.location is None:
                self.files["location"] = generate_location_txt()

    def install(self, config_folder, ydb_ssl_port=None):

        self.__install_cms_configs()

        self.__params = [
            "--domain", self.domain,
            "--ic-port", str(self.ic_port),
            "--mon-port", str(self.mon_port),
            "--load-configs-from-cms",
            "--mon-address", "localhost",
            ]

        if self.ssl_registration:
            self.__params += ["--use-secure-registration"]
            self.__params += [
                "--node-broker",
                f"localhost:{self.ydb_ssl_port if ydb_ssl_port is None else ydb_ssl_port}",
            ]
        else:
            self.__params += ["--node-broker", f"localhost:{self.ydb_port}"]

        if self.node_type:
            self.__params += ["--node-type", self.node_type]

        def filename(name):
            if name != "profile":
                return os.path.join(config_folder, f"nbs-{name}.txt")

            return os.path.join(config_folder, f"nbs-profile-{self.ic_port}.log")

        for name, config in self.files.items():
            path = filename(name)
            with open(path, 'w') as f:
                f.write(MessageToString(config))

            self.__params += [f"--{name}-file", path]

    def __install_cms_configs(self):

        if len(self.cms) == 0:
            return

        request = TConsoleRequest()
        action = request.ConfigureRequest.Actions.add()
        action.AddConfigItem.EnableAutoSplit = True
        config_item = action.AddConfigItem.ConfigItem

        for name, proto in self.cms.items():
            config = config_item.Config.NamedConfigs.add()
            config.Name = f"Cloud.NBS.{name}"
            config.Config = MessageToString(proto, as_one_line=True).encode()

        tenant_and_node_type = config_item.UsageScope.TenantAndNodeTypeFilter
        tenant_and_node_type.Tenant = '/Root/nbs'
        tenant_and_node_type.NodeType = self.node_type

        config_item.MergeStrategy = 1  # OVERWRITE

        response = self.__ydb.client.invoke(request, 'ConsoleRequest')

        assert response.Status.Code == StatusIds.SUCCESS


def generate_server_txt(server_port, data_port):
    app = TServerAppConfig()
    server = TServerConfig()
    server.Port = server_port
    server.DataPort = data_port
    app.ServerConfig.CopyFrom(server)
    app.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    return app


def generate_location_txt():
    location = TLocation()
    location.Rack = "rack"

    return location


def generate_sys_txt():
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


def generate_log_txt():
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
        b"BLOCKSTORE_EXTERNAL_ENDPOINT",
        b"BLOCKSTORE_VHOST",
        b"BLOCKSTORE_RDMA",
    ]

    log_config = TLogConfig()
    for service_name in services_info:
        log_config.Entry.add(Component=service_name, Level=6)

    for service_name in services_dbg:
        log_config.Entry.add(Component=service_name, Level=7)

    log_config.AllowDropEntries = False

    return log_config


def generate_diag_txt():
    diag = TDiagnosticsConfig()
    diag.ProfileLogTimeThreshold = 100
    diag.SlowRequestSamplingRate = 0

    return diag


def generate_storage_txt():
    storage = TStorageServiceConfig()
    storage.SchemeShardDir = "/Root/nbs"
    storage.DiskSpaceScoreThrottlingEnabled = True
    return storage


def generate_dyn_ns_txt():
    configs = TDynamicNameserviceConfig()
    configs.MaxStaticNodeId = 1000
    configs.MaxDynamicNodeId = 1064
    return configs


def generate_dr_proxy_txt():
    config = TDiskRegistryProxyConfig()
    config.Owner = 16045690984503103501
    config.OwnerIdx = 2
    return config


def generate_disk_agent_txt(
        agent_id,
        file_devices=None,
        device_erase_method=None,
        storage_discovery_config=None):

    config = TDiskAgentConfig()
    config.Enabled = True
    config.DedicatedDiskAgent = True
    config.Backend = DISK_AGENT_BACKEND_AIO
    config.DirectIoFlagDisabled = True
    config.AgentId = agent_id
    config.NvmeTarget.Nqn = "nqn.2018-09.io.spdk:cnode1"
    config.AcquireRequired = True
    config.RegisterRetryTimeout = 1000  # 1 second
    config.ShutdownTimeout = 0
    config.IOParserActorCount = 4
    config.OffloadAllIORequestsParsingEnabled = True
    config.IOParserActorAllocateStorageEnabled = True
    config.PathsPerFileIOService = 1
    config.UseLocalStorageSubmissionThread = False
    config.UseOneSubmissionThreadPerAIOServiceEnabled = True

    if device_erase_method is not None:
        config.DeviceEraseMethod = EDeviceEraseMethod.Value(device_erase_method)

    if file_devices is not None:
        for device in file_devices:
            config.FileDevices.add().CopyFrom(
                ParseDict(device, TFileDeviceArgs()))

    if storage_discovery_config is not None:
        config.StorageDiscoveryConfig.CopyFrom(
            ParseDict(storage_discovery_config, TStorageDiscoveryConfig()))

    return config


def storage_config_with_default_limits():
    bw = 1 << 5     # 32 MiB/s
    iops = 1 << 16

    storage = TStorageServiceConfig()
    storage.ThrottlingEnabledSSD = True
    storage.ThrottlingEnabled = True

    storage.SSDUnitReadBandwidth = bw
    storage.SSDUnitWriteBandwidth = bw
    storage.SSDMaxReadBandwidth = bw
    storage.SSDMaxWriteBandwidth = bw
    storage.SSDUnitReadIops = iops
    storage.SSDUnitWriteIops = iops
    storage.SSDMaxReadIops = iops
    storage.SSDMaxWriteIops = iops

    storage.HDDUnitReadBandwidth = bw
    storage.HDDUnitWriteBandwidth = bw
    storage.HDDMaxReadBandwidth = bw
    storage.HDDMaxWriteBandwidth = bw
    storage.HDDUnitReadIops = iops
    storage.HDDUnitWriteIops = iops
    storage.HDDMaxReadIops = iops
    storage.HDDMaxWriteIops = iops

    return storage
