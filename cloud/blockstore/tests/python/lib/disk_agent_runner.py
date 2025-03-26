import copy
import os
import logging
import subprocess
import contrib.ydb.tests.library.common.yatest_common as yatest_common

from contrib.ydb.tests.library.harness.daemon import Daemon
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
from cloud.blockstore.config.diagnostics_pb2 import TDiagnosticsConfig
from cloud.blockstore.config.disk_pb2 import TDiskRegistryProxyConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig
from cloud.blockstore.config.server_pb2 import TServerAppConfig, TKikimrServiceConfig, TServerConfig, TLocation
from cloud.storage.core.config.features_pb2 import TFeaturesConfig
from cloud.storage.core.tools.common.python.core_pattern import core_pattern
from cloud.storage.core.tools.testing.access_service.lib import AccessService
from contrib.ydb.core.protos.auth_pb2 import TAuthConfig
from contrib.ydb.core.protos.config_pb2 import TActorSystemConfig
from contrib.ydb.core.protos.config_pb2 import TDynamicNameserviceConfig
from contrib.ydb.core.protos.config_pb2 import TLogConfig
from contrib.ydb.core.protos import console_config_pb2 as console
from contrib.ydb.core.protos import msgbus_pb2 as msgbus
from google.protobuf.text_format import MessageToBytes, MessageToString
from contrib.ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

logger = logging.getLogger(__name__)


class LocalDiskAgent(Daemon):

    def __init__(
            self,
            grpc_port,
            domains_txt,
            server_app_config=None,
            contract_validation=False,
            config_sub_folder=None,
            log_sub_folder=None,
            storage_config_patches=None,
            tracking_enabled=False,
            enable_access_service=False,
            enable_tls=False,
            discovery_config=None,
            restart_interval=None,
            downtime_after_restart=None,
            dynamic_storage_pools=None,
            load_configs_from_cms=False,
            kikimr_binary_path=None,
            disk_agent_binary_path=None,
            features_config_patch=None,
            rack="rack",
            node_type=None,
            grpc_trace=None,
            temporary_agent=False):
        if dynamic_storage_pools is not None:
            assert len(dynamic_storage_pools) >= 2
            self.__dynamic_storage_pools = dynamic_storage_pools
        else:
            self.__dynamic_storage_pools = [
                dict(name="dynamic_storage_pool:1", kind="rot"),
                dict(name="dynamic_storage_pool:2", kind="ssd")]

        self.__service_type = LocalDiskAgent.__get_service_type(server_app_config)
        self.__port_manager = yatest_common.PortManager()
        self.__grpc_port = grpc_port
        self.__grpc_trace = grpc_trace
        self.__ic_port = self.__port_manager.get_port()
        self.__mon_port = self.__port_manager.get_port()
        self.__temporary_agent = temporary_agent
        if disk_agent_binary_path is not None:
            self.__binary_path = disk_agent_binary_path
        else:
            self.__binary_path = yatest_common.binary_path(
                "cloud/blockstore/apps/disk_agent/diskagentd")
        if kikimr_binary_path is not None:
            self.__kikimr_binary_path = kikimr_binary_path
        else:
            self.__kikimr_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")

        self.__unstable_process_args = None

        if restart_interval is not None:
            self.__unstable_process_args = [
                "--restart-interval", str(restart_interval),
                "--ping-port", str(self.__mon_port),
                "--ping-success-codes", '200',
                "--ping-path", "/blockstore/disk_agent",
                "--ping-timeout", "2",
                # "-vvvvv",
            ]
            if downtime_after_restart is not None:
                self.__unstable_process_args += ['--downtime',
                                                 str(downtime_after_restart)]

        self.__output_path = yatest_common.output_path()
        self.__cwd = get_unique_path_for_current_test(
            output_path=self.__output_path,
            sub_folder=""
        )
        ensure_path_exists(self.__cwd)

        self.__config_sub_folder = config_sub_folder
        if self.__config_sub_folder is None or len(self.__config_sub_folder) == 0:
            self.__config_sub_folder = "disk_agent_configs"

        self.__log_sub_folder = log_sub_folder
        if self.__log_sub_folder is None or len(self.__log_sub_folder) == 0:
            self.__log_sub_folder = "disk_agent_logs"

        self.__server_app_config = server_app_config
        self.contract_validation = contract_validation
        self.storage_config_patches = storage_config_patches
        self.features_config_patch = features_config_patch
        self.tracking_enabled = tracking_enabled
        self.__node_type = node_type
        self.__rack = rack

        self.__proto_configs = {
            "diag.txt": self.__generate_diag_txt(),
            "domains.txt": domains_txt,
            "server-log.txt": self.__generate_server_log_txt(),
            "server-sys.txt": self.__generate_server_sys_txt(),
            "server.txt": self.__generate_server_txt(),
            "storage.txt": self.__generate_storage_txt(),
            "features.txt": self.__generate_features_txt(),
            "dyn_ns.txt": self.__generate_dyn_ns_txt(),
            "location.txt": self.__generate_location_txt(),
            "dr_proxy.txt": self.__generate_dr_proxy_txt(),
        }

        if storage_config_patches is not None and len(storage_config_patches) > 0:
            for i in range(len(storage_config_patches)):
                self.__proto_configs["storage-%s.txt" % i] = self.__generate_patched_storage_txt(i)

        if discovery_config is not None:
            self.__proto_configs["discovery.txt"] = discovery_config
            self.__use_discovery = True
        else:
            self.__use_discovery = False

        if self.__server_app_config is None or self.__server_app_config.HasField('KikimrServiceConfig'):
            self.init_scheme()

        self.__access_service = None
        if enable_access_service:
            host = "localhost"
            port = self.__port_manager.get_port()
            control_server_port = self.__port_manager.get_port()
            self.__access_service = AccessService(host, port, control_server_port)
            self.__proto_configs["auth.txt"] = self.__generate_auth_txt(port)

        self.__load_configs_from_cms = load_configs_from_cms

        cp = None
        if self.__binary_path:
            cp = core_pattern(self.__binary_path, self.__cwd)

        command = self.__make_start_command()
        logger.info("command is {}".format(" ".join(command)))
        super(LocalDiskAgent, self).__init__(
            command=command,
            cwd=self.__cwd,
            timeout=180,
            core_pattern=cp,
            stdout_file=os.path.join(self.log_path(
            ), 'temporary_agent_stdout.txt' if self.__temporary_agent else 'agent_stdout.txt'),
            stderr_file=os.path.join(self.log_path(
            ), 'temporary_agent_stderr.txt' if self.__temporary_agent else 'agent_stderr.txt'),
        )

    @staticmethod
    def __get_service_type(server_app_config):
        if server_app_config is None:
            return 'kikimr'
        if server_app_config.HasField('KikimrServiceConfig'):
            return 'kikimr'
        if server_app_config.HasField('LocalServiceConfig'):
            return 'local'
        return 'null'

    def init_scheme(self):
        scheme_op = """
ModifyScheme {
    WorkingDir: "/Root"
    OperationType: ESchemeOpCreateSubDomain
    SubDomain {
        Name: "nbs"
        Coordinators: 0
        Mediators: 0
        PlanResolution: 50
        TimeCastBucketsPerMediator: 2
"""

        for pool in self.__dynamic_storage_pools:
            scheme_op += """
        StoragePools {
            Name: "%s"
            Kind: "%s"
        }
""" % (pool['name'], pool['kind'])

        scheme_op += """
    }
}
"""

        command = [
            self.__kikimr_binary_path,
            "--server",
            "grpc://localhost:" + str(self.__grpc_port),
            "db", "schema", "exec", scheme_op
        ]

        logger.info("Init scheme {}".format(command))
        subprocess.check_call(command)

    @property
    def mon_port(self):
        return self.__mon_port

    @property
    def ic_port(self):
        return self.__ic_port

    @property
    def pid(self):
        return self.__pid

    @property
    def access_service(self):
        return self.__access_service

    def __generate_server_log_txt(self):
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
            b"BLOCKSTORE_DISK_AGENT",
            b"BLOCKSTORE_DISK_AGENT_WORKER",
        ]

        log_config = TLogConfig()
        for service_name in services_info:
            log_config.Entry.add(Component=service_name, Level=6)

        for service_name in services_dbg:
            log_config.Entry.add(Component=service_name, Level=7)

        if self.__grpc_trace:
            log_config.DefaultLevel = 6

        log_config.AllowDropEntries = False

        return log_config

    def __generate_diag_txt(self):
        diag = TDiagnosticsConfig()
        diag.ProfileLogTimeThreshold = 100
        diag.SlowRequestSamplingRate = int(self.tracking_enabled)
        return diag

    def __generate_storage_txt(self):
        storage = TStorageServiceConfig()
        storage.SchemeShardDir = "/Root/nbs"
        return storage

    def __generate_patched_storage_txt(self, i):
        storage = TStorageServiceConfig()
        storage.CopyFrom(self.storage_config_patches[i])
        storage.SchemeShardDir = "/Root/nbs"
        return storage

    def __generate_features_txt(self):
        features = TFeaturesConfig()
        if self.features_config_patch is not None:
            features.CopyFrom(self.features_config_patch)
        return features

    def __generate_dyn_ns_txt(self):
        configs = TDynamicNameserviceConfig()
        configs.MaxStaticNodeId = 1000
        configs.MaxDynamicNodeId = 1064
        return configs

    def __generate_location_txt(self):
        configs = TLocation()
        configs.Rack = self.__rack
        return configs

    def __generate_server_txt(self):
        if self.__server_app_config is None:
            server = TServerAppConfig()
            server.ServerConfig.CopyFrom(TServerConfig())
            server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
            server.ServerConfig.StrictContractValidation = self.contract_validation
        else:
            server = self.__server_app_config
        return server

    def __generate_server_sys_txt(self):
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

    def __generate_auth_txt(self, port):
        auth_config = TAuthConfig()
        auth_config.UseAccessService = True
        auth_config.UseAccessServiceTLS = False
        auth_config.UseBlackBox = False
        auth_config.UseStaff = False
        auth_config.AccessServiceEndpoint = "localhost:{}".format(port)
        return auth_config

    def __generate_dr_proxy_txt(self):
        config = TDiskRegistryProxyConfig()
        config.Owner = 16045690984503103501
        config.OwnerIdx = 2
        return config

    def config_path(self):
        config_path = get_unique_path_for_current_test(
            output_path=self.__output_path,
            sub_folder=self.__config_sub_folder
        )
        ensure_path_exists(config_path)
        return config_path

    def log_path(self):
        log_path = get_unique_path_for_current_test(
            output_path=self.__output_path,
            sub_folder=self.__log_sub_folder
        )
        ensure_path_exists(log_path)
        return log_path

    def __write_configs(self):
        for file_name, proto in self.__proto_configs.items():
            config_file_path = os.path.join(self.config_path(), file_name)
            with open(config_file_path, "w") as config_file:
                config_file.write(MessageToString(proto))

    def setup_cms(self, kikimr):
        name_table = {
            'server.txt': 'ServerAppConfig',
            'auth.txt': 'AuthConfig',
            'diag.txt': 'DiagnosticsConfig',
            'discovery.txt': 'DiscoveryServiceConfig',
            'ic.txt': 'InterconnectConfig',
            'server-sys.txt': 'ActorSystemConfig',
            'server-log.txt': 'LogConfig',
            'features.txt': 'FeaturesConfig'
        }

        req = msgbus.TConsoleRequest()
        action = req.ConfigureRequest.Actions.add()
        action.AddConfigItem.ConfigItem.Config.DomainsConfig.CopyFrom(
            self.__proto_configs["domains.txt"])
        action.AddConfigItem.EnableAutoSplit = True

        response = kikimr.invoke(req, 'ConsoleRequest')
        assert response.Status.Code == StatusIds.SUCCESS

        req = msgbus.TConsoleRequest()
        action = req.ConfigureRequest.Actions.add()
        action.AddConfigItem.ConfigItem.Config.DynamicNameserviceConfig.CopyFrom(
            self.__proto_configs["dyn_ns.txt"])

        response = kikimr.invoke(req, 'ConsoleRequest')
        assert response.Status.Code == StatusIds.SUCCESS

        # setup DiskAgent config

        req = msgbus.TConsoleRequest()
        configs_config = req.SetConfigRequest.Config.ConfigsConfig
        restrictions = configs_config.UsageScopeRestrictions
        restrictions.AllowedTenantUsageScopeKinds.append(
            console.TConfigItem.NamedConfigsItem)

        response = kikimr.invoke(req, 'ConsoleRequest')
        assert response.Status.Code == StatusIds.SUCCESS

        req = msgbus.TConsoleRequest()
        action = req.ConfigureRequest.Actions.add()
        action.AddConfigItem.ConfigItem.UsageScope.TenantAndNodeTypeFilter.\
            Tenant = '/Root/nbs'
        action.AddConfigItem.EnableAutoSplit = True
        action.AddConfigItem.ConfigItem.MergeStrategy = 1  # OVERWRITE

        for file_name, proto in self.__proto_configs.items():
            config_name = name_table.get(file_name)
            if not config_name:
                continue
            custom_cfg = action.AddConfigItem.ConfigItem.Config.NamedConfigs.\
                add()
            custom_cfg.Name = "Cloud.DiskAgent." + config_name
            custom_cfg.Config = MessageToBytes(proto)
        response = kikimr.invoke(req, 'ConsoleRequest')
        assert response.Status.Code == StatusIds.SUCCESS

    def __start_daemon(self):
        self.__write_configs()
        super(LocalDiskAgent, self).start()
        self.__pid = super(LocalDiskAgent, self).daemon.process.pid
        logger.info(
            "blockstore-disk-agent started and initialized at {}".format(self.config_path()))

    def __make_start_command(self):

        def append_conf_file_arg(command, config_path, option_name, conf_file):
            conf_file = os.path.join(config_path, conf_file)
            command.append("{name}".format(name=option_name))
            command.append("{conf_file}".format(conf_file=conf_file))

        command = [self.__binary_path]
        command += [
            "--domain", "Root",
            "--node-broker", "localhost:" + str(self.__grpc_port),
            "--ic-port", str(self.__ic_port),
            "--mon-port", str(self.__mon_port),
            "--scheme-shard-dir", "nbs",
            "--load-configs-from-cms"]

        if self.__temporary_agent:
            command += ["--temporary-agent"]

        if self.__node_type is not None:
            command += ["--node-type", self.__node_type]

        if not self.__load_configs_from_cms:
            config_files = {
                "--diag-file": "diag.txt",
                "--domains-file": "domains.txt",
                "--log-file": "server-log.txt",
                "--sys-file": "server-sys.txt",
                "--server-file": "server.txt",
                "--features-file": "features.txt",
                "--dynamic-naming-file": "dyn_ns.txt",
                "--location-file": "location.txt",
                "--dr-proxy-file": "dr_proxy.txt",
            }
            if self.__use_discovery:
                config_files["--discovery-file"] = "discovery.txt"
            if self.__access_service:
                config_files["--auth-file"] = "auth.txt"

            for option, filename in config_files.items():
                append_conf_file_arg(command, self.config_path(), option,
                                     filename)

        if self.__grpc_trace:
            command += ["--grpc-trace"]

        command += [
            "--profile-file",
            os.path.join(self.__cwd, "profile-%s.log" % self.__ic_port)
        ]

        commands = []

        if self.storage_config_patches is None or len(self.storage_config_patches) == 0:
            command += [
                "--storage-file",
                os.path.join(self.config_path(), "storage.txt")
            ]
            commands.append(command)
        else:
            for i in range(len(self.storage_config_patches)):
                command_copy = copy.deepcopy(command)
                command_copy += [
                    "--storage-file",
                    os.path.join(self.config_path(), "storage-%s.txt" % i)
                ]
                commands.append(command_copy)

        command = commands[0]

        if self.__unstable_process_args is not None:
            launcher_path = yatest_common.binary_path(
                "cloud/storage/core/tools/testing/unstable-process/storage-unstable-process")
            command = [
                launcher_path,
            ] + self.__unstable_process_args

            for cmd in commands:
                command += [
                    "--cmdline", " ".join(cmd)
                ]

        logger.info(command)

        return command

    @property
    def command(self):
        return self.daemon.command

    @property
    def returncode(self):
        return self.daemon.returncode

    def start(self):
        try:
            logger.debug("Working directory:\n" + self.__cwd)
            if self.__access_service:
                self.__access_service.start()
            self.__start_daemon()
            return
        except Exception:
            logger.exception("blockstore server start failed")
            self.stop()
            raise

    def __stop_daemon(self):
        super(LocalDiskAgent, self).stop()

    def stop(self):
        if self.__access_service:
            self.__access_service.stop()
        self.__stop_daemon()
