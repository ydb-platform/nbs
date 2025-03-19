import copy
import os
import logging
import subprocess
import contrib.ydb.tests.library.common.yatest_common as yatest_common

from cloud.blockstore.config.diagnostics_pb2 import TDiagnosticsConfig
from cloud.blockstore.config.disk_pb2 import TDiskRegistryProxyConfig
from cloud.blockstore.config.root_kms_pb2 import TRootKmsConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig
from cloud.blockstore.config.server_pb2 import TServerAppConfig, TKikimrServiceConfig, TServerConfig, TLocation
from cloud.storage.core.config.features_pb2 import TFeaturesConfig
from cloud.storage.core.tools.common.python.core_pattern import core_pattern
from cloud.storage.core.tools.common.python.daemon import Daemon
from cloud.storage.core.tools.testing.access_service.lib import AccessService
from google.protobuf.text_format import MessageToBytes, MessageToString
from contrib.ydb.core.protos.auth_pb2 import TAuthConfig
from contrib.ydb.core.protos.config_pb2 import TActorSystemConfig
from contrib.ydb.core.protos.config_pb2 import TDomainsConfig
from contrib.ydb.core.protos.config_pb2 import TDynamicNameserviceConfig
from contrib.ydb.core.protos.config_pb2 import TLogConfig
from contrib.ydb.core.protos import console_config_pb2 as console
from contrib.ydb.core.protos import msgbus_pb2 as msgbus
from contrib.ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists

logger = logging.getLogger(__name__)


class LocalNbs(Daemon):

    def __init__(
            self,
            grpc_port,
            domains_txt=None,
            server_app_config=None,
            contract_validation=False,
            config_sub_folder=None,
            storage_config_patches=None,
            tracking_enabled=False,
            enable_access_service=False,
            enable_tls=False,
            discovery_config=None,
            restart_interval=None,
            dynamic_storage_pools=None,
            load_configs_from_cms=False,
            nbs_secure_port=None,
            nbs_port=None,
            kikimr_binary_path=None,
            nbs_binary_path=None,
            features_config_patch=None,
            grpc_trace=None,
            ydbstats_config=None,
            compute_config=None,
            kms_config=None,
            ping_path='/blockstore',
            rack="the_rack",
            use_ic_version_check=False,
            use_secure_registration=False,
            grpc_ssl_port=None,
            access_service_type=AccessService,
    ):

        if dynamic_storage_pools is not None:
            assert len(dynamic_storage_pools) >= 2
            self.__dynamic_storage_pools = dynamic_storage_pools
        else:
            self.__dynamic_storage_pools = [
                dict(name="dynamic_storage_pool:1", kind="rot"),
                dict(name="dynamic_storage_pool:2", kind="ssd")]

        self.__service_type = LocalNbs.__get_service_type(server_app_config)
        self.__port_manager = yatest_common.PortManager()
        self.__grpc_port = grpc_port
        self.__grpc_trace = grpc_trace
        self.__grpc_ssl_port = grpc_ssl_port
        if nbs_port is None:
            self.__nbs_port = self.__port_manager.get_port()
        else:
            self.__nbs_port = nbs_port
        self.__nbs_data_port = self.__port_manager.get_port()
        if nbs_secure_port is not None:
            self.__nbs_secure_port = nbs_secure_port
        else:
            self.__nbs_secure_port = self.__port_manager.get_port() if enable_tls else 0
        self.__ic_port = self.__port_manager.get_port()
        self.__profile_log_name = "profile-%s.log" % self.__ic_port
        self.__mon_port = self.__port_manager.get_port()
        if nbs_binary_path is not None:
            self.__binary_path = nbs_binary_path
        else:
            self.__binary_path = yatest_common.binary_path(
                "cloud/blockstore/apps/server/nbsd")

        self.__output_path = yatest_common.output_path()
        self.__cwd = get_unique_path_for_current_test(
            output_path=self.__output_path,
            sub_folder=""
        )
        ensure_path_exists(self.__cwd)

        self.__config_sub_folder = config_sub_folder
        if self.__config_sub_folder is None or len(self.__config_sub_folder) == 0:
            self.__config_sub_folder = "nbs_configs"

        self.__config_dir = self.config_path()
        self.__rack = rack

        self.__kikimr_binary_path = kikimr_binary_path
        self.__server_app_config = server_app_config
        self.contract_validation = contract_validation
        self.storage_config_patches = storage_config_patches
        self.features_config_patch = features_config_patch
        self.tracking_enabled = tracking_enabled
        self.ydbstats_config = ydbstats_config
        self.compute_config = compute_config
        self.kms_config = kms_config

        self.__proto_configs = {
            "diag.txt": self.__generate_diag_txt(),
            "domains.txt": domains_txt if domains_txt is not None else TDomainsConfig(),
            "server-log.txt": self.__generate_server_log_txt(),
            "server-sys.txt": self.__generate_server_sys_txt(),
            "server.txt": self.__generate_server_txt(),
            "storage.txt": self.__generate_storage_txt(),
            "features.txt": self.__generate_features_txt(),
            "dyn_ns.txt": self.__generate_dyn_ns_txt(),
            "dr_proxy.txt": self.__generate_dr_proxy_txt(),
            "location.txt": self.__generate_location_txt(),
        }

        if ydbstats_config is not None:
            self.__proto_configs["ydbstats.txt"] = ydbstats_config

        if compute_config is not None:
            self.__proto_configs["compute.txt"] = compute_config

        if kms_config is not None:
            self.__proto_configs["kms.txt"] = kms_config

        if discovery_config is not None:
            self.__proto_configs["discovery.txt"] = discovery_config
            self.__use_discovery = True
        else:
            self.__use_discovery = False

        if self.__server_app_config is None or self.__server_app_config.HasField('KikimrServiceConfig'):
            self.init_scheme()

        root_kms_port = os.environ.get("FAKE_ROOT_KMS_PORT")
        if root_kms_port is not None:
            root_kms = TRootKmsConfig()
            root_kms.Address = f'localhost:{root_kms_port}'
            root_kms.KeyId = 'nbs'
            root_kms.RootCertsFile = os.environ.get("FAKE_ROOT_KMS_CA")
            root_kms.CertChainFile = os.environ.get("FAKE_ROOT_KMS_CLIENT_CRT")
            root_kms.PrivateKeyFile = os.environ.get("FAKE_ROOT_KMS_CLIENT_KEY")
            self.__proto_configs['root-kms.txt'] = root_kms

        self.__access_service = None
        if enable_access_service:
            host = "localhost"
            port = self.__port_manager.get_port()
            control_server_port = self.__port_manager.get_port()
            self._access_service_type = access_service_type
            self.__access_service = self._access_service_type(host, port, control_server_port)
            self.__proto_configs["auth.txt"] = self.__generate_auth_txt(port)

        self.__load_configs_from_cms = load_configs_from_cms
        self.__use_ic_version_check = use_ic_version_check
        self.__restart_interval = restart_interval
        self.__ping_path = ping_path
        self.__use_secure_registration = use_secure_registration

        self.__init_daemon()

    def __init_daemon(self):
        if self.storage_config_patches is not None and len(self.storage_config_patches) > 0:
            for i in range(len(self.storage_config_patches)):
                self.__proto_configs["storage-%s.txt" % i] = self.__generate_patched_storage_txt(i)

        cp = None
        if self.__binary_path:
            cp = core_pattern(self.__binary_path, self.__cwd)

        restart_allowed = False
        if self.__restart_interval is not None:
            restart_allowed = True

        commands = self.__make_start_commands()
        logger.info("commands is {}".format(commands))
        super(LocalNbs, self).__init__(
            commands=commands, cwd=self.__cwd,
            restart_allowed=restart_allowed,
            restart_interval=self.__restart_interval, ping_port=self.__mon_port,
            ping_path=self.__ping_path, ping_success_codes=[200],
            core_pattern=cp)

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

        if self.__kikimr_binary_path is None:
            self.__kikimr_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")

        command = [
            self.__kikimr_binary_path,
            "--server",
            "grpc://localhost:" + str(self.__grpc_port),
            "db", "schema", "exec", scheme_op
        ]

        logger.info("Init scheme {}".format(command))
        with open(self.__cwd + "/ydbd_output.log", "w") as ydbd_output:
            subprocess.check_call(command, stdout=ydbd_output, stderr=ydbd_output)

    @property
    def nbs_port(self):
        return self.__nbs_port

    @property
    def nbs_data_port(self):
        return self.__nbs_data_port

    @property
    def nbs_secure_port(self):
        return self.__nbs_secure_port

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

    @property
    def profile_log_path(self):
        return os.path.join(self.__cwd, self.__profile_log_name)

    @property
    def cwd(self):
        return self.__cwd

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
            b"BLOCKSTORE_RDMA",
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

        if self.__grpc_trace:
            log_config.DefaultLevel = 6

        log_config.AllowDropEntries = False

        return log_config

    def __generate_diag_txt(self):
        diag = TDiagnosticsConfig()
        diag.ProfileLogTimeThreshold = 100
        diag.SlowRequestSamplingRate = int(self.tracking_enabled)
        return diag

    def __init_storage_txt(self, storage):
        storage.SchemeShardDir = "/Root/nbs"
        storage.DiskSpaceScoreThrottlingEnabled = True
        return storage

    def __generate_storage_txt(self):
        storage = TStorageServiceConfig()
        self.__init_storage_txt(storage)
        return storage

    def __generate_patched_storage_txt(self, i):
        storage = TStorageServiceConfig()
        storage.CopyFrom(self.storage_config_patches[i])
        self.__init_storage_txt(storage)
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
        auth_config.AccessServiceType = self.__access_service.access_service_type
        return auth_config

    def __generate_dr_proxy_txt(self):
        config = TDiskRegistryProxyConfig()
        config.Owner = 16045690984503103501
        config.OwnerIdx = 2
        return config

    def __generate_location_txt(self):
        configs = TLocation()
        configs.Rack = self.__rack
        return configs

    def config_path(self):
        config_path = get_unique_path_for_current_test(
            output_path=self.__output_path,
            sub_folder=self.__config_sub_folder
        )
        ensure_path_exists(config_path)
        return config_path

    def write_configs(self):
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

        # setup NBS config

        req = msgbus.TConsoleRequest()
        configs_config = req.SetConfigRequest.Config.ConfigsConfig
        restrictions = configs_config.UsageScopeRestrictions
        restrictions.AllowedTenantUsageScopeKinds.append(
            console.TConfigItem.NamedConfigsItem)
        restrictions.AllowedNodeTypeUsageScopeKinds.append(
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
            custom_cfg.Name = "Cloud.NBS." + config_name
            custom_cfg.Config = MessageToBytes(proto)
        response = kikimr.invoke(req, 'ConsoleRequest')
        assert response.Status.Code == StatusIds.SUCCESS

    def __start_daemon(self):
        self.write_configs()
        super(LocalNbs, self).start()
        self.__pid = super(LocalNbs, self).pid
        logger.info(
            "blockstore-server started and initialized at {}".format(self.config_path()))

    def __make_start_commands(self):

        def append_conf_file_arg(command, config_path, option_name, conf_file):
            conf_file = os.path.join(config_path, conf_file)
            command.append("{name}".format(name=option_name))
            command.append("{conf_file}".format(conf_file=conf_file))

        command = [self.__binary_path]
        command += [
            "--service", self.__service_type,
            "--server-port", str(self.__nbs_port),
            "--data-server-port", str(self.__nbs_data_port),
            "--secure-server-port", str(self.__nbs_secure_port),
            "--mon-port", str(self.__mon_port),
            "--profile-file", self.profile_log_path,
        ]

        if self.__grpc_trace:
            command += ["--grpc-trace"]

        if not self.__load_configs_from_cms:
            config_files = {
                "--diag-file": "diag.txt",
                "--server-file": "server.txt",
                "--dr-proxy-file": "dr_proxy.txt",
            }
            if self.__use_discovery:
                config_files["--discovery-file"] = "discovery.txt"

            for option, filename in config_files.items():
                append_conf_file_arg(command, self.config_path(), option,
                                     filename)

        if self.__service_type == 'local' or self.__service_type == 'null':
            commands = []
            commands.append(command)
            logger.info(commands)
            return commands

        # fill command with args for kikimr service

        command += [
            "--domain", "Root",
            "--ic-port", str(self.__ic_port),
            "--scheme-shard-dir", "nbs",
            "--load-configs-from-cms",
        ]

        if not self.__use_secure_registration or self.__grpc_ssl_port is None:
            command += [
                "--node-broker", "localhost:" + str(self.__grpc_port),
            ]
        else:
            command += [
                "--node-broker", "localhost:" + str(self.__grpc_ssl_port),
                "--use-secure-registration",
            ]

        if not self.__use_ic_version_check:
            command.append("--suppress-version-check")

        if self.ydbstats_config is not None:
            command += [
                "--ydbstats-file",
                os.path.join(self.config_path(), "ydbstats.txt")
            ]

        if self.compute_config is not None:
            command += ["--compute-file", os.path.join(self.config_path(), "compute.txt")]

        if self.kms_config is not None:
            command += ["--kms-file", os.path.join(self.config_path(), "kms.txt")]

        if 'root-kms.txt' in self.__proto_configs:
            command += ["--root-kms-file", os.path.join(self.config_path(), "root-kms.txt")]

        append_conf_file_arg(command, self.config_path(),
                             "--location-file", "location.txt")

        if not self.__load_configs_from_cms:
            config_files = {
                "--domains-file": "domains.txt",
                "--log-file": "server-log.txt",
                "--sys-file": "server-sys.txt",
                "--features-file": "features.txt",
                "--dynamic-naming-file": "dyn_ns.txt",
            }
            if self.__access_service:
                config_files["--auth-file"] = "auth.txt"

            for option, filename in config_files.items():
                append_conf_file_arg(command, self.config_path(), option,
                                     filename)

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

        logger.info(commands)
        return commands

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
        super(LocalNbs, self).stop()

    def stop(self):
        if self.__access_service:
            self.__access_service.stop()
        self.__stop_daemon()

    def restart(self):
        self.__stop_daemon()
        self.__init_daemon()
        self.__start_daemon()
