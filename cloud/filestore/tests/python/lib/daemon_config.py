import os

import yatest.common as common

from google.protobuf.text_format import MessageToString

from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
from contrib.ydb.core.protos.config_pb2 import (
    TDomainsConfig,
    TStaticNameserviceConfig,
    TLogConfig,
    TActorSystemConfig,
    TDynamicNameserviceConfig,
)
from contrib.ydb.core.protos.auth_pb2 import TAuthConfig
import contrib.ydb.tests.library.common.yatest_common as yatest_common

from cloud.filestore.config.diagnostics_pb2 import TDiagnosticsConfig
from cloud.filestore.config.storage_pb2 import TStorageConfig

from cloud.storage.core.protos.media_pb2 import STORAGE_MEDIA_HDD
from cloud.storage.core.tools.testing.access_service.lib import AccessService

LOG_WARN = 4
LOG_NOTICE = 5
LOG_INFO = 6
LOG_DEBUG = 7
LOG_TRACE = 8


def get_directories():
    output_path = yatest_common.output_path()
    cwd = get_unique_path_for_current_test(output_path=output_path, sub_folder="")
    configs_dir = get_unique_path_for_current_test(output_path=output_path, sub_folder="nfs_configs")

    ensure_path_exists(cwd)
    ensure_path_exists(configs_dir)

    return (cwd, configs_dir)


class FilestoreDaemonConfigGenerator:
    def __init__(
        self,
        binary_path,
        config_file=None,
        storage_config_file=None,
        app_config=None,
        diag_config_file=None,
        profile_log=None,
        verbose=False,
        service_type=None,
        kikimr_port=0,
        domain=None,
        restart_interval=None,
        restart_flag=None,
        access_service_port=0,
        storage_config=None,
        use_secure_registration=False,
        secure=False,
        access_service_type=AccessService,
        ic_port=None,
    ):
        self.__binary_path = binary_path
        self.__working_dir, self.__configs_dir = get_directories()

        self.__app_config = app_config
        self.__service_type = service_type
        self.__verbose = verbose
        self.__kikimr_port = kikimr_port
        self.__domain = domain
        self.__app_config_file_path = self.__config_file_path(config_file)
        self.__storage_config_file_path = self.__config_file_path(storage_config_file)
        self.__storage_config = storage_config or TStorageConfig()

        self.__profile_log_path = self.__profile_file_path(profile_log)

        if diag_config_file:
            self.diag_config_file_path = self.__config_file_path(diag_config_file)
        else:
            self.diag_config_file_path = None

        self.__restart_interval = restart_interval
        self.__restart_flag = restart_flag

        self._port_manager = yatest_common.PortManager()
        self.__port = self._port_manager.get_port()
        self.__mon_port = self._port_manager.get_port()
        self.__ic_port = self._port_manager.get_port() if ic_port is None else ic_port
        self.__access_service_type = access_service_type
        self.__access_service_port = access_service_port

        self.__use_secure_registration = use_secure_registration

        if secure:
            self.__secure_port = self._port_manager.get_port()
            self.__app_config.ServerConfig.SecurePort = self.__secure_port

        with open(self.__app_config_file_path, "w") as config_file:
            if self.__app_config:
                config_file.write(MessageToString(self.__app_config))
                config_file.flush()

    @property
    def port(self):
        return self.__port

    @property
    def secure_port(self):
        return self.__secure_port

    @property
    def mon_port(self):
        return self.__mon_port

    @property
    def working_dir(self):
        return self.__working_dir

    @property
    def configs_dir(self):
        return self.__configs_dir

    @property
    def ic_port(self):
        return self.__ic_port

    @property
    def kikimr_port(self):
        return self.__kikimr_port

    def __generate_domains_txt(self, domains_txt):
        config = TDomainsConfig()
        if domains_txt is not None:
            config.CopyFrom(domains_txt)

        # TODO
        return config

    def __generate_names_txt(self, names_txt):
        config = TStaticNameserviceConfig()
        if names_txt is not None:
            config.CopyFrom(names_txt)

        # TODO
        return config

    def __generate_log_txt(self):
        kikimr_services = [
            "BS_NODE",
            "BS_PROXY",
            "BS_PROXY_COLLECT",
            "BS_PROXY_DISCOVER",
            "BS_PROXY_PUT",
            "BS_QUEUE",
            "INTERCONNECT",
            "INTERCONNECT_NETWORK",
            "INTERCONNECT_SESSION",
            "INTERCONNECT_STATUS",
            "LABELS_MAINTAINER",
            "OPS_COMPACT",
            "SCHEME_BOARD_SUBSCRIBER",
            "TABLET_EXECUTOR",
            "TABLET_FLATBOOT",
            "TABLET_MAIN",
            "TABLET_OPS_HOST",
            "TENANT_POOL",
            "TX_PROXY",
        ]

        filestore_services = [
            "NFS_HIVE_PROXY",
            "NFS_SS_PROXY",
            # "NFS_TABLET_PROXY",
            "NFS_FUSE",
            "NFS_SERVER",
            "NFS_SERVICE",
            "NFS_SERVICE_WORKER",
            "NFS_TABLET",
            "NFS_TABLET_WORKER",
            "NFS_VHOST",
            "NFS_TRACE",
        ]

        filestore_level = LOG_DEBUG if self.__verbose else LOG_INFO
        kikimr_level = LOG_NOTICE if self.__verbose else LOG_WARN

        config = TLogConfig()
        for service_name in filestore_services:
            config.Entry.add(Component=service_name.encode(), Level=filestore_level)

        for service_name in kikimr_services:
            config.Entry.add(Component=service_name.encode(), Level=kikimr_level)
        config.DefaultLevel = kikimr_level

        return config

    def __generate_sys_txt(self):
        config = TActorSystemConfig()
        config.Scheduler.Resolution = 2048
        config.Scheduler.SpinThreshold = 0
        config.Scheduler.ProgressThreshold = 10000

        config.SysExecutor = 0
        config.Executor.add(Type=config.TExecutor.EType.Value("BASIC"), Threads=4, SpinThreshold=20, Name="System")

        config.UserExecutor = 1
        config.Executor.add(Type=config.TExecutor.EType.Value("BASIC"), Threads=4, SpinThreshold=20, Name="User")

        config.BatchExecutor = 2
        config.Executor.add(Type=config.TExecutor.EType.Value("BASIC"), Threads=4, SpinThreshold=20, Name="Batch")

        config.IoExecutor = 3
        config.Executor.add(Type=config.TExecutor.EType.Value("IO"), Threads=1, Name="IO")

        config.Executor.add(
            Type=config.TExecutor.EType.Value("BASIC"),
            Threads=1,
            SpinThreshold=10,
            Name="IC",
            TimePerMailboxMicroSecs=100,
        )
        config.ServiceExecutor.add(ServiceName="Interconnect", ExecutorId=4)

        return config

    def __generate_storage_txt(self, disableLocalService=False):
        config = TStorageConfig()
        config.MergeFrom(self.__storage_config)

        config.DisableLocalService = disableLocalService

        config.HDDSystemChannelPoolKind = "hdd"
        config.HDDLogChannelPoolKind = "hdd"
        config.HDDIndexChannelPoolKind = "hdd"
        config.HDDFreshChannelPoolKind = "hdd"
        config.HDDMixedChannelPoolKind = "hdd"

        # FIXME: no ssd in the recipe
        config.SSDSystemChannelPoolKind = "hdd"
        config.SSDLogChannelPoolKind = "hdd"
        config.SSDIndexChannelPoolKind = "hdd"
        config.SSDFreshChannelPoolKind = "hdd"
        config.SSDMixedChannelPoolKind = "hdd"

        config.HybridSystemChannelPoolKind = "hdd"
        config.HybridLogChannelPoolKind = "hdd"
        config.HybridIndexChannelPoolKind = "hdd"
        config.HybridFreshChannelPoolKind = "hdd"
        config.HybridMixedChannelPoolKind = "hdd"

        return config

    def __generate_diag_txt(self):
        config = TDiagnosticsConfig()

        config.ProfileLogTimeThreshold = 100
        config.SamplingRate = 100000
        config.SlowRequestSamplingRate = 1000
        config.HDDSlowRequestThreshold = 1000
        config.SSDSlowRequestThreshold = 1000
        config.RequestThresholds.add(Default=1000)
        config.RequestThresholds.add(Default=1000, MediaKind=STORAGE_MEDIA_HDD)
        config.LWTraceShuttleCount = 50000

        return config

    def __generate_dyn_ns_txt(self):
        config = TDynamicNameserviceConfig()
        config.MaxStaticNodeId = 1000
        config.MaxDynamicNodeId = 1064
        return config

    def __generate_auth_txt(self, port):
        auth_config = TAuthConfig()
        auth_config.UseAccessService = True
        auth_config.UseAccessServiceTLS = False
        auth_config.UseBlackBox = False
        auth_config.UseStaff = False
        auth_config.AccessServiceEndpoint = "localhost:{}".format(port)
        auth_config.AccessServiceType = self.__access_service_type.access_service_type
        return auth_config

    def __config_file_path(self, name):
        return os.path.join(self.__configs_dir, name)

    def __profile_file_path(self, name):
        return os.path.join(self.__working_dir, name)

    def __write_configs(self):
        for name, proto in self.__proto_configs.items():
            path = self.__config_file_path(name)
            with open(path, "w") as config_file:
                config_file.write(MessageToString(proto))
                config_file.flush()

    def generate_configs(self, domains_txt, names_txt):
        self.__proto_configs = {}
        if self.__service_type == "kikimr":
            self.__proto_configs.update(
                {
                    "domains.txt": self.__generate_domains_txt(domains_txt),
                    "names.txt": self.__generate_names_txt(names_txt),
                    "log.txt": self.__generate_log_txt(),
                    "sys.txt": self.__generate_sys_txt(),
                    "storage.txt": self.__generate_storage_txt(),
                    "storage-nolocal.txt": self.__generate_storage_txt(True),
                    "diag.txt": self.__generate_diag_txt(),
                    "dyn_ns.txt": self.__generate_dyn_ns_txt(),
                    "auth.txt": self.__generate_auth_txt(self.__access_service_port),
                }
            )
        self.__write_configs()

    def generate_aux_params(self):
        return []

    def generate_command(self):
        command = [
            self.__binary_path,
            "--app-config",
            self.__app_config_file_path,
            "--server-port",
            str(self.__port),
            "--service",
            self.__service_type,
            "--mon-port",
            str(self.__mon_port),
            "--profile-file",
            self.__profile_log_path,
        ] + self.generate_aux_params()

        if self.diag_config_file_path:
            command += [
                "--diag-file", self.diag_config_file_path
            ]

        if self.__service_type == "kikimr":
            command += [
                "--domain",
                self.__domain,
                "--ic-port",
                str(self.__ic_port),
                "--diag-file",
                self.__config_file_path("diag.txt"),
                "--domains-file",
                self.__config_file_path("domains.txt"),
                "--naming-file",
                self.__config_file_path("names.txt"),
                "--log-file",
                self.__config_file_path("log.txt"),
                "--sys-file",
                self.__config_file_path("sys.txt"),
                "--storage-file",
                self.__storage_config_file_path,
                "--dynamic-naming-file",
                self.__config_file_path("dyn_ns.txt"),
                "--suppress-version-check",
                "--load-configs-from-cms",
                "--node-broker",
                "localhost:{}".format(self.__kikimr_port),
            ]

            if self.__use_secure_registration:
                command += ["--use-secure-registration"]

            if self.__access_service_port:
                command += [
                    "--auth-file",
                    self.__config_file_path("auth.txt"),
                ]

        if self.__restart_interval:
            launcher_path = common.binary_path(
                "cloud/storage/core/tools/testing/unstable-process/storage-unstable-process"
            )

            command = [
                launcher_path,
                "--ping-port",
                str(self.mon_port),
                "--ping-timeout",
                str(2),
                "--restart-interval",
                str(self.__restart_interval),
                "--cmdline",
                " ".join(command),
            ] + (["--allow-restart-flag", self.__restart_flag] if self.__restart_flag else [])

        return command

    def get_domain(self):
        if not self.__storage_config.HasField('SchemeShardDir'):
            return None
        return self.__storage_config.SchemeShardDir

class FilestoreServerConfigGenerator(FilestoreDaemonConfigGenerator):
    def __init__(
        self,
        binary_path,
        app_config,
        service_type,
        verbose,
        kikimr_port=0,
        domain=None,
        restart_interval=None,
        access_service_port=0,
        storage_config=None,
        diag_config_file=None,
        use_secure_registration=False,
        secure=False,
        access_service_type=AccessService,
        ic_port=None,
    ):
        super().__init__(
            binary_path,
            config_file="server.txt",
            storage_config_file="storage.txt",
            app_config=app_config,
            diag_config_file="diag.txt",
            profile_log="nfs-profile.log",
            service_type=service_type,
            verbose=verbose,
            kikimr_port=kikimr_port,
            domain=domain,
            restart_interval=restart_interval,
            restart_flag=None,
            access_service_port=access_service_port,
            storage_config=storage_config,
            use_secure_registration=use_secure_registration,
            secure=secure,
            access_service_type=access_service_type,
            ic_port=ic_port
        )

        self.__diag_config = self.__generate_diag_txt()
        with open(self.diag_config_file_path, "w") as config_file:
            if self.__diag_config:
                config_file.write(MessageToString(self.__diag_config))
                config_file.flush()

    def __generate_diag_txt(self):
        diag = TDiagnosticsConfig()
        diag.ProfileLogTimeThreshold = 100
        return diag


class FilestoreVhostConfigGenerator(FilestoreDaemonConfigGenerator):
    def __init__(
        self,
        binary_path,
        app_config,
        service_type,
        verbose,
        kikimr_port,
        domain,
        restart_interval=None,
        restart_flag=None,
        access_service_port=0,
        storage_config=None,
        use_secure_registration=False,
        ic_port=None,
        access_service_type=AccessService,
        secure=False,
        diag_config_file=None,
    ):
        super().__init__(
            binary_path,
            config_file="vhost.txt",
            storage_config_file="storage-nolocal.txt",
            app_config=app_config,
            diag_config_file="diag.txt",
            profile_log="vhost-profile.log",
            service_type=service_type,
            verbose=verbose,
            kikimr_port=kikimr_port,
            domain=domain,
            restart_interval=restart_interval,
            restart_flag=restart_flag,
            access_service_port=access_service_port,
            storage_config=storage_config,
            use_secure_registration=use_secure_registration,
            ic_port=ic_port,
            access_service_type=access_service_type,
            secure=secure
        )

        self.__local_service_port = self._port_manager.get_port()

        self.__diag_config = self.__generate_diag_txt()
        with open(self.diag_config_file_path, "w") as config_file:
            if self.__diag_config:
                config_file.write(MessageToString(self.__diag_config))
                config_file.flush()

    def generate_aux_params(self):
        return ["--local-service-port", str(self.__local_service_port)]

    @property
    def local_service_port(self):
        return self.__local_service_port

    def __generate_diag_txt(self):
        diag = TDiagnosticsConfig()
        diag.ProfileLogTimeThreshold = 100
        return diag
