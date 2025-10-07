import yatest.common as common

from cloud.filestore.config.server_pb2 import TServerAppConfig, TKikimrServiceConfig
from cloud.filestore.config.storage_pb2 import TStorageConfig

from cloud.filestore.tests.python.lib.daemon_config import FilestoreServerConfigGenerator
from cloud.filestore.tests.python.lib.server import FilestoreServer, wait_for_filestore_server
from cloud.tasks.test.common.processes import register_process, kill_processes

SERVICE_NAME = "nfs"


class NfsLauncher:

    def __init__(
        self,
        ydb_domain: str,
        ydb_port: int,
        domains_txt: str,
        names_txt: str,
        nfs_binary_path: str,
        ydb_binary_path: str,
        dynamic_storage_pools: list[str],
    ):
        server_config = TServerAppConfig()
        server_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
        server_config.ServerConfig.RootCertsFile = common.source_path("cloud/filestore/tests/certs/server.crt")
        server_config.ServerConfig.Certs.add()
        server_config.ServerConfig.Certs[0].CertFile = common.source_path("cloud/filestore/tests/certs/server.crt")
        server_config.ServerConfig.Certs[0].CertPrivateKeyFile = common.source_path(
            "cloud/filestore/tests/certs/server.key"
        )
        storage_config = TStorageConfig()
        self.__nfs_configurator = FilestoreServerConfigGenerator(
            binary_path=nfs_binary_path,
            app_config=server_config,
            service_type="kikimr",
            verbose=True,
            kikimr_port=ydb_port,
            domain=ydb_domain,
            secure=True,
            storage_config=storage_config,
        )
        self.__nfs_configurator.generate_configs(domains_txt, names_txt)

        self.__nfs_server = FilestoreServer(
            kikimr_binary_path=ydb_binary_path,
            configurator=self.__nfs_configurator,
            dynamic_storage_pools=dynamic_storage_pools,
        )

    @property
    def nfs_server(self):
        return self.__nfs_server

    def start(self):
        self.__nfs_server.start()
        wait_for_filestore_server(self.__nfs_server, self.__nfs_configurator.port)
        register_process(SERVICE_NAME, self.__nfs_server.pid)

    @staticmethod
    def stop():
        kill_processes(SERVICE_NAME)

    @property
    def port(self):
        return self.__nfs_configurator.secure_port
