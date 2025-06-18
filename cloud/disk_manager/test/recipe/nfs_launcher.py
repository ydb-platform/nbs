import yatest.common as common

from cloud.filestore.config.server_pb2 import TServerAppConfig, TLocalServiceConfig
from cloud.filestore.tests.python.lib.daemon_config import FilestoreServerConfigGenerator
from cloud.filestore.tests.python.lib.server import FilestoreServer, wait_for_filestore_server
from cloud.tasks.test.common.processes import register_process, kill_processes

SERVICE_NAME = "nfs"


class NfsLauncher:

    def __init__(self, ydb_port, domains_txt, names_txt, nfs_binary_path):
        server_config = TServerAppConfig()
        server_config.LocalServiceConfig.CopyFrom(TLocalServiceConfig())

        fs_root_path = common.ram_drive_path()
        if fs_root_path:
            server_config.LocalServiceConfig.RootPath = fs_root_path

        server_config.ServerConfig.RootCertsFile = common.source_path("cloud/filestore/tests/certs/server.crt")
        server_config.ServerConfig.Certs.add()
        server_config.ServerConfig.Certs[0].CertFile = common.source_path("cloud/filestore/tests/certs/server.crt")
        server_config.ServerConfig.Certs[0].CertPrivateKeyFile = common.source_path(
            "cloud/filestore/tests/certs/server.key"
        )

        self.__nfs_configurator = FilestoreServerConfigGenerator(
            binary_path=nfs_binary_path,
            app_config=server_config,
            # FIXME: use kikimr service, resolve tenant config issues
            service_type="local",
            verbose=True,
            kikimr_port=ydb_port,
            secure=True,
        )
        self.__nfs_configurator.generate_configs(domains_txt, names_txt)

        self.__nfs_server = FilestoreServer(configurator=self.__nfs_configurator)

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
