import yatest.common as common

from cloud.filestore.config.server_pb2 import TServerAppConfig, TLocalServiceConfig
from cloud.filestore.tests.python.lib.daemon_config import NfsServerConfigGenerator
from cloud.filestore.tests.python.lib.server import NfsServer, wait_for_nfs_server
from cloud.tasks.test.common.processes import register_process, kill_processes

SERVICE_NAME = "nfs"


class NfsLauncher:

    def __init__(self, ydb_port, domains_txt, names_txt, nfs_binary_path):
        server_config = TServerAppConfig()
        server_config.LocalServiceConfig.CopyFrom(TLocalServiceConfig())

        fs_root_path = common.ram_drive_path()
        if fs_root_path:
            server_config.LocalServiceConfig.RootPath = fs_root_path

        self.__nfs_configurator = NfsServerConfigGenerator(
            binary_path=nfs_binary_path,
            app_config=server_config,
            # FIXME: use kikimr service, resolve tenant config issues
            service_type="local",
            verbose=True,
            kikimr_port=ydb_port,
        )
        self.__nfs_configurator.generate_configs(domains_txt, names_txt)

        self.__nfs_server = NfsServer(configurator=self.__nfs_configurator)

    def start(self):
        self.__nfs_server.start()
        wait_for_nfs_server(self.__nfs_server, self.__nfs_configurator.port)
        register_process(SERVICE_NAME, self.__nfs_server.pid)

    @staticmethod
    def stop():
        kill_processes(SERVICE_NAME)

    @property
    def port(self):
        return self.__nfs_configurator.port
