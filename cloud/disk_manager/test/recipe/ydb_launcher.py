import os

from cloud.tasks.test.common.processes import register_process, kill_processes
from cloud.storage.core.tests.ydb import Ydb

SERVICE_NAME = "ydb"


class YDBLauncher:

    def __init__(self, ydb_binary_path):
        dynamic_storage_pools = [
            dict(name="dynamic_storage_pool:1", kind="rot", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:3", kind="rotencrypted", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:4", kind="ssdencrypted", pdisk_user_kind=0),
        ]
        self.__binary_path = ydb_binary_path
        self.__dynamic_storage_pools = dynamic_storage_pools

    def start(self):
        # Make ydbd use the harness-allocated PGWire port; otherwise its default
        # port 5432 may collide with a dynamically allocated monitoring port.
        os.environ["YDB_ALLOCATE_PGWIRE_PORT"] = "true"
        self.__ydb = Ydb.start(
            binary_path=self.__binary_path,
            static_pdisk_size=64 * 2**30,
            dynamic_storage_pools=self.__dynamic_storage_pools,
            enable_public_api_external_blobs=True)
        register_process(SERVICE_NAME, self.__ydb.pid)

    @staticmethod
    def stop():
        kill_processes(SERVICE_NAME)

    def stop_service(self):
        self.__ydb.stop()

    @property
    def port(self):
        return self.__ydb.port

    @property
    def mon_port(self):
        return self.__ydb.mon_port

    @property
    def domains_txt(self):
        return self.__ydb.domains_txt

    @property
    def names_txt(self):
        return self.__ydb.names_txt

    @property
    def dynamic_storage_pools(self):
        return self.__dynamic_storage_pools

    @property
    def client(self):
        return self.__ydb.client

    @property
    def domain(self):
        return self.__ydb.domains_txt.Domain[0].Name
