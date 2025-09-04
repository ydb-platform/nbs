from cloud.tasks.test.common.processes import register_process, kill_processes
from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

SERVICE_NAME = "ydb"


class YDBLauncher:

    def __init__(self, ydb_binary_path):
        dynamic_storage_pools = [
            dict(name="dynamic_storage_pool:1", kind="rot", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:3", kind="rotencrypted", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:4", kind="ssdencrypted", pdisk_user_kind=0),
        ]
        configurator = KikimrConfigGenerator(
            binary_paths=[ydb_binary_path],
            erasure=None,
            static_pdisk_size=64 * 2**30,
            dynamic_storage_pools=dynamic_storage_pools,
            enable_public_api_external_blobs=True)

        self.__cluster = kikimr_cluster_factory(configurator=configurator)
        self.__dynamic_storage_pools = dynamic_storage_pools

    def start(self):
        self.__cluster.start()
        for kimimr_node in list(self.__cluster.nodes.values()):
            register_process(SERVICE_NAME, kimimr_node.pid)

    @staticmethod
    def stop():
        kill_processes(SERVICE_NAME)

    @property
    def port(self):
        return list(self.__cluster.nodes.values())[0].port

    @property
    def domains_txt(self):
        return self.__cluster.config.domains_txt

    @property
    def names_txt(self):
        return self.__cluster.config.names_txt

    @property
    def dynamic_storage_pools(self):
        return self.__dynamic_storage_pools

    @property
    def client(self):
        return self.__cluster.client
