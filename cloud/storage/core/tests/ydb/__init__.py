import json
import os
import signal
import time

import google.protobuf.text_format as text_format
import yatest.common as common

from contrib.ydb.core.protos.config_pb2 import (
    TDomainsConfig,
    TStaticNameserviceConfig,
)
from contrib.ydb.tests.library.clients.kikimr_client import kikimr_client_factory
from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import (
    KikimrConfigGenerator,
    PDISK_SIZE,
)

BINARY_PATH = "cloud/storage/core/tools/testing/ydb/bin/ydbd"
DYNAMIC_STORAGE_POOLS = [
    dict(name="dynamic_storage_pool:1", kind="rot", pdisk_user_kind=0),
    dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0),
]
SHUTDOWN_TIMEOUT = 60


def _kill(pid, sig):
    try:
        os.kill(pid, sig)
    except ProcessLookupError:
        return False
    return True


class Ydb:
    """
    A single node ydb for tests: start() launches it, save() and load() pass
    it to another process. Only the process that started it can restart it,
    format its pdisks and rewrite its config.
    """

    def __init__(
            self,
            binary_path,
            port,
            ssl_port,
            mon_port,
            ic_port,
            pid,
            dynamic_storage_pools,
            pdisks_info,
            static_erasure,
            domains_txt,
            names_txt,
            yaml_config,
            tls_ca_path,
            tls_cert_path,
            tls_key_path):
        self.binary_path = binary_path
        self.port = port
        self.ssl_port = ssl_port
        self.mon_port = mon_port
        self.ic_port = ic_port
        self.pid = pid
        self.dynamic_storage_pools = dynamic_storage_pools
        self.pdisks_info = pdisks_info
        self.static_erasure = static_erasure
        self.domains_txt = domains_txt
        self.names_txt = names_txt
        self.yaml_config = yaml_config
        self.tls_ca_path = tls_ca_path
        self.tls_cert_path = tls_cert_path
        self.tls_key_path = tls_key_path
        self.client = kikimr_client_factory("localhost", port, retry_count=10)
        self.__cluster = None

    @classmethod
    def start(
            cls,
            binary_path=None,
            dynamic_storage_pools=DYNAMIC_STORAGE_POOLS,
            dynamic_pdisks=(),
            static_pdisk_size=PDISK_SIZE,
            dynamic_pdisk_size=PDISK_SIZE,
            output_path=None,
            use_in_memory_pdisks=True,
            enable_external_data_channel=False,
            enable_public_api_external_blobs=False,
            additional_log_configs=None,
            bs_cache_file_path=None,
            grpc_ssl_enable=False,
            extra_feature_flags=()):
        if binary_path is None:
            binary_path = common.binary_path(BINARY_PATH)

        configurator = KikimrConfigGenerator(
            erasure=None,
            output_path=output_path,
            use_in_memory_pdisks=use_in_memory_pdisks,
            binary_paths=[binary_path],
            static_pdisk_size=static_pdisk_size,
            dynamic_pdisk_size=dynamic_pdisk_size,
            dynamic_pdisks=list(dynamic_pdisks),
            enable_public_api_external_blobs=enable_public_api_external_blobs,
            additional_log_configs=additional_log_configs,
            bs_cache_file_path=bs_cache_file_path,
            grpc_ssl_enable=grpc_ssl_enable,
            extra_feature_flags=list(extra_feature_flags),
            dynamic_storage_pools=list(dynamic_storage_pools),
        )

        if enable_external_data_channel:
            ic_config = configurator.yaml_config.setdefault(
                "interconnect_config",
                {})
            ic_config["enable_external_data_channel"] = True

        cluster = kikimr_cluster_factory(configurator=configurator)
        cluster.start()

        tls_ca_path = None
        tls_cert_path = None
        tls_key_path = None
        if grpc_ssl_enable:
            tls_ca_path = configurator.grpc_tls_ca_path
            tls_cert_path = configurator.grpc_tls_cert_path
            tls_key_path = configurator.grpc_tls_key_path

        node = list(cluster.nodes.values())[0]
        ydb = cls(
            binary_path=binary_path,
            port=node.port,
            ssl_port=node.grpc_ssl_port,
            mon_port=node.mon_port,
            ic_port=node.ic_port,
            pid=node.pid,
            dynamic_storage_pools=configurator.dynamic_storage_pools,
            pdisks_info=configurator.pdisks_info,
            static_erasure=str(configurator.static_erasure),
            domains_txt=configurator.domains_txt,
            names_txt=configurator.names_txt,
            yaml_config=configurator.yaml_config,
            tls_ca_path=tls_ca_path,
            tls_cert_path=tls_cert_path,
            tls_key_path=tls_key_path)
        ydb.__cluster = cluster
        return ydb

    def __node(self):
        assert self.__cluster is not None, "ydb is started by another process"
        return list(self.__cluster.nodes.values())[0]

    def restart(self):
        node = self.__node()
        node.stop()
        node.start()
        self.pid = node.pid

    def format_pdisk(self, pdisk_info):
        node = self.__node()
        node.format_pdisk(**pdisk_info)

    def write_config(self):
        """
        Makes the changes of yaml_config visible to the next restart().
        """
        self.__node()
        self.__cluster.config.write_proto_configs(self.__cluster.config_path)

    def stop(self):
        if self.__cluster is not None:
            self.__cluster.stop()
            return

        _kill(self.pid, signal.SIGTERM)
        deadline = time.monotonic() + SHUTDOWN_TIMEOUT
        while _kill(self.pid, 0):
            if time.monotonic() > deadline:
                _kill(self.pid, signal.SIGKILL)
                break
            time.sleep(1)

    def save(self, path):
        state = dict(
            binary_path=self.binary_path,
            port=self.port,
            ssl_port=self.ssl_port,
            mon_port=self.mon_port,
            ic_port=self.ic_port,
            pid=self.pid,
            dynamic_storage_pools=self.dynamic_storage_pools,
            pdisks_info=self.pdisks_info,
            static_erasure=self.static_erasure,
            domains_txt=text_format.MessageToString(self.domains_txt),
            names_txt=text_format.MessageToString(self.names_txt),
            yaml_config=self.yaml_config,
            tls_ca_path=self.tls_ca_path,
            tls_cert_path=self.tls_cert_path,
            tls_key_path=self.tls_key_path)
        with open(path, "w") as f:
            json.dump(state, f)

    @classmethod
    def load(cls, path):
        with open(path) as f:
            state = json.load(f)
        state["domains_txt"] = text_format.Parse(
            state["domains_txt"],
            TDomainsConfig())
        state["names_txt"] = text_format.Parse(
            state["names_txt"],
            TStaticNameserviceConfig())
        return cls(**state)
