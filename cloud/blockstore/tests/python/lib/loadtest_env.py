from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test
from contrib.ydb.tests.library.harness.util import LogLevels

from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig

from cloud.blockstore.tests.python.lib.nonreplicated_setup import create_file_devices, \
    setup_disk_registry_proxy_config, setup_disk_agent_config

from cloud.blockstore.tests.python.lib.test_base import wait_for_nbs_server
from cloud.storage.core.tools.testing.access_service.lib import AccessService

from .nbs_runner import LocalNbs
from .endpoint_proxy import EndpointProxy

import yatest.common as yatest_common

import logging
import os
import subprocess


logger = logging.getLogger(__name__)


class LocalLoadTest:

    def __init__(
            self,
            endpoint,
            run_kikimr=True,
            server_app_config=None,
            contract_validation=False,
            storage_config_patches=None,
            tracking_enabled=False,
            enable_access_service=False,
            use_in_memory_pdisks=False,
            enable_tls=False,
            discovery_config=None,
            restart_interval=None,
            dynamic_pdisks=[],
            dynamic_storage_pools=[
                dict(name="dynamic_storage_pool:1", kind="rot", pdisk_user_kind=0),
                dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0)
            ],
            features_config_patch=None,
            grpc_trace=False,
            with_nrd=False,
            nrd_device_count=1,
            rack='',
            bs_cache_file_path=None,
            kikimr_binary_path=None,
            with_endpoint_proxy=False,
            with_netlink=False,
            access_service_type=AccessService,
            load_configs_from_cms=True,
            stored_endpoints_path=None,
            nbd_request_timeout=None,
            nbd_reconnect_delay=None,
            proxy_restart_events=None,
    ):

        self.__endpoint = endpoint

        if kikimr_binary_path is None:
            kikimr_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")

        self.configurator = KikimrConfigGenerator(
            erasure=None,
            binary_path=kikimr_binary_path,
            use_in_memory_pdisks=use_in_memory_pdisks,
            dynamic_pdisks=dynamic_pdisks,
            dynamic_storage_pools=dynamic_storage_pools,
            additional_log_configs={"HIVE": LogLevels.TRACE},
            bs_cache_file_path=bs_cache_file_path,
        )
        self.kikimr_cluster = kikimr_cluster_factory(
            configurator=self.configurator)

        kikimr_port = 0

        if run_kikimr:
            self.kikimr_cluster.start()
            kikimr_port = list(self.kikimr_cluster.nodes.values())[0].port
        else:
            # makes sense only when Kikimr is running
            load_configs_from_cms = False

        self.__devices = []

        if with_nrd:
            assert run_kikimr

            storage = TStorageServiceConfig()
            storage.AllocationUnitNonReplicatedSSD = 1
            storage.AcquireNonReplicatedDevices = True
            storage.NonReplicatedInfraTimeout = 60000
            storage.NonReplicatedAgentMinTimeout = 3000
            storage.NonReplicatedAgentMaxTimeout = 3000
            storage.NonReplicatedDiskRecyclingPeriod = 5000

            if storage_config_patches:
                storage_config_patches += [storage]
            else:
                storage_config_patches = [storage]

        self.nbs = LocalNbs(
            kikimr_port,
            self.configurator.domains_txt,
            server_app_config=server_app_config,
            contract_validation=contract_validation,
            storage_config_patches=storage_config_patches,
            tracking_enabled=tracking_enabled,
            enable_access_service=enable_access_service,
            enable_tls=enable_tls,
            discovery_config=discovery_config,
            restart_interval=restart_interval,
            dynamic_storage_pools=dynamic_storage_pools,
            load_configs_from_cms=load_configs_from_cms,
            features_config_patch=features_config_patch,
            grpc_trace=grpc_trace,
            rack=rack,
            access_service_type=access_service_type,
        )

        self.endpoint_proxy = None
        if with_endpoint_proxy:
            self.endpoint_proxy = EndpointProxy(
                working_dir=self.nbs.cwd,
                unix_socket_path=server_app_config.ServerConfig.EndpointProxySocketPath,
                with_netlink=with_netlink,
                stored_endpoints_path=stored_endpoints_path,
                nbd_request_timeout=nbd_request_timeout,
                nbd_reconnect_delay=nbd_reconnect_delay,
                restart_events=proxy_restart_events)

        if run_kikimr:
            self.nbs.setup_cms(self.kikimr_cluster.client)

        if with_nrd:
            self.__devices = create_file_devices(
                None,   # dir
                nrd_device_count,
                4096,
                262144)

            setup_disk_registry_proxy_config(self.kikimr_cluster.client)
            setup_disk_agent_config(
                self.kikimr_cluster.client,
                self.__devices,
                device_erase_method=None,
                node_type=None)

        if self.endpoint_proxy:
            self.endpoint_proxy.start()
        self.nbs.start()
        wait_for_nbs_server(self.nbs.nbs_port)

    def tear_down(self):
        try:
            self.nbs.stop()
            self.kikimr_cluster.stop()

            if self.endpoint_proxy:
                self.endpoint_proxy.stop()

            for d in self.__devices:
                d.handle.close()
                os.unlink(d.path)
        finally:
            # It may be beneficial to save dmesg output for debugging purposes.
            try:
                test_output_path = get_unique_path_for_current_test(
                    output_path=yatest_common.output_path(),
                    sub_folder="")
                with open(test_output_path + "/dmesg.txt", "w") as dmesg_output:
                    subprocess.run(
                        ["sudo", "-n", "dmesg", "-T"],
                        stdout=dmesg_output,
                        stderr=dmesg_output,
                        timeout=10
                    )
            except Exception as dmesg_error:
                logging.info(f"Failed to save dmesg output: {dmesg_error}")
                pass
            subprocess.check_call(["sync"])

    @property
    def endpoint(self):
        return 'localhost:' + str(self.nbs.nbs_port)

    @property
    def nbs_port(self):
        return self.nbs.nbs_port

    @property
    def nbs_data_port(self):
        return self.nbs.nbs_data_port

    @property
    def nbs_secure_port(self):
        return self.nbs.nbs_secure_port

    @property
    def counters_url(self):
        return "http://localhost:%s/counters/counters=blockstore/json" % self.mon_port

    @property
    def mon_port(self):
        return self.nbs.mon_port

    @property
    def nbs_log_path(self):
        return self.nbs.stderr_file_name

    @property
    def nbs_profile_log_path(self):
        return self.nbs.profile_log_path

    @property
    def access_service(self):
        return self.nbs.access_service

    @property
    def nbs_cwd(self):
        return self.nbs.cwd

    @property
    def devices(self):
        return self.__devices

    @property
    def pdisks_info(self):
        return self.configurator.pdisks_info
