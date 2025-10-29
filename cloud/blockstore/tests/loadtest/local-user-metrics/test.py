import time
import requests

import yatest.common as common

from cloud.blockstore.config.server_pb2 import \
    TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import \
    TStorageServiceConfig

from cloud.blockstore.tests.python.lib.nbs_runner import \
    LocalNbs
from cloud.blockstore.tests.python.lib.test_base import \
    thread_count, wait_for_nbs_server

from contrib.ydb.tests.library.harness.kikimr_cluster import \
    kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import \
    KikimrConfigGenerator

from subprocess import call
from threading import Thread

STORAGE_POOL = [
    dict(name="dynamic_storage_pool:1", kind="rot", pdisk_user_kind=0),
    dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0),
]


def kikimr_start():
    kikimr_binary_path = common.binary_path("ydb/apps/ydbd/ydbd")

    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=kikimr_binary_path,
        use_in_memory_pdisks=True,
        dynamic_pdisks=[],
        dynamic_storage_pools=STORAGE_POOL)

    kikimr_cluster = kikimr_cluster_factory(configurator=configurator)
    kikimr_cluster.start()

    return kikimr_cluster, configurator


def nbs_server_start(kikimr_cluster, configurator):
    server_app_config = TServerAppConfig()
    server_app_config.ServerConfig.CopyFrom(TServerConfig())
    server_app_config.ServerConfig.ThreadsCount = thread_count()
    server_app_config.ServerConfig.StrictContractValidation = False
    server_app_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    storage = TStorageServiceConfig()
    storage.SchemeShardDir = "/Root/nbs"
    storage.StatsUploadInterval = 10000

    nbs = LocalNbs(
        kikimr_port,
        configurator.domains_txt,
        server_app_config=server_app_config,
        contract_validation=False,
        storage_config_patches=[storage],
        tracking_enabled=False,
        enable_access_service=False,
        enable_tls=False,
        dynamic_storage_pools=STORAGE_POOL)

    nbs.start()

    return nbs


def find_value(jsonResult, name):
    for metric in jsonResult["sensors"]:
        if metric["labels"]["name"] == name:
            if "value" in metric:
                return metric["value"]
            return metric["hist"]
    assert False, name


def check_metrics(port):
    time.sleep(30)
    jsonResult = requests.get(url="http://localhost:"+str(port)+"/blockstore/user_stats/json").json()

    find_value(jsonResult, "disk.read_ops")
    find_value(jsonResult, "disk.read_ops_burst")
    find_value(jsonResult, "disk.read_ops_in_flight")
    find_value(jsonResult, "disk.read_ops_in_flight_burst")
    find_value(jsonResult, "disk.read_errors")
    find_value(jsonResult, "disk.read_bytes")
    find_value(jsonResult, "disk.read_bytes_burst")
    find_value(jsonResult, "disk.read_bytes_in_flight")
    find_value(jsonResult, "disk.read_bytes_in_flight_burst")
    find_value(jsonResult, "disk.read_throttler_delay")
    find_value(jsonResult, "disk.read_latency")
    find_value(jsonResult, "disk.write_ops")
    find_value(jsonResult, "disk.write_ops_burst")
    find_value(jsonResult, "disk.write_ops_in_flight")
    find_value(jsonResult, "disk.write_ops_in_flight_burst")
    find_value(jsonResult, "disk.write_errors")
    find_value(jsonResult, "disk.write_bytes")
    find_value(jsonResult, "disk.write_bytes_burst")
    find_value(jsonResult, "disk.write_bytes_in_flight")
    find_value(jsonResult, "disk.write_bytes_in_flight_burst")
    find_value(jsonResult, "disk.write_throttler_delay")
    find_value(jsonResult, "disk.write_latency")
    find_value(jsonResult, "disk.write_latency")
    find_value(jsonResult, "disk.io_quota_utilization_percentage")
    find_value(jsonResult, "disk.io_quota_utilization_percentage_burst")


def test_user_metrics():
    kikimr_cluster, configurator = kikimr_start()
    nbs = nbs_server_start(kikimr_cluster, configurator)

    wait_for_nbs_server(nbs.nbs_port)

    nbs_loadtest_binary_path = \
        common.binary_path("cloud/blockstore/tools/testing/loadtest/bin/blockstore-loadtest")

    check_thread = Thread(target=check_metrics, args=(nbs.mon_port))
    check_thread.start()

    result = call([
        nbs_loadtest_binary_path,
        "--host", "localhost",
        "--port", str(nbs.nbs_port),
        "--config", common.test_source_path("test-case.txt"),
        "--client-config", common.test_source_path("nbs-client.txt")
    ])

    assert result == 0

    check_thread.join()
