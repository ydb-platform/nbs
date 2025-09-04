import time
import json

import yatest.common as common

from cloud.blockstore.config.server_pb2 import \
    TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import \
    TStorageServiceConfig
from cloud.blockstore.config.ydbstats_pb2 import \
    TYdbStatsConfig

from cloud.blockstore.tests.python.lib.nbs_runner import \
    LocalNbs
from cloud.blockstore.tests.python.lib.test_base import \
    thread_count, wait_for_nbs_server

from contrib.ydb.tests.library.harness.kikimr_cluster import \
    kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import \
    KikimrConfigGenerator

from subprocess import call, check_output

from datetime import date

STORAGE_POOL = [
    dict(name="dynamic_storage_pool:1", kind="rot", pdisk_user_kind=0),
    dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0),
]

StatsTableName = "cloud-local-perfstats"
HistoryTablePrefix = "cloud-local-perfstats"
ArchiveStatsTableName = "cloud-local-archive-perfstats"


def kikimr_start():
    kikimr_binary_path = common.binary_path("contrib/ydb/apps/ydbd/ydbd")

    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_paths=[kikimr_binary_path],
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

    # in current realization StatsTableName and HistoryTablePrefix are necessary
    ydbstat = TYdbStatsConfig()
    ydbstat.BlobLoadMetricsTableName = "cloud-local-metrics"
    ydbstat.StatsTableName = StatsTableName
    ydbstat.HistoryTablePrefix = HistoryTablePrefix
    ydbstat.ArchiveStatsTableName = ArchiveStatsTableName
    ydbstat.DatabaseName = "/Root"
    ydbstat.ServerAddress = "localhost:" + str(kikimr_port)
    ydbstat.HistoryTableLifetimeDays = 2
    ydbstat.StatsTableRotationAfterDays = 1

    nbs = LocalNbs(
        kikimr_port,
        configurator.domains_txt,
        server_app_config=server_app_config,
        contract_validation=False,
        storage_config_patches=[storage],
        tracking_enabled=False,
        enable_access_service=False,
        enable_tls=False,
        dynamic_storage_pools=STORAGE_POOL,
        ydbstats_config=ydbstat)

    nbs.start()

    return nbs


def get_load_data(kikimr_port):
    ydb_binary_path = common.binary_path("contrib/ydb/apps/ydb/ydb")

    json_string = check_output([
        ydb_binary_path,
        "--endpoint", "grpc://localhost:" + str(kikimr_port),
        "--database", "/Root",
        "yql",
        "--format", "json-unicode-array",
        "--script", "SELECT * FROM `/Root/cloud-local-metrics`;",
    ])

    json_data = json.loads(json_string)
    parsed_load_data = json.loads(json_data[len(json_data) - 1]["LoadData"])

    read_data_size = 0
    read_data_iops = 0
    write_data_size = 0
    write_data_iops = 0

    for index in range(len(parsed_load_data)):
        # skip scheme
        if index == 0:
            continue
        read_data_size += parsed_load_data[index][2]
        read_data_iops += parsed_load_data[index][3]
        write_data_size += parsed_load_data[index][4]
        write_data_iops += parsed_load_data[index][5]

    return read_data_size, read_data_iops, write_data_size, write_data_iops


def check_ydb_volume_metrics(kikimr_port):
    ydb_binary_path = common.binary_path("contrib/ydb/apps/ydb/ydb")

    dailyTable = "{}-{}".format(HistoryTablePrefix, date.today())

    for table in [StatsTableName, ArchiveStatsTableName, dailyTable]:
        json_string = check_output([
            ydb_binary_path,
            "--endpoint", "grpc://localhost:" + str(kikimr_port),
            "--database", "/Root",
            "yql",
            "--format", "json-unicode-array",
            "--script", "SELECT * FROM `/Root/{}`;".format(table),
        ])

        json_data = json.loads(json_string)
        assert len(json_data) != 0
        assert json_data[0]["DiskId"] == "vol0"


def test_metrics():
    kikimr_cluster, configurator = kikimr_start()
    nbs = nbs_server_start(kikimr_cluster, configurator)

    wait_for_nbs_server(nbs.nbs_port)

    nbs_client_binary_path = \
        common.binary_path("cloud/blockstore/apps/client/blockstore-client")

    result = call([
        nbs_client_binary_path, "createvolume",
        "--disk-id", "vol0",
        "--blocks-count", "100000",
        "--host", "localhost",
        "--storage-media-kind", "ssd",
        "--port", str(nbs.nbs_port),
    ])

    assert result == 0

    # create checkpoint for starting stats upload
    result = call([
        nbs_client_binary_path, "createcheckpoint",
        "--disk-id", "vol0",
        "--checkpoint-id", "vol0_checkpoint",
        "--host", "localhost",
        "--port", str(nbs.nbs_port),
    ])

    assert result == 0

    raw_data_size = 4096 * 1024

    with open("writed_data.txt", "w") as wr:
        wr.write(" " * raw_data_size)

    result = call([
        nbs_client_binary_path, "writeblocks",
        "--disk-id", "vol0",
        "--input", "writed_data.txt",
        "--host", "localhost",
        "--port", str(nbs.nbs_port),
    ])

    assert result == 0

    # stats will be sent only after 30 sec
    # we wait 60 sec to  make sure
    time.sleep(60)

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    read_data_size, read_data_iops, \
        write_data_size, write_data_iops = get_load_data(kikimr_port)

    assert read_data_size == 0
    assert read_data_iops == 0
    assert write_data_size >= raw_data_size
    assert write_data_iops >= 1

    result = call([
        nbs_client_binary_path, "readblocks",
        "--disk-id", "vol0",
        "--host", "localhost",
        "--port", str(nbs.nbs_port),
        "--read-all"
    ])

    assert result == 0

    time.sleep(60)

    read_data_size, read_data_iops, \
        write_data_size, write_data_iops = get_load_data(kikimr_port)

    check_ydb_volume_metrics(kikimr_port)

    nbs.stop()

    assert read_data_size >= raw_data_size
    assert read_data_iops >= 1
    assert write_data_size >= raw_data_size
    assert write_data_iops >= 1
