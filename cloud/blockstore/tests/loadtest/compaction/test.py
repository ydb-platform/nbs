import json
import time
import logging

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

import yatest.common as yatest_common

from subprocess import call, check_output, run

PDISK_SIZE = 32 * 1024 * 1024 * 1024
STORAGE_POOL = [
    dict(name="dynamic_storage_pool:1", kind="rot", pdisk_user_kind=0),
    dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0),
]

DEFAULT_BLOCK_SIZE = 4096
DEFAULT_BLOCK_COUNT = 4096


def kikimr_start():
    kikimr_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")

    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=kikimr_binary_path,
        use_in_memory_pdisks=True,
        static_pdisk_size=PDISK_SIZE,
        dynamic_pdisk_size=PDISK_SIZE,
        dynamic_pdisks=[],
        dynamic_storage_pools=STORAGE_POOL)

    kikimr_cluster = kikimr_cluster_factory(configurator=configurator)
    kikimr_cluster.start()

    return kikimr_cluster, configurator


def nbs_server_start(kikimr_cluster, configurator, storage):
    server_app_config = TServerAppConfig()
    server_app_config.ServerConfig.CopyFrom(TServerConfig())
    server_app_config.ServerConfig.ThreadsCount = thread_count()
    server_app_config.ServerConfig.StrictContractValidation = False
    server_app_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    storage.SchemeShardDir = "/Root/nbs"
    storage.ThrottlingEnabled = False
    storage.ThrottlingEnabledSSD = False
    storage.CleanupThreshold = 1
    storage.CollectGarbageThreshold = 1
    storage.MaxCompactionRangesLoadingPerTx = 32
    storage.MaxOutOfOrderCompactionMapChunksInflight = 10

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

    wait_for_nbs_server(nbs.nbs_port)

    return nbs, server_app_config, storage


def data_count_by_channels(nbs_client_binary_path, nbs_port):
    partition_info = check_output([
        nbs_client_binary_path, "ExecuteAction",
        "--action", "getpartitioninfo",
        "--input-bytes", '{"DiskId":"vol0"}',
        "--port", str(nbs_port),
    ])

    json_data = json.loads(partition_info)
    if "Stats" not in json_data:
        return 0, 0, 0, 0, 0
    stats = json_data["Stats"]
    fresh_blocks = 0 if "FreshBlocksCount" not in stats else stats["FreshBlocksCount"]
    mixed_blocks = 0 if "MixedBlocksCount" not in stats else stats["MixedBlocksCount"]
    mixed_blobs = 0 if "MixedBlobsCount" not in stats else stats["MixedBlobsCount"]
    merged_blocks = 0 if "MergedBlocksCount" not in stats else stats["MergedBlocksCount"]
    merged_blobs = 0 if "MergedBlobsCount" not in stats else stats["MergedBlobsCount"]
    return fresh_blocks, mixed_blocks, mixed_blobs, merged_blocks, merged_blobs


def write_data(nbs_client_binary_path, nbs_port):
    written_data = bytearray()
    for i in range(DEFAULT_BLOCK_COUNT):
        written_data += bytes(i % 256 for j in range(DEFAULT_BLOCK_SIZE))

    p_result = run([
        nbs_client_binary_path, "WriteBlocks",
        "--disk-id", "vol0",
        "--host", "localhost",
        "--port", str(nbs_port),
    ], input=written_data)
    assert p_result.returncode == 0

    return written_data


def compact_disk(nbs_client_binary_path, nbs_port):
    compaction_result = check_output([
        nbs_client_binary_path, "ExecuteAction",
        "--action", "compactrange",
        "--input-bytes", '{"DiskId":"vol0",'
                         '"StartIndex":"0",'
                         '"BlocksCount":' + "{}".format(DEFAULT_BLOCK_COUNT) + '}',
        "--port", str(nbs_port),
    ])
    operation_id = json.loads(compaction_result)["OperationId"]

    is_completed = False
    while not is_completed:
        compaction_status = check_output([
            nbs_client_binary_path, "ExecuteAction",
            "--action", "getcompactionstatus",
            "--input-bytes", '{"DiskId":"vol0",'
                             '"OperationId":"' + str(operation_id) + '"}',
            "--port", str(nbs_port),
        ])
        compaction_status_json = json.loads(compaction_status)
        logging.info(f"compaction_status: {compaction_status}")
        is_completed = False if "IsCompleted" not in compaction_status_json \
            else compaction_status_json["IsCompleted"]
        if not is_completed:
            time.sleep(1)


def check_data(nbs_client_binary_path,
               nbs_port,
               expected_mixed_blocks,
               expected_mixed_blobs,
               expected_merged_blocks,
               expected_merged_blobs,
               written_data):
    fresh_blocks, mixed_blocks, mixed_blobs, merged_blocks, merged_blobs = \
        data_count_by_channels(nbs_client_binary_path, nbs_port)

    logging.info(f"fresh_blocks={fresh_blocks}")
    logging.info(f"mixed_blocks={mixed_blocks} -> expected={expected_mixed_blocks}")
    logging.info(f"mixed_blobs={mixed_blobs} -> expected={expected_mixed_blobs}")
    logging.info(f"merged_blocks={merged_blocks} -> expected={expected_merged_blocks}")
    logging.info(f"merged_blobs={merged_blobs} -> expected={expected_merged_blobs}")

    assert merged_blocks == expected_merged_blocks
    assert merged_blobs == expected_merged_blobs
    assert mixed_blocks == expected_mixed_blocks
    assert mixed_blobs == expected_mixed_blobs

    read_data = check_output([
        nbs_client_binary_path, "ReadBlocks",
        "--disk-id", "vol0",
        "--blocks-count", "{}".format(DEFAULT_BLOCK_COUNT),
        "--host", "localhost",
        "--port", str(nbs_port),
    ])
    assert read_data == written_data


def compaction_test(compaction_threshold,
                    expected_mixed_blocks,
                    expected_mixed_blobs,
                    expected_merged_blocks,
                    expected_merged_blobs):
    nbs_client_binary_path = \
        yatest_common.binary_path(
            "cloud/blockstore/apps/client/blockstore-client")

    kikimr_cluster, configurator = \
        kikimr_start()

    storage = TStorageServiceConfig()
    storage.CompactionMergedBlobThresholdHDD = compaction_threshold

    nbs, server_app_config, storage = \
        nbs_server_start(kikimr_cluster, configurator, storage)

    result = call([
        nbs_client_binary_path, "CreateVolume",
        "--disk-id", "vol0",
        "--blocks-count", str(DEFAULT_BLOCK_COUNT),
        "--host", "localhost",
        "--storage-media-kind", "hybrid",
        "--port", str(nbs.nbs_port),
    ])
    assert result == 0

    written_data = write_data(nbs_client_binary_path, nbs.nbs_port)
    compact_disk(nbs_client_binary_path, nbs.nbs_port)
    check_data(nbs_client_binary_path,
               nbs.nbs_port,
               expected_mixed_blocks,
               expected_mixed_blobs,
               expected_merged_blocks,
               expected_merged_blobs,
               written_data)

    nbs.restart()

    written_data = write_data(nbs_client_binary_path, nbs.nbs_port)
    compact_disk(nbs_client_binary_path, nbs.nbs_port)
    check_data(nbs_client_binary_path,
               nbs.nbs_port,
               expected_mixed_blocks,
               expected_mixed_blobs,
               expected_merged_blocks,
               expected_merged_blobs,
               written_data)

    nbs.stop()
    kikimr_cluster.stop()


def test_compaction():
    compaction_test(0, 0, 0, DEFAULT_BLOCK_COUNT, DEFAULT_BLOCK_COUNT / 1024)
    compaction_test(4 * 1024 * 1024 + 1, DEFAULT_BLOCK_COUNT, DEFAULT_BLOCK_COUNT / 1024, 0, 0)
