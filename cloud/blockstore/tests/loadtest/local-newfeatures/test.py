import pytest
import uuid

import yatest.common as common

from cloud.blockstore.config.client_pb2 import TClientConfig
from cloud.blockstore.config.server_pb2 import TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import CT_LOAD
from cloud.blockstore.tests.python.lib.config import storage_config_with_default_limits
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import thread_count, run_test, \
    get_restart_interval
from cloud.storage.core.config.features_pb2 import TFeaturesConfig
from cloud.storage.core.protos.endpoints_pb2 import EEndpointStorageType


def default_storage_config():
    storage = storage_config_with_default_limits()

    storage.SSDCompactionType = CT_LOAD
    storage.HDDCompactionType = CT_LOAD
    storage.V1GarbageCompactionEnabled = True
    storage.DiskPrefixLengthWithBlockChecksumsInBlobs = 1 << 30
    storage.CheckBlockChecksumsInBlobsUponRead = True

    return storage


def storage_config_with_incremental_compaction():
    storage = default_storage_config()
    storage.IncrementalCompactionEnabled = True

    return storage


def storage_config_with_incremental_batch_compaction():
    storage = storage_config_with_incremental_compaction()
    storage.BatchCompactionEnabled = True
    storage.CompactionRangeCountPerRun = 5
    storage.SSDMaxBlobsPerRange = 5
    storage.HDDMaxBlobsPerRange = 5

    return storage


def storage_config_with_garbage_batch_compaction():
    storage = default_storage_config()
    storage.BatchCompactionEnabled = True
    storage.GarbageCompactionRangeCountPerRun = 20
    storage.V1GarbageCompactionEnabled = True
    storage.CompactionGarbageThreshold = 20
    storage.CompactionRangeGarbageThreshold = 999999
    storage.SSDMaxBlobsPerRange = 5
    storage.HDDMaxBlobsPerRange = 5

    return storage


def storage_config_with_incremental_compaction_and_patching():
    storage = storage_config_with_incremental_compaction()
    storage.BlobPatchingEnabled = True
    # checksums are currently not supported for patched blobs
    storage.DiskPrefixLengthWithBlockChecksumsInBlobs = 0
    storage.CheckBlockChecksumsInBlobsUponRead = False

    return storage


def storage_config_with_multiple_partitions(max_partitions):
    storage = default_storage_config()
    storage.BytesPerPartition = 4096
    storage.BytesPerPartitionSSD = 4096
    storage.MultipartitionVolumesEnabled = True
    storage.MaxPartitionsPerVolume = max_partitions

    return storage


def default_storage_config_with_multipartition_volumes_feature():
    storage = default_storage_config()
    storage.MultipartitionVolumesEnabled = True

    return storage


def features_config_with_incremental_compaction():
    config = TFeaturesConfig()

    config.Features.add()
    config.Features[0].Name = 'IncrementalCompaction'
    config.Features[0].Whitelist.CloudIds.append("test_cloud")

    return config


def storage_config_with_fresh_channel_writes_enabled(enabled):
    storage = default_storage_config()
    storage.FreshChannelCount = 1
    storage.FreshChannelWriteRequestsEnabled = enabled

    return storage


def storage_config_with_mixed_index_cache_enabled():
    storage = default_storage_config()
    storage.MixedIndexCacheV1Enabled = True

    return storage


def storage_config_with_adding_unconfirmed_blobs_enabled():
    storage = default_storage_config()
    storage.AddingUnconfirmedBlobsEnabled = True
    # checksums are currently not supported for unconfirmed blobs
    storage.DiskPrefixLengthWithBlockChecksumsInBlobs = 0
    storage.CheckBlockChecksumsInBlobsUponRead = False

    return storage


def ordinary_prod_storage_config():
    storage = default_storage_config()
    storage.WriteRequestBatchingEnabled = True
    storage.IncrementalCompactionEnabled = True
    storage.FreshChannelCount = 1
    storage.FreshChannelWriteRequestsEnabled = True
    storage.BatchCompactionEnabled = True
    storage.HDDMaxBlobsPerRange = 5

    return storage


def storage_config_with_compaction_merged_blob_threshold_hdd():
    storage = ordinary_prod_storage_config()
    storage.HDDMaxBlobsPerRange = 5
    storage.CompactionMergedBlobThresholdHDD = 1025 * 4096

    return storage


class TestCase(object):

    def __init__(
            self,
            name,
            config_path,
            storage_config_patches,
            features_config_patch,
            restart_interval=get_restart_interval(),
    ):
        self.name = name
        self.config_path = config_path
        self.storage_config_patches = storage_config_patches
        self.features_config_patch = features_config_patch
        self.restart_interval = restart_interval


TESTS = [
    TestCase(
        "version1-two-partitions",
        "cloud/blockstore/tests/loadtest/local-newfeatures/local-tablet-version-1-two-partitions.txt",
        [
            storage_config_with_multiple_partitions(2),
            storage_config_with_multiple_partitions(1),
        ],
        None,
    ),
    TestCase(
        "version1-two-partitions-and-checkpoints",
        "cloud/blockstore/tests/loadtest/local-newfeatures/local-tablet-version-1-two-partitions-and-checkpoints.txt",
        [
            storage_config_with_multiple_partitions(2),
            storage_config_with_multiple_partitions(1),
        ],
        None,
    ),
    TestCase(
        "version1-incremental-compaction",
        "cloud/blockstore/tests/loadtest/local-newfeatures/local-tablet-version-1-multiple-ranges.txt",
        [
            storage_config_with_incremental_compaction(),
            default_storage_config(),
        ],
        None,
    ),
    TestCase(
        "version1-incremental-batch-compaction",
        "cloud/blockstore/tests/loadtest/local-newfeatures/local-tablet-version-1-multiple-ranges.txt",
        [
            storage_config_with_incremental_batch_compaction(),
            default_storage_config(),
        ],
        None,
    ),
    TestCase(
        "version1-incremental-compaction-and-patching",
        "cloud/blockstore/tests/loadtest/local-newfeatures/local-tablet-version-1-multiple-ranges.txt",
        [
            storage_config_with_incremental_compaction_and_patching(),
            default_storage_config(),
        ],
        None,
    ),
    TestCase(
        "version1-incremental-compaction-and-patching-small-disk",
        "cloud/blockstore/tests/loadtest/local-newfeatures/local-tablet-version-1-small-disk.txt",
        [
            storage_config_with_incremental_compaction_and_patching(),
            default_storage_config(),
        ],
        None,
        restart_interval=None,
    ),
    TestCase(
        "version1-large-multipartition-disk",
        "cloud/blockstore/tests/loadtest/local-newfeatures/local-tablet-version-1-large-multipartition-disk.txt",
        [
            storage_config_with_multiple_partitions(16),
            storage_config_with_multiple_partitions(1),
        ],
        None,
    ),
    TestCase(
        "version1-largest-two-partition-disk",
        "cloud/blockstore/tests/loadtest/local-newfeatures/local-tablet-version-1-largest-two-partition-disk.txt",
        [
            default_storage_config_with_multipartition_volumes_feature(),
        ],
        None,
    ),
    TestCase(
        "version1-incremental-compaction-cloudid",
        "cloud/blockstore/tests/loadtest/local-newfeatures/local-tablet-version-1-multiple-ranges.txt",
        None,
        features_config_with_incremental_compaction(),
    ),
    TestCase(
        "version1-fresh-channel-writes-enabled",
        "cloud/blockstore/tests/loadtest/local-newfeatures/local-tablet-version-1-fresh-channel-writes.txt",
        [
            storage_config_with_fresh_channel_writes_enabled(False),
            storage_config_with_fresh_channel_writes_enabled(True),
        ],
        None,
    ),
    TestCase(
        "version1-fresh-channel-writes-enabled-no-restarts",
        "cloud/blockstore/tests/loadtest/local-newfeatures/local-tablet-version-1-fresh-channel-writes.txt",
        [
            storage_config_with_fresh_channel_writes_enabled(True),
        ],
        None,
        restart_interval=None,
    ),
    TestCase(
        "version1-mixed-index-cache",
        "cloud/blockstore/tests/loadtest/local-newfeatures/local-tablet-version-1-mixed-index-cache.txt",
        [
            storage_config_with_mixed_index_cache_enabled(),
        ],
        None,
    ),
    TestCase(
        "version1-adding-unconfirmed-blobs",
        "cloud/blockstore/tests/loadtest/local-newfeatures/local-tablet-version-1-adding-unconfirmed-blobs.txt",
        [
            storage_config_with_adding_unconfirmed_blobs_enabled(),
            default_storage_config(),
        ],
        None,
    ),
    TestCase(
        "version1-compaction-to-mixed-channel",
        "cloud/blockstore/tests/loadtest/local-newfeatures/local-tablet-version-1-compaction-to-mixed-channel.txt",
        [
            ordinary_prod_storage_config(),
            storage_config_with_compaction_merged_blob_threshold_hdd(),
            ordinary_prod_storage_config(),
        ],
        None,
    ),
]


def __run_test(test_case):
    endpoint_storage_dir = common.output_path() + '/endpoints-' + str(uuid.uuid4())
    nbd_socket_suffix = "_nbd"

    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(TServerConfig())
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.StrictContractValidation = True
    server.ServerConfig.NbdEnabled = True
    server.ServerConfig.NbdSocketSuffix = nbd_socket_suffix
    server.ServerConfig.EndpointStorageType = EEndpointStorageType.ENDPOINT_STORAGE_FILE
    server.ServerConfig.EndpointStorageDir = endpoint_storage_dir
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    env = LocalLoadTest(
        "",
        server_app_config=server,
        storage_config_patches=test_case.storage_config_patches,
        use_in_memory_pdisks=True,
        features_config_patch=test_case.features_config_patch,
        restart_interval=test_case.restart_interval,
    )

    client = TClientConfig()
    client.NbdSocketSuffix = nbd_socket_suffix

    try:
        ret = run_test(
            test_case.name,
            test_case.config_path,
            env.nbs_port,
            env.mon_port,
            client_config=client,
            endpoint_storage_dir=endpoint_storage_dir,
            env_processes=[env.nbs],
        )
    finally:
        env.tear_down()

    return ret


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_load(test_case):
    config = common.get_param("config")
    if config is None:
        test_case.config_path = common.source_path(test_case.config_path)
        return __run_test(test_case)
