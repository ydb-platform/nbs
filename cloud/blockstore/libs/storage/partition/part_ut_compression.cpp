#include "part_ut.h"

#include "part_events_private.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/storage/partition_common/blob_compressor.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/testlib/part_client.h>

#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>
#include <util/random/fast.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

namespace {

struct TPartitionWithRuntime
{
    std::unique_ptr<TTestActorRuntime> Runtime;
    std::unique_ptr<TPartitionClient> Partition;
};

TPartitionWithRuntime SetupPartition(
    const NProto::TStorageServiceConfig& config,
    TMaybe<ui32> maxBlocksInBlob = {})
{
    TPartitionWithRuntime result;
    result.Runtime = PrepareTestActorRuntime(
        config,
        1024,
        {},
        {.MaxBlocksInBlob = maxBlocksInBlob});
    result.Partition = std::make_unique<TPartitionClient>(*result.Runtime);
    result.Partition->WaitReady();
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionCompressionTest)
{
    void DoShouldCompressUserMergedWrites(
        bool local,
        bool deferred,
        bool rebootBeforeIndex = false,
        bool compressionEnabled = true,
        ui64 minSavingsPercent = 0,
        bool checksumsEnabled = true,
        const TString& codecName = "lz4")
    {
        constexpr ui32 maxBlocksInBlob = 8;
        constexpr ui32 blocksPerChunk = 3;
        constexpr ui32 blockCount = 2 * maxBlocksInBlob + 1;
        const auto range = TBlockRange32::WithLength(0, blockCount);
        const bool expectCompression =
            compressionEnabled && minSavingsPercent < 100;

        NProto::TStorageServiceConfig config;
        config.SetBlobCompressionCodec(codecName);
        config.SetWriteBlobThreshold(1);
        config.SetWriteBlobThresholdSSD(1);
        config.SetAddingUnconfirmedBlobsEnabled(deferred);
        config.SetMergedBlobsCompressionEnabled(compressionEnabled);
        config.SetMergedBlobsCompressionChunkSize(
            blocksPerChunk * DefaultBlockSize);
        config.SetMergedBlobsCompressionMinSavingsPercent(minSavingsPercent);
        config.SetDiskPrefixLengthWithBlockChecksumsInBlobs(
            checksumsEnabled ? 1_GB : 0);
        config.SetCheckBlockChecksumsInBlobsUponRead(true);
        config.SetExtendedBlobCountersEnabled(true);
        config.SetHDDMaxBlobsPerRange(100);
        config.SetSSDMaxBlobsPerRange(100);
        auto env = SetupPartition(config, maxBlocksInBlob);
        auto& partition = *env.Partition;
        auto& runtime = *env.Runtime;

        TVector<TString> blocks;
        for (ui32 i = 0; i < blockCount; ++i) {
            blocks.emplace_back(DefaultBlockSize, 'a' + i);
        }

        bool inspectWrites = true;
        bool dropAddConfirmed = rebootBeforeIndex;
        ui32 indexedBlobs = 0;
        ui32 unconfirmedBlobs = 0;
        TVector<NKikimr::TLogoBlobID> writtenBlobs;
        ui64 storedBytes = 0;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                if (dropAddConfirmed &&
                    event->GetTypeRewrite() ==
                        TEvPartitionPrivate::EvAddConfirmedBlobsRequest)
                {
                    return true;
                }
                if (!inspectWrites) {
                    return false;
                }
                switch (event->GetTypeRewrite()) {
                    case NKikimr::TEvBlobStorage::EvPut: {
                        const auto* msg =
                            event->Get<NKikimr::TEvBlobStorage::TEvPut>();
                        if (msg->Id.Channel() >= 3) {
                            UNIT_ASSERT_VALUES_EQUAL(
                                msg->Id.BlobSize(),
                                msg->Buffer.size());
                            writtenBlobs.push_back(msg->Id);
                            storedBytes += msg->Buffer.size();
                        }
                        break;
                    }
                    case TEvPartitionPrivate::EvAddUnconfirmedBlobsRequest: {
                        const auto* msg =
                            event->Get<TEvPartitionPrivate::
                                           TEvAddUnconfirmedBlobsRequest>();
                        for (const auto& blob: msg->Blobs) {
                            UNIT_ASSERT_VALUES_EQUAL(
                                expectCompression,
                                blob.CompressionMetadata.has_value());
                            ++unconfirmedBlobs;
                        }
                        break;
                    }
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        const auto* msg = event->Get<
                            TEvPartitionPrivate::TEvAddBlobsRequest>();
                        if (msg->Mode != ADD_WRITE_RESULT) {
                            break;
                        }
                        UNIT_ASSERT(msg->MixedBlobs.empty());
                        for (const auto& blob: msg->MergedBlobs) {
                            UNIT_ASSERT_VALUES_EQUAL(
                                expectCompression,
                                blob.CompressionMetadata.has_value());
                            UNIT_ASSERT(
                                Find(
                                    writtenBlobs,
                                    MakeBlobId(TestTabletId, blob.BlobId)) !=
                                writtenBlobs.end());
                            if (expectCompression) {
                                const auto& info = *blob.CompressionMetadata;
                                UNIT_ASSERT(
                                    ParseCompressionCodec(codecName)
                                        .GetResult() == info.GetCodec());
                                UNIT_ASSERT_VALUES_EQUAL(
                                    blocksPerChunk * DefaultBlockSize,
                                    info.GetChunkSize());
                                UNIT_ASSERT_VALUES_EQUAL(
                                    (blob.BlockRange.Size() + blocksPerChunk -
                                     1) /
                                        blocksPerChunk,
                                    info.GetPlainChunks()
                                            .CompressedChunkSizeSize() +
                                        1);
                                UNIT_ASSERT(info.HasPlainChunks());
                                ui32 totalSize = 0;
                                for (const auto size:
                                     info.GetPlainChunks()
                                         .GetCompressedChunkSize())
                                {
                                    UNIT_ASSERT_GT(size, 0);
                                    totalSize += size;
                                    UNIT_ASSERT_LT(
                                        totalSize,
                                        blob.BlobId.BlobSize());
                                }
                                const ui32 size = blob.BlobId.BlobSize();
                                UNIT_ASSERT(
                                    size <
                                    blob.BlockRange.Size() * DefaultBlockSize);
                            } else {
                                UNIT_ASSERT_VALUES_EQUAL(
                                    blob.BlockRange.Size() * DefaultBlockSize,
                                    blob.BlobId.BlobSize());
                            }
                            // Deferred checksums are not persisted across
                            // reboot.
                            if (checksumsEnabled && !rebootBeforeIndex) {
                                UNIT_ASSERT_VALUES_EQUAL(
                                    blob.BlockRange.Size(),
                                    blob.Checksums.size());
                                for (ui32 i = 0; i < blob.Checksums.size(); ++i)
                                {
                                    const auto& block =
                                        blocks[blob.BlockRange.Start + i];
                                    UNIT_ASSERT_VALUES_EQUAL(
                                        ComputeDefaultDigest(
                                            {block.data(), block.size()}),
                                        blob.Checksums[i]);
                                }
                            } else {
                                UNIT_ASSERT(blob.Checksums.empty());
                            }
                            ++indexedBlobs;
                        }
                        break;
                    }
                }
                return false;
            });

        if (local) {
            auto request =
                partition.CreateWriteBlocksLocalRequest(range, blocks.front());
            TSgList sgList;
            for (const auto& block: blocks) {
                sgList.emplace_back(block.data(), block.size());
            }
            request->Record.Sglist = TGuardedSgList(std::move(sgList));
            request->Record.SetBlockSize(DefaultBlockSize);
            partition.SendToPipe(std::move(request));
            UNIT_ASSERT_C(
                !HasError(partition.RecvWriteBlocksLocalResponse()->GetError()),
                "Local write failed");
        } else {
            auto request = partition.CreateWriteBlocksRequest(range);
            for (ui32 i = 0; i < blockCount; ++i) {
                *request->Record.MutableBlocks()->MutableBuffers(i) = blocks[i];
            }
            partition.SendToPipe(std::move(request));
            UNIT_ASSERT_C(
                !HasError(partition.RecvWriteBlocksResponse()->GetError()),
                "Write failed");
        }

        UNIT_ASSERT_VALUES_EQUAL(3, writtenBlobs.size());
        UNIT_ASSERT_VALUES_EQUAL(deferred ? 3 : 0, unconfirmedBlobs);
        if (rebootBeforeIndex) {
            UNIT_ASSERT_VALUES_EQUAL(0, indexedBlobs);
            dropAddConfirmed = false;
            partition.RebootTablet();
            partition.WaitReady();
        }

        const auto checkReads = [&]
        {
            for (const auto readRange:
                 {range,
                  TBlockRange32::WithLength(2, 4),
                  TBlockRange32::WithLength(7, 3),
                  TBlockRange32::WithLength(blockCount - 1, 1)})
            {
                const auto response = partition.ReadBlocks(readRange);
                for (ui32 i = 0; i < readRange.Size(); ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        blocks[readRange.Start + i],
                        response->Record.GetBlocks().GetBuffers(i));
                }
            }
        };
        checkReads();
        UNIT_ASSERT_VALUES_EQUAL(3, indexedBlobs);
        inspectWrites = false;

        const auto checkCounters = [&]
        {
            partition.SendToPipe(
                std::make_unique<
                    TEvPartitionCommonPrivate::TEvGetPartCountersRequest>());
            const auto response = partition.RecvResponse<
                TEvPartitionCommonPrivate::TEvGetPartCountersResponse>();
            const auto& counters = response->DiskCounters->Simple;
            UNIT_ASSERT_VALUES_EQUAL(
                storedBytes,
                counters.MergedBytesCount.Value);
            UNIT_ASSERT_VALUES_EQUAL(
                storedBytes,
                counters.MergedIndexBytesCount.Value);
            UNIT_ASSERT_VALUES_EQUAL(
                blockCount * DefaultBlockSize,
                counters.LogicalMergedBytesCount.Value);
            UNIT_ASSERT_VALUES_EQUAL(
                blockCount * DefaultBlockSize,
                counters.LogicalMergedIndexBytesCount.Value);
        };
        checkCounters();
        if (!rebootBeforeIndex) {
            UNIT_ASSERT_VALUES_EQUAL(
                blockCount,
                partition.StatPartition()
                    ->Record.GetStats()
                    .GetUserWriteCounters()
                    .GetBlocksCount());
        }
        partition.RebootTablet();
        partition.WaitReady();
        checkReads();
        checkCounters();

        blocks[0] = TString(DefaultBlockSize, 'z');
        partition.WriteBlocks(0, 'z');
        partition.Compaction(0);
        partition.Cleanup();
        checkReads();
        partition.RebootTablet();
        partition.WaitReady();
        checkReads();
    }

    Y_UNIT_TEST(ShouldCompressUserMergedWrites)
    {
        for (const bool local: {false, true}) {
            for (const bool deferred: {false, true}) {
                DoShouldCompressUserMergedWrites(local, deferred);
            }
        }
    }

    Y_UNIT_TEST(ShouldUseConfiguredCodecForMergedWrites)
    {
        for (const TString& codecName: {TString("snappy"), TString("zstd")}) {
            DoShouldCompressUserMergedWrites(
                false,
                true,
                false,
                true,
                0,
                true,
                codecName);
        }
    }

    Y_UNIT_TEST(ShouldRecoverCompressedUserWritesBeforeDeferredIndexing)
    {
        for (const bool local: {false, true}) {
            DoShouldCompressUserMergedWrites(local, true, true);
        }
    }

    Y_UNIT_TEST(ShouldKeepUserMergedWritesUncompressedWhenDisabledOrRejected)
    {
        for (const bool local: {false, true}) {
            for (const bool deferred: {false, true}) {
                DoShouldCompressUserMergedWrites(local, deferred, false, false);
                DoShouldCompressUserMergedWrites(
                    local,
                    deferred,
                    false,
                    true,
                    100);
            }
        }
    }

    Y_UNIT_TEST(ShouldCompressUserMergedWritesWithoutChecksums)
    {
        for (const bool deferred: {false, true}) {
            DoShouldCompressUserMergedWrites(
                false,
                deferred,
                false,
                true,
                0,
                false);
        }
    }

    Y_UNIT_TEST(ShouldChecksumCompressedLocalZeroBlocks)
    {
        for (const bool deferred: {false, true}) {
            NProto::TStorageServiceConfig config;
            config.SetWriteBlobThreshold(1);
            config.SetAddingUnconfirmedBlobsEnabled(deferred);
            config.SetMergedBlobsCompressionEnabled(true);
            config.SetDiskPrefixLengthWithBlockChecksumsInBlobs(1_GB);
            config.SetCheckBlockChecksumsInBlobsUponRead(true);
            auto env = SetupPartition(config);
            const auto range = TBlockRange32::WithLength(0, 9);
            const TString zeroBlock(DefaultBlockSize, 0);
            auto request =
                env.Partition->CreateWriteBlocksLocalRequest(range, zeroBlock);
            request->Record.Sglist = TGuardedSgList(TSgList(
                range.Size(),
                TBlockDataRef::CreateZeroBlock(DefaultBlockSize)));
            request->Record.SetBlockSize(DefaultBlockSize);
            env.Partition->SendToPipe(std::move(request));
            UNIT_ASSERT(!HasError(
                env.Partition->RecvWriteBlocksLocalResponse()->GetError()));
            for (const bool reboot: {false, true}) {
                if (reboot) {
                    env.Partition->RebootTablet();
                    env.Partition->WaitReady();
                }
                const auto response = env.Partition->ReadBlocks(range);
                for (const auto& block:
                     response->Record.GetBlocks().GetBuffers())
                {
                    UNIT_ASSERT_VALUES_EQUAL(zeroBlock, block);
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldMixCompressedAndIncompressibleBlobsInUserWrite)
    {
        for (const bool deferred: {false, true}) {
            NProto::TStorageServiceConfig config;
            config.SetWriteBlobThreshold(1);
            config.SetAddingUnconfirmedBlobsEnabled(deferred);
            config.SetMergedBlobsCompressionEnabled(true);
            config.SetDiskPrefixLengthWithBlockChecksumsInBlobs(1_GB);
            config.SetCheckBlockChecksumsInBlobsUponRead(true);
            auto env = SetupPartition(config, 8);
            const auto range = TBlockRange32::WithLength(0, 16);
            auto request = env.Partition->CreateWriteBlocksRequest(range, 'a');
            TFastRng64 rng(123);
            for (ui32 i = 8; i < range.Size(); ++i) {
                for (auto& byte:
                     *request->Record.MutableBlocks()->MutableBuffers(i))
                {
                    byte = static_cast<char>(rng.GenRand());
                }
            }
            const auto expected = request->Record.GetBlocks();
            ui32 compressed = 0;
            ui32 uncompressed = 0;
            env.Runtime->SetObserverFunc(
                [&](TAutoPtr<IEventHandle>& event)
                {
                    if (event->GetTypeRewrite() ==
                        TEvPartitionPrivate::EvAddBlobsRequest)
                    {
                        const auto* msg = event->Get<
                            TEvPartitionPrivate::TEvAddBlobsRequest>();
                        for (const auto& blob: msg->MergedBlobs) {
                            UNIT_ASSERT_VALUES_EQUAL(
                                blob.BlockRange.Start == 0,
                                blob.CompressionMetadata.has_value());
                            UNIT_ASSERT_VALUES_EQUAL(
                                blob.BlockRange.Size(),
                                blob.Checksums.size());
                            if (blob.CompressionMetadata) {
                                ++compressed;
                            } else {
                                ++uncompressed;
                            }
                        }
                    }
                    return TTestActorRuntime::DefaultObserverFunc(event);
                });
            env.Partition->SendToPipe(std::move(request));
            UNIT_ASSERT(!HasError(
                env.Partition->RecvWriteBlocksResponse()->GetError()));
            for (const bool reboot: {false, true}) {
                if (reboot) {
                    env.Partition->RebootTablet();
                    env.Partition->WaitReady();
                }
                const auto response = env.Partition->ReadBlocks(range);
                UNIT_ASSERT_VALUES_EQUAL(
                    expected.SerializeAsString(),
                    response->Record.GetBlocks().SerializeAsString());
            }
            UNIT_ASSERT_VALUES_EQUAL(1, compressed);
            UNIT_ASSERT_VALUES_EQUAL(1, uncompressed);
        }
    }

    Y_UNIT_TEST(ShouldRejectUserWriteWhenCompressionFails)
    {
        for (const bool deferred: {false, true}) {
            NProto::TStorageServiceConfig config;
            config.SetWriteBlobThreshold(1);
            config.SetAddingUnconfirmedBlobsEnabled(deferred);
            config.SetMergedBlobsCompressionEnabled(true);
            config.SetMergedBlobsCompressionChunkSize(DefaultBlockSize + 1);
            auto env = SetupPartition(config);
            auto& partition = *env.Partition;
            ui32 writes = 0;
            env.Runtime->SetObserverFunc(
                [&](TAutoPtr<IEventHandle>& event)
                {
                    if (event->GetTypeRewrite() ==
                            TEvPartitionCommonPrivate::EvWriteBlobRequest ||
                        event->GetTypeRewrite() ==
                            TEvPartitionPrivate::EvAddUnconfirmedBlobsRequest)
                    {
                        ++writes;
                    }
                    return TTestActorRuntime::DefaultObserverFunc(event);
                });
            partition.SendWriteBlocksRequest(
                TBlockRange32::WithLength(0, 9),
                'a');
            const auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                response->GetError().GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, writes);
            partition.Drain();
            const auto stats = partition.StatPartition()->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUnconfirmedBlobCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetConfirmedBlobCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }
    }

    void DoShouldCompressCompactionBlobs(
        bool compressionEnabled,
        ui64 minSavingsPercent = 0)
    {
        constexpr ui32 dataChannelOffset = 3;
        constexpr ui32 blocksPerChunk = 4;
        constexpr ui32 blockCount = 2 * blocksPerChunk + 1;
        constexpr ui32 chunkSize = blocksPerChunk * DefaultBlockSize;
        const auto range = TBlockRange32::WithLength(0, blockCount);
        const bool expectCompression =
            compressionEnabled && minSavingsPercent < 100;

        NProto::TStorageServiceConfig config;
        config.SetFlushBlobSizeThreshold(1_MB);
        config.SetDiskPrefixLengthWithBlockChecksumsInBlobs(1_GB);
        config.SetMergedBlobsCompressionEnabled(compressionEnabled);
        config.SetMergedBlobsCompressionMinSavingsPercent(minSavingsPercent);
        config.SetMergedBlobsCompressionChunkSize(chunkSize);
        config.SetCheckBlockChecksumsInBlobsUponRead(true);
        config.SetHDDMaxBlobsPerRange(100);
        config.SetSSDMaxBlobsPerRange(100);
        auto partitionWithRuntime = SetupPartition(config);
        auto& partition = *partitionWithRuntime.Partition;
        auto& runtime = *partitionWithRuntime.Runtime;

        for (ui32 i = 0; i < blockCount; ++i) {
            partition.WriteBlocks(i, i + 1);
        }
        partition.Flush();
        partition.WriteBlocks(0, 100);
        partition.Flush();

        TVector<NKikimr::TLogoBlobID> writtenBlobs;
        TPartialBlobId compactedBlobId;
        std::optional<NProto::TCompressionMetadata> compressionMetadata;
        ui32 compactionResults = 0;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case NKikimr::TEvBlobStorage::EvPut: {
                        const auto* msg =
                            event->Get<NKikimr::TEvBlobStorage::TEvPut>();
                        if (msg->Id.Channel() >= dataChannelOffset) {
                            UNIT_ASSERT_VALUES_EQUAL(
                                msg->Id.BlobSize(),
                                msg->Buffer.size());
                            writtenBlobs.push_back(msg->Id);
                        }
                        break;
                    }
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        const auto* msg = event->Get<
                            TEvPartitionPrivate::TEvAddBlobsRequest>();
                        if (msg->Mode == ADD_COMPACTION_RESULT) {
                            UNIT_ASSERT(msg->MixedBlobs.empty());
                            UNIT_ASSERT_VALUES_EQUAL(
                                1,
                                msg->MergedBlobs.size());
                            const auto& blob = msg->MergedBlobs.front();
                            compactedBlobId = blob.BlobId;
                            compressionMetadata = blob.CompressionMetadata;
                            ++compactionResults;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        const auto statsBefore = partition.StatPartition()->Record.GetStats();
        partition.Compaction();
        const auto statsAfter = partition.StatPartition()->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(
            blockCount,
            statsAfter.GetSysWriteCounters().GetBlocksCount() -
                statsBefore.GetSysWriteCounters().GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            blockCount,
            statsAfter.GetRealSysWriteCounters().GetBlocksCount() -
                statsBefore.GetRealSysWriteCounters().GetBlocksCount());

        UNIT_ASSERT_VALUES_EQUAL(1, compactionResults);
        UNIT_ASSERT_VALUES_EQUAL(1, writtenBlobs.size());
        UNIT_ASSERT(
            writtenBlobs.front() == MakeBlobId(TestTabletId, compactedBlobId));
        UNIT_ASSERT_VALUES_EQUAL_C(
            expectCompression,
            compressionMetadata.has_value(),
            "Compaction blob compression metadata must match "
            "MergedBlobsCompressionEnabled");
        if (expectCompression) {
            UNIT_ASSERT(
                compactedBlobId.BlobSize() < blockCount * DefaultBlockSize);
            UNIT_ASSERT(
                NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE ==
                compressionMetadata->GetCodec());
            UNIT_ASSERT_VALUES_EQUAL(
                chunkSize,
                compressionMetadata->GetChunkSize());
            UNIT_ASSERT(compressionMetadata->HasPlainChunks());
            UNIT_ASSERT_VALUES_EQUAL(
                2,
                compressionMetadata->GetPlainChunks()
                    .CompressedChunkSizeSize());
            ui32 totalSize = 0;
            for (const auto size:
                 compressionMetadata->GetPlainChunks().GetCompressedChunkSize())
            {
                UNIT_ASSERT_GT(size, 0);
                totalSize += size;
                UNIT_ASSERT_LT(totalSize, compactedBlobId.BlobSize());
            }
        } else {
            UNIT_ASSERT_VALUES_EQUAL(
                blockCount * DefaultBlockSize,
                compactedBlobId.BlobSize());
        }

        for (const bool reboot: {false, true}) {
            if (reboot) {
                partition.RebootTablet();
                partition.WaitReady();
                // Read the persisted blob again through compaction.
                partition.WriteBlocks(0, 100);
                partition.Flush();
                writtenBlobs.clear();
                compactionResults = 0;
                partition.Compaction();
                UNIT_ASSERT_VALUES_EQUAL(1, compactionResults);
                UNIT_ASSERT_VALUES_EQUAL(1, writtenBlobs.size());
                UNIT_ASSERT_VALUES_EQUAL(
                    expectCompression,
                    compressionMetadata.has_value());
                partition.Cleanup();
            }
            const auto response = partition.ReadBlocks(range);
            for (ui32 i = 0; i < blockCount; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, i == 0 ? 100 : i + 1),
                    response->Record.GetBlocks().GetBuffers(i));
            }
        }
    }

    Y_UNIT_TEST(ShouldCompressCompactionBlobs)
    {
        DoShouldCompressCompactionBlobs(false);
        DoShouldCompressCompactionBlobs(true);
        DoShouldCompressCompactionBlobs(true, 100);
    }

    Y_UNIT_TEST(ShouldReadSingleBlocksFromCompressedChunks)
    {
        constexpr ui32 blocksPerChunk = 4;
        constexpr ui32 blockCount = 2 * blocksPerChunk + 1;
        constexpr ui32 chunkSize = blocksPerChunk * DefaultBlockSize;

        NProto::TStorageServiceConfig config;
        config.SetFlushBlobSizeThreshold(1_MB);
        config.SetDiskPrefixLengthWithBlockChecksumsInBlobs(1_GB);
        config.SetMergedBlobsCompressionEnabled(true);
        config.SetMergedBlobsCompressionChunkSize(chunkSize);
        config.SetCheckBlockChecksumsInBlobsUponRead(true);
        config.SetHDDMaxBlobsPerRange(100);
        config.SetSSDMaxBlobsPerRange(100);
        auto partitionWithRuntime = SetupPartition(config);
        auto& partition = *partitionWithRuntime.Partition;
        auto& runtime = *partitionWithRuntime.Runtime;

        for (ui32 i = 0; i < blockCount; ++i) {
            partition.WriteBlocks(i, 'a' + i);
            if (i + 1 == blocksPerChunk) {
                partition.Flush();
            }
        }
        partition.Flush();

        TPartialBlobId compactedBlobId;
        NProto::TCompressionMetadata compressionMetadata;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvAddBlobsRequest)
                {
                    const auto* msg =
                        event->Get<TEvPartitionPrivate::TEvAddBlobsRequest>();
                    if (msg->Mode == ADD_COMPACTION_RESULT) {
                        UNIT_ASSERT_VALUES_EQUAL(1, msg->MergedBlobs.size());
                        const auto& blob = msg->MergedBlobs.front();
                        UNIT_ASSERT(blob.CompressionMetadata);
                        compactedBlobId = blob.BlobId;
                        compressionMetadata = *blob.CompressionMetadata;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        partition.Compaction();

        UNIT_ASSERT(compactedBlobId);
        UNIT_ASSERT_VALUES_EQUAL(chunkSize, compressionMetadata.GetChunkSize());
        UNIT_ASSERT(compressionMetadata.HasPlainChunks());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            compressionMetadata.GetPlainChunks().CompressedChunkSizeSize());
        const auto blobId = MakeBlobId(TestTabletId, compactedBlobId);
        TVector<ui32> chunkOffsets = {0};
        for (const auto size:
             compressionMetadata.GetPlainChunks().GetCompressedChunkSize())
        {
            UNIT_ASSERT_GT(size, 0);
            chunkOffsets.push_back(chunkOffsets.back() + size);
            UNIT_ASSERT_LT(chunkOffsets.back(), blobId.BlobSize());
        }
        chunkOffsets.push_back(blobId.BlobSize());

        ui32 expectedChunk = 0;
        ui32 blobReads = 0;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == NKikimr::TEvBlobStorage::EvGet) {
                    const auto* msg =
                        event->Get<NKikimr::TEvBlobStorage::TEvGet>();
                    for (ui32 i = 0; i < msg->QuerySize; ++i) {
                        const auto& query = msg->Queries[i];
                        if (query.Id == blobId) {
                            UNIT_ASSERT_VALUES_EQUAL(
                                chunkOffsets[expectedChunk],
                                query.Shift);
                            UNIT_ASSERT_VALUES_EQUAL(
                                chunkOffsets[expectedChunk + 1] -
                                    chunkOffsets[expectedChunk],
                                query.Size);
                            ++blobReads;
                        }
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        for (const bool reboot: {false, true}) {
            if (reboot) {
                partition.RebootTablet();
                partition.WaitReady();
            }
            // Read every position within each chunk, including its boundaries
            // and the single block in the partial final chunk.
            for (ui32 i = 0; i < blockCount; ++i) {
                expectedChunk = i / blocksPerChunk;
                const TString expected(DefaultBlockSize, 'a' + i);
                blobReads = 0;
                const auto response = partition.ReadBlocks(i);
                UNIT_ASSERT_VALUES_EQUAL(
                    1,
                    response->Record.GetBlocks().BuffersSize());
                UNIT_ASSERT_VALUES_EQUAL(
                    expected,
                    response->Record.GetBlocks().GetBuffers(0));
                UNIT_ASSERT_VALUES_EQUAL(1, blobReads);

                TString buffer(DefaultBlockSize, '?');
                blobReads = 0;
                partition.ReadBlocksLocal(i, TStringBuf(buffer));
                UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
                UNIT_ASSERT_VALUES_EQUAL(1, blobReads);
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
