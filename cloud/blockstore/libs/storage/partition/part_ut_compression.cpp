#include "part_ut.h"

#include "part_events_private.h"
#include "part_ut_overlay.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/critical_events_init.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/core/compaction_options.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/testlib/part_client.h>

#include <cloud/storage/core/libs/common/sglist_test.h>

#include <library/cpp/blockcodecs/codecs.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

void DoTestCompression(
    ui32 writeBlobThreshold,
    ui32 uncompressedExpected,
    ui32 compressedExpected,
    ui64 chunkSize = 0)
{
    auto config = DefaultConfig();
    config.SetBlobCompressionRate(1);
    config.SetWriteBlobThreshold(writeBlobThreshold);
    config.SetFreshChannelWriteRequestsEnabled(true);
    NProto::TDiagnosticsConfig diagnosticsConfig;
    if (chunkSize) {
        diagnosticsConfig.AddBlobCompressionChunkSizes(chunkSize);
    }

    auto runtime = PrepareTestActorRuntime(
        config,
        1024,
        {},
        TTestPartitionInfo(),
        {},
        EStorageAccessMode::Default,
        std::move(diagnosticsConfig));

    TPartitionClient partition(*runtime);
    partition.WaitReady();

    ui32 uncompressed = 0;
    ui32 compressed = 0;

    auto obs = [&](TAutoPtr<IEventHandle>& event)
    {
        if (event->GetTypeRewrite() == TEvStatsService::EvVolumePartCounters) {
            auto* msg = event->Get<TEvStatsService::TEvVolumePartCounters>();

            if (chunkSize) {
                const auto& cc =
                    msg->DiskCounters->BlobCompression.at(chunkSize);
                uncompressed = cc.UncompressedBytesWritten.Value;
                compressed = cc.CompressedBytesWritten.Value;
            } else {
                const auto& cc = msg->DiskCounters->Cumulative;
                uncompressed = cc.UncompressedBytesWritten.Value;
                compressed = cc.CompressedBytesWritten.Value;
            }
        }

        return TTestActorRuntime::DefaultObserverFunc(event);
    };

    runtime->SetObserverFunc(obs);

    partition.WriteBlocks(1, 1);

    partition.SendToPipe(
        std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvStatsService::EvVolumePartCounters);
        runtime->DispatchEvents(options);
    }

    UNIT_ASSERT_VALUES_EQUAL(uncompressedExpected, uncompressed);
    UNIT_ASSERT_VALUES_EQUAL(compressedExpected, compressed);
}

enum class ECompressionBlobSource
{
    MergedWrite,
    MixedWrite,
    MixedFlush,
    MergedCompaction,
    MixedCompaction,
};

void DoTestChunkCompression(
    bool local,
    ui32 compressionRate,
    ECompressionBlobSource source = ECompressionBlobSource::MergedWrite)
{
    const bool mixedWrite = source == ECompressionBlobSource::MixedWrite ||
                            source == ECompressionBlobSource::MixedFlush;
    auto config = DefaultConfig();
    config.SetBlobCompressionRate(compressionRate);
    config.SetBlobCompressionCodec("lz4");
    config.SetWriteBlobThreshold(mixedWrite ? Max<ui32>() : 1);
    if (source == ECompressionBlobSource::MixedWrite) {
        config.SetWriteRequestBatchingEnabled(true);
        config.SetWriteMixedBlobThresholdHDD(DefaultBlockSize);
    }
    config.SetCompactionMergedBlobThresholdHDD(
        source == ECompressionBlobSource::MixedCompaction ? Max<ui32>() : 0);
    config.SetBlobPatchingEnabled(false);
    NProto::TDiagnosticsConfig diagnosticsConfig;
    const TVector<ui64> chunkSizes{
        0,
        DefaultBlockSize,
        2 * DefaultBlockSize,
        10 * DefaultBlockSize,
        (1ull << 32) + 1};
    for (const ui64 size: chunkSizes) {
        diagnosticsConfig.AddBlobCompressionChunkSizes(size);
    }
    // Repeating a size should not count the same experiment twice.
    diagnosticsConfig.AddBlobCompressionChunkSizes(2 * DefaultBlockSize);

    auto runtime = PrepareTestActorRuntime(
        config,
        1024,
        {},
        TTestPartitionInfo(),
        {},
        EStorageAccessMode::Default,
        std::move(diagnosticsConfig));
    TPartitionClient partition(*runtime);
    partition.WaitReady();

    THashMap<ui64, TBlobCompressionCounters> compressionCounters;
    ui64 uncompressedBytes = 0;
    ui64 compressedBytes = 0;
    ui32 estimatedWrites = 0;
    ui32 skippedWrites = 0;
    runtime->SetObserverFunc(
        [&](auto& event)
        {
            if (event->GetTypeRewrite() ==
                TEvPartitionCommonPrivate::EvWriteBlobRequest)
            {
                const auto* msg = event->template Get<
                    TEvPartitionCommonPrivate::TEvWriteBlobRequest>();
                skippedWrites += !msg->CompressedSizeEstimationCandidate;
                estimatedWrites += msg->CompressedSizeEstimationCandidate;
            }
            if (event->GetTypeRewrite() ==
                TEvStatsService::EvVolumePartCounters) {
                const auto* msg = event->template Get<
                    TEvStatsService::TEvVolumePartCounters>();
                compressionCounters = msg->DiskCounters->BlobCompression;
                uncompressedBytes = msg->DiskCounters->Cumulative
                                        .UncompressedBytesWritten.Value;
                compressedBytes =
                    msg->DiskCounters->Cumulative.CompressedBytesWritten.Value;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });

    const TString block(DefaultBlockSize, 1);
    if (local) {
        auto request = partition.CreateWriteBlocksLocalRequest(
            TBlockRange32::WithLength(1, 5),
            block);
        request->Record.SetBlockSize(DefaultBlockSize);
        partition.SendToPipe(std::move(request));
        const auto response = partition.RecvWriteBlocksLocalResponse();
        UNIT_ASSERT_C(
            SUCCEEDED(response->GetStatus()),
            response->GetErrorReason());
    } else {
        partition.WriteBlocks(TBlockRange32::WithLength(1, 5), 1);
    }

    UNIT_ASSERT_VALUES_EQUAL(mixedWrite ? 0 : 1, estimatedWrites);
    if (source == ECompressionBlobSource::MixedWrite) {
        UNIT_ASSERT(skippedWrites > 0);
    }
    if (source == ECompressionBlobSource::MixedFlush) {
        partition.Flush();
        UNIT_ASSERT(skippedWrites > 0);
        UNIT_ASSERT_VALUES_EQUAL(0, estimatedWrites);
    } else if (
        source == ECompressionBlobSource::MergedCompaction ||
        source == ECompressionBlobSource::MixedCompaction)
    {
        partition.Compaction(
            0,
            TCompactionOptions().set(ToBit(ECompactionOption::Full)));
        if (source == ECompressionBlobSource::MergedCompaction) {
            UNIT_ASSERT_VALUES_EQUAL(2, estimatedWrites);
            UNIT_ASSERT_VALUES_EQUAL(0, skippedWrites);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(1, estimatedWrites);
            UNIT_ASSERT(skippedWrites > 0);
        }
    }

    // Compression is only an estimate; the original data must be stored.
    UNIT_ASSERT_VALUES_EQUAL(block, GetBlockContent(partition.ReadBlocks(1)));

    partition.SendToPipe(
        std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvStatsService::EvVolumePartCounters);
    runtime->DispatchEvents(options);

    if (!compressionRate || mixedWrite) {
        UNIT_ASSERT_VALUES_EQUAL(0, uncompressedBytes);
        UNIT_ASSERT_VALUES_EQUAL(0, compressedBytes);
        UNIT_ASSERT_VALUES_EQUAL(chunkSizes.size(), compressionCounters.size());
        for (const auto& [size, counters]: compressionCounters) {
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                counters.UncompressedBytesWritten.Value);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.CompressedBytesWritten.Value);
        }
        return;
    }

    const auto* codec = NBlockCodecs::Codec(config.GetBlobCompressionCodec());
    const auto oneBlockSize = codec->Encode(block).size();
    const auto twoBlocksSize =
        codec->Encode(TString(2 * DefaultBlockSize, 1)).size();
    const auto wholeBlobSize =
        codec->Encode(TString(5 * DefaultBlockSize, 1)).size();
    UNIT_ASSERT_VALUES_EQUAL(
        estimatedWrites * 5 * DefaultBlockSize,
        uncompressedBytes);
    UNIT_ASSERT_VALUES_EQUAL(estimatedWrites * wholeBlobSize, compressedBytes);
    UNIT_ASSERT_VALUES_EQUAL(chunkSizes.size(), compressionCounters.size());
    for (const ui64 size: chunkSizes) {
        const auto& counters = compressionCounters.at(size);
        UNIT_ASSERT_VALUES_EQUAL(
            estimatedWrites * 5 * DefaultBlockSize,
            counters.UncompressedBytesWritten.Value);
        ui64 expected = wholeBlobSize;
        if (size == DefaultBlockSize) {
            expected = 5 * oneBlockSize;
        } else if (size == 2 * DefaultBlockSize) {
            expected = 2 * twoBlocksSize + oneBlockSize;
        }
        UNIT_ASSERT_VALUES_EQUAL(
            estimatedWrites * expected,
            counters.CompressedBytesWritten.Value);
    }
}

void TestScanWithMissingCompressionMetadata(bool duplicateBlob)
{
    auto config = DefaultConfig();
    config.SetWriteBlobThreshold(1);
    config.SetVerifyBlobCompressionMetadata(true);
    auto runtime = PrepareTestActorRuntime(config);
    TPartitionClient partition(*runtime);
    partition.WaitReady();
    partition.WriteBlocks(TBlockRange32::WithLength(0, 4), 'a');
    auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    InitCriticalEventsCounter(counters);
    auto missingMetadata = counters->GetCounter(
        GetCriticalEventForBlobCompressionMetadataAbsent(),
        true);
    ui32 reads = 0;
    ui32 completionStatus = S_OK;
    runtime->SetObserverFunc(
        [&](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvPartitionPrivate::EvScanDiskBatchResponse)
            {
                auto* batch =
                    event->Get<TEvPartitionPrivate::TEvScanDiskBatchResponse>();
                if (duplicateBlob) {
                    UNIT_ASSERT_VALUES_EQUAL(1, batch->BlobsInBatch.size());
                    const auto blob = batch->BlobsInBatch.front();
                    batch->BlobsInBatch.push_back(blob);
                } else {
                    batch->BlobId2CompressionMetadata.clear();
                }
            }
            if (event->GetTypeRewrite() ==
                TEvPartitionCommonPrivate::EvReadBlobRequest)
            {
                ++reads;
            }
            if (event->GetTypeRewrite() ==
                TEvPartitionPrivate::EvScanDiskCompleted) {
                completionStatus =
                    event->Get<TEvPartitionPrivate::TEvScanDiskCompleted>()
                        ->GetStatus();
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });
    partition.ScanDisk(10);
    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvPartitionPrivate::EvScanDiskCompleted);
    runtime->DispatchEvents(options, TDuration::Seconds(1));
    UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, completionStatus);
    UNIT_ASSERT_VALUES_EQUAL(1, missingMetadata->Val());
    UNIT_ASSERT_VALUES_EQUAL(duplicateBlob ? 1 : 0, reads);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionCompressionTest)
{
    Y_UNIT_TEST(ShouldPassMergedBlobCompressionMetadataToReaders)
    {
        for (bool splitCompactionTx: {false, true}) {
            auto config = DefaultConfig();
            config.SetWriteBlobThreshold(1);
            config.SetVerifyBlobCompressionMetadata(true);
            config.SetBlobPatchingEnabled(true);
            config.SetCompactionMergedBlobThresholdHDD(1);
            config.SetSplitCompactionTxEnabled(splitCompactionTx);
            auto runtime = PrepareTestActorRuntime(config, 2 * MaxBlocksCount);
            TPartitionClient partition(*runtime);
            partition.WaitReady();

            NProto::TCompressionMetadata compressionMetadata;
            compressionMetadata.SetCodec(
                NCloud::NProto::COMPRESSION_CODEC_ZSTD_3);
            compressionMetadata.SetChunkSize(2 * DefaultBlockSize);
            compressionMetadata.SetLastChunkSize(2 * DefaultBlockSize);
            compressionMetadata.MutablePlainChunks()->AddCompressedChunkSize(
                123);
            ui32 readsWithCompressionMetadata = 0;
            bool patchSeen = false;
            runtime->SetObserverFunc(
                [&](TAutoPtr<IEventHandle>& event)
                {
                    if (event->GetTypeRewrite() ==
                        TEvPartitionPrivate::EvAddBlobsRequest)
                    {
                        auto* msg = event->Get<
                            TEvPartitionPrivate::TEvAddBlobsRequest>();
                        if (msg->Mode == ADD_WRITE_RESULT) {
                            TVector<TAddMergedBlob> blobs;
                            for (const auto& blob: msg->MergedBlobs) {
                                blobs.emplace_back(
                                    blob.BlobId,
                                    blob.BlockRange,
                                    blob.SkipMask,
                                    blob.Checksums,
                                    compressionMetadata);
                            }
                            msg->MergedBlobs = std::move(blobs);
                        }
                    }
                    if (event->GetTypeRewrite() ==
                        TEvPartitionCommonPrivate::EvReadBlobRequest)
                    {
                        auto* msg = event->Get<
                            TEvPartitionCommonPrivate::TEvReadBlobRequest>();
                        if (msg->CompressionMetadata) {
                            UNIT_ASSERT_VALUES_EQUAL(
                                msg->CompressionMetadata->SerializeAsString(),
                                compressionMetadata.SerializeAsString());
                            ++readsWithCompressionMetadata;
                            // This test checks metadata transport. The writer
                            // stores plain data; decompression has its own
                            // tests.
                            msg->CompressionMetadata.reset();
                        }
                    }
                    patchSeen |= event->GetTypeRewrite() ==
                                 TEvPartitionCommonPrivate::EvPatchBlobRequest;
                    return TTestActorRuntime::DefaultObserverFunc(event);
                });

            const auto range = TBlockRange32::WithLength(0, 4);
            partition.WriteBlocks(range, 'a');
            partition.RebootTablet();
            partition.WaitReady();
            auto response = partition.ReadBlocks(range);
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent('a', range.Size()),
                GetBlocksContent(response));
            UNIT_ASSERT_VALUES_EQUAL(1, readsWithCompressionMetadata);

            const auto described = partition.DescribeBlocks(range);
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                described->Record.BlobId2CompressionMetadataSize());
            UNIT_ASSERT_VALUES_EQUAL(
                compressionMetadata.SerializeAsString(),
                described->Record.GetBlobId2CompressionMetadata(0)
                    .GetCompressionMetadata()
                    .SerializeAsString());

            readsWithCompressionMetadata = 0;
            partition.ScanDisk(10);
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionPrivate::EvScanDiskCompleted);
            runtime->DispatchEvents(options, TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(1, readsWithCompressionMetadata);

            readsWithCompressionMetadata = 0;
            partition.Compaction(
                0,
                TCompactionOptions().set(ToBit(ECompactionOption::Full)));
            UNIT_ASSERT_VALUES_EQUAL(1, readsWithCompressionMetadata);
            UNIT_ASSERT(!patchSeen);
            response = partition.ReadBlocks(range);
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent('a', range.Size()),
                GetBlocksContent(response));
            UNIT_ASSERT_VALUES_EQUAL(1, readsWithCompressionMetadata);

            // Both compaction ranges need the metadata of this single blob.
            const auto crossingRange =
                TBlockRange32::WithLength(MaxBlocksCount - 2, 4);
            partition.WriteBlocks(crossingRange, 'b');
            readsWithCompressionMetadata = 0;
            partition.Compaction(
                TVector<ui32>{crossingRange.Start, crossingRange.End},
                TCompactionOptions().set(ToBit(ECompactionOption::Full)));
            UNIT_ASSERT_VALUES_EQUAL(2, readsWithCompressionMetadata);
        }
    }

    Y_UNIT_TEST(ShouldRejectScanWithoutCompressionMetadata)
    {
        TestScanWithMissingCompressionMetadata(false);
    }

    Y_UNIT_TEST(ShouldRejectSecondExtractionOfScanCompressionMetadata)
    {
        TestScanWithMissingCompressionMetadata(true);
    }

    Y_UNIT_TEST(ShouldRejectMissingMetadataBeforeDispatchingLocalReads)
    {
        auto config = DefaultConfig();
        config.SetVerifyBlobCompressionMetadata(true);
        auto [runtime, partition] = NOverlayTest::SetupOverlayPartition(
            TestTabletId,
            TestTabletId2,
            {NOverlayTest::TBlob(1, 0), NOverlayTest::TBlob(2, 0)},
            {},
            DefaultBlockSize,
            2,
            config);

        ui32 reads = 0;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                using TDescribeCompleted =
                    TEvPartitionCommonPrivate::TEvDescribeBlocksCompleted;
                if (event->GetTypeRewrite() == TDescribeCompleted::EventType) {
                    auto& metadata = event->Get<TDescribeCompleted>()
                                         ->BlobId2CompressionMetadata;
                    if (metadata) {
                        UNIT_ASSERT_VALUES_EQUAL(2, metadata.size());
                        UNIT_ASSERT(
                            metadata.front().BlobId < metadata.back().BlobId);
                        metadata.pop_back();
                    }
                }
                if (event->GetTypeRewrite() ==
                    TEvPartitionCommonPrivate::EvReadBlobRequest)
                {
                    ++reads;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        const auto range = TBlockRange32::WithLength(0, 2);
        TVector<TString> blocks;
        auto sglist = ResizeBlocks(blocks, range.Size(), GetBlockContent(1));
        partition->SendReadBlocksLocalRequest(range, std::move(sglist));
        const auto response = partition->RecvReadBlocksLocalResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(0, reads);
    }

    Y_UNIT_TEST(ShouldCompressBlobs)
    {
        DoTestCompression(1, DefaultBlockSize, 34);
    }

    Y_UNIT_TEST(ShouldSkipCompressionForFreshBlocks)
    {
        DoTestCompression(Max<ui32>(), 0, 0);
    }

    Y_UNIT_TEST(ShouldSkipChunkCompressionForFreshBlobs)
    {
        DoTestCompression(Max<ui32>(), 0, 0, 1);
    }

    Y_UNIT_TEST(ShouldEstimateCompressionForEachChunkSize)
    {
        DoTestChunkCompression(false, 1);
    }

    Y_UNIT_TEST(ShouldEstimateChunkCompressionForLocalWrites)
    {
        DoTestChunkCompression(true, 1);
    }

    Y_UNIT_TEST(ShouldDisableChunkCompressionWhenRateIsZero)
    {
        DoTestChunkCompression(false, 0);
    }

    Y_UNIT_TEST(ShouldEstimateChunkCompressionForMergedCompactionBlobs)
    {
        DoTestChunkCompression(
            false,
            1,
            ECompressionBlobSource::MergedCompaction);
    }

    Y_UNIT_TEST(ShouldSkipChunkCompressionForMixedFlushBlobs)
    {
        DoTestChunkCompression(false, 1, ECompressionBlobSource::MixedFlush);
    }

    Y_UNIT_TEST(ShouldSkipChunkCompressionForMixedUserBlobs)
    {
        DoTestChunkCompression(false, 1, ECompressionBlobSource::MixedWrite);
    }

    Y_UNIT_TEST(ShouldSkipChunkCompressionForMixedCompactionBlobs)
    {
        DoTestChunkCompression(
            false,
            1,
            ECompressionBlobSource::MixedCompaction);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
