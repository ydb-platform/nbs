#include "part_ut.h"

#include <cloud/blockstore/libs/storage/partition_common/part_thread_safe_state.h>
#include <cloud/blockstore/libs/storage/testlib/part_client.h>

#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <contrib/ydb/core/base/blobstorage.h>

#include <library/cpp/testing/unittest/registar.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;
using namespace NKikimr;
using namespace NCloud::NStorage;
using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionRebuildMetadataTest)
{
    Y_UNIT_TEST(ShouldKeepIndexCountersEmptyWhenDisabled)
    {
        auto config = DefaultConfig();
        config.SetExtendedBlobCountersEnabled(false);

        auto runtime = PrepareTestActorRuntime(config, 2048);
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        auto assertIndexCountersEmpty = [&]
        {
            const auto stats = partition.StatPartition()->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedIndexBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedIndexBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedIndexBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedIndexBlocksCount());
        };

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);
        partition.ZeroBlocks(4);
        partition.Flush();
        // Flush the remaining fresh zero block as a deletion marker.
        partition.Flush();
        partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024), 1);
        assertIndexCountersEmpty();

        const auto stats = partition.StatPartition()->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(1024, stats.GetMergedBlocksCount());

        partition.Compaction();
        partition.Cleanup();
        assertIndexCountersEmpty();

        partition.RebuildMetadata(NProto::BLOCK_COUNT, 100);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            TEvPartitionPrivate::EvMetadataRebuildCompleted);
        runtime->DispatchEvents(options, TDuration::Seconds(1));
        UNIT_ASSERT(partition.GetRebuildMetadataStatus()
                        ->Record.GetProgress()
                        .GetIsCompleted());
        assertIndexCountersEmpty();

        partition.RebootTablet();
        assertIndexCountersEmpty();
        const auto finalStats = partition.StatPartition()->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(
            1027,
            finalStats.GetMixedBlocksCount() +
                finalStats.GetMergedBlocksCount());
    }

    Y_UNIT_TEST(ShouldTrackBlobBytesThroughCompactionCleanupAndReboot)
    {
        for (const bool enabled: {false, true}) {
            auto config = DefaultConfig();
            config.SetExtendedBlobCountersEnabled(enabled);
            config.SetHDDMaxBlobsPerRange(100);
            config.SetSSDMaxBlobsPerRange(100);
            auto runtime = PrepareTestActorRuntime(config, 2048);
            TPartitionClient partition(*runtime);
            partition.WaitReady();

            const auto assertBytes = [&](ui64 mixedBlocks, ui64 mergedBlocks)
            {
                partition.SendToPipe(
                    std::make_unique<TEvPartitionCommonPrivate::
                                         TEvGetPartCountersRequest>());
                const auto response = partition.RecvResponse<
                    TEvPartitionCommonPrivate::TEvGetPartCountersResponse>();
                const auto& counters = response->DiskCounters->Simple;
                const ui64 mixedBytes =
                    enabled ? 0 : mixedBlocks * DefaultBlockSize;
                const ui64 mergedBytes =
                    (mergedBlocks + (enabled ? mixedBlocks : 0)) *
                    DefaultBlockSize;
                UNIT_ASSERT_VALUES_EQUAL(
                    mixedBytes,
                    counters.MixedBytesCount.Value);
                UNIT_ASSERT_VALUES_EQUAL(
                    mergedBytes,
                    counters.MergedBytesCount.Value);
                UNIT_ASSERT_VALUES_EQUAL(
                    mixedBytes,
                    counters.LogicalMixedBytesCount.Value);
                UNIT_ASSERT_VALUES_EQUAL(
                    mergedBytes,
                    counters.LogicalMergedBytesCount.Value);
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled ? mixedBlocks * DefaultBlockSize : 0,
                    counters.MixedIndexBytesCount.Value);
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled ? mergedBlocks * DefaultBlockSize : 0,
                    counters.MergedIndexBytesCount.Value);
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled ? mixedBlocks * DefaultBlockSize : 0,
                    counters.LogicalMixedIndexBytesCount.Value);
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled ? mergedBlocks * DefaultBlockSize : 0,
                    counters.LogicalMergedIndexBytesCount.Value);
            };

            assertBytes(0, 0);
            partition.WriteBlocks(1, 1);
            partition.WriteBlocks(2, 2);
            partition.WriteBlocks(3, 3);
            partition.ZeroBlocks(4);
            partition.Flush();
            partition.Flush();   // Flush the deletion marker as well.
            partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024), 1);
            assertBytes(3, 1024);
            partition.RebuildMetadata(NProto::BLOCK_COUNT, 1);
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionPrivate::EvMetadataRebuildCompleted);
            runtime->DispatchEvents(options, TDuration::Seconds(1));
            UNIT_ASSERT(partition.GetRebuildMetadataStatus()
                            ->Record.GetProgress()
                            .GetIsCompleted());
            assertBytes(3, 1024);
            partition.RebootTablet();
            assertBytes(3, 1024);

            partition.Compaction();
            partition.Cleanup();
            assertBytes(0, 1027);
            partition.RebootTablet();
            assertBytes(0, 1027);

            partition.ZeroBlocks(TBlockRange32::WithLength(1, 3));
            partition.Flush();
            partition.Compaction();
            partition.Cleanup();
            assertBytes(0, 1024);
            partition.RebootTablet();
            assertBytes(0, 1024);
        }
    }

    Y_UNIT_TEST(ShouldTrackBlobBytesWhenReplacingBlobWithSmallerOne)
    {
        auto config = DefaultConfig();
        config.SetExtendedBlobCountersEnabled(true);
        config.SetHDDMaxBlobsPerRange(100);
        config.SetSSDMaxBlobsPerRange(100);
        auto runtime = PrepareTestActorRuntime(config, 4096);
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const ui32 replacementBlobSize = 123;
        const ui64 untouchedBlobSize = 1024 * DefaultBlockSize;
        const auto assertBytes = [&](ui64 storedBytes, ui64 logicalBytes)
        {
            partition.SendToPipe(
                std::make_unique<
                    TEvPartitionCommonPrivate::TEvGetPartCountersRequest>());
            const auto response = partition.RecvResponse<
                TEvPartitionCommonPrivate::TEvGetPartCountersResponse>();
            const auto& counters = response->DiskCounters->Simple;
            UNIT_ASSERT_VALUES_EQUAL(0, counters.MixedBytesCount.Value);
            UNIT_ASSERT_VALUES_EQUAL(
                storedBytes,
                counters.MergedBytesCount.Value);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.LogicalMixedBytesCount.Value);
            UNIT_ASSERT_VALUES_EQUAL(
                logicalBytes,
                counters.LogicalMergedBytesCount.Value);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.MixedIndexBytesCount.Value);
            UNIT_ASSERT_VALUES_EQUAL(
                storedBytes,
                counters.MergedIndexBytesCount.Value);
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                counters.LogicalMixedIndexBytesCount.Value);
            UNIT_ASSERT_VALUES_EQUAL(
                logicalBytes,
                counters.LogicalMergedIndexBytesCount.Value);
        };

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);
        partition.Flush();
        // Keep another blob so an excessive decrement during cleanup
        // cannot be hidden by clamping the byte counter to zero.
        partition.WriteBlocks(TBlockRange32::WithLength(2048, 1024), 1);

        ui32 replacedBlobs = 0;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvAddBlobsRequest)
                {
                    auto* msg =
                        event->Get<TEvPartitionPrivate::TEvAddBlobsRequest>();
                    if (msg->Mode == ADD_COMPACTION_RESULT && !replacedBlobs) {
                        UNIT_ASSERT_VALUES_EQUAL(1, msg->MergedBlobs.size());
                        const auto& blob = msg->MergedBlobs.front();
                        const auto& blobId = blob.BlobId;
                        UNIT_ASSERT_VALUES_EQUAL(
                            3 * DefaultBlockSize,
                            blobId.BlobSize());
                        // Simulate compression without changing the
                        // logical blocks described by the blob.
                        NProto::TBlobCompressionInfo compression;
                        compression.SetCodec(
                            NCloud::NProto::COMPRESSION_CODEC_SNAPPY);
                        compression.AddCompressedChunkSizes(
                            replacementBlobSize);
                        auto* chunks = compression.MutableRegularChunks();
                        chunks->SetMaxUncompressedChunkSize(blobId.BlobSize());
                        chunks->SetLastUncompressedChunkSize(blobId.BlobSize());
                        TVector<TAddMergedBlob> replacementBlobs;
                        replacementBlobs.emplace_back(
                            TPartialBlobId(
                                blobId.Generation(),
                                blobId.Step(),
                                blobId.Channel(),
                                replacementBlobSize,
                                blobId.Cookie(),
                                blobId.PartId()),
                            blob.BlockRange,
                            blob.SkipMask,
                            blob.Checksums,
                            std::move(compression));
                        msg->MergedBlobs.swap(replacementBlobs);
                        ++replacedBlobs;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.Compaction(0);
        UNIT_ASSERT_VALUES_EQUAL(1, replacedBlobs);
        partition.Cleanup();
        assertBytes(
            untouchedBlobSize + replacementBlobSize,
            untouchedBlobSize + 3 * DefaultBlockSize);
        partition.RebootTablet();
        assertBytes(
            untouchedBlobSize + replacementBlobSize,
            untouchedBlobSize + 3 * DefaultBlockSize);

        partition.RebuildMetadata(NProto::BLOCK_COUNT, 1);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            TEvPartitionPrivate::EvMetadataRebuildCompleted);
        runtime->DispatchEvents(options, TDuration::Seconds(1));
        UNIT_ASSERT(partition.GetRebuildMetadataStatus()
                        ->Record.GetProgress()
                        .GetIsCompleted());
        assertBytes(
            untouchedBlobSize + replacementBlobSize,
            untouchedBlobSize + 3 * DefaultBlockSize);
        partition.RebootTablet();
        assertBytes(
            untouchedBlobSize + replacementBlobSize,
            untouchedBlobSize + 3 * DefaultBlockSize);

        // Overwrite every block so compaction does not read the synthetic
        // blob, and cleanup must subtract its stored size.
        partition.ZeroBlocks(TBlockRange32::WithLength(1, 3));
        partition.Flush();
        partition.Compaction(0);
        partition.Cleanup();
        assertBytes(untouchedBlobSize, untouchedBlobSize);
        partition.RebootTablet();
        assertBytes(untouchedBlobSize, untouchedBlobSize);
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateUsedBlocksCount)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024 * 10 + 1), 1);
        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1024 * 5, 1024 * 11),
            1);

        const auto step = 16;
        for (ui32 i = 1024 * 10; i < 1024 * 12; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step), 1);
        }

        for (ui32 i = 1024 * 20; i < 1024 * 21; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step + 1), 1);
        }

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1001111, 1001210),
            1);

        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(1024, 3023));
        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(5024, 5033));

        const auto expected = 1024 * 12 + 1024 + 1 + 100 - 2000 - 10;

        /*
        partition.WriteBlocks(TBlockRange32(5024, 5043), 1);
        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(5024, 5033));
        const auto expected = 10;
        */

        auto response = partition.StatPartition();
        auto stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetUsedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetLogicalUsedBlocksCount());

        partition.RebootTablet();

        response = partition.StatPartition();
        stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetUsedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetLogicalUsedBlocksCount());

        ui32 completionStatus = -1;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvMetadataRebuildCompleted: {
                        using TEv =
                            TEvPartitionPrivate::TEvMetadataRebuildCompleted;
                        auto* msg = event->Get<TEv>();
                        completionStatus = msg->GetStatus();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        const auto rangesPerBatch = 100;
        partition.RebuildMetadata(
            NProto::ERebuildMetadataType::USED_BLOCKS,
            rangesPerBatch);

        UNIT_ASSERT_VALUES_EQUAL(S_OK, completionStatus);

        response = partition.StatPartition();
        stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetUsedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetLogicalUsedBlocksCount());

        partition.RebootTablet();

        response = partition.StatPartition();
        stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetUsedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetLogicalUsedBlocksCount());
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateBlocksCount)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(0, 1024 * 10),
            1);
        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1024 * 5, 1024 * 11),
            1);

        const auto step = 16;
        for (ui32 i = 1024 * 10; i < 1024 * 12; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step), 1);
        }

        for (ui32 i = 1024 * 20; i < 1024 * 21; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step + 1), 1);
        }

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1001111, 1001210),
            1);

        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(1024, 3023));
        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(5024, 5033));

        const auto expected = 1024 * 12 + 1024 + 1 + 100 - 2000 - 10;

        /*
        partition.WriteBlocks(TBlockRange32(5024, 5043), 1);
        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(5024, 5033));
        const auto expected = 10;
        */

        auto response = partition.StatPartition();
        auto stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetUsedBlocksCount());

        partition.RebootTablet();

        response = partition.StatPartition();
        stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetUsedBlocksCount());

        ui32 completionStatus = -1;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvMetadataRebuildCompleted: {
                        using TEv =
                            TEvPartitionPrivate::TEvMetadataRebuildCompleted;
                        auto* msg = event->Get<TEv>();
                        completionStatus = msg->GetStatus();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        const auto rangesPerBatch = 100;
        partition.RebuildMetadata(
            NProto::ERebuildMetadataType::USED_BLOCKS,
            rangesPerBatch);

        UNIT_ASSERT_VALUES_EQUAL(S_OK, completionStatus);

        response = partition.StatPartition();
        stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetUsedBlocksCount());

        partition.RebootTablet();

        response = partition.StatPartition();
        stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetUsedBlocksCount());
    }

    Y_UNIT_TEST(
        ShouldPostponeBlockCountCalculationAndScanDiskUntilInflightWriteRequestsAreCompleted)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        ui64 writeCommitId = 0;

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event) mutable
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        writeCommitId =
                            event
                                ->Get<TEvPartitionPrivate::TEvAddBlobsRequest>()
                                ->CommitId;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.SendWriteBlocksRequest(
            TBlockRange32::WithLength(0, 1024 * 10),
            1);
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            partition.SendMetadataRebuildBlockCountRequest(
                MakePartialBlobId(writeCommitId + 1, 0),
                100,
                MakePartialBlobId(writeCommitId + 1, 0));

            const auto response =
                partition.RecvMetadataRebuildBlockCountResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }

        {
            partition.SendScanDiskBatchRequest(
                MakePartialBlobId(writeCommitId, 0),
                100,
                MakePartialBlobId(writeCommitId, 0));

            const auto response = partition.RecvScanDiskBatchResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }
    }

// Commit ID overflow reports an impossible event, which aborts debug builds.
#ifdef NDEBUG
    Y_UNIT_TEST(ShouldRejectMetadataRebuildOnCommitIdOverflow)
    {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.SendToPipe(
            std::make_unique<
                TEvPartitionCommonPrivate::TEvGetFreshChannelsInfoRequest>());
        auto channelsInfo = partition.RecvResponse<
            TEvPartitionCommonPrivate::TEvGetFreshChannelsInfoResponse>();

        bool rebuildStarted = false;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvVolume::EvRebuildMetadataRequest) {
                    channelsInfo->SharedState->Init(
                        event->GetRecipientRewrite(),
                        channelsInfo->Generation,
                        Max<ui32>(),
                        runtime->GetCurrentTime(),
                        runtime->GetCurrentMonotonicTime());
                } else if (
                    event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvMetadataRebuildBlockCountRequest)
                {
                    rebuildStarted = true;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.SendRebuildMetadataRequest(NProto::BLOCK_COUNT, 10);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            partition.RecvRebuildMetadataResponse()->GetStatus());
        UNIT_ASSERT(!rebuildStarted);
    }
#endif

    Y_UNIT_TEST(ShouldWaitForCommitsBeforeTakingMetadataRebuildSnapshot)
    {
        for (ui32 testCase = 0; testCase < 4; ++testCase) {
            const bool useChannelCounters = testCase & 1;
            const bool completeNewerWriteFirst = testCase & 2;
            auto config = DefaultConfig();
            config.SetExtendedBlobCountersEnabled(useChannelCounters);
            auto runtime = PrepareTestActorRuntime(config, 4096);
            TPartitionClient partition(*runtime);
            partition.WaitReady();

            TAutoPtr<IEventHandle> savedWrite;
            TBlockCountRebuildState initialCounters;
            bool rebuildStarted = false;
            bool completed = false;
            runtime->SetObserverFunc(
                [&](TAutoPtr<IEventHandle>& event)
                {
                    switch (event->GetTypeRewrite()) {
                        case TEvPartitionPrivate::EvAddBlobsRequest:
                            savedWrite = event.Release();
                            return TTestActorRuntime::EEventAction::DROP;
                        case TEvPartitionPrivate::
                            EvMetadataRebuildBlockCountRequest:
                            rebuildStarted = true;
                            break;
                        case TEvPartitionPrivate::EvMetadataRebuildCompleted:
                            completed = true;
                            break;
                    }
                    return TTestActorRuntime::DefaultObserverFunc(event);
                });

            partition.SendWriteBlocksRequest(
                TBlockRange32::WithLength(0, 1024),
                1);
            runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
            UNIT_ASSERT(savedWrite);

            partition.RebuildMetadata(NProto::BLOCK_COUNT, 1);
            runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
            UNIT_ASSERT(!rebuildStarted);
            UNIT_ASSERT(!partition.GetRebuildMetadataStatus()
                             ->Record.GetProgress()
                             .GetIsCompleted());

            partition.SendCleanupRequest();
            UNIT_ASSERT_VALUES_EQUAL(
                E_TRY_AGAIN,
                partition.RecvCleanupResponse()->GetStatus());

            // This newer write is outside the fixed rebuild boundary.
            TAutoPtr<IEventHandle> firstWrite = savedWrite.Release();
            partition.SendWriteBlocksRequest(
                TBlockRange32::WithLength(1024, 1024),
                2);
            runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
            UNIT_ASSERT(savedWrite);

            // Replay the writes without intercepting them again.
            runtime->SetObserverFunc(
                [&](TAutoPtr<IEventHandle>& event)
                {
                    if (event->GetTypeRewrite() ==
                        TEvPartitionPrivate::EvMetadataRebuildCompleted)
                    {
                        completed = true;
                    } else if (
                        event->GetTypeRewrite() ==
                        TEvPartitionPrivate::EvMetadataRebuildBlockCountRequest)
                    {
                        rebuildStarted = true;
                        initialCounters =
                            event
                                ->Get<TEvPartitionPrivate::
                                          TEvMetadataRebuildBlockCountRequest>()
                                ->RebuildState;
                    }
                    return TTestActorRuntime::DefaultObserverFunc(event);
                });

            if (completeNewerWriteFirst) {
                // Exclude the newer write even if it commits first.
                runtime->Send(savedWrite.Release());
                partition.RecvWriteBlocksResponse();
                UNIT_ASSERT(!rebuildStarted);
            }

            runtime->Send(firstWrite.Release());
            partition.RecvWriteBlocksResponse();
            if (!completed) {
                TDispatchOptions options;
                options.CustomFinalCondition = [&]
                {
                    return completed;
                };
                runtime->DispatchEvents(options);
            }
            UNIT_ASSERT(completed);
            UNIT_ASSERT_VALUES_EQUAL(
                useChannelCounters ? 1024 : 0,
                initialCounters.Initial.Channel.MergedBlocks);
            UNIT_ASSERT_VALUES_EQUAL(
                useChannelCounters ? 1 : 0,
                initialCounters.Initial.Channel.MergedBlobs);
            UNIT_ASSERT_VALUES_EQUAL(
                1024,
                initialCounters.Initial.Index.MergedBlocks);
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                initialCounters.Initial.Index.MergedBlobs);
            const ui64 initialBytes =
                useChannelCounters ? 1024 * DefaultBlockSize : 0;
            UNIT_ASSERT_VALUES_EQUAL(
                initialBytes,
                initialCounters.Initial.Index.MergedBytes);
            UNIT_ASSERT_VALUES_EQUAL(
                initialBytes,
                initialCounters.Initial.Channel.MergedBytes);

            if (savedWrite) {
                // Rebuild must finish without waiting for the newer write.
                runtime->Send(savedWrite.Release());
                partition.RecvWriteBlocksResponse();
            }

            const auto assertCounters = [&]
            {
                const auto stats = partition.StatPartition()->Record.GetStats();
                UNIT_ASSERT_VALUES_EQUAL(2048, stats.GetMergedBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    useChannelCounters ? 2048 : 0,
                    stats.GetMergedIndexBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    useChannelCounters ? 2 : 0,
                    stats.GetMergedIndexBlobsCount());
                partition.SendToPipe(
                    std::make_unique<TEvPartitionCommonPrivate::
                                         TEvGetPartCountersRequest>());
                const auto response = partition.RecvResponse<
                    TEvPartitionCommonPrivate::TEvGetPartCountersResponse>();
                const auto& counters = response->DiskCounters->Simple;
                UNIT_ASSERT_VALUES_EQUAL(
                    2048 * DefaultBlockSize,
                    counters.MergedBytesCount.Value);
                UNIT_ASSERT_VALUES_EQUAL(
                    useChannelCounters ? 2048 * DefaultBlockSize : 0,
                    counters.MergedIndexBytesCount.Value);
            };
            assertCounters();
            partition.RebootTablet();
            assertCounters();
        }
    }

    Y_UNIT_TEST(ShouldRebuildBlockCountSensors)
    {
        for (bool useChannelCounters: {false, true}) {
            constexpr ui32 blockCount = 1024 * 1024;
            auto config = DefaultConfig();
            config.SetExtendedBlobCountersEnabled(useChannelCounters);
            auto runtime = PrepareTestActorRuntime(config, blockCount);

            TPartitionClient partition(*runtime);
            partition.WaitReady();

            partition.WriteBlocks(
                TBlockRange32::MakeClosedInterval(0, 1024 * 10),
                1);

            partition.RebuildMetadata(NProto::BLOCK_COUNT, 100);
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);

            const auto stats = partition.StatPartition()->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                useChannelCounters ? 1024 * 11 + 1 : 0,
                stats.GetMergedIndexBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                1024 * 11 + 1,
                stats.GetMergedBlocksCount());
        }
    }

    Y_UNIT_TEST(ShouldPreserveBlobBytesWrittenDuringMetadataRebuild)
    {
        for (const bool useChannelCounters: {false, true}) {
            auto config = DefaultConfig();
            config.SetExtendedBlobCountersEnabled(useChannelCounters);
            auto runtime = PrepareTestActorRuntime(config, 4096);
            TPartitionClient partition(*runtime);
            partition.WaitReady();

            partition.WriteBlocks(1, 1);
            partition.Flush();
            partition.ZeroBlocks(2);
            partition.Flush();
            partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024), 1);

            TAutoPtr<IEventHandle> savedEvent;
            TBlockCountRebuildState rebuilt;
            bool pause = true;
            runtime->SetObserverFunc(
                [&](TAutoPtr<IEventHandle>& event)
                {
                    using TRequest = TEvPartitionPrivate::
                        TEvMetadataRebuildBlockCountRequest;
                    using TResponse = TEvPartitionPrivate::
                        TEvMetadataRebuildBlockCountResponse;
                    if (event->GetTypeRewrite() == TRequest::EventType && pause)
                    {
                        pause = false;
                        savedEvent = event.Release();
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    if (event->GetTypeRewrite() == TResponse::EventType) {
                        rebuilt = event->Get<TResponse>()->RebuildState;
                    }
                    return TTestActorRuntime::DefaultObserverFunc(event);
                });

            partition.RebuildMetadata(NProto::BLOCK_COUNT, 1);
            partition.WriteBlocks(3, 1);
            partition.Flush();
            partition.WriteBlocks(TBlockRange32::WithLength(2048, 1024), 1);
            UNIT_ASSERT(savedEvent);
            runtime->Send(savedEvent.Release());

            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionPrivate::EvMetadataRebuildCompleted);
            runtime->DispatchEvents(options, TDuration::Seconds(1));
            UNIT_ASSERT(partition.GetRebuildMetadataStatus()
                            ->Record.GetProgress()
                            .GetIsCompleted());
            UNIT_ASSERT_VALUES_EQUAL(
                DefaultBlockSize,
                rebuilt.Rebuilt.Index.MixedBytes);
            UNIT_ASSERT_VALUES_EQUAL(
                1024 * DefaultBlockSize,
                rebuilt.Rebuilt.Index.MergedBytes);
            UNIT_ASSERT_VALUES_EQUAL(0, rebuilt.Rebuilt.Channel.MixedBytes);
            UNIT_ASSERT_VALUES_EQUAL(
                1025 * DefaultBlockSize,
                rebuilt.Rebuilt.Channel.MergedBytes);

            const auto assertBytes = [&]
            {
                partition.SendToPipe(
                    std::make_unique<TEvPartitionCommonPrivate::
                                         TEvGetPartCountersRequest>());
                const auto response = partition.RecvResponse<
                    TEvPartitionCommonPrivate::TEvGetPartCountersResponse>();
                const auto& counters = response->DiskCounters->Simple;
                UNIT_ASSERT_VALUES_EQUAL(
                    (useChannelCounters ? 0 : 2) * DefaultBlockSize,
                    counters.MixedBytesCount.Value);
                UNIT_ASSERT_VALUES_EQUAL(
                    (useChannelCounters ? 2050 : 2048) * DefaultBlockSize,
                    counters.MergedBytesCount.Value);
            };
            assertBytes();
            partition.RebootTablet();
            assertBytes();
        }
    }

    Y_UNIT_TEST(ShouldRebuildBlobCountSensors)
    {
        for (bool useChannelCounters: {false, true}) {
            auto config = DefaultConfig();
            config.SetExtendedBlobCountersEnabled(useChannelCounters);
            auto runtime = PrepareTestActorRuntime(config, 4096);

            TPartitionClient partition(*runtime);
            partition.WaitReady();

            partition.WriteBlocks(1, 1);
            partition.WriteBlocks(2, 2);
            partition.WriteBlocks(3, 3);
            partition.ZeroBlocks(4);
            partition.Flush();
            // Flush the remaining zero block as a mixed deletion marker.
            partition.Flush();
            partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024), 1);

            TAutoPtr<IEventHandle> savedResponse;
            TBlockCountRebuildState rebuildState;
            bool firstBatch = true;
            bool completed = false;
            runtime->SetObserverFunc(
                [&](TAutoPtr<IEventHandle>& event)
                {
                    if (event->GetTypeRewrite() ==
                        TEvPartitionPrivate::
                            EvMetadataRebuildBlockCountResponse)
                    {
                        using TResponse = TEvPartitionPrivate::
                            TEvMetadataRebuildBlockCountResponse;
                        rebuildState = event->Get<TResponse>()->RebuildState;
                        if (firstBatch) {
                            firstBatch = false;
                            savedResponse = event.Release();
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    } else if (
                        event->GetTypeRewrite() ==
                        TEvPartitionPrivate::EvMetadataRebuildCompleted)
                    {
                        completed = true;
                    }
                    return TTestActorRuntime::DefaultObserverFunc(event);
                });

            partition.RebuildMetadata(NProto::BLOCK_COUNT, 1);
            if (!savedResponse) {
                TDispatchOptions options;
                options.CustomFinalCondition = [&]
                {
                    return !!savedResponse;
                };
                runtime->DispatchEvents(options);
            }
            UNIT_ASSERT(savedResponse);
            UNIT_ASSERT_VALUES_EQUAL(2, rebuildState.Initial.Index.MixedBlobs);
            UNIT_ASSERT_VALUES_EQUAL(1, rebuildState.Initial.Index.MergedBlobs);
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                rebuildState.Initial.Channel.MixedBlobs);
            UNIT_ASSERT_VALUES_EQUAL(
                useChannelCounters ? 2 : 0,
                rebuildState.Initial.Channel.MergedBlobs);

            // New blobs must be retained even though they are outside the scan.
            partition.WriteBlocks(5, 5);
            partition.Flush();
            partition.WriteBlocks(TBlockRange32::WithLength(2048, 1024), 1);
            runtime->Send(savedResponse.Release());
            if (!completed) {
                TDispatchOptions options;
                options.CustomFinalCondition = [&]
                {
                    return completed;
                };
                runtime->DispatchEvents(options);
            }
            UNIT_ASSERT(completed);
            UNIT_ASSERT_VALUES_EQUAL(2, rebuildState.Rebuilt.Index.MixedBlobs);
            UNIT_ASSERT_VALUES_EQUAL(1, rebuildState.Rebuilt.Index.MergedBlobs);
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                rebuildState.Rebuilt.Channel.MixedBlobs);
            UNIT_ASSERT_VALUES_EQUAL(
                2,
                rebuildState.Rebuilt.Channel.MergedBlobs);

            const auto assertCounters = [&]
            {
                const auto stats = partition.StatPartition()->Record.GetStats();
                UNIT_ASSERT_VALUES_EQUAL(
                    useChannelCounters ? 3 : 0,
                    stats.GetMixedIndexBlobsCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    useChannelCounters ? 2 : 0,
                    stats.GetMergedIndexBlobsCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    useChannelCounters ? 0 : 3,
                    stats.GetMixedBlobsCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    useChannelCounters ? 4 : 2,
                    stats.GetMergedBlobsCount());
            };
            assertCounters();
            partition.RebootTablet();
            assertCounters();
        }
    }

    Y_UNIT_TEST(ShouldFailGetMetadataRebuildStatusIfNoOperationRunning)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(0, 1024 * 10),
            1);

        partition.SendGetRebuildMetadataStatusRequest();
        auto progress = partition.RecvGetRebuildMetadataStatusResponse();
        UNIT_ASSERT(FAILED(progress->GetStatus()));
    }

    Y_UNIT_TEST(ShouldSuccessfullyRunMetadataRebuildOnEmptyDisk)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.RebuildMetadata(
            NProto::ERebuildMetadataType::BLOCK_COUNT,
            10);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            TEvPartitionPrivate::EvMetadataRebuildCompleted);
        runtime->DispatchEvents(options, TDuration::Seconds(1));

        auto progress = partition.GetRebuildMetadataStatus();
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            progress->Record.GetProgress().GetProcessed());
        UNIT_ASSERT_VALUES_EQUAL(0, progress->Record.GetProgress().GetTotal());
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            progress->Record.GetProgress().GetIsCompleted());
    }

    Y_UNIT_TEST(ShouldReturnRebuildMetadataProgressDuringExecution)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024 * 10), 1);
        partition.WriteBlocks(
            TBlockRange32::WithLength(1024 * 10, 1024 * 10),
            1);

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event) mutable
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::
                        EvMetadataRebuildBlockCountResponse: {
                        const auto* msg = event->Get<
                            TEvPartitionPrivate::
                                TEvMetadataRebuildBlockCountResponse>();
                        UNIT_ASSERT_VALUES_UNEQUAL(
                            0,
                            msg->RebuildState.Rebuilt.Index.MixedBlocks +
                                msg->RebuildState.Rebuilt.Index.MergedBlocks);
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto response = partition.RebuildMetadata(
            NProto::ERebuildMetadataType::BLOCK_COUNT,
            10);

        auto progress = partition.GetRebuildMetadataStatus();
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            progress->Record.GetProgress().GetProcessed());
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            progress->Record.GetProgress().GetIsCompleted());
    }

    Y_UNIT_TEST(ShouldRejectMetadataRebuildDuringCleanup)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 2);
        partition.Compaction();

        // Keep cleanup active until its transaction has been committed.
        std::unique_ptr<IEventHandle> putEvent;
        bool interceptPut = true;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (interceptPut &&
                    event->GetTypeRewrite() == TEvBlobStorage::EvPut &&
                    event->Get<TEvBlobStorage::TEvPut>()->Id.Channel() == 0)
                {
                    putEvent.reset(event.Release());
                    interceptPut = false;
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.SendCleanupRequest();
        runtime->DispatchEvents({}, 10ms);
        UNIT_ASSERT(putEvent);

        partition.SendRebuildMetadataRequest(NProto::BLOCK_COUNT, 10);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            partition.RecvRebuildMetadataResponse()->GetStatus());

        partition.SendGetRebuildMetadataStatusRequest();
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_FOUND,
            partition.RecvGetRebuildMetadataStatusResponse()->GetStatus());

        bool completed = false;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvMetadataRebuildCompleted)
                {
                    completed = true;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        runtime->Send(putEvent.release());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            partition.RecvCleanupResponse()->GetStatus());

        partition.RebuildMetadata(NProto::BLOCK_COUNT, 10);
        if (!completed) {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return completed;
            };
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT(partition.GetRebuildMetadataStatus()
                        ->Record.GetProgress()
                        .GetIsCompleted());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            partition.StatPartition()->Record.GetStats().GetMergedBlobsCount());

        partition.RebootTablet();
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            partition.StatPartition()->Record.GetStats().GetMergedBlobsCount());
    }

    Y_UNIT_TEST(ShouldBlockCleanupDuringMetadataRebuild)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);

        ui32 cnt = 0;

        TAutoPtr<IEventHandle> savedEvent;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event) mutable
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::
                        EvMetadataRebuildBlockCountRequest: {
                        if (++cnt == 1) {
                            savedEvent = event.Release();
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto response = partition.RebuildMetadata(
            NProto::ERebuildMetadataType::BLOCK_COUNT,
            10);
        partition.Compaction();

        {
            auto stats = partition.StatPartition()->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(5, stats.GetMergedBlobsCount());
        }

        {
            partition.SendCleanupRequest();
            auto response = partition.RecvCleanupResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, response->GetStatus());
        }

        runtime->Send(savedEvent.Release());

        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            TEvPartitionPrivate::EvMetadataRebuildCompleted);
        runtime->DispatchEvents(options);

        partition.Cleanup();

        {
            auto stats = partition.StatPartition()->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }

        auto progress = partition.GetRebuildMetadataStatus();
        UNIT_ASSERT_VALUES_EQUAL(
            4,
            progress->Record.GetProgress().GetProcessed());
        UNIT_ASSERT_VALUES_EQUAL(4, progress->Record.GetProgress().GetTotal());
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            progress->Record.GetProgress().GetIsCompleted());
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
