#include "part_ut.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/critical_events_init.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/core/compaction_options.h>
#include <cloud/blockstore/libs/storage/testlib/part_client.h>

#include <contrib/ydb/core/blobstorage/vdisk/common/vdisk_events.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;
using namespace NKikimr;
using namespace NCloud::NStorage;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

void DoTestFullCompaction(bool forced)
{
    constexpr ui32 rangesCount = 5;
    auto storageConfig = DefaultConfig();
    storageConfig.SetWriteBlobThreshold(1_MB);
    auto runtime = PrepareTestActorRuntime(storageConfig, rangesCount * 1024);

    TPartitionClient partition(*runtime);
    partition.WaitReady();

    for (ui32 range = 0; range < rangesCount; ++range) {
        partition.WriteBlocks(TBlockRange32::WithLength(range * 1024, 1024), 1);
    }
    partition.Flush();

    auto response = partition.StatPartition();
    auto oldStats = response->Record.GetStats();

    TCompactionOptions options;
    options.set(ToBit(ECompactionOption::Full));
    if (forced) {
        options.set(ToBit(ECompactionOption::Forced));
    }
    for (ui32 range = 0; range < rangesCount; ++range) {
        partition.Compaction(range * 1024, options);
    }

    response = partition.StatPartition();
    auto newStats = response->Record.GetStats();
    UNIT_ASSERT_VALUES_EQUAL(
        oldStats.GetMixedBlobsCount() + oldStats.GetMergedBlobsCount() +
            rangesCount,
        newStats.GetMixedBlobsCount() + newStats.GetMergedBlobsCount());
}

void DoTestEmptyRangesFullCompaction(bool forced)
{
    constexpr ui32 rangesCount = 5;
    constexpr ui32 emptyRange = 2;
    auto storageConfig = DefaultConfig();
    storageConfig.SetWriteBlobThreshold(1_MB);
    auto runtime = PrepareTestActorRuntime(storageConfig, rangesCount * 1024);

    TPartitionClient partition(*runtime);
    partition.WaitReady();

    for (ui32 range = 0; range < rangesCount; ++range) {
        if (range != emptyRange) {
            partition.WriteBlocks(
                TBlockRange32::WithLength(range * 1024, 900),
                1);
        }
    }
    partition.Flush();

    auto response = partition.StatPartition();
    auto oldStats = response->Record.GetStats();

    TCompactionOptions options;
    options.set(ToBit(ECompactionOption::Full));
    if (forced) {
        options.set(ToBit(ECompactionOption::Forced));
    }

    for (ui32 range = 0; range < rangesCount; ++range) {
        partition.Compaction(range * 1024, options);
    }

    response = partition.StatPartition();
    auto newStats = response->Record.GetStats();
    UNIT_ASSERT_VALUES_EQUAL(
        oldStats.GetMixedBlobsCount() + oldStats.GetMergedBlobsCount() +
            rangesCount - 1,
        newStats.GetMixedBlobsCount() + newStats.GetMergedBlobsCount());
}

void DoTestIncrementalCompaction(
    NProto::TStorageServiceConfig config,
    NProto::EStorageMediaKind mediaKind,
    bool incrementalCompactionExpected)
{
    if (mediaKind == NCloud::NProto::STORAGE_MEDIA_SSD) {
        config.SetWriteBlobThresholdSSD(1);   // disable FreshBlocks
        config.SetSSDMaxBlobsPerRange(4);
        config.SetMaxSkippedBlobsDuringCompaction(1);
    } else {
        config.SetWriteBlobThreshold(1);   // disable FreshBlocks
        config.SetHDDMaxBlobsPerRange(4);
        config.SetMaxSkippedBlobsDuringCompactionHDD(1);
    }
    config.SetIncrementalCompactionEnabled(true);
    config.SetTargetCompactionBytesPerOp(64_KB);

    auto runtime =
        PrepareTestActorRuntime(config, 1024, {}, {.MediaKind = mediaKind});

    TPartitionClient partition(*runtime);
    partition.WaitReady();

    std::unique_ptr<IEventHandle> compactionRequest;
    bool intercept = true;

    runtime->SetObserverFunc(
        [&](TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvPartitionPrivate::EvCompactionRequest: {
                    if (intercept) {
                        auto request = event->Get<
                            TEvPartitionPrivate::TEvCompactionRequest>();
                        UNIT_ASSERT_VALUES_EQUAL(
                            incrementalCompactionExpected,
                            !request->CompactionOptions.test(
                                ToBit(ECompactionOption::Full)));

                        compactionRequest.reset(event.Release());
                        intercept = false;

                        return TTestActorRuntime::EEventAction::DROP;
                    }

                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

    partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
    partition.WriteBlocks(TBlockRange32::WithLength(0, 5), 2);
    partition.WriteBlocks(TBlockRange32::MakeClosedInterval(10, 14), 3);
    partition.WriteBlocks(TBlockRange32::MakeClosedInterval(20, 25), 4);

    {
        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(1040, stats.GetMergedBlocksCount());
    }

    runtime->DispatchEvents({}, TDuration::Seconds(1));
    UNIT_ASSERT(compactionRequest);
    runtime->Send(compactionRequest.release());
    runtime->DispatchEvents({}, TDuration::Seconds(1));
    partition.Cleanup();
    partition.CollectGarbage();

    {
        ui32 blobs = incrementalCompactionExpected ? 2 : 1;
        ui32 blocks = incrementalCompactionExpected ? 1040 : 1024;

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(blobs, stats.GetMergedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(blocks, stats.GetMergedBlocksCount());
    }

    for (ui32 i = 0; i <= 4; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(2),
            GetBlocksContent(partition.ReadBlocks(i)));
    }

    for (ui32 i = 5; i < 10; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(1),
            GetBlocksContent(partition.ReadBlocks(i)));
    }

    for (ui32 i = 10; i <= 14; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(3),
            GetBlocksContent(partition.ReadBlocks(i)));
    }

    for (ui32 i = 15; i < 20; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(1),
            GetBlocksContent(partition.ReadBlocks(i)));
    }

    for (ui32 i = 20; i <= 25; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(4),
            GetBlocksContent(partition.ReadBlocks(i)));
    }

    for (ui32 i = 26; i < 1023; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(1),
            GetBlocksContent(partition.ReadBlocks(i)));
    }
}

void DoShouldPreserveSkippedCountersWhenCompactionCreatesTwoBlobs(
    bool compactToMixed)
{
    auto config = DefaultConfig();
    config.SetWriteBlobThreshold(1);   // disable FreshBlocks
    config.SetHDDMaxBlobsPerRange(3);
    config.SetMaxSkippedBlobsDuringCompactionHDD(1);
    config.SetIncrementalCompactionEnabled(true);
    config.SetTargetCompactionBytesPerOp(64_KB);
    if (compactToMixed) {
        config.SetCompactionMergedBlobThresholdHDD(1_MB);
    }

    auto runtime = PrepareTestActorRuntime(config);

    TPartitionClient partition(*runtime);
    partition.WaitReady();

    std::unique_ptr<IEventHandle> firstCompactionRequest;
    bool firstCompactionRequestCaptured = false;
    bool compactionResultObserved = false;

    auto observer = runtime->AddObserver(
        [&](TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvPartitionPrivate::EvCompactionRequest: {
                    if (!firstCompactionRequestCaptured) {
                        firstCompactionRequestCaptured = true;
                        firstCompactionRequest.reset(event.Release());
                        return;
                    }
                    break;
                }
                case TEvPartitionPrivate::EvAddBlobsRequest: {
                    auto* msg =
                        event->Get<TEvPartitionPrivate::TEvAddBlobsRequest>();
                    if (msg->Mode != EAddBlobMode::ADD_COMPACTION_RESULT ||
                        compactionResultObserved)
                    {
                        break;
                    }

                    compactionResultObserved = true;
                    UNIT_ASSERT_VALUES_EQUAL(
                        compactToMixed ? 2 : 0,
                        msg->MixedBlobs.size());
                    UNIT_ASSERT_VALUES_EQUAL(
                        compactToMixed ? 0 : 2,
                        msg->MergedBlobs.size());

                    const auto& compactionInfos =
                        compactToMixed ? msg->MixedBlobCompactionInfos
                                       : msg->MergedBlobCompactionInfos;
                    UNIT_ASSERT_VALUES_EQUAL(2, compactionInfos.size());
                    UNIT_ASSERT_VALUES_EQUAL(
                        1,
                        compactionInfos[0].BlobsSkippedByCompaction);
                    UNIT_ASSERT_VALUES_EQUAL(
                        0,
                        compactionInfos[1].BlobsSkippedByCompaction);
                    break;
                }
            }
        });

    partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
    partition.WriteBlocks(TBlockRange32::WithLength(0, 5));
    partition.WriteBlocks(TBlockRange32::WithLength(10, 5));

    UNIT_ASSERT(firstCompactionRequest);

    partition.ZeroBlocks(TBlockRange32::WithLength(20, 6));

    runtime->SendAsync(firstCompactionRequest.release());
    runtime->DispatchEvents({}, TDuration::Seconds(1));

    UNIT_ASSERT(compactionResultObserved);

    const auto counters = partition.GetCompactionCounters(0);
    UNIT_ASSERT_VALUES_EQUAL(3, counters->Counters.BlobCount);
    // All 1024 blocks remain in the skipped blob, plus 10 compacted blocks.
    UNIT_ASSERT_VALUES_EQUAL(1034, counters->Counters.BlockCount);
}

void DoShouldAbortCompactionIfBlobStorageRequestFailsWithDeadlineExceeded(
    int requestType)
{
    NProto::TStorageServiceConfig config;
    config.SetBlobStorageAsyncRequestTimeoutHDD(
        TDuration::Seconds(1).MilliSeconds());
    auto runtime = PrepareTestActorRuntime(config);

    TPartitionClient partition(*runtime);
    partition.WaitReady();

    partition.WriteBlocks(3, 33);
    partition.Flush();

    ui32 failedReadBlob = 0;

    runtime->SetEventFilter(
        [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
        {
            Y_UNUSED(runtime);

            if (requestType == TEvBlobStorage::EvVGet &&
                ev->GetTypeRewrite() == TEvBlobStorage::EvVGet)
            {
                const auto* msg = ev->Get<TEvBlobStorage::TEvVGet>();
                if (msg->Record.GetHandleClass() ==
                        NKikimrBlobStorage::AsyncRead &&
                    msg->Record.GetMsgQoS().HasDeadlineSeconds())
                {
                    return true;
                }
            } else if (
                requestType == TEvBlobStorage::EvVPut &&
                ev->GetTypeRewrite() == TEvBlobStorage::EvVPut)
            {
                const auto* msg = ev->Get<TEvBlobStorage::TEvVPut>();
                if (msg->Record.GetHandleClass() ==
                        NKikimrBlobStorage::AsyncBlob &&
                    msg->Record.GetMsgQoS().HasDeadlineSeconds())
                {
                    return true;
                }
            } else if (
                ev->GetTypeRewrite() == TEvStatsService::EvVolumePartCounters)
            {
                auto* msg = ev->Get<TEvStatsService::TEvVolumePartCounters>();
                failedReadBlob =
                    msg->DiskCounters->Simple.ReadBlobDeadlineCount.Value;
            }
            return false;
        });

    partition.SendCompactionRequest();
    runtime->AdvanceCurrentTime(TDuration::Seconds(1));
    auto response = partition.RecvCompactionResponse();
    UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());

    runtime->AdvanceCurrentTime(TDuration::Seconds(15));

    if (requestType != TEvBlobStorage::EvVGet) {
        return;
    }

    partition.SendToPipe(
        std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvStatsService::EvVolumePartCounters);
        runtime->DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(1, failedReadBlob);
}

void TestForcedCompaction(ui32 rangesPerRun)
{
    auto config = DefaultConfig();
    config.SetBatchCompactionEnabled(true);
    config.SetForcedCompactionRangeCountPerRun(rangesPerRun);
    config.SetV1GarbageCompactionEnabled(false);
    config.SetWriteBlobThreshold(15_KB);
    config.SetIncrementalCompactionEnabled(true);

    auto runtime = PrepareTestActorRuntime(
        config,
        MaxPartitionBlocksCount,
        {},
        {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

    TPartitionClient partition(*runtime);
    partition.WaitReady();

    {
        partition.WriteBlocks(TBlockRange32::WithLength(0, 5), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(1024, 5), 2);

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1), 3);
        partition.WriteBlocks(TBlockRange32::WithLength(1028, 3), 4);
        partition.Flush();

        const auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(10, stats.GetMergedBlocksCount());
    }

    {
        const auto response = partition.CompactRange(0, 0);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

        const auto statResponse = partition.StatPartition();
        const auto& stats = statResponse->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(22, stats.GetMergedBlocksCount());
    }

    {
        partition.Cleanup();

        const auto statResponse = partition.StatPartition();
        const auto& stats = statResponse->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(12, stats.GetMergedBlocksCount());

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(0)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(1024)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(4),
            GetBlockContent(partition.ReadBlocks(1028)));
    }

    {
        const auto response = partition.CompactRange(0, 0);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        partition.Cleanup();
    }

    // write to the same blocks, we need several blobs in one compacted range
    {
        partition.WriteBlocks(TBlockRange32::WithLength(0, 5), 12);
        partition.WriteBlocks(TBlockRange32::WithLength(1024, 5), 22);

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1), 32);
        partition.WriteBlocks(TBlockRange32::WithLength(1028, 3), 42);
        partition.Flush();

        const auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(22, stats.GetMergedBlocksCount());
    }

    {
        const auto response = partition.CompactRange(0, 0);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        partition.Cleanup();

        const auto statResponse = partition.StatPartition();
        const auto& stats = statResponse->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(12, stats.GetMergedBlocksCount());

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(32),
            GetBlockContent(partition.ReadBlocks(0)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(12),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(22),
            GetBlockContent(partition.ReadBlocks(1024)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(42),
            GetBlockContent(partition.ReadBlocks(1028)));
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionCompactionTest)
{
    Y_UNIT_TEST(ShouldReplaceBlobsOnCompaction)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);
        partition.Flush();

        partition.WriteBlocks(1, 11);
        partition.Flush();

        partition.WriteBlocks(2, 22);
        partition.Flush();

        partition.WriteBlocks(3, 33);
        partition.Flush();

        partition.Compaction();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(11),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(22),
            GetBlockContent(partition.ReadBlocks(2)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(33),
            GetBlockContent(partition.ReadBlocks(3)));

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();

        UNIT_ASSERT(stats.GetSysReadCounters().GetExecTime() != 0);
        UNIT_ASSERT(stats.GetSysWriteCounters().GetExecTime() != 0);
        UNIT_ASSERT(stats.GetUserReadCounters().GetExecTime() != 0);
    }

    Y_UNIT_TEST(CompactionShouldTakeCareOfFreshBlocks)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);
        partition.Flush();

        partition.WriteBlocks(1, 11);
        partition.Flush();

        partition.WriteBlocks(2, 22);
        partition.Flush();

        partition.WriteBlocks(3, 33);
        // partition.Flush();

        partition.Compaction();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(11),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(22),
            GetBlockContent(partition.ReadBlocks(2)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(33),
            GetBlockContent(partition.ReadBlocks(3)));
    }

    Y_UNIT_TEST(ShouldHandleZeroedBlocksInCompaction)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount));
        partition.Flush();

        partition.ZeroBlocks(TBlockRange32::WithLength(0, MaxBlocksCount));
        partition.Flush();

        partition.Compaction();
    }

    Y_UNIT_TEST(ShouldCreateBlobsForEveryWrittenRangeDuringFullCompaction)
    {
        DoTestFullCompaction(false);
    }

    Y_UNIT_TEST(ShouldCreateBlobsForEveryWrittenRangeDuringForcedFullCompaction)
    {
        DoTestFullCompaction(true);
    }

    Y_UNIT_TEST(ShouldNotCreateBlobsForEmptyRangesDuringFullCompaction)
    {
        DoTestEmptyRangesFullCompaction(false);
    }

    Y_UNIT_TEST(ShouldNotCreateBlobsForEmptyRangesDuringForcedFullCompaction)
    {
        DoTestEmptyRangesFullCompaction(true);
    }

    Y_UNIT_TEST(ShouldMakeUnderlyingBlobsEligibleForCleanupAfterCompaction)
    {
        auto config = DefaultConfig();
        config.SetCleanupThreshold(1);
        config.SetCollectGarbageThreshold(3);
        config.SetSSDMaxBlobsPerRange(4);
        config.SetHDDMaxBlobsPerRange(4);
        config.SetFlushThresholdHDD(8_MB);
        config.SetWriteBlobThreshold(4_MB);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 version = 0; version < 3; ++version) {
            for (ui32 pos = 0; pos < 2; ++pos) {
                partition.WriteBlocks(
                    TBlockRange32::WithLength(512 * pos, 512));
            }
            partition.Flush();
        }

        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyMarkCrossRangeBlocksDuringCompaction)
    {
        auto config = DefaultConfig();
        config.SetCleanupThreshold(1);
        config.SetCollectGarbageThreshold(3);
        config.SetSSDMaxBlobsPerRange(4);
        config.SetHDDMaxBlobsPerRange(4);
        config.SetFlushThresholdHDD(8_MB);
        config.SetWriteBlobThreshold(4_MB);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 version = 0; version < 2; ++version) {
            for (ui32 pos = 0; pos < 4; ++pos) {
                partition.WriteBlocks(
                    TBlockRange32::WithLength(512 * pos, 512));
            }
            partition.Flush();
        }

        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1000, 1500));
        partition.Flush();

        partition.Compaction();
        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldNotWriteBlobIfCompactionRangeWasOverwrittenWithZeroBlocks)
    {
        auto config = DefaultConfig();
        config.SetCleanupThreshold(1);
        config.SetSSDMaxBlobsPerRange(2);
        config.SetHDDMaxBlobsPerRange(2);
        config.SetCollectGarbageThreshold(1);
        config.SetFlushThresholdHDD(8_MB);
        config.SetWriteBlobThreshold(4_MB);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 write_step = 0; write_step < 2; ++write_step) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 512));
        }

        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }

        for (ui32 zero_step = 0; zero_step < 2; ++zero_step) {
            partition.ZeroBlocks(TBlockRange32::WithLength(0, 512));
        }

        bool writeBlobSeen = false;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionCommonPrivate::EvWriteBlobRequest: {
                        writeBlobSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        UNIT_ASSERT(!writeBlobSeen);
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldWriteBlobIfCompactionUsesOnlyFreshBlocks)
    {
        auto config = DefaultConfig();
        config.SetCleanupThreshold(3);
        config.SetHDDMaxBlobsPerRange(2);
        config.SetSSDMaxBlobsPerRange(2);
        config.SetCollectGarbageThreshold(3);
        config.SetFlushThresholdHDD(8_MB);
        config.SetWriteBlobThreshold(4_MB);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 write_step = 0; write_step < 2; ++write_step) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 512));
        }

        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }

        for (ui32 write_step = 0; write_step < 2; ++write_step) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 512));
        }

        bool writeBlobSeen = false;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionCommonPrivate::EvWriteBlobRequest: {
                        writeBlobSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.Compaction();

        UNIT_ASSERT(writeBlobSeen);
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldConsolidateZeroedBlocksOnCompaction)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1);   // disable FreshBlocks

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 501));
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(501, 1000));
        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(501, 1000));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMergedBlobsCount());
        }

        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
        }

        partition.ZeroBlocks(TBlockRange32::WithLength(0, 1001));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMergedBlobsCount());
        }

        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldProduceSparseMergedBlobsUponCompaction)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1);   // disable FreshBlocks
        config.SetSSDMaxBlobsPerRange(999);
        config.SetHDDMaxBlobsPerRange(999);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(5, 1);
        partition.WriteBlocks(10, 2);
        partition.WriteBlocks(20, 3);
        partition.WriteBlocks(30, 4);
        partition.ZeroBlocks(30);
        partition.ZeroBlocks(40);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(6, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlocksCount());
        }

        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMergedBlocksCount());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(4)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(1),
            GetBlocksContent(partition.ReadBlocks(5)));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(6)));

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(9)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(2),
            GetBlocksContent(partition.ReadBlocks(10)));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(11)));

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(19)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(3),
            GetBlocksContent(partition.ReadBlocks(20)));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(21)));

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(30)));

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(40)));
    }

    Y_UNIT_TEST(ShouldCompactIncrementallySsd)
    {
        auto config = DefaultConfig();
        config.SetCompactionGarbageThreshold(20);
        DoTestIncrementalCompaction(
            std::move(config),
            NProto::STORAGE_MEDIA_SSD,
            true);
    }

    Y_UNIT_TEST(ShouldCompactIncrementallyHybrid)
    {
        auto config = DefaultConfig();
        config.SetCompactionGarbageThreshold(20);
        DoTestIncrementalCompaction(
            std::move(config),
            NProto::STORAGE_MEDIA_HYBRID,
            true);
    }

    Y_UNIT_TEST(ShouldPreserveSkippedCountersForMixedCompactionResults)
    {
        DoShouldPreserveSkippedCountersWhenCompactionCreatesTwoBlobs(true);
    }

    Y_UNIT_TEST(ShouldPreserveSkippedCountersForMergedCompactionResults)
    {
        DoShouldPreserveSkippedCountersWhenCompactionCreatesTwoBlobs(false);
    }

    Y_UNIT_TEST(ShouldNotCompactIncrementallyIfSsdDiskGarbageLevelIsTooHigh)
    {
        auto config = DefaultConfig();
        config.SetCompactionGarbageThreshold(1);
        DoTestIncrementalCompaction(
            std::move(config),
            NProto::STORAGE_MEDIA_SSD,
            false);
    }

    Y_UNIT_TEST(ShouldNotCompactIncrementallyIfHybridDiskGarbageLevelIsTooHigh)
    {
        auto config = DefaultConfig();
        config.SetCompactionGarbageThreshold(1);
        DoTestIncrementalCompaction(
            std::move(config),
            NProto::STORAGE_MEDIA_HYBRID,
            false);
    }

    Y_UNIT_TEST(
        ShouldNotEraseBlocksForSkippedBlobsFromIndexDuringIncrementalCompaction)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_GB);   // everything goes to fresh
        config.SetIncrementalCompactionEnabled(true);
        config.SetHDDMaxBlobsPerRange(999);
        config.SetSSDMaxBlobsPerRange(999);
        config.SetMaxSkippedBlobsDuringCompaction(1);
        config.SetTargetCompactionBytesPerOp(1);

        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_SSD});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // blob 1 needs to eventually have more live blocks than blob 2 in order
        // not to be compacted
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(2, 22), 1);
        partition.Flush();
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1, 10), 2);
        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(31, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
        }

        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(21, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(10, stats.GetMergedBlocksCount());
        }

        for (ui32 i = 11; i <= 22; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(1),
                GetBlocksContent(partition.ReadBlocks(i)));
        }

        for (ui32 i = 1; i <= 10; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(2),
                GetBlocksContent(partition.ReadBlocks(i)));
        }
    }

    Y_UNIT_TEST(ShouldProperlyProcessZeroesDuringIncrementalCompaction)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1);   // disabling fresh
        config.SetIncrementalCompactionEnabled(true);
        config.SetHDDMaxBlobsPerRange(999);
        config.SetSSDMaxBlobsPerRange(999);
        config.SetMaxSkippedBlobsDuringCompaction(1);
        config.SetTargetCompactionBytesPerOp(1);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // writing some data
        partition.WriteBlocks(TBlockRange32::WithLength(0, 11), 1);
        // zeroing it
        partition.ZeroBlocks(TBlockRange32::WithLength(0, 11));
        // writing some more data above
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1, 12), 2);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(23, stats.GetMergedBlocksCount());
        }

        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(12, stats.GetMergedBlocksCount());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(0)));

        for (ui32 i = 1; i <= 12; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(2),
                GetBlocksContent(partition.ReadBlocks(i)));
        }
    }

    Y_UNIT_TEST(ShouldReturnErrorIfCompactionIsAlreadyRunning)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024));

        TAutoPtr<IEventHandle> addBlobs;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        if (!addBlobs) {
                            addBlobs = event.Release();
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.SendCompactionRequest(0);

        partition.SendCompactionRequest(1024);

        {
            auto compactResponse = partition.RecvCompactionResponse();
            UNIT_ASSERT(FAILED(compactResponse->GetStatus()));
            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, compactResponse->GetStatus());
        }

        runtime->SetObserverFunc(&TTestActorRuntimeBase::DefaultObserverFunc);
        runtime->Send(addBlobs.Release());

        {
            auto compactResponse = partition.RecvCompactionResponse();
            UNIT_ASSERT(SUCCEEDED(compactResponse->GetStatus()));
        }

        {
            partition.SendCompactionRequest(0);
            auto compactResponse = partition.RecvCompactionResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, compactResponse->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldStartCompactionOnCompactRagesRequest)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 100);
        partition.SendCompactRangeRequest(0, 100);

        auto response = partition.RecvCompactRangeResponse();
        UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            response->Record.GetOperationId().empty());
    }

    Y_UNIT_TEST(ShouldReturnErrorForUnknownIdInCompactionStatusRequest)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 100);
        partition.SendGetCompactionStatusRequest("xxx");

        auto response = partition.RecvGetCompactionStatusResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldReturnCompactionProgress)
    {
        auto runtime = PrepareTestActorRuntime();

        TActorId rangeActor;
        TActorId partActor;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        rangeActor = event->Sender;
                        partActor = event->Recipient;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 100);
        auto compResponse = partition.CompactRange(0, 100);

        partition.SendGetCompactionStatusRequest(
            compResponse->Record.GetOperationId());

        auto statusResponse1 = partition.RecvGetCompactionStatusResponse();
        UNIT_ASSERT(SUCCEEDED(statusResponse1->GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            statusResponse1->Record.GetIsCompleted());
        UNIT_ASSERT_VALUES_EQUAL(1, statusResponse1->Record.GetTotal());

        auto compactionResponse =
            std::make_unique<TEvPartitionPrivate::TEvCompactionResponse>();
        runtime->Send(
            new IEventHandle(
                rangeActor,
                partActor,
                compactionResponse.release(),
                0,   // flags
                0),
            0);

        partition.SendGetCompactionStatusRequest(
            compResponse->Record.GetOperationId());

        auto statusResponse2 = partition.RecvGetCompactionStatusResponse();
        UNIT_ASSERT(SUCCEEDED(statusResponse2->GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            statusResponse2->Record.GetIsCompleted());
        UNIT_ASSERT_VALUES_EQUAL(1, statusResponse2->Record.GetTotal());
    }

    Y_UNIT_TEST(ShouldReturnCompactionStatusForLastCompletedCompaction)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 100);
        auto compResponse = partition.CompactRange(0, 100);

        auto statusResponse = partition.GetCompactionStatus(
            compResponse->Record.GetOperationId());

        UNIT_ASSERT_VALUES_EQUAL(true, statusResponse->Record.GetIsCompleted());
        UNIT_ASSERT_VALUES_EQUAL(1, statusResponse->Record.GetTotal());
    }

    Y_UNIT_TEST(ShouldForgetTooOldCompactRangeOperations)
    {
        constexpr TDuration CompactOpHistoryDuration = TDuration::Days(1);

        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TVector<TString> op;

        for (ui32 i = 0; i < 4; ++i) {
            partition.WriteBlocks(0, 100);
            partition.SendCompactRangeRequest(0, 100);

            {
                TDispatchOptions options;
                options.FinalEvents.emplace_back(
                    TEvPartitionPrivate::EvForcedCompactionCompleted,
                    1);
                runtime->DispatchEvents(options);
            }

            auto response = partition.RecvCompactRangeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            op.push_back(response->Record.GetOperationId());
        }

        runtime->AdvanceCurrentTime(
            CompactOpHistoryDuration + TDuration::Seconds(1));

        for (ui32 i = 0; i < 4; ++i) {
            partition.SendGetCompactionStatusRequest(op[i]);
            auto response = partition.RecvGetCompactionStatusResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldProcessMultipleRangesUponCompaction)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetBatchCompactionEnabled(true);
        config.SetCompactionRangeCountPerRun(3);
        config.SetSSDMaxBlobsPerRange(999);
        config.SetHDDMaxBlobsPerRange(999);
        config.SetCompactionGarbageThreshold(999);
        config.SetCompactionRangeGarbageThreshold(999);
        auto runtime = PrepareTestActorRuntime(config, MaxPartitionBlocksCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto blockRange1 = TBlockRange32::WithLength(0, 1024);
        const auto blockRange2 = TBlockRange32::WithLength(1024 * 1024, 1024);
        const auto blockRange3 =
            TBlockRange32::WithLength(2 * 1024 * 1024, 1024);
        const auto blockRange4 =
            TBlockRange32::WithLength(3 * 1024 * 1024, 1024);

        partition.WriteBlocks(blockRange1, 1);
        partition.WriteBlocks(blockRange1, 2);
        partition.WriteBlocks(blockRange1, 3);

        partition.WriteBlocks(blockRange2, 4);
        partition.WriteBlocks(blockRange2, 5);
        partition.WriteBlocks(blockRange2, 6);

        partition.WriteBlocks(blockRange3, 7);
        partition.WriteBlocks(blockRange3, 8);
        partition.WriteBlocks(blockRange3, 9);

        partition.WriteBlocks(blockRange4, 10);
        partition.WriteBlocks(blockRange4, 11);

        // blockRange4 should not be compacted, other ranges - should
        partition.Compaction();
        partition.Cleanup();

        // checking that data wasn't corrupted
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(blockRange1.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(blockRange1.End)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(6),
            GetBlockContent(partition.ReadBlocks(blockRange2.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(6),
            GetBlockContent(partition.ReadBlocks(blockRange2.End)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(9),
            GetBlockContent(partition.ReadBlocks(blockRange3.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(9),
            GetBlockContent(partition.ReadBlocks(blockRange3.End)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(11),
            GetBlockContent(partition.ReadBlocks(blockRange4.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(11),
            GetBlockContent(partition.ReadBlocks(blockRange4.End)));

        // checking that we now have 1 blob in each of the first 3 ranges
        // and 2 blobs in the last range
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(5, stats.GetMergedBlobsCount());
        }

        // blockRange4 and any other 2 ranges should be compacted
        partition.Compaction();
        partition.Cleanup();

        // all ranges should contain 1 blob now
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldPatchBlobsDuringCompaction)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetBlobPatchingEnabled(true);
        config.SetHDDMaxBlobsPerRange(999);
        config.SetSSDMaxBlobsPerRange(999);
        config.SetCompactionGarbageThreshold(999);
        config.SetCompactionRangeGarbageThreshold(999);
        auto runtime = PrepareTestActorRuntime(config, MaxPartitionBlocksCount);

        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(counters);
        bool evPatchObserved = false;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPatch: {
                        evPatchObserved = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto blockRange1 = TBlockRange32::WithLength(0, 1024);
        const auto blockRange2 = TBlockRange32::WithLength(0, 512);
        const auto blockRange3 = TBlockRange32::WithLength(0, 256);

        partition.WriteBlocks(blockRange1, 1);
        partition.WriteBlocks(blockRange2, 2);
        partition.WriteBlocks(blockRange3, 3);

        partition.Compaction();
        partition.Cleanup();

        // checking that data wasn't corrupted
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(0)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(255)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(256)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(511)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(512)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1023)));

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlockContent(partition.ReadBlocks(1024)));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }

        UNIT_ASSERT(evPatchObserved);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            counters
                ->GetCounter(
                    GetCriticalEventForContractViolationWriteBlobError(),
                    true)
                ->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            counters
                ->GetCounter(
                    GetCriticalEventForContractViolationPatchBlobError(),
                    true)
                ->Val());
    }

    Y_UNIT_TEST(ShouldOnlyKillTabletOnBlobStoragePatchErrors)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetBlobPatchingEnabled(true);
        config.SetHDDMaxBlobsPerRange(999);
        config.SetSSDMaxBlobsPerRange(999);
        config.SetCompactionGarbageThreshold(999);
        config.SetCompactionRangeGarbageThreshold(999);
        config.SetMaxIORequestsInFlight(1);
        auto runtime = PrepareTestActorRuntime(config, MaxPartitionBlocksCount);

        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(counters);
        auto contractViolationErrors = counters->GetCounter(
            GetCriticalEventForContractViolationPatchBlobError(),
            true);

        TPartitionClient partition(*runtime);
        partition.WaitReady();
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 512), 2);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 256), 3);

        UNIT_ASSERT_VALUES_EQUAL(0, contractViolationErrors->Val());
        ui64 expectedContractViolationErrors = 0;
        bool blobStorageError = false;
        bool suicideHappened = false;
        ui32 patchRequests = 0;
        ui32 patchResponses = 0;
        ui32 patchCompletions = 0;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev)
            {
                if (ev->GetTypeRewrite() != TEvBlobStorage::EvPatch) {
                    return false;
                }

                ++patchRequests;
                // An OK status with an invalid blob id violates the contract.
                auto response =
                    std::make_unique<TEvBlobStorage::TEvPatchResult>(
                        blobStorageError ? NKikimrProto::ERROR
                                         : NKikimrProto::OK,
                        TLogoBlobID(),
                        0,
                        TGroupId::FromValue(0),
                        0);
                runtime->Schedule(
                    new IEventHandle(
                        ev->Sender,
                        ev->Recipient,
                        response.release(),
                        0,
                        ev->Cookie),
                    TDuration());
                return true;
            });

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& ev)
            {
                switch (ev->GetTypeRewrite()) {
                    case TEvPartitionCommonPrivate::EvPatchBlobResponse: {
                        const auto* msg = ev->Get<
                            TEvPartitionCommonPrivate::TEvPatchBlobResponse>();
                        UNIT_ASSERT(HasError(msg->GetError()));
                        UNIT_ASSERT(
                            msg->ErrorSource ==
                            (blobStorageError
                                 ? EErrorSource::BlobStorage
                                 : EErrorSource::ContractViolation));
                        ++patchResponses;
                        break;
                    }
                    case TEvPartitionCommonPrivate::EvPatchBlobCompleted: {
                        const auto* msg = ev->Get<
                            TEvPartitionCommonPrivate::TEvPatchBlobCompleted>();
                        UNIT_ASSERT(HasError(msg->GetError()));
                        UNIT_ASSERT(
                            msg->ErrorSource ==
                            (blobStorageError
                                 ? EErrorSource::BlobStorage
                                 : EErrorSource::ContractViolation));
                        ++patchCompletions;
                        expectedContractViolationErrors +=
                            msg->ErrorSource == EErrorSource::ContractViolation;
                        UNIT_ASSERT_VALUES_EQUAL(
                            expectedContractViolationErrors,
                            contractViolationErrors->Val());
                        break;
                    }
                    case TEvTablet::EvTabletDead: {
                        suicideHappened = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(ev);
            });

        for (ui32 i = 0; i < 3; ++i) {
            partition.SendCompactionRequest();
            auto response = partition.RecvCompactionResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
            UNIT_ASSERT(!suicideHappened);
            UNIT_ASSERT_VALUES_EQUAL(i + 1, patchRequests);
            UNIT_ASSERT_VALUES_EQUAL(i + 1, patchResponses);
            UNIT_ASSERT_VALUES_EQUAL(i + 1, patchCompletions);
        }

        blobStorageError = true;
        partition.SendCompactionRequest();
        auto response = partition.RecvCompactionResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(4, patchRequests);
        UNIT_ASSERT_VALUES_EQUAL(4, patchCompletions);
        UNIT_ASSERT(suicideHappened);
    }

    Y_UNIT_TEST(WritingBlobsInsteadOfPatchingIfDiffIsGreaterThanThreshold)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetBlobPatchingEnabled(true);
        config.SetHDDMaxBlobsPerRange(999);
        config.SetSSDMaxBlobsPerRange(999);
        config.SetCompactionGarbageThreshold(999);
        config.SetCompactionRangeGarbageThreshold(999);
        config.SetMaxDiffPercentageForBlobPatching(75);
        auto runtime = PrepareTestActorRuntime(config, MaxPartitionBlocksCount);

        bool evPatchObserved = false;
        bool evWriteObserved = false;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPatch: {
                        evPatchObserved = true;
                        break;
                    }
                    case TEvPartitionCommonPrivate::EvWriteBlobRequest: {
                        evWriteObserved = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto blockRange1 = TBlockRange32::WithLength(0, 1024);
        const auto blockRange2 = TBlockRange32::WithLength(0, 512 + 256 + 64);
        const auto blockRange3 = TBlockRange32::WithLength(0, 512);

        partition.WriteBlocks(blockRange1, 1);
        partition.WriteBlocks(blockRange2, 2);

        partition.Compaction();
        partition.Cleanup();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(256)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(512 + 256 + 63)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(900)));

        UNIT_ASSERT(!evPatchObserved);
        UNIT_ASSERT(evWriteObserved);

        evWriteObserved = false;
        partition.WriteBlocks(blockRange3, 3);

        partition.Compaction();
        partition.Cleanup();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(511)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(512)));

        UNIT_ASSERT(evPatchObserved);
        UNIT_ASSERT(evWriteObserved);
    }

    Y_UNIT_TEST(ShouldPatchBlobsDuringIncrementalCompaction)
    {
        auto config = DefaultConfig();
        config.SetBlobPatchingEnabled(true);
        DoTestIncrementalCompaction(config, NProto::STORAGE_MEDIA_SSD, true);
        DoTestIncrementalCompaction(
            std::move(config),
            NProto::STORAGE_MEDIA_HYBRID,
            true);
    }

    Y_UNIT_TEST(ShouldProperlyPatchBlobWithDifferentLayout)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetBlobPatchingEnabled(true);
        config.SetHDDMaxBlobsPerRange(999);
        config.SetSSDMaxBlobsPerRange(999);
        config.SetCompactionGarbageThreshold(999);
        config.SetCompactionRangeGarbageThreshold(999);
        auto runtime = PrepareTestActorRuntime(config, MaxPartitionBlocksCount);

        bool evPatchObserved = false;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPatch: {
                        evPatchObserved = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(1), 1);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(3), 3);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(4), 4);

        partition.Flush();
        partition.Compaction();
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }

        UNIT_ASSERT(evPatchObserved);
        evPatchObserved = false;

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1)));

        UNIT_ASSERT_VALUES_EQUAL(
            TString(),
            GetBlockContent(partition.ReadBlocks(2)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(3)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(4),
            GetBlockContent(partition.ReadBlocks(4)));

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(2), 2);
        partition.ZeroBlocks(TBlockRange32::MakeOneBlock(4));

        partition.Flush();
        partition.Compaction();
        partition.Cleanup();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(2)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(3)));

        UNIT_ASSERT_VALUES_EQUAL(
            TString(),
            GetBlockContent(partition.ReadBlocks(4)));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
        }

        UNIT_ASSERT(evPatchObserved);
    }

    Y_UNIT_TEST(ShouldAbortCompactionIfReadBlobFailsWithDeadlineExceeded)
    {
        DoShouldAbortCompactionIfBlobStorageRequestFailsWithDeadlineExceeded(
            TEvBlobStorage::EvVGet);
    }

    Y_UNIT_TEST(ShouldAbortCompactionIfWriteBlobFailsWithDeadlineExceeded)
    {
        DoShouldAbortCompactionIfBlobStorageRequestFailsWithDeadlineExceeded(
            TEvBlobStorage::EvVPut);
    }

    Y_UNIT_TEST(ShouldAllowForcedCompactionRequestsInPresenseOfTabletCompaction)
    {
        constexpr ui32 rangesCount = 5;
        auto runtime =
            PrepareTestActorRuntime(DefaultConfig(), rangesCount * 1024);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 range = 0; range < rangesCount; ++range) {
            partition.WriteBlocks(
                TBlockRange32::WithLength(range * 1024, 1024),
                1);
        }
        partition.Flush();

        bool steal = true;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
            {
                Y_UNUSED(runtime);

                if (ev->GetTypeRewrite() ==
                        TEvPartitionPrivate::EvCompactionCompleted &&
                    steal)
                {
                    steal = false;
                    return true;
                }
                return false;
            });

        partition.SendCompactionRequest(0, TCompactionOptions());
        partition.Compaction(
            0,
            TCompactionOptions().set(ToBit(ECompactionOption::Forced)));
    }

    Y_UNIT_TEST(ShouldAllowOnlyOneForcedCompactionRequestAtATime)
    {
        constexpr ui32 rangesCount = 5;
        auto runtime =
            PrepareTestActorRuntime(DefaultConfig(), rangesCount * 1024);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 range = 0; range < rangesCount; ++range) {
            partition.WriteBlocks(
                TBlockRange32::WithLength(range * 1024, 1024),
                1);
        }
        partition.Flush();

        bool steal = true;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
            {
                Y_UNUSED(runtime);

                if (ev->GetTypeRewrite() ==
                        TEvPartitionPrivate::EvCompactionCompleted &&
                    steal)
                {
                    steal = false;
                    return true;
                }
                return false;
            });

        partition.SendCompactionRequest(
            0,
            TCompactionOptions().set(ToBit(ECompactionOption::Forced)));
        partition.SendCompactionRequest(
            0,
            TCompactionOptions().set(ToBit(ECompactionOption::Forced)));

        auto response = partition.RecvCompactionResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldProcessMultipleRangesUponForceCompaction)
    {
        auto config = DefaultConfig();
        config.SetBatchCompactionEnabled(true);
        config.SetForcedCompactionRangeCountPerRun(3);
        config.SetV1GarbageCompactionEnabled(false);

        auto runtime = PrepareTestActorRuntime(config, MaxPartitionBlocksCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        TVector<size_t> rangeSizes;
        const auto interceptCompactionRequest =
            [&rangeSizes](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvPartitionPrivate::EvCompactionRequest)
            {
                auto* msg =
                    event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                rangeSizes.push_back(msg->RangeBlockIndices.size());
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime->SetObserverFunc(interceptCompactionRequest);

        const auto blockRange1 = TBlockRange32::WithLength(0, 1024);
        const auto blockRange2 = TBlockRange32::WithLength(1024 * 1024, 1024);
        const auto blockRange3 =
            TBlockRange32::WithLength(2 * 1024 * 1024, 1024);
        const auto blockRange4 =
            TBlockRange32::WithLength(3 * 1024 * 1024, 1024);

        partition.WriteBlocks(blockRange1, 1);
        partition.WriteBlocks(blockRange1, 2);

        partition.WriteBlocks(blockRange2, 3);
        partition.WriteBlocks(blockRange2, 4);
        partition.WriteBlocks(blockRange2, 5);

        partition.WriteBlocks(blockRange3, 6);
        partition.WriteBlocks(blockRange3, 7);
        partition.WriteBlocks(blockRange3, 8);

        partition.WriteBlocks(blockRange4, 9);

        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(9, stats.GetMergedBlobsCount());
        }

        partition.SendCompactRangeRequest(0, 0);
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        partition.Cleanup();

        UNIT_ASSERT_EQUAL(2, rangeSizes.size());
        UNIT_ASSERT_EQUAL(3, rangeSizes[0]);
        UNIT_ASSERT_EQUAL(1, rangeSizes[1]);

        // checking that data wasn't corrupted
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(blockRange1.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(blockRange1.End)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(5),
            GetBlockContent(partition.ReadBlocks(blockRange2.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(5),
            GetBlockContent(partition.ReadBlocks(blockRange2.End)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(8),
            GetBlockContent(partition.ReadBlocks(blockRange3.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(8),
            GetBlockContent(partition.ReadBlocks(blockRange3.End)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(9),
            GetBlockContent(partition.ReadBlocks(blockRange4.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(9),
            GetBlockContent(partition.ReadBlocks(blockRange4.End)));

        // checking that we now have 1 blob in each of the ranges
        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldSkipEmptyRangesUponForcedCompactionWithMultipleRanges)
    {
        auto config = DefaultConfig();
        config.SetBatchCompactionEnabled(true);
        config.SetForcedCompactionRangeCountPerRun(3);
        config.SetV1GarbageCompactionEnabled(false);

        auto runtime = PrepareTestActorRuntime(config, MaxPartitionBlocksCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        const auto blockRange1 = TBlockRange32::WithLength(3 * 1024, 1024);
        const auto blockRange2 = TBlockRange32::WithLength(4 * 1024, 1024);

        partition.WriteBlocks(blockRange1, 1);
        partition.WriteBlocks(blockRange2, 2);

        const auto response = partition.CompactRange(0, 5119);
        UNIT_ASSERT_EQUAL(S_OK, response->GetError().GetCode());

        partition.SendGetCompactionStatusRequest(
            response->Record.GetOperationId());
        const auto compactionStatus =
            partition.RecvGetCompactionStatusResponse();
        UNIT_ASSERT(SUCCEEDED(compactionStatus->GetStatus()));
        UNIT_ASSERT_EQUAL(5, compactionStatus->Record.GetTotal());
    }

    Y_UNIT_TEST(
        ShouldAutomaticallyRunCompactionAndWriteBlobToMixedChannelIfBlobSizeIsBelowTheThreshold)
    {
        static constexpr ui32 compactionThreshold = 4;

        auto config = DefaultConfig();
        config.SetHDDMaxBlobsPerRange(compactionThreshold);
        config.SetCompactionMergedBlobThresholdHDD(17_KB);

        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (size_t i = 1; i < compactionThreshold; ++i) {
            partition.WriteBlocks(i, i);
            partition.Flush();
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold - 1,
                stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold - 1,
                stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        partition.WriteBlocks(0, 0);
        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // data size for compaction is less than
        // CompactionMergedBlobThresholdHDD so the whole data should be moved to
        // a mixed channel
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold + 1,
                stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold * 2,
                stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold,
                stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(
        CompactionShouldWriteDataToDifferentChannelsDependingOnThreshold)
    {
        auto config = DefaultConfig();
        config.SetCompactionMergedBlobThresholdHDD(17_KB);

        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.Flush();
        partition.WriteBlocks(2, 2);
        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        partition.WriteBlocks(0, 0);

        // data should be written to a mixed channel if data size is less than
        // threshold
        partition.Compaction();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        partition.WriteBlocks(TBlockRange32::WithLength(3, 5), 3);
        partition.Flush();

        // data should be written to a merged channel if data size is greater
        // than threshold
        partition.Compaction();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }

        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(CompactionShouldWriteDataToMergedChannelWhenForced)
    {
        auto config = DefaultConfig();
        config.SetCompactionMergedBlobThresholdHDD(17_KB);

        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 1);
        partition.Flush();

        TCompactionOptions options;
        options.set(ToBit(ECompactionOption::ForceMixedBlocksCountCompaction));
        partition.Compaction(0, options);
        partition.Cleanup();

        const auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
    }

    Y_UNIT_TEST(
        CompactionShouldMoveDataFromMergedToMixedWhenDataSizeIsAboveTheTreshold)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(3_KB);
        config.SetCompactionMergedBlobThresholdHDD(17_KB);

        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // all data should be written to a merged channel directly because data
        // size is less than threshold
        for (int i = 0; i < 4; ++i) {
            partition.WriteBlocks(i, i);
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
        }

        // data should be moved to a mixed channel as a result of compaction,
        // because data is less than threshold
        partition.Compaction();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
        }

        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        for (int i = 0; i < 4; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(i),
                GetBlockContent(partition.ReadBlocks(i)));
        }
    }

    Y_UNIT_TEST(CompactionShouldMoveMergedBlobWithHolesToMixedChannel)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(12_KB);
        config.SetCompactionMergedBlobThresholdHDD(1_MB);

        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(5, 5), '1');   // merged
        partition.WriteBlocks(TBlockRange32::WithLength(12, 1), '2');   // fresh
        partition.WriteBlocks(
            TBlockRange32::WithLength(35, 20),
            '3');   // merged

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
        }

        // data should be moved to a mixed channel as a result of compaction,
        // because data is less than threshold
        partition.Flush();
        partition.Compaction();
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        const auto checkValue =
            [&](std::vector<std::tuple<int, int, char>> values)
        {
            for (const auto& value: values) {
                for (int i = std::get<0>(value); i < std::get<1>(value); ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        GetBlockContent(std::get<2>(value)),
                        GetBlockContent(partition.ReadBlocks(i)));
                }
            }
        };

        checkValue({{5, 5, '1'}, {12, 1, '2'}, {35, 20, '3'}});

        partition.WriteBlocks(TBlockRange32::WithLength(0, 2), '4');   // fresh
        partition.Flush();                                             // mixed
        partition.WriteBlocks(TBlockRange32::WithLength(2, 2), '5');   // fresh
        partition.WriteBlocks(
            TBlockRange32::WithLength(12, 3),
            '6');   // merged
        partition.WriteBlocks(TBlockRange32::WithLength(27, 1), '7');   // fresh

        partition.Compaction();
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        checkValue(
            {{0, 2, '4'},
             {2, 2, '5'},
             {5, 5, '1'},
             {12, 3, '6'},
             {27, 1, '7'},
             {35, 20, '3'}});
    }

    Y_UNIT_TEST(
        ShouldNotEraseSkippedBlocksFromIndexDuringIncrementalCompactionToMixed)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetCompactionMergedBlobThresholdHDD(1_MB);
        config.SetIncrementalCompactionEnabled(true);
        config.SetMaxSkippedBlobsDuringCompactionHDD(1);
        config.SetTargetCompactionBytesPerOp(1);
        config.SetMixedBlocksCountCompactionEnabledHDD(true);

        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // blob 1 needs to eventually have more live blocks than other blobs in
        // order not to be compacted
        partition.WriteBlocks(TBlockRange32::WithLength(5, 55), '1');
        partition.Flush();
        partition.WriteBlocks(TBlockRange32::WithLength(2, 10), '2');
        partition.Flush();
        partition.WriteBlocks(TBlockRange32::WithLength(45, 20), '3');
        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(85, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
        }

        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(85, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
        }

        // 55 blocks from the skipped blob and 30 blocks from the compacted
        // blobs must all remain in the range's mixed-block counter.
        auto counters = partition.GetCompactionCounters(0);
        UNIT_ASSERT_VALUES_EQUAL(85, counters->Counters.MixedBlockCount);

        partition.RebootTablet();
        partition.WaitReady();

        counters = partition.GetCompactionCounters(0);
        UNIT_ASSERT_VALUES_EQUAL(85, counters->Counters.MixedBlockCount);

        for (ui32 i = 12; i < 45; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent('1'),
                GetBlocksContent(partition.ReadBlocks(i)));
        }

        for (ui32 i = 2; i < 12; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent('2'),
                GetBlocksContent(partition.ReadBlocks(i)));
        }

        for (ui32 i = 45; i < 65; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent('3'),
                GetBlocksContent(partition.ReadBlocks(i)));
        }
    }

    Y_UNIT_TEST(ShouldCompactSeveralBlobsInTheSameRangeWithOneRangePerRun)
    {
        TestForcedCompaction(1);
    }

    Y_UNIT_TEST(ShouldCompactSeveralBlobsInTheSameRangeWithSeveralRangesPerRun)
    {
        TestForcedCompaction(10);
    }

    Y_UNIT_TEST(ShouldCompactWithSplitCompactionTx)
    {
        auto config = DefaultConfig();
        config.SetSplitCompactionTxEnabled(true);
        config.SetReadBlockMaskOnCompactionOptimizationEnabled(true);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(
            TBlockRange32::WithLength(0, MaxBlocksCount),
            '1');
        partition.WriteBlocks(
            TBlockRange32::WithLength(0, MaxBlocksCount),
            '2');
        partition.WriteBlocks(
            TBlockRange32::WithLength(MaxBlocksCount, MaxBlocksCount),
            '3');
        partition.WriteBlocks(
            TBlockRange32::WithLength(512, MaxBlocksCount),
            '4');

        partition.Compaction();

        // The split-tx path must produce the same observable counters as the
        // legacy path — these match
        // ShouldCleanupMergedBlobsWithoutReadingBlockMasks.
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(5, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                3,
                stats.GetBlobsProcessedDuringCompaction());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                stats.GetBlockMaskReadDuringCompaction());
        }

        partition.Cleanup();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent('2', 512) + GetBlocksContent('4', 512),
            GetBlocksContent(partition.ReadBlocks(
                TBlockRange32::WithLength(0, MaxBlocksCount))));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent('4', 512) + GetBlocksContent('3', 512),
            GetBlocksContent(partition.ReadBlocks(
                TBlockRange32::WithLength(MaxBlocksCount, MaxBlocksCount))));
    }

    Y_UNIT_TEST(ShouldCompactWithSplitCompactionTxWithoutOptimization)
    {
        auto config = DefaultConfig();
        config.SetSplitCompactionTxEnabled(true);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(
            TBlockRange32::WithLength(0, MaxBlocksCount),
            '1');
        partition.WriteBlocks(
            TBlockRange32::WithLength(0, MaxBlocksCount),
            '2');
        partition.WriteBlocks(
            TBlockRange32::WithLength(MaxBlocksCount, MaxBlocksCount),
            '3');

        partition.Compaction();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent('2', MaxBlocksCount),
            GetBlocksContent(partition.ReadBlocks(
                TBlockRange32::WithLength(0, MaxBlocksCount))));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent('3', MaxBlocksCount),
            GetBlocksContent(partition.ReadBlocks(
                TBlockRange32::WithLength(MaxBlocksCount, MaxBlocksCount))));
    }

    Y_UNIT_TEST(ShouldCompactZeroedRangeWithSplitCompactionTx)
    {
        auto config = DefaultConfig();
        config.SetSplitCompactionTxEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), '1');
        partition.ZeroBlocks(TBlockRange32::WithLength(0, 1024));

        partition.Compaction();

        // Reads after compaction must return zeros, not '1's.
        UNIT_ASSERT_VALUES_EQUAL(
            TString(),
            GetBlocksContent(
                partition.ReadBlocks(TBlockRange32::WithLength(0, 1024))));
    }

    Y_UNIT_TEST(ShouldCompactWithGarbageBlobsWithSplitCompactionTx)
    {
        auto config = DefaultConfig();
        config.SetSplitCompactionTxEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 512), '1');
        partition.WriteBlocks(TBlockRange32::WithLength(512, 512), '2');

        partition.Compaction();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 512), '3');

        bool intercept = true;

        std::unique_ptr<IEventHandle> compactionReadBlobInfoRequest;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvPartitionPrivate::EvCompactionReadBlobInfoRequest &&
                    intercept)
                {
                    compactionReadBlobInfoRequest.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.SendCompactionRequest();

        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT(compactionReadBlobInfoRequest);
        intercept = false;

        partition.Cleanup();

        runtime->SendAsync(compactionReadBlobInfoRequest.release());

        auto response = partition.RecvCompactionResponse();
        UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), S_OK);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent('3', 512) + GetBlocksContent('2', 512),
            GetBlocksContent(
                partition.ReadBlocks(TBlockRange32::WithLength(0, 1024))));
    }

    Y_UNIT_TEST(
        ShouldNotStartAddBlobsUntilBlobInfosAreReadWithSplitCompactionTx)
    {
        auto config = DefaultConfig();
        config.SetSplitCompactionTxEnabled(true);
        config.SetReadBlockMaskOnCompactionOptimizationEnabled(false);
        config.SetDiskPrefixLengthWithBlockChecksumsInBlobs(0);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), '1');
        partition.ZeroBlocks(TBlockRange32::WithLength(0, 1024));

        bool intercept = true;

        std::unique_ptr<IEventHandle> compactionReadBlobInfoRequest;
        bool addCompactionBlobsRequestObserved = false;

        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionReadBlobInfoRequest:
                        if (intercept) {
                            compactionReadBlobInfoRequest.reset(
                                event.Release());
                            return true;
                        }
                        break;
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        auto* msg = event->Get<
                            TEvPartitionPrivate::TEvAddBlobsRequest>();
                        if (msg->Mode == EAddBlobMode::ADD_COMPACTION_RESULT) {
                            addCompactionBlobsRequestObserved = true;
                        }
                        break;
                    }
                }

                return false;
            });

        partition.SendCompactionRequest();

        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT(compactionReadBlobInfoRequest);
        UNIT_ASSERT(!addCompactionBlobsRequestObserved);

        intercept = false;

        runtime->SendAsync(compactionReadBlobInfoRequest.release());

        auto response = partition.RecvCompactionResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
    }

    Y_UNIT_TEST(
        ShouldNotVerifyChecksumsUntilBlobInfosAreReadWithSplitCompactionTx)
    {
        auto config = DefaultConfig();
        config.SetSplitCompactionTxEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 512), '1');
        partition.WriteBlocks(TBlockRange32::WithLength(512, 512), '2');

        partition.Compaction();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 512), '3');

        bool intercept = true;

        std::unique_ptr<IEventHandle> compactionReadBlobInfoRequest;
        bool writeBlobRequestObserved = false;
        bool addCompactionBlobsRequestObserved = false;
        bool readBlobResponseObserved = false;

        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionReadBlobInfoRequest:
                        if (intercept) {
                            compactionReadBlobInfoRequest.reset(
                                event.Release());
                            return true;
                        }
                        break;
                    case TEvPartitionCommonPrivate::EvWriteBlobRequest:
                        writeBlobRequestObserved = true;
                        break;
                    case TEvPartitionCommonPrivate::EvReadBlobResponse:
                        readBlobResponseObserved = true;
                        break;
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        auto* msg = event->Get<
                            TEvPartitionPrivate::TEvAddBlobsRequest>();
                        if (msg->Mode == EAddBlobMode::ADD_COMPACTION_RESULT) {
                            addCompactionBlobsRequestObserved = true;
                        }
                        break;
                    }
                }

                return false;
            });

        partition.SendCompactionRequest();

        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT(compactionReadBlobInfoRequest);
        UNIT_ASSERT(readBlobResponseObserved);
        UNIT_ASSERT(!writeBlobRequestObserved);
        UNIT_ASSERT(!addCompactionBlobsRequestObserved);

        intercept = false;

        runtime->SendAsync(compactionReadBlobInfoRequest.release());

        auto response = partition.RecvCompactionResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
    }

    Y_UNIT_TEST(
        ShouldNotDeleteBlobsThatAreNotFullyAvailableForCompactionWithoutReadingBlockMasks)
    {
        auto config = DefaultConfig();
        config.SetReadBlockMaskOnCompactionOptimizationEnabled(true);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 'A');

        partition.WriteBlocks(0, '0');

        std::unique_ptr<IEventHandle> writeBlobRequest;

        bool interceptWriteBlobRequest = true;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvPartitionCommonPrivate::EvWriteBlobRequest &&
                    interceptWriteBlobRequest)
                {
                    writeBlobRequest.reset(event.Release());

                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.SendWriteBlocksRequest(1, '1');

        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT(writeBlobRequest);
        interceptWriteBlobRequest = false;

        partition.SendCompactionRequest();

        runtime->DispatchEvents({}, 10ms);

        partition.WriteBlocks(2, '2');

        partition.Flush();
        partition.TrimFreshLog();

        runtime->SendAsync(writeBlobRequest.release());

        auto response = partition.RecvCompactionResponse();
        UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), S_OK);

        partition.Cleanup();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('0'),
            GetBlockContent(partition.ReadBlocks(0)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('1'),
            GetBlockContent(partition.ReadBlocks(1)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('2'),
            GetBlockContent(partition.ReadBlocks(2)));
    }

    // Test that writes with commit id newer than compaction commit id and can
    // be observed in compaction tx should be accounted for skipped blobs and
    // blocks.
    Y_UNIT_TEST(ShouldAccountForWritesNewerThanCompactionCommit)
    {
        auto config = DefaultConfig();
        config.SetReadBlockMaskOnCompactionOptimizationEnabled(true);
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetCompactionStatsTrackerEnabled(true);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // Just some writes before compaction.
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 'A');
        partition.WriteBlocks(0, '0');

        std::unique_ptr<IEventHandle> writeBlobRequest;

        bool interceptWriteBlobRequest = false;

        auto observer = runtime->AddObserver(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvPartitionCommonPrivate::EvWriteBlobRequest &&
                    interceptWriteBlobRequest)
                {
                    writeBlobRequest.reset(event.Release());
                }
            });

        // Intercepting write blob request to slow down compaction (as it should
        // wait for it before start executing).
        interceptWriteBlobRequest = true;
        partition.SendWriteBlocksRequest(1, '1');
        runtime->DispatchEvents({}, 10ms);
        UNIT_ASSERT(writeBlobRequest);
        interceptWriteBlobRequest = false;

        partition.SendCompactionRequest();

        runtime->DispatchEvents({}, 10ms);

        // Do fresh write with commit id higher than compaction commit id and
        // then flush to create a mixed blob that has commit ids less and
        // greater than compaction commit id.
        partition.WriteBlocks(2, '2');
        partition.Flush();
        partition.TrimFreshLog();

        // Merged blob newer than compaction.
        partition.WriteBlocks(TBlockRange32::WithLength(511, 512), 'B');

        // Unblock write blob request to continue compaction.
        runtime->SendAsync(writeBlobRequest.release());

        auto response = partition.RecvCompactionResponse();
        UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), S_OK);
        partition.Cleanup();

        const auto counters = partition.GetCompactionCounters(0);

        // Check that compaction accounted for blocks and blobs that was written
        // with commit id higher than compaction commit id.

        // 1 compaction blob + 1 flush blob + 1 merged blob
        UNIT_ASSERT_VALUES_EQUAL(3, counters->Counters.BlobCount);
        // 512 blocks form merged blob + 1 block from
        // flush blob newer than compaction + 1024
        // blocks from compaction merged blob
        UNIT_ASSERT_VALUES_EQUAL(512 + 1 + 1024, counters->Counters.BlockCount);
        UNIT_ASSERT(!counters->Counters.Compacted);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
