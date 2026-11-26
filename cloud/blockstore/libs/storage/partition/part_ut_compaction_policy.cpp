#include "part_ut.h"

#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/core/compaction_options.h>
#include <cloud/blockstore/libs/storage/testlib/part_client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;
using namespace NKikimr;
using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

void CheckMixedBlocksPercentageCompactionPerDisk(
    NCloud::NProto::EStorageMediaKind mediaKind,
    ui64 percentageThreshold,
    ui64 startBytes,
    ui32 triggerAfterMixedBlocks,
    bool enabled = true,
    bool reboot = false,
    ui32 diskBlockCount = 2 * MaxBlocksCount,
    ui32 mergedBlocksPerRange = MaxBlocksCount,
    bool useByteCounters = false)
{
    auto config = DefaultConfig(1_MB);
    config.SetMixedBlocksCompactionByteCountersEnabled(useByteCounters);
    config.SetExtendedBlobCountersEnabled(useByteCounters);
    config.SetMixedBlocksCountCompactionEnabledHDD(enabled);
    config.SetMixedBlocksCountCompactionEnabledSSD(enabled);
    // The other media kind must not control this disk's trigger.
    config.SetMixedBlocksCompactionThresholdPercentageHDD(100);
    config.SetMixedBlocksCompactionThresholdPercentageSSD(100);
    if (mediaKind == NCloud::NProto::STORAGE_MEDIA_SSD) {
        config.SetMixedBlocksCompactionThresholdPercentageSSD(
            percentageThreshold);
        config.SetMixedBlocksCompactionMinStoredBytesSSD(startBytes);
    } else {
        config.SetMixedBlocksCompactionThresholdPercentageHDD(
            percentageThreshold);
        config.SetMixedBlocksCompactionMinStoredBytesHDD(startBytes);
    }

    TTestPartitionInfo partitionInfo;
    partitionInfo.MediaKind = mediaKind;
    TMaybe<ui32> channelCount;
    if (useByteCounters) {
        // Stored byte counters classify blobs by channel kind. Provide
        // separate mixed and merged channels for this calculation.
        partitionInfo.MixedChannelCount = 1;
        channelCount = 6;
    }
    auto runtime = PrepareTestActorRuntime(
        config,
        diskBlockCount,
        channelCount,
        partitionInfo);

    TPartitionClient partition(*runtime);
    partition.WaitReady();

    bool compactionRequestObserved = false;
    bool observeCompactionRequests = false;
    ui64 compactionByMixedBlockCountPerRange = 0;
    ui64 compactionByMixedBlockCountPerDisk = 0;
    ui64 mixedBlocksScore = 0;
    ui64 mixedBlocksPercentage = 0;
    runtime->SetEventFilter(
        [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvPartitionPrivate::EvCompactionRequest: {
                    if (!observeCompactionRequests) {
                        break;
                    }
                    const auto* msg =
                        event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                    UNIT_ASSERT_VALUES_EQUAL(
                        static_cast<ui32>(
                            TEvPartitionPrivate::MixedBlocksCountCompaction),
                        static_cast<ui32>(msg->Mode));
                    compactionRequestObserved = true;
                    return true;
                }
                case TEvStatsService::EvVolumePartCounters: {
                    const auto* msg =
                        event->Get<TEvStatsService::TEvVolumePartCounters>();
                    const auto& counters = msg->DiskCounters->Cumulative;
                    compactionByMixedBlockCountPerRange =
                        counters.CompactionByMixedBlockCountPerRange.Value;
                    compactionByMixedBlockCountPerDisk =
                        counters.CompactionByMixedBlockCountPerDisk.Value;
                    mixedBlocksScore = msg->DiskCounters->Simple
                                           .CompactionMixedBlocksScore.Value;
                    mixedBlocksPercentage =
                        msg->DiskCounters->Simple
                            .CompactionMixedBlocksPercentage.Value;
                    break;
                }
            }
            return false;
        });

    // Fill each compaction range with a merged write to satisfy
    // the used-blocks gate and keep the ranges non-empty.
    for (ui32 startIndex = 0; startIndex < 2 * MaxBlocksCount;
         startIndex += MaxBlocksCount)
    {
        partition.WriteBlocks(
            TBlockRange32::WithLength(startIndex, mergedBlocksPerRange),
            1);
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
    }
    if (diskBlockCount > 2 * MaxBlocksCount) {
        // This sparse range stays below the used-byte minimum.
        partition.WriteBlocks(
            TBlockRange32::WithLength(2 * MaxBlocksCount, 10),
            1);
        partition.Flush();
    }

    partition.SendToPipe(
        std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvStatsService::EvVolumePartCounters);
        runtime->DispatchEvents(options);
    }
    // Each stats push reports deltas; seeding must trigger no compactions.
    UNIT_ASSERT_VALUES_EQUAL(0, compactionByMixedBlockCountPerRange);
    UNIT_ASSERT_VALUES_EQUAL(0, compactionByMixedBlockCountPerDisk);
    observeCompactionRequests = true;

    // Each range stays below its mixed-byte threshold. The disk has
    // 2048 merged blocks, so 20 mixed blocks are below 1% of all stored
    // bytes, while 21 mixed blocks exceed it (21 / 2069).
    partition.WriteBlocks(TBlockRange32::WithLength(0, 10), 2);
    partition.Flush();
    partition.WriteBlocks(TBlockRange32::WithLength(MaxBlocksCount, 10), 3);
    partition.Flush();
    UNIT_ASSERT_VALUES_EQUAL(
        triggerAfterMixedBlocks == 20,
        compactionRequestObserved);

    if (reboot) {
        partition.RebootTablet();
        partition.WaitReady();
        UNIT_ASSERT(!compactionRequestObserved);
    }

    if (!compactionRequestObserved) {
        partition.WriteBlocks(MaxBlocksCount + 10, 4);
        partition.Flush();
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            triggerAfterMixedBlocks == 21,
            compactionRequestObserved);
    }

    if (!compactionRequestObserved) {
        partition.WriteBlocks(MaxBlocksCount + 11, 5);
        partition.Flush();
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            triggerAfterMixedBlocks == 22,
            compactionRequestObserved);
    }

    partition.SendToPipe(
        std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvStatsService::EvVolumePartCounters);
        runtime->DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(0, compactionByMixedBlockCountPerRange);
    UNIT_ASSERT_VALUES_EQUAL(
        triggerAfterMixedBlocks ? 1 : 0,
        compactionByMixedBlockCountPerDisk);
    const ui64 mixedBlocks =
        triggerAfterMixedBlocks ? triggerAfterMixedBlocks : 22;
    UNIT_ASSERT_VALUES_EQUAL(enabled ? mixedBlocks - 10 : 0, mixedBlocksScore);
    const ui64 totalBlocks = 2 * mergedBlocksPerRange + mixedBlocks;
    const ui64 expectedPercentage =
        (enabled || useByteCounters) &&
                totalBlocks * DefaultBlockSize >= startBytes
            ? mixedBlocks * 100 / totalBlocks
            : 0;
    UNIT_ASSERT_VALUES_EQUAL(expectedPercentage, mixedBlocksPercentage);
}

void CheckLoadOptimizingCompaction(ui32 maxBlobsPerUnit)
{
    auto config = DefaultConfig();
    config.SetHDDCompactionType(NProto::CT_LOAD);
    config.SetHDDMaxBlobsPerRange(999);
    config.SetAllocationUnitHDD(1);
    config.SetHDDMaxBlobsPerUnit(maxBlobsPerUnit);

    auto runtime = PrepareTestActorRuntime(config);

    TPartitionClient partition(*runtime);
    partition.WaitReady();

    for (ui32 i = 0; i < 512; ++i) {
        partition.WriteBlocks(TBlockRange32::WithLength(i * 2, 2));
        partition.Flush();
    }

    {
        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(512, stats.GetMixedBlobsCount());
    }

    ui64 compactionByBlobCount = -1;
    ui64 compactionByReadStats = -1;

    bool compactionRequestObserved = false;
    runtime->SetObserverFunc(
        [&](TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvPartitionPrivate::EvCompactionRequest: {
                    auto* msg =
                        event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                    if (msg->Mode == TEvPartitionPrivate::RangeCompaction) {
                        compactionRequestObserved = true;
                    }
                    break;
                }
                case TEvStatsService::EvVolumePartCounters: {
                    auto* msg =
                        event->Get<TEvStatsService::TEvVolumePartCounters>();

                    const auto& cc = msg->DiskCounters->Cumulative;
                    compactionByBlobCount =
                        cc.CompactionByBlobCountPerRange.Value;
                    compactionByReadStats = cc.CompactionByReadStats.Value;
                }
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });

    for (ui32 i = 0; i < 10; ++i) {
        for (ui32 j = 0; j < 10; ++j) {
            partition.ReadBlocks(TBlockRange32::WithLength(j, 101));
        }
    }

    // triggering EnqueueCompactionIfNeeded(...)
    partition.WriteBlocks(0, 0);
    partition.Flush();

    // wait for background operations completion
    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    UNIT_ASSERT(compactionRequestObserved);
    partition.SendToPipe(
        std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvStatsService::EvVolumePartCounters);
        runtime->DispatchEvents(options);
    }
    UNIT_ASSERT_EQUAL(0, compactionByBlobCount);
    UNIT_ASSERT_EQUAL(1, compactionByReadStats);
}

void DoShouldAutomaticallyRunGarbageOrIgnoringZeroedCompaction(
    bool ignoringZeroedCompactionEnabled)
{
    auto config = DefaultConfig();
    config.SetHDDCompactionType(NProto::CT_LOAD);
    config.SetV1GarbageCompactionEnabled(true);
    config.SetIgnoringZeroedCompactionEnabled(ignoringZeroedCompactionEnabled);
    config.SetCompactionGarbageThreshold(20);
    config.SetCompactionRangeGarbageThreshold(999999);

    auto runtime = PrepareTestActorRuntime(config, 2048);

    TPartitionClient partition(*runtime);
    partition.WaitReady();

    ui64 compactionByGarbageBlocksPerRange = 0;
    ui64 compactionByGarbageBlocksPerDisk = 0;
    ui64 compactionByIgnoringZeroedPerRange = 0;
    ui64 compactionByIgnoringZeroedPerDisk = 0;
    bool garbageCompactionRequestObserved = false;
    bool ignoringZeroedCompactionRequestObserved = false;
    runtime->SetEventFilter(
        [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvPartitionPrivate::EvCompactionRequest: {
                    auto* msg =
                        event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                    if (msg->Mode == TEvPartitionPrivate::GarbageCompaction) {
                        garbageCompactionRequestObserved = true;
                    } else if (
                        msg->Mode ==
                        TEvPartitionPrivate::IgnoringZeroedCompaction)
                    {
                        ignoringZeroedCompactionRequestObserved = true;
                    }
                    break;
                }
                case TEvStatsService::EvVolumePartCounters: {
                    auto* msg =
                        event->Get<TEvStatsService::TEvVolumePartCounters>();
                    const auto& cc = msg->DiskCounters->Cumulative;
                    compactionByIgnoringZeroedPerRange =
                        cc.CompactionByIgnoringZeroedPerRange.Value;
                    compactionByIgnoringZeroedPerDisk =
                        cc.CompactionByIgnoringZeroedPerDisk.Value;
                    compactionByGarbageBlocksPerRange =
                        cc.CompactionByGarbageBlocksPerRange.Value;
                    compactionByGarbageBlocksPerDisk =
                        cc.CompactionByGarbageBlocksPerDisk.Value;
                    break;
                }
            }
            return false;
        });

    auto checkCompactionRequests = [&](bool shouldObserveRequest)
    {
        if (!shouldObserveRequest) {
            UNIT_ASSERT(!garbageCompactionRequestObserved);
            UNIT_ASSERT(!ignoringZeroedCompactionRequestObserved);
        } else if (ignoringZeroedCompactionEnabled) {
            UNIT_ASSERT(!garbageCompactionRequestObserved);
            UNIT_ASSERT(ignoringZeroedCompactionRequestObserved);
        } else {
            UNIT_ASSERT(garbageCompactionRequestObserved);
            UNIT_ASSERT(!ignoringZeroedCompactionRequestObserved);
        }
        garbageCompactionRequestObserved = false;
        ignoringZeroedCompactionRequestObserved = false;
    };

    partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
    partition.WriteBlocks(TBlockRange32::WithLength(0, 301));

    // wait for background operations completion
    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    checkCompactionRequests(true);

    // marking range 0 as non-compacted
    partition.WriteBlocks(0);
    partition.Flush();

    partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1024, 1400));
    // 50% garbage
    partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1024, 1400));

    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    checkCompactionRequests(true);

    partition.CreateCheckpoint("c1");

    // writing lots of blocks into range 1
    partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024));
    partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024));

    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    // there is a checkpoint => zeroed compaction should not run
    checkCompactionRequests(false);

    partition.DeleteCheckpoint("c1");

    // triggering compaction attempt, block index does not matter
    partition.WriteBlocks(0);
    partition.Flush();

    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    checkCompactionRequests(true);

    partition.SendToPipe(
        std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvStatsService::EvVolumePartCounters);
        runtime->DispatchEvents(options);
    }

    UNIT_ASSERT_EQUAL(0, compactionByIgnoringZeroedPerRange);
    UNIT_ASSERT_EQUAL(0, compactionByGarbageBlocksPerRange);

    if (ignoringZeroedCompactionEnabled) {
        UNIT_ASSERT(compactionByIgnoringZeroedPerDisk > 0);
        UNIT_ASSERT_EQUAL(0, compactionByGarbageBlocksPerDisk);
    } else {
        UNIT_ASSERT_EQUAL(0, compactionByIgnoringZeroedPerDisk);
        UNIT_ASSERT(compactionByGarbageBlocksPerDisk > 0);
    }
}

void DoShouldAutomaticallyRunGarbageCompactionForSuperDirtyRanges(
    bool ignoringZeroedCompactionEnabled)
{
    auto config = DefaultConfig();
    config.SetHDDCompactionType(NProto::CT_LOAD);
    config.SetV1GarbageCompactionEnabled(true);
    config.SetIgnoringZeroedCompactionEnabled(ignoringZeroedCompactionEnabled);
    config.SetCompactionGarbageThreshold(999999);
    config.SetCompactionRangeGarbageThreshold(200);

    auto runtime = PrepareTestActorRuntime(config);

    TPartitionClient partition(*runtime);
    partition.WaitReady();

    ui64 compactionByGarbageBlocksPerRange = 0;
    ui64 compactionByGarbageBlocksPerDisk = 0;
    ui64 compactionByIgnoringZeroedPerRange = 0;
    ui64 compactionByIgnoringZeroedPerDisk = 0;
    bool garbageCompactionRequestObserved = false;
    bool ignoringZeroedCompactionRequestObserved = false;
    runtime->SetEventFilter(
        [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvPartitionPrivate::EvCompactionRequest: {
                    auto* msg =
                        event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                    if (msg->Mode == TEvPartitionPrivate::GarbageCompaction) {
                        garbageCompactionRequestObserved = true;
                    } else if (
                        msg->Mode ==
                        TEvPartitionPrivate::IgnoringZeroedCompaction)
                    {
                        ignoringZeroedCompactionRequestObserved = true;
                    }
                    break;
                }
                case TEvStatsService::EvVolumePartCounters: {
                    auto* msg =
                        event->Get<TEvStatsService::TEvVolumePartCounters>();
                    const auto& cc = msg->DiskCounters->Cumulative;
                    compactionByIgnoringZeroedPerRange =
                        cc.CompactionByIgnoringZeroedPerRange.Value;
                    compactionByIgnoringZeroedPerDisk =
                        cc.CompactionByIgnoringZeroedPerDisk.Value;
                    compactionByGarbageBlocksPerRange =
                        cc.CompactionByGarbageBlocksPerRange.Value;
                    compactionByGarbageBlocksPerDisk =
                        cc.CompactionByGarbageBlocksPerDisk.Value;
                    break;
                }
            }
            return false;
        });

    auto checkCompactionRequests = [&](bool shouldObserveRequest)
    {
        if (!shouldObserveRequest) {
            UNIT_ASSERT(!garbageCompactionRequestObserved);
            UNIT_ASSERT(!ignoringZeroedCompactionRequestObserved);
        } else if (ignoringZeroedCompactionEnabled) {
            UNIT_ASSERT(!garbageCompactionRequestObserved);
            UNIT_ASSERT(ignoringZeroedCompactionRequestObserved);
        } else {
            UNIT_ASSERT(garbageCompactionRequestObserved);
            UNIT_ASSERT(!ignoringZeroedCompactionRequestObserved);
        }
        garbageCompactionRequestObserved = false;
        ignoringZeroedCompactionRequestObserved = false;
    };

    partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
    partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
    partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

    // wait for background operations completion
    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    // garbage == used x2 => compaction
    checkCompactionRequests(true);

    garbageCompactionRequestObserved = false;

    partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

    // wait for background operations completion
    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    // garbage == used x1 => no compaction
    checkCompactionRequests(false);

    partition.CreateCheckpoint("c1");

    {
        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(1024, stats.GetCompactionGarbageScore());
    }

    partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

    // wait for background operations completion
    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    // garbage == used x2 => still no compaction because there is a
    // checkpoint
    checkCompactionRequests(false);

    partition.DeleteCheckpoint("c1");

    partition.WriteBlocks(TBlockRange32::MakeOneBlock(0));
    partition.Flush();

    // wait for background operations completion
    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    // garbage == used x2 => compaction
    checkCompactionRequests(true);

    garbageCompactionRequestObserved = false;

    partition.WriteBlocks(TBlockRange32::MakeOneBlock(0));
    partition.Flush();

    {
        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(1, stats.GetCompactionGarbageScore());
    }

    // wait for background operations completion
    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    // block count for this range should've been reset
    checkCompactionRequests(false);

    // a range with no used blocks
    partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
    partition.ZeroBlocks(TBlockRange32::WithLength(0, 1024));

    // wait for background operations completion
    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    // only garbage => compaction
    UNIT_ASSERT(garbageCompactionRequestObserved);
    UNIT_ASSERT(!ignoringZeroedCompactionRequestObserved);
    garbageCompactionRequestObserved = false;

    partition.WriteBlocks(TBlockRange32::WithLength(0, 101));
    partition.Flush();

    // wait for background operations completion
    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    // no garbage (99% of the blocks belong to a deletion marker) => no
    // compaction
    checkCompactionRequests(false);

    partition.WriteBlocks(TBlockRange32::WithLength(0, 101));
    partition.Flush();

    // wait for background operations completion
    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    // garbage == used => no compaction
    checkCompactionRequests(false);

    partition.WriteBlocks(TBlockRange32::WithLength(0, 101));
    partition.Flush();

    // wait for background operations completion
    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    // garbage == 2 * used => compaction
    checkCompactionRequests(true);

    partition.SendToPipe(
        std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvStatsService::EvVolumePartCounters);
        runtime->DispatchEvents(options);
    }

    UNIT_ASSERT_EQUAL(0, compactionByGarbageBlocksPerDisk);
    UNIT_ASSERT(compactionByGarbageBlocksPerRange > 0);
    UNIT_ASSERT_EQUAL(0, compactionByIgnoringZeroedPerDisk);

    if (ignoringZeroedCompactionEnabled) {
        UNIT_ASSERT(compactionByIgnoringZeroedPerRange > 0);
    } else {
        UNIT_ASSERT_EQUAL(0, compactionByIgnoringZeroedPerRange);
    }
}

void CheckInterpolatedGarbageCompactionExecTimePerSecond(
    ui32 throttleBelowFillPercentage,
    ui32 stopThrottlingAboveFillPercentage,
    ui32 fillPercentage,
    ui32 expectedExecTime)
{
    auto config = DefaultConfig();
    config.SetV1GarbageCompactionEnabled(true);
    config.SetEnableDynamicGarbageCompactionThrottling(true);
    config.SetThrottleGarbageCompactionBelowFillPercentage(
        throttleBelowFillPercentage);
    config.SetStopGarbageCompactionThrottlingAboveFillPercentage(
        stopThrottlingAboveFillPercentage);
    config.SetMinGarbageCompactionExecTimePerSecond(200);
    config.SetCompactionGarbageThreshold(20);
    config.SetCompactionRangeGarbageThreshold(20);
    config.SetMinCompactionDelay(0);
    config.SetMaxCompactionDelay(999'999'999);

    constexpr ui32 blockCount = 1000;
    auto runtime = PrepareTestActorRuntime(config, blockCount);

    TPartitionClient partition(*runtime);
    partition.WaitReady();

    bool garbageCompactionRequestObserved = false;
    ui64 garbageCompactionExecTimePerSecondLimit = 0;

    runtime->SetEventFilter(
        [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvPartitionPrivate::EvCompactionRequest: {
                    auto* msg =
                        event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                    if (msg->Mode == TEvPartitionPrivate::GarbageCompaction) {
                        garbageCompactionRequestObserved = true;
                    }
                    break;
                }
                case TEvStatsService::EvVolumePartCounters: {
                    auto* msg =
                        event->Get<TEvStatsService::TEvVolumePartCounters>();
                    garbageCompactionExecTimePerSecondLimit =
                        msg->DiskCounters->Simple
                            .GarbageCompactionExecTimePerSecondLimit.Value;
                    break;
                }
            }
            return false;
        });

    // Writing blocks in such a way that
    // `BlockCount / DiskSize` is exactly `fillPercentage` percent.
    const ui32 writeBlockCount = blockCount * fillPercentage / 200;
    UNIT_ASSERT(writeBlockCount < blockCount);
    const auto blockRange = TBlockRange32::WithLength(0, writeBlockCount);
    partition.WriteBlocks(blockRange);
    partition.WriteBlocks(blockRange);
    partition.Flush();

    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    UNIT_ASSERT(garbageCompactionRequestObserved);

    partition.SendToPipe(
        std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvStatsService::EvVolumePartCounters);
        runtime->DispatchEvents(options);
    }

    UNIT_ASSERT_VALUES_EQUAL(
        expectedExecTime,
        garbageCompactionExecTimePerSecondLimit);
}

void CheckCompactionOfRangesWithMostBlobs(bool batchCompaction)
{
    auto config = DefaultConfig();
    config.SetHDDCompactionType(NProto::CT_LOAD);
    config.SetAllocationUnitHDD(16);
    config.SetHDDMaxBlobsPerUnit(11);
    config.SetHDDMaxBlobsPerRange(100);
    config.SetBatchCompactionEnabled(batchCompaction);
    config.SetCompactionRangeCountPerRun(2);
    config.SetCompactionCountPerRunIncreasingThreshold(99999);
    config.SetCompactionCountPerRunDecreasingThreshold(0);
    config.SetCompactionGarbageThreshold(999999999);
    config.SetCompactionRangeGarbageThreshold(999999999);
    auto runtime = PrepareTestActorRuntime(config, 4 * 1024 * 1024);
    TPartitionClient partition(*runtime);
    partition.WaitReady();

    ui32 compactionRequests = 0;
    runtime->SetObserverFunc(
        [&](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvPartitionPrivate::EvCompactionRequest) {
                auto* msg =
                    event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                UNIT_ASSERT(
                    msg->Mode == TEvPartitionPrivate::BlobCountCompaction);
                ++compactionRequests;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });

    // Leave empty ranges in the group: their load score is higher than
    // that of written ranges without reads.
    for (ui32 i = 0; i < 6; ++i) {
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), i);
    }
    for (ui32 i = 1; i < 4; ++i) {
        for (ui32 j = 0; j < 2; ++j) {
            partition.WriteBlocks(
                TBlockRange32::WithLength(i * 1024 * 1024, 1024),
                j);
        }
    }
    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
    partition.Cleanup();
    UNIT_ASSERT_VALUES_EQUAL(1, compactionRequests);
    // The first range removes five blobs; a second range removes one more.
    UNIT_ASSERT_VALUES_EQUAL(
        batchCompaction ? 6 : 7,
        partition.StatPartition()->Record.GetStats().GetMergedBlobsCount());
}

void CheckIncrementAndDecrementCompactionPerRun(
    ui32 rangeCountPerRun,
    ui32 maxBlobsPerUnit,
    ui32 maxBlobsPerRange,
    ui32 diskGarbageThreshold,
    ui32 rangeGarbageThreshold,
    ui32 blobsCountAfterCompaction,
    ui32 increasingPercentageThreshold,
    ui32 decreasingPercentageThreshold,
    ui32 compactionRangeCountPerRun,
    ui32 maxCompactionRangeCountPerRun = 10)
{
    auto config = DefaultConfig();
    config.SetWriteBlobThreshold(1_MB);
    config.SetBatchCompactionEnabled(true);
    config.SetV1GarbageCompactionEnabled(true);
    config.SetCompactionGarbageThreshold(diskGarbageThreshold);
    config.SetCompactionRangeGarbageThreshold(rangeGarbageThreshold);
    config.SetCompactionRangeCountPerRun(rangeCountPerRun);
    config.SetCompactionCountPerRunIncreasingThreshold(
        increasingPercentageThreshold);
    config.SetCompactionCountPerRunDecreasingThreshold(
        decreasingPercentageThreshold);
    config.SetHDDMaxBlobsPerUnit(maxBlobsPerUnit);
    config.SetSSDMaxBlobsPerUnit(maxBlobsPerUnit);
    config.SetAllocationUnitSSD(16);
    config.SetAllocationUnitHDD(16);
    config.SetMaxCompactionRangeCountPerRun(maxCompactionRangeCountPerRun);
    config.SetCompactionCountPerRunChangingPeriod(1);
    config.SetSSDMaxBlobsPerRange(maxBlobsPerRange);
    config.SetHDDMaxBlobsPerRange(maxBlobsPerRange);

    auto runtime = PrepareTestActorRuntime(config, 4 * 1024 * 1024);
    runtime->AdvanceCurrentTime(TDuration::Seconds(5));

    TPartitionClient partition(*runtime);
    partition.WaitReady();

    const auto blockRange1 = TBlockRange32::WithLength(0, 1024);
    const auto blockRange2 = TBlockRange32::WithLength(1024 * 1024, 1024);
    const auto blockRange3 = TBlockRange32::WithLength(2 * 1024 * 1024, 1024);

    bool compactionFilter = true;
    ui32 resultCompactionRangeCountPerRun = 0;

    runtime->SetEventFilter(
        [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvPartitionPrivate::EvCompactionRequest: {
                    if (compactionFilter) {
                        return true;
                    }
                    compactionFilter = true;
                    break;
                }
                case TEvStatsService::EvVolumePartCounters: {
                    auto* msg =
                        event->Get<TEvStatsService::TEvVolumePartCounters>();
                    const auto& cc = msg->DiskCounters->Simple;
                    resultCompactionRangeCountPerRun =
                        cc.CompactionRangeCountPerRun.Value;
                }
            }
            return false;
        });

    partition.WriteBlocks(blockRange1, 1);
    partition.WriteBlocks(blockRange2, 4);
    partition.WriteBlocks(blockRange3, 7);

    partition.WriteBlocks(blockRange1, 2);
    partition.WriteBlocks(blockRange1, 3);
    partition.WriteBlocks(blockRange2, 5);
    partition.WriteBlocks(blockRange2, 6);
    partition.WriteBlocks(blockRange3, 8);
    partition.WriteBlocks(blockRange3, 9);
    partition.WriteBlocks(blockRange3, 10);
    // wait for background operations completion
    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    compactionFilter = false;

    partition.Compaction();
    partition.Cleanup();

    {
        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(
            blobsCountAfterCompaction,
            stats.GetMergedBlobsCount());
    }

    partition.SendToPipe(
        std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvStatsService::EvVolumePartCounters);
        runtime->DispatchEvents(options);
        UNIT_ASSERT_VALUES_EQUAL(
            compactionRangeCountPerRun,
            resultCompactionRangeCountPerRun);
    }
}

void DoShouldRunCompactionWhenUsedBlocksCountGreaterThanBlockCount(
    bool ignoringZeroedCompactionEnabled)
{
    auto config = DefaultConfig();
    config.SetFreshChannelWriteRequestsEnabled(true);
    config.SetFreshChannelZeroRequestsEnabled(true);
    config.SetWriteBlobThresholdSSD(1_MB);
    config.SetCleanupThreshold(1000);
    config.SetSSDMaxBlobsPerRange(1000);
    config.SetFlushThresholdSSD(1000_MB);
    config.SetV1GarbageCompactionEnabled(true);
    config.SetIgnoringZeroedCompactionEnabled(ignoringZeroedCompactionEnabled);
    config.SetCompactionGarbageThreshold(999999);
    config.SetCompactionRangeGarbageThreshold(20);

    auto runtime = PrepareTestActorRuntime(
        config,
        2048,
        {},
        {.MediaKind = NCloud::NProto::STORAGE_MEDIA_SSD});

    TPartitionClient partition(*runtime);
    partition.WaitReady();

    const auto expectedCompactionMode =
        ignoringZeroedCompactionEnabled
            ? TEvPartitionPrivate::IgnoringZeroedCompaction
            : TEvPartitionPrivate::GarbageCompaction;

    TAutoPtr<IEventHandle> compactionRequest;

    runtime->SetEventFilter(
        [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvPartitionPrivate::EvCompactionRequest) {
                auto* msg =
                    event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                if (msg->Mode == expectedCompactionMode && !compactionRequest) {
                    compactionRequest = event.Release();
                    return true;
                }
            }
            return false;
        });

    const auto assertUsedBlocksCountGreaterThanBlockCount = [&]
    {
        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        const ui64 blockCount =
            stats.GetMixedBlocksCount() + stats.GetMergedBlocksCount();
        UNIT_ASSERT_C(
            stats.GetUsedBlocksCount() > blockCount,
            "UsedBlocksCount=" << stats.GetUsedBlocksCount()
                               << ", blockCount=" << blockCount);
    };

    partition.WriteBlocks(TBlockRange32::WithLength(0, 512));
    // Zero blocks should be written to fresh. They will not unset used
    // blocks until flush.
    partition.ZeroBlocks(TBlockRange32::WithLength(0, 128));
    partition.ZeroBlocks(TBlockRange32::WithLength(128, 128));
    partition.ZeroBlocks(TBlockRange32::WithLength(256, 128));

    TCompactionOptions options;
    options.set(ToBit(ECompactionOption::Full));
    options.set(ToBit(ECompactionOption::Forced));
    partition.Compaction(0, options);
    partition.Cleanup();

    assertUsedBlocksCountGreaterThanBlockCount();
    {
        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(128, stats.GetMergedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(512, stats.GetUsedBlocksCount());
    }

    partition.WriteBlocks(TBlockRange32::WithLength(1024, 512));
    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
    UNIT_ASSERT(!compactionRequest);
    assertUsedBlocksCountGreaterThanBlockCount();

    partition.WriteBlocks(TBlockRange32::WithLength(1024, 256));
    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
    // Too many garbage in the range [1024, 2048), compaction should be
    // triggered.
    UNIT_ASSERT(compactionRequest);

    assertUsedBlocksCountGreaterThanBlockCount();
    {
        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(128 + 768, stats.GetMergedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(512 + 512, stats.GetUsedBlocksCount());
    }

    runtime->Send(compactionRequest.Release());
    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
    partition.Cleanup();

    assertUsedBlocksCountGreaterThanBlockCount();
    {
        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(128 + 512, stats.GetMergedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(512 + 512, stats.GetUsedBlocksCount());
    }

    partition.Flush();
    runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    {
        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(128 + 512, stats.GetMergedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(128 + 512, stats.GetUsedBlocksCount());
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionCompactionPolicyTest)
{
    Y_UNIT_TEST(ShouldAutomaticallyRunCompaction)
    {
        static constexpr ui32 compactionThreshold = 4;

        auto config = DefaultConfig();
        config.SetSSDMaxBlobsPerRange(compactionThreshold);
        config.SetHDDMaxBlobsPerRange(compactionThreshold);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByBlobCount = 0;
        ui64 compactionByReadStats = 0;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event
                                ->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByBlobCount =
                            cc.CompactionByBlobCountPerRange.Value;
                        compactionByReadStats = cc.CompactionByReadStats.Value;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

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
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        partition.WriteBlocks(0, 0);
        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold,
                stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_EQUAL(1, compactionByBlobCount);
        UNIT_ASSERT_EQUAL(0, compactionByReadStats);
    }

    Y_UNIT_TEST(ShouldAutomaticallyRunCompactionForManyMixedBlocks)
    {
        auto config = DefaultConfig(1_MB);
        config.SetMixedBlocksCountCompactionEnabledHDD(true);
        config.SetMixedBlocksCompactionMinUsedBytesHDD(
            3 * DefaultBlockSize + 1);
        config.SetMixedBytesCountCompactionThresholdHDD(2 * DefaultBlockSize);
        config.SetWriteBlobThreshold(1_MB);
        config.SetSSDMaxBlobsPerRange(100);
        config.SetHDDMaxBlobsPerRange(100);
        config.SetCompactionMergedBlobThresholdHDD(1_MB);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        bool compactionRequestObserved = false;
        ui64 compactionByMixedBlockCount = 0;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        auto* msg = event->Get<
                            TEvPartitionPrivate::TEvCompactionRequest>();
                        const auto expectedMode = static_cast<ui32>(
                            TEvPartitionPrivate::MixedBlocksCountCompaction);
                        UNIT_ASSERT_VALUES_EQUAL(
                            expectedMode,
                            static_cast<ui32>(msg->Mode));
                        UNIT_ASSERT(msg->CompactionOptions.test(
                            ToBit(ECompactionOption::
                                      ForceMixedBlocksCountCompaction)));
                        compactionRequestObserved = true;
                        break;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event
                                ->Get<TEvStatsService::TEvVolumePartCounters>();
                        compactionByMixedBlockCount =
                            msg->DiskCounters->Cumulative
                                .CompactionByMixedBlockCountPerRange.Value;
                        break;
                    }
                }
                return false;
            });

        partition.WriteBlocks(TBlockRange32::WithLength(0, 3), 1);
        partition.Flush();
        UNIT_ASSERT(!compactionRequestObserved);

        partition.WriteBlocks(3, 2);
        partition.Flush();

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        UNIT_ASSERT(compactionRequestObserved);

        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlocksCount());
        }

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(1, compactionByMixedBlockCount);
    }

    Y_UNIT_TEST(ShouldTriggerMixedCompactionWithCheckpointData)
    {
        for (bool useByteCounters: {false, true}) {
            for (bool withoutData: {false, true}) {
                for (bool perDisk: {false, true}) {
                    auto config = DefaultConfig(1_MB);
                    config.SetMixedBlocksCountCompactionEnabledHDD(true);
                    config.SetMixedBlocksCompactionByteCountersEnabled(
                        useByteCounters);
                    config.SetExtendedBlobCountersEnabled(true);
                    config.SetMixedBytesCountCompactionThresholdHDD(
                        perDisk ? 1_MB : 4 * DefaultBlockSize);
                    config.SetMixedBlocksCompactionThresholdPercentageHDD(
                        perDisk ? 50 : 0);
                    config.SetMixedBlocksCompactionMinStoredBytesHDD(0);
                    config.SetMixedBlocksCompactionMinUsedBytesHDD(
                        4 * DefaultBlockSize);
                    config.SetWriteBlobThreshold(4 * DefaultBlockSize);
                    config.SetHDDMaxBlobsPerRange(100);
                    config.SetCompactionMergedBlobThresholdHDD(1);

                    TTestPartitionInfo partitionInfo;
                    partitionInfo.MixedChannelCount = 1;
                    auto runtime = PrepareTestActorRuntime(
                        config,
                        MaxBlocksCount,
                        6,
                        partitionInfo);
                    TPartitionClient partition(*runtime);
                    partition.WaitReady();

                    bool compactionRequestObserved = false;
                    runtime->SetEventFilter(
                        [&](TTestActorRuntimeBase&,
                            TAutoPtr<IEventHandle>& event)
                        {
                            if (event->GetTypeRewrite() ==
                                TEvPartitionPrivate::EvCompactionRequest)
                            {
                                const auto* msg =
                                    event->Get<TEvPartitionPrivate::
                                                   TEvCompactionRequest>();
                                UNIT_ASSERT_VALUES_EQUAL(
                                    static_cast<ui32>(
                                        TEvPartitionPrivate::
                                            MixedBlocksCountCompaction),
                                    static_cast<ui32>(msg->Mode));
                                compactionRequestObserved = true;
                                return true;
                            }
                            return false;
                        });

                    partition.WriteBlocks(TBlockRange32::WithLength(0, 4), 1);
                    partition.CreateCheckpoint("checkpoint", "", withoutData);
                    partition.WriteBlocks(TBlockRange32::WithLength(0, 2), 2);
                    partition.Flush();
                    UNIT_ASSERT(!compactionRequestObserved);

                    // Four mixed blocks reach both the range threshold and
                    // 50% of the stored bytes (four merged + four mixed).
                    partition.WriteBlocks(TBlockRange32::WithLength(2, 2), 3);
                    partition.Flush();
                    runtime->DispatchEvents(
                        TDispatchOptions(),
                        TDuration::Seconds(1));
                    UNIT_ASSERT(compactionRequestObserved);
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldExcludeQueuedMixedBlobsAfterCompactionAndReboot)
    {
        for (bool extendedCounters: {false, true}) {
            for (bool hasMixedChannel: {false, true}) {
                // Different index and channel classifications require extended
                // counters for accurate percentage-based compaction.
                if (!extendedCounters && !hasMixedChannel) {
                    continue;
                }

                auto config = DefaultConfig(1_MB);
                config.SetMixedBlocksCountCompactionEnabledHDD(true);
                config.SetMixedBlocksCompactionByteCountersEnabled(true);
                config.SetExtendedBlobCountersEnabled(extendedCounters);
                config.SetMixedBytesCountCompactionThresholdHDD(1_MB);
                config.SetMixedBlocksCompactionThresholdPercentageHDD(50);
                config.SetMixedBlocksCompactionMinStoredBytesHDD(0);
                config.SetMixedBlocksCompactionMinUsedBytesHDD(
                    4 * DefaultBlockSize);
                config.SetWriteBlobThreshold(4 * DefaultBlockSize);
                config.SetHDDMaxBlobsPerRange(100);
                config.SetCompactionMergedBlobThresholdHDD(1);

                TTestPartitionInfo partitionInfo;
                partitionInfo.MixedChannelCount = hasMixedChannel ? 1 : 0;
                auto runtime = PrepareTestActorRuntime(
                    config,
                    MaxBlocksCount,
                    hasMixedChannel ? 6 : 5,
                    partitionInfo);
                TPartitionClient partition(*runtime);
                partition.WaitReady();

                ui32 compactionRequests = 0;
                ui64 mixedPercentage = Max<ui64>();
                runtime->SetEventFilter(
                    [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
                    {
                        if (event->GetTypeRewrite() ==
                            TEvPartitionPrivate::EvCompactionRequest)
                        {
                            ++compactionRequests;
                        } else if (
                            event->GetTypeRewrite() ==
                            TEvStatsService::EvVolumePartCounters)
                        {
                            mixedPercentage =
                                event
                                    ->Get<TEvStatsService::
                                              TEvVolumePartCounters>()
                                    ->DiskCounters->Simple
                                    .CompactionMixedBlocksPercentage.Value;
                        }
                        return false;
                    });

                const auto updateCounters = [&]
                {
                    partition.SendToPipe(
                        std::make_unique<
                            TEvPartitionPrivate::TEvUpdateCounters>());
                    TDispatchOptions options;
                    options.FinalEvents.emplace_back(
                        TEvStatsService::EvVolumePartCounters);
                    runtime->DispatchEvents(options);
                };

                partition.WriteBlocks(TBlockRange32::WithLength(0, 4), 1);
                // Keep overwritten blobs in the cleanup queue.
                partition.CreateCheckpoint("checkpoint");
                partition.WriteBlocks(TBlockRange32::WithLength(0, 2), 2);
                partition.Flush();
                partition.WriteBlocks(TBlockRange32::WithLength(2, 2), 3);
                partition.Flush();
                runtime->DispatchEvents(
                    TDispatchOptions(),
                    TDuration::Seconds(1));
                UNIT_ASSERT_VALUES_EQUAL(
                    hasMixedChannel ? 1 : 0,
                    compactionRequests);
                if (!hasMixedChannel) {
                    // Mixed-index blobs on merged channels must not trigger the
                    // percentage-based compaction. Compact manually to test
                    // that queued blobs keep the same classification after
                    // reboot.
                    partition.Compaction(0);
                }

                updateCounters();
                UNIT_ASSERT_VALUES_EQUAL(0, mixedPercentage);
                const auto stats = partition.StatPartition()->Record.GetStats();
                UNIT_ASSERT_VALUES_EQUAL(
                    extendedCounters && !hasMixedChannel ? 0 : 4,
                    stats.GetMixedBlocksCount());

                partition.RebootTablet();
                partition.WaitReady();
                updateCounters();
                UNIT_ASSERT_VALUES_EQUAL(0, mixedPercentage);
                UNIT_ASSERT_VALUES_EQUAL(1, compactionRequests);

                partition.WriteBlocks(TBlockRange32::WithLength(0, 2), 4);
                partition.Flush();
                updateCounters();
                UNIT_ASSERT_VALUES_EQUAL(
                    hasMixedChannel ? 33 : 0,
                    mixedPercentage);
                UNIT_ASSERT_VALUES_EQUAL(1, compactionRequests);
            }
        }
    }

    Y_UNIT_TEST(ShouldPrioritizeMixedBlocksCompactionOverGarbageCompaction)
    {
        for (bool ignoringZeroed: {false, true}) {
            for (bool mixedCompaction: {false, true}) {
                auto config = DefaultConfig(1_MB);
                config.SetV1GarbageCompactionEnabled(true);
                config.SetIgnoringZeroedCompactionEnabled(ignoringZeroed);
                config.SetCompactionGarbageThreshold(100);
                config.SetCompactionRangeGarbageThreshold(100);
                config.SetMixedBlocksCountCompactionEnabledHDD(mixedCompaction);
                config.SetMixedBytesCountCompactionThresholdHDD(
                    8 * DefaultBlockSize);
                config.SetMixedBlocksCompactionMinUsedBytesHDD(
                    8 * DefaultBlockSize);
                config.SetWriteBlobThreshold(8 * DefaultBlockSize);
                config.SetCompactionMergedBlobThresholdHDD(1);

                auto runtime = PrepareTestActorRuntime(config);
                TPartitionClient partition(*runtime);
                partition.WaitReady();

                std::optional<TEvPartitionPrivate::ECompactionMode> mode;
                runtime->SetEventFilter(
                    [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
                    {
                        if (event->GetTypeRewrite() ==
                            TEvPartitionPrivate::EvCompactionRequest)
                        {
                            mode = event
                                       ->Get<TEvPartitionPrivate::
                                                 TEvCompactionRequest>()
                                       ->Mode;
                            return true;
                        }
                        return false;
                    });

                partition.WriteBlocks(TBlockRange32::WithLength(0, 8), 1);
                partition.WriteBlocks(TBlockRange32::WithLength(0, 4), 2);
                partition.Flush();
                UNIT_ASSERT(!mode);
                // Eight mixed blocks and 100% garbage become eligible together.
                partition.WriteBlocks(TBlockRange32::WithLength(0, 4), 3);
                partition.Flush();
                runtime->DispatchEvents(
                    TDispatchOptions(),
                    TDuration::Seconds(1));
                UNIT_ASSERT(mode);
                const auto expected =
                    mixedCompaction
                        ? TEvPartitionPrivate::MixedBlocksCountCompaction
                        : (ignoringZeroed
                               ? TEvPartitionPrivate::IgnoringZeroedCompaction
                               : TEvPartitionPrivate::GarbageCompaction);
                UNIT_ASSERT_VALUES_EQUAL(
                    static_cast<ui32>(expected),
                    static_cast<ui32>(*mode));
            }
        }
    }

    Y_UNIT_TEST(ShouldProcessMultipleRangesUponMixedBlocksCountCompaction)
    {
        auto config = DefaultConfig();
        config.SetBatchCompactionEnabled(true);
        config.SetGarbageCompactionRangeCountPerRun(2);
        config.SetMixedBlocksCountCompactionRangeCountPerRun(3);
        config.SetMixedBlocksCountCompactionEnabledHDD(true);
        config.SetMixedBytesCountCompactionThresholdHDD(1);
        config.SetWriteBlobThreshold(1_MB);
        config.SetHDDMaxBlobsPerRange(100);
        config.SetCompactionMergedBlobThresholdHDD(1);

        auto runtime = PrepareTestActorRuntime(config, MaxPartitionBlocksCount);
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // Keep each range large enough for mixed-block compaction after the
        // seed blob is compacted and a sparse mixed overwrite is added.
        const ui32 writeBlobBlockCount =
            config.GetWriteBlobThreshold() / DefaultBlockSize;
        for (ui32 i = 0; i < 3; ++i) {
            partition.WriteBlocks(
                TBlockRange32::WithLength(i * 1024 * 1024, writeBlobBlockCount),
                i + 1);
            runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        }

        TAutoPtr<IEventHandle> compactionRequest;
        bool stealCompactionRequest = true;
        ui32 compactedRangeCount = 0;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvCompactionRequest)
                {
                    auto* msg =
                        event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                    if (msg->Mode ==
                            TEvPartitionPrivate::MixedBlocksCountCompaction &&
                        stealCompactionRequest)
                    {
                        stealCompactionRequest = false;
                        compactionRequest = event.Release();
                        return true;
                    }
                } else if (
                    event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvCompactionCompleted)
                {
                    const auto* msg = event->Get<
                        TEvPartitionPrivate::TEvCompactionCompleted>();
                    compactedRangeCount = msg->AffectedRanges.size();
                }
                return false;
            });

        for (ui32 i = 0; i < 3; ++i) {
            partition.WriteBlocks(i * 1024 * 1024, i + 1);
            partition.Flush();
        }

        UNIT_ASSERT(compactionRequest);
        runtime->Send(compactionRequest.Release());
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(3, compactedRangeCount);
    }

    Y_UNIT_TEST(ShouldCompactByMixedBytesPercentagePerDisk)
    {
        for (const auto mediaKind:
             {NCloud::NProto::STORAGE_MEDIA_HDD,
              NCloud::NProto::STORAGE_MEDIA_SSD})
        {
            for (bool reboot: {false, true}) {
                CheckMixedBlocksPercentageCompactionPerDisk(
                    mediaKind,
                    1,
                    0,
                    21,
                    true,
                    reboot,
                    2 * MaxBlocksCount,
                    MaxBlocksCount,
                    true);
            }
            CheckMixedBlocksPercentageCompactionPerDisk(
                mediaKind,
                1,
                (2 * MaxBlocksCount + 22) * DefaultBlockSize,
                22,
                true,
                false,
                2 * MaxBlocksCount,
                MaxBlocksCount,
                true);
            CheckMixedBlocksPercentageCompactionPerDisk(
                mediaKind,
                1,
                1_GB,
                0,
                true,
                false,
                2 * MaxBlocksCount,
                MaxBlocksCount,
                true);
            CheckMixedBlocksPercentageCompactionPerDisk(
                mediaKind,
                0,
                0,
                0,
                true,
                false,
                2 * MaxBlocksCount,
                MaxBlocksCount,
                true);
        }
    }

    Y_UNIT_TEST(ShouldCompactByMixedBlocksPercentagePerDisk)
    {
        for (const auto mediaKind:
             {NCloud::NProto::STORAGE_MEDIA_HDD,
              NCloud::NProto::STORAGE_MEDIA_SSD})
        {
            CheckMixedBlocksPercentageCompactionPerDisk(mediaKind, 1, 0, 21);
            // 20 mixed blocks out of 2000 stored blocks reach exactly 1%.
            CheckMixedBlocksPercentageCompactionPerDisk(
                mediaKind,
                1,
                0,
                20,
                true,
                false,
                2 * MaxBlocksCount,
                990);
            CheckMixedBlocksPercentageCompactionPerDisk(
                mediaKind,
                1,
                0,
                21,
                true,
                true);
            // Mixed blocks in a third, ineligible range do not advance the
            // trigger.
            CheckMixedBlocksPercentageCompactionPerDisk(
                mediaKind,
                1,
                0,
                21,
                true,
                false,
                4 * MaxBlocksCount);
            CheckMixedBlocksPercentageCompactionPerDisk(mediaKind, 2, 0, 0);
            CheckMixedBlocksPercentageCompactionPerDisk(mediaKind, 0, 0, 0);
            CheckMixedBlocksPercentageCompactionPerDisk(
                mediaKind,
                1,
                0,
                0,
                false);
        }
    }

    Y_UNIT_TEST(ShouldWaitForMinimumStoredBytesBeforeMixedBlocksCompaction)
    {
        for (const auto mediaKind:
             {NCloud::NProto::STORAGE_MEDIA_HDD,
              NCloud::NProto::STORAGE_MEDIA_SSD})
        {
            // Start exactly at the byte threshold even if the percentage was
            // already high enough one block earlier.
            CheckMixedBlocksPercentageCompactionPerDisk(
                mediaKind,
                1,
                (2 * MaxBlocksCount + 22) * DefaultBlockSize,
                22);
            CheckMixedBlocksPercentageCompactionPerDisk(mediaKind, 1, 1_GB, 0);
        }
    }

    Y_UNIT_TEST(ShouldEnableMixedBlocksCountCompactionByMediaKind)
    {
        const auto isCompactionTriggered =
            [](NCloud::NProto::EStorageMediaKind mediaKind,
               bool enabledHDD,
               bool enabledSSD,
               ui32 thresholdHDD,
               ui32 thresholdSSD)
        {
            auto config = DefaultConfig(1_MB);
            config.SetMixedBlocksCountCompactionEnabledHDD(enabledHDD);
            config.SetMixedBlocksCountCompactionEnabledSSD(enabledSSD);
            config.SetMixedBytesCountCompactionThresholdHDD(
                thresholdHDD * DefaultBlockSize);
            config.SetMixedBytesCountCompactionThresholdSSD(
                thresholdSSD * DefaultBlockSize);
            config.SetMixedBlocksCompactionMinUsedBytesHDD(
                4 * DefaultBlockSize);
            config.SetMixedBlocksCompactionMinUsedBytesSSD(0);
            config.SetSSDMaxBlobsPerRange(100);
            config.SetHDDMaxBlobsPerRange(100);

            TTestPartitionInfo partitionInfo;
            partitionInfo.MediaKind = mediaKind;
            auto runtime =
                PrepareTestActorRuntime(config, 1024, {}, partitionInfo);

            TPartitionClient partition(*runtime);
            partition.WaitReady();

            bool compactionRequestObserved = false;
            runtime->SetEventFilter(
                [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
                {
                    if (event->GetTypeRewrite() ==
                        TEvPartitionPrivate::EvCompactionRequest)
                    {
                        const auto* msg = event->Get<
                            TEvPartitionPrivate::TEvCompactionRequest>();
                        if (msg->Mode ==
                            TEvPartitionPrivate::MixedBlocksCountCompaction)
                        {
                            compactionRequestObserved = true;
                        }
                    }
                    return false;
                });

            partition.WriteBlocks(TBlockRange32::WithLength(0, 2), 1);
            partition.Flush();
            partition.WriteBlocks(TBlockRange32::WithLength(2, 2), 2);
            partition.Flush();
            runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

            return compactionRequestObserved;
        };

        UNIT_ASSERT(!isCompactionTriggered(
            NCloud::NProto::STORAGE_MEDIA_DEFAULT,
            false,
            true,
            4,
            4));
        UNIT_ASSERT(!isCompactionTriggered(
            NCloud::NProto::STORAGE_MEDIA_SSD,
            true,
            false,
            4,
            4));
        UNIT_ASSERT(!isCompactionTriggered(
            NCloud::NProto::STORAGE_MEDIA_DEFAULT,
            true,
            false,
            5,
            4));
        UNIT_ASSERT(isCompactionTriggered(
            NCloud::NProto::STORAGE_MEDIA_SSD,
            false,
            true,
            5,
            4));
    }

    Y_UNIT_TEST(ShouldAutomaticallyRunCompactionForDeletionMarkers)
    {
        static constexpr ui32 compactionThreshold = 4;

        auto config = DefaultConfig();
        config.SetSSDMaxBlobsPerRange(compactionThreshold);
        config.SetHDDMaxBlobsPerRange(compactionThreshold);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByBlobCount = 0;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvStatsService::EvVolumePartCounters)
                {
                    const auto* msg =
                        event->Get<TEvStatsService::TEvVolumePartCounters>();
                    compactionByBlobCount =
                        msg->DiskCounters->Cumulative
                            .CompactionByBlobCountPerRange.Value;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        for (size_t i = 1; i < compactionThreshold; ++i) {
            partition.ZeroBlocks(TBlockRange32::WithLength(0, 1024));
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold - 1,
                stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold - 1,
                stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
        }

        partition.ZeroBlocks(TBlockRange32::WithLength(0, 1024));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold + 1,
                stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold + 1,
                stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
        }

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_EQUAL(1, compactionByBlobCount);
    }

    Y_UNIT_TEST(ShouldRespectCompactionDelay)
    {
        static constexpr ui32 compactionThreshold = 4;

        auto config = DefaultConfig();
        config.SetSSDMaxBlobsPerRange(compactionThreshold);
        config.SetHDDMaxBlobsPerRange(compactionThreshold);
        config.SetMinCompactionDelay(10000);
        config.SetMaxCompactionDelay(10000);
        config.SetCompactionScoreHistorySize(1);

        auto runtime = PrepareTestActorRuntime(config);
        runtime->AdvanceCurrentTime(TDuration::Seconds(10));

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (size_t i = 1; i < compactionThreshold + 1; ++i) {
            partition.WriteBlocks(i, i);
            partition.Flush();
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold,
                stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        runtime->AdvanceCurrentTime(TDuration::Seconds(10));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold,
                stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldCalculateCompactionAndCleanupDelays)
    {
        static constexpr ui32 compactionThreshold = 4;
        static constexpr ui32 cleanupThreshold = 10;

        auto config = DefaultConfig();
        config.SetSSDMaxBlobsPerRange(compactionThreshold);
        config.SetHDDMaxBlobsPerRange(compactionThreshold);
        config.SetCleanupThreshold(cleanupThreshold);
        config.SetMinCompactionDelay(0);
        config.SetMaxCompactionDelay(999'999'999);
        config.SetMinCleanupDelay(0);
        config.SetMaxCleanupDelay(999'999'999);
        config.SetMaxCompactionExecTimePerSecond(1);
        config.SetMaxCleanupExecTimePerSecond(1);

        auto runtime = PrepareTestActorRuntime(config);
        runtime->AdvanceCurrentTime(TDuration::Seconds(10));

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        // initializing compaction exec time
        partition.Compaction();

        for (size_t i = 0; i < compactionThreshold - 1; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        }

        TDuration delay;
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold + 1,
                stats.GetMergedBlobsCount());
            delay = TDuration::MilliSeconds(stats.GetCompactionDelay());
            UNIT_ASSERT_VALUES_UNEQUAL(0, delay.MicroSeconds());
        }

        runtime->AdvanceCurrentTime(delay);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold + 2,
                stats.GetMergedBlobsCount());
        }

        // initializing cleanup exec time
        partition.Cleanup();

        // generating enough dirty blobs for automatic cleanup
        for (ui32 i = 0; i < cleanupThreshold - 1; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        }
        partition.Compaction();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT(stats.GetMergedBlobsCount() >= cleanupThreshold);
            delay = TDuration::MilliSeconds(stats.GetCleanupDelay());
            UNIT_ASSERT_VALUES_UNEQUAL(0, delay.MicroSeconds());
        }

        runtime->AdvanceCurrentTime(delay);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldCompactAndCleanupWithoutDelayUponScoreOverflow)
    {
        static constexpr ui32 compactionThreshold = 4;
        static constexpr ui32 cleanupThreshold = 10;

        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetHDDMaxBlobsPerRange(compactionThreshold - 1);
        config.SetCleanupThreshold(cleanupThreshold);
        config.SetMinCompactionDelay(999'999'999);
        config.SetMaxCompactionDelay(999'999'999);
        config.SetMinCleanupDelay(999'999'999);
        config.SetMaxCleanupDelay(999'999'999);
        config.SetCompactionScoreLimitForThrottling(compactionThreshold);
        config.SetCleanupQueueBytesLimitForThrottling(16_MB);

        auto runtime = PrepareTestActorRuntime(config);
        runtime->AdvanceCurrentTime(TDuration::Seconds(10));

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (size_t i = 0; i < compactionThreshold; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        }
        // initializing compaction exec time
        partition.Compaction();
        // initializing cleanup exec time
        partition.Cleanup();

        for (size_t i = 0; i < compactionThreshold - 1; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold + 1,
                stats.GetMergedBlobsCount());
        }

        // generating enough dirty blobs for automatic cleanup
        for (ui32 i = 0; i < cleanupThreshold - compactionThreshold - 1; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        }
        partition.Compaction(0);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldAutomaticallyRunLoadOptimizingCompaction)
    {
        CheckLoadOptimizingCompaction(0);
    }

    Y_UNIT_TEST(ShouldKeepReadOptimizationCompactionWithBlobBudget)
    {
        CheckLoadOptimizingCompaction(1000000);
    }

    Y_UNIT_TEST(ShouldPrioritizeIgnoringZeroedCompactionOverGarbageCompaction)
    {
        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetIgnoringZeroedCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(20);
        config.SetCompactionRangeGarbageThreshold(200);

        auto runtime = PrepareTestActorRuntime(config, 3072);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByGarbageBlocksPerRange = 0;
        ui64 compactionByGarbageBlocksPerDisk = 0;
        ui64 compactionByIgnoringZeroedPerRange = 0;
        ui64 compactionByIgnoringZeroedPerDisk = 0;

        bool garbageCompactionRequestObserved = false;
        bool ignoringZeroedCompactionRequestObserved = false;

        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        auto* msg = event->Get<
                            TEvPartitionPrivate::TEvCompactionRequest>();
                        if (msg->Mode == TEvPartitionPrivate::GarbageCompaction)
                        {
                            garbageCompactionRequestObserved = true;
                        } else if (
                            msg->Mode ==
                            TEvPartitionPrivate::IgnoringZeroedCompaction)
                        {
                            ignoringZeroedCompactionRequestObserved = true;
                        }
                        break;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event
                                ->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByIgnoringZeroedPerRange =
                            cc.CompactionByIgnoringZeroedPerRange.Value;
                        compactionByIgnoringZeroedPerDisk =
                            cc.CompactionByIgnoringZeroedPerDisk.Value;
                        compactionByGarbageBlocksPerRange =
                            cc.CompactionByGarbageBlocksPerRange.Value;
                        compactionByGarbageBlocksPerDisk =
                            cc.CompactionByGarbageBlocksPerDisk.Value;
                        break;
                    }
                }
                return false;
            });

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024));
        partition.WriteBlocks(TBlockRange32::WithLength(2048, 1024));

        partition.WriteBlocks(TBlockRange32::WithLength(2048, 512));
        //  range 0    range 1    range 2
        // |xxxxxxxxxx|xxxxxxxxxx|xxxxxxxxxx|
        // |          |          |     xxxxx|
        //
        // top range garbage (range 2):    50% < 200%
        // disk garbage:                   16.7% < 20%
        //
        // Must not compact.

        // Wait for background operations completion.
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(!garbageCompactionRequestObserved);
        UNIT_ASSERT(!ignoringZeroedCompactionRequestObserved);

        partition.ZeroBlocks(TBlockRange32::WithLength(0, 1024 + 512));
        //  range 0    range 1    range 2
        // |0000000000|00000xxxxx|xxxxxxxxxx|
        // |xxxxxxxxxx|xxxxx     |     xxxxx|
        //
        // top range garbage ignoring zeroed (range 2):    50%  < 200%
        // disk garbage ignoring zeroed:                   17%  < 20%
        // top range garbage (range 0):                    inf% > 200%
        // disk garbage:                                   133% > 20%
        //
        // Must compact range 0 by garbage per range.
        //
        //  range 0    range 1    range 2
        // |          |00000xxxxx|xxxxxxxxxx|
        // |          |xxxxx     |     xxxxx|
        //
        // top range garbage ignoring zeroed (range 2):    50%  < 200%
        // disk garbage ignoring zeroed:                   25%  > 20%
        // top range garbage (range 1):                    100% < 200%
        // disk garbage:                                   66%  > 20%
        //
        // Must compact range 2 by ignoring zeroed per disk.
        //
        //  range 0    range 1    range 2
        // |          |00000xxxxx|xxxxxxxxxx|
        // |          |xxxxx     |          |
        //
        // top range garbage ignoring zeroed:              0%   < 200%
        // disk garbage ignoring zeroed:                   0%   < 20%
        // top range garbage (range 1):                    100% < 200%
        // disk garbage:                                   33%  > 20%
        //
        // Must compact range 1 by garbage per disk.

        // Wait for background operations completion.
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(garbageCompactionRequestObserved);
        UNIT_ASSERT(ignoringZeroedCompactionRequestObserved);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT(compactionByGarbageBlocksPerRange > 0);
        UNIT_ASSERT(compactionByGarbageBlocksPerDisk > 0);
        UNIT_ASSERT_EQUAL(0, compactionByIgnoringZeroedPerRange);
        UNIT_ASSERT(compactionByIgnoringZeroedPerDisk > 0);
    }

    Y_UNIT_TEST(ShouldRunGarbageCompactionAfterZeroBlocks)
    {
        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetIgnoringZeroedCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(20);
        config.SetCompactionRangeGarbageThreshold(999999);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByGarbageBlocksPerRange = 0;
        ui64 compactionByGarbageBlocksPerDisk = 0;

        bool garbageCompactionRequestObserved = false;
        bool ignoringZeroedCompactionRequestObserved = false;

        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        auto* msg = event->Get<
                            TEvPartitionPrivate::TEvCompactionRequest>();
                        if (msg->Mode == TEvPartitionPrivate::GarbageCompaction)
                        {
                            garbageCompactionRequestObserved = true;
                        } else if (
                            msg->Mode ==
                            TEvPartitionPrivate::IgnoringZeroedCompaction)
                        {
                            ignoringZeroedCompactionRequestObserved = true;
                        }
                        break;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event
                                ->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByGarbageBlocksPerRange =
                            cc.CompactionByGarbageBlocksPerRange.Value;
                        compactionByGarbageBlocksPerDisk =
                            cc.CompactionByGarbageBlocksPerDisk.Value;
                        break;
                    }
                }
                return false;
            });

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024));

        // < 20% on the whole disk, not enough to trigger compaction.
        partition.ZeroBlocks(TBlockRange32::WithLength(0, 300));
        partition.Flush();

        // Wait for background operations completion.
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(!garbageCompactionRequestObserved);
        UNIT_ASSERT(!ignoringZeroedCompactionRequestObserved);

        // > 20% on the whole disk, not enough to trigger compaction.
        partition.ZeroBlocks(TBlockRange32::WithLength(1024, 300));
        partition.Flush();

        // Wait for background operations completion.
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(garbageCompactionRequestObserved);
        UNIT_ASSERT(!ignoringZeroedCompactionRequestObserved);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_EQUAL(0, compactionByGarbageBlocksPerRange);
        UNIT_ASSERT(compactionByGarbageBlocksPerDisk > 0);
    }

    Y_UNIT_TEST(ShouldRunGarbageCompactionAfterZeroBlocksForRange)
    {
        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetIgnoringZeroedCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(999999);
        config.SetCompactionRangeGarbageThreshold(200);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByGarbageBlocksPerRange = 0;
        ui64 compactionByGarbageBlocksPerDisk = 0;
        bool garbageCompactionRequestObserved = false;
        bool ignoringZeroedCompactionRequestObserved = false;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        auto* msg = event->Get<
                            TEvPartitionPrivate::TEvCompactionRequest>();
                        if (msg->Mode == TEvPartitionPrivate::GarbageCompaction)
                        {
                            garbageCompactionRequestObserved = true;
                        } else if (
                            msg->Mode ==
                            TEvPartitionPrivate::IgnoringZeroedCompaction)
                        {
                            ignoringZeroedCompactionRequestObserved = true;
                        }
                        break;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event
                                ->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByGarbageBlocksPerRange =
                            cc.CompactionByGarbageBlocksPerRange.Value;
                        compactionByGarbageBlocksPerDisk =
                            cc.CompactionByGarbageBlocksPerDisk.Value;
                        break;
                    }
                }
                return false;
            });

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        partition.ZeroBlocks(TBlockRange32::WithLength(0, 600));
        partition.Flush();

        // Wait for background operations completion.
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(!garbageCompactionRequestObserved);
        UNIT_ASSERT(!ignoringZeroedCompactionRequestObserved);

        partition.ZeroBlocks(TBlockRange32::WithLength(600, 100));
        partition.Flush();

        // Wait for background operations completion.
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(garbageCompactionRequestObserved);
        UNIT_ASSERT(!ignoringZeroedCompactionRequestObserved);
        garbageCompactionRequestObserved = false;

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT(compactionByGarbageBlocksPerRange > 0);
        UNIT_ASSERT_EQUAL(0, compactionByGarbageBlocksPerDisk);
    }

    Y_UNIT_TEST(ShouldAutomaticallyRunGarbageCompaction)
    {
        DoShouldAutomaticallyRunGarbageOrIgnoringZeroedCompaction(false);
    }

    Y_UNIT_TEST(ShouldAutomaticallyRunIgnoringZeroedCompaction)
    {
        DoShouldAutomaticallyRunGarbageOrIgnoringZeroedCompaction(true);
    }

    Y_UNIT_TEST(ShouldAutomaticallyRunGarbageCompactionForSuperDirtyRanges)
    {
        DoShouldAutomaticallyRunGarbageCompactionForSuperDirtyRanges(false);
    }

    Y_UNIT_TEST(
        ShouldAutomaticallyRunIgnoringZeroedCompactionForSuperDirtyRanges)
    {
        DoShouldAutomaticallyRunGarbageCompactionForSuperDirtyRanges(true);
    }

    Y_UNIT_TEST(ShouldCalculateGarbageCompactionExecTimePerSecondLimit)
    {
        CheckInterpolatedGarbageCompactionExecTimePerSecond(80, 160, 70, 200);
        CheckInterpolatedGarbageCompactionExecTimePerSecond(80, 160, 90, 300);
        CheckInterpolatedGarbageCompactionExecTimePerSecond(80, 160, 120, 600);
        CheckInterpolatedGarbageCompactionExecTimePerSecond(80, 160, 180, 1000);
        CheckInterpolatedGarbageCompactionExecTimePerSecond(120, 120, 120, 200);
    }

    Y_UNIT_TEST(ShouldRunCompactionIfBlobCountIsGreaterThanThreshold)
    {
        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(999999999);
        config.SetCompactionRangeGarbageThreshold(999999999);
        config.SetAllocationUnitSSD(4);
        config.SetAllocationUnitHDD(4);
        config.SetSSDMaxBlobsPerUnit(7);
        config.SetHDDMaxBlobsPerUnit(7);

        ui32 blockCount = 1024 * 1024;

        auto runtime = PrepareTestActorRuntime(config, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByBlobCount = 0;
        bool compactionRequestObserved = false;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        auto* msg = event->Get<
                            TEvPartitionPrivate::TEvCompactionRequest>();
                        UNIT_ASSERT(
                            msg->Mode ==
                            TEvPartitionPrivate::BlobCountCompaction);
                        compactionRequestObserved = true;
                        break;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event
                                ->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByBlobCount =
                            cc.CompactionByBlobCountPerDisk.Value;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        for (size_t i = 0; i < 3; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(i * 1024, 1024), i);
            partition.WriteBlocks(TBlockRange32::WithLength(i * 1024, 1024), i);
        }

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_EQUAL(0, compactionByBlobCount);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // Six blobs are below the disk budget of seven.
        UNIT_ASSERT(!compactionRequestObserved);

        partition.WriteBlocks(TBlockRange32::WithLength(3 * 1024, 1024), 3);
        partition.WriteBlocks(TBlockRange32::WithLength(3 * 1024, 1024), 3);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // blob count is greater than threshold on disk => compaction
        UNIT_ASSERT(compactionRequestObserved);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT(0 < compactionByBlobCount);
    }

    Y_UNIT_TEST(ShouldNotCompactSingleBlobRangesOverBlobBudget)
    {
        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetHDDMaxBlobsPerUnit(7);
        config.SetCompactionGarbageThreshold(999999999);
        config.SetCompactionRangeGarbageThreshold(999999999);
        auto runtime = PrepareTestActorRuntime(config, 1024 * 1024);
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui32 compactionRequests = 0;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvCompactionRequest)
                {
                    ++compactionRequests;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        for (ui32 i = 0; i < 10; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(i * 1024, 1024), i);
        }
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(0, compactionRequests);
        UNIT_ASSERT_VALUES_EQUAL(
            10,
            partition.StatPartition()->Record.GetStats().GetMergedBlobsCount());
    }

    Y_UNIT_TEST(ShouldCompactRangeWithMostBlobsWhenOverBlobBudget)
    {
        CheckCompactionOfRangesWithMostBlobs(false);
    }

    Y_UNIT_TEST(ShouldCompactBatchWithMostBlobsWhenOverBlobBudget)
    {
        CheckCompactionOfRangesWithMostBlobs(true);
    }

    Y_UNIT_TEST(ShouldDecrementBatchSizeWhenBlobsCountPerDiskIsSmall)
    {
        // blobs percentage: (10 - 9 / 10) * 100 = 10
        // 10 < 100, so batch size should decrement
        CheckIncrementAndDecrementCompactionPerRun(
            2,
            9,
            999999,
            999999,
            99999,
            7,
            200,
            100,
            1);
    }

    Y_UNIT_TEST(ShouldChangeBatchSizeDueToBlocksPerDisk)
    {
        // real blocks per disk: 1024 * 10 = 10240
        // total blocks per disk: = 1024 * 3 = 3072
        // garbage percentage: 100 * (10 - 3) * 1024 / 3072 ~ 233
        // percentage more threshold: 100 * (233 - 203) / 203 ~ 14.8

        // 14.8 > 13, so should increment and compact 2 ranges
        CheckIncrementAndDecrementCompactionPerRun(
            1,
            1000,
            99999,
            203,
            99999,
            5,
            13,
            5,
            2);

        // 14.8 < 15, so should decrement and compact only 1 range
        CheckIncrementAndDecrementCompactionPerRun(
            2,
            1000,
            99999,
            203,
            99999,
            7,
            30,
            15,
            1);
    }

    Y_UNIT_TEST(ShouldChangeBatchSizeDueToBlocksPerRangeCount)
    {
        // real blocks per last range: 1024 * 4
        // total blocks per disk: 1024
        // garbage percentage: 100 * (4 - 1) * 1024 / 1024 = 300
        // 100 * (300 - 280) / 280 = 7

        // 7 > 6, so should increment and compact 2 ranges
        CheckIncrementAndDecrementCompactionPerRun(
            1,
            1000,
            99999,
            9999,
            280,
            5,
            6,
            4,
            2);

        // 7 > 6, but should compact only 1 range due to maxRangeCountPerRun
        CheckIncrementAndDecrementCompactionPerRun(
            1,
            1000,
            99999,
            9999,
            280,
            7,
            6,
            4,
            1,
            1);

        // 7 < 8, so should decrement and compact only 1 range
        CheckIncrementAndDecrementCompactionPerRun(
            2,
            1000,
            99999,
            9999,
            280,
            7,
            30,
            8,
            1);
    }

    Y_UNIT_TEST(ShouldDecrementBatchSizeWhenBlobsPerRangeCountIsSmall)
    {
        // CompactionScore in range3: 4 - 4 + eps
        // 100 * (eps - 0) / 1024 < 3, so should decrement and compact 1 range
        CheckIncrementAndDecrementCompactionPerRun(
            2,
            1000,
            4,
            9999,
            9999,
            7,
            50,
            10,
            1);
    }

    Y_UNIT_TEST(ShouldRespectCompactionCountPerRunChangingPeriod)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetBatchCompactionEnabled(true);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(99999);
        config.SetCompactionRangeGarbageThreshold(280);
        config.SetCompactionRangeCountPerRun(1);
        config.SetCompactionCountPerRunIncreasingThreshold(6);
        config.SetCompactionCountPerRunDecreasingThreshold(4);
        config.SetHDDMaxBlobsPerUnit(1000);
        config.SetSSDMaxBlobsPerUnit(1000);
        config.SetMaxCompactionRangeCountPerRun(10);
        config.SetCompactionCountPerRunChangingPeriod(100000);

        auto runtime = PrepareTestActorRuntime(config, 4 * 1024 * 1024);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto blockRange = TBlockRange32::WithLength(0, 1024);

        ui32 resultCompactionRangeCountPerRun = 0;

        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event
                                ->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Simple;
                        resultCompactionRangeCountPerRun =
                            cc.CompactionRangeCountPerRun.Value;
                        break;
                    }
                }
                return false;
            });

        for (int i = 0; i < 10; ++i) {
            partition.WriteBlocks(blockRange, i);
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.Compaction();
        partition.Cleanup();

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(1, resultCompactionRangeCountPerRun);
        }

        // CompactionRangeCountPerRun was updated more then period seconds ago
        // So it should be changed
        runtime->AdvanceCurrentTime(TDuration::Seconds(102));

        for (int i = 0; i < 10; ++i) {
            partition.WriteBlocks(blockRange, i);
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.Compaction();
        partition.Cleanup();

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(2, resultCompactionRangeCountPerRun);
        }

        runtime->AdvanceCurrentTime(TDuration::Seconds(50));

        for (int i = 0; i < 10; ++i) {
            partition.WriteBlocks(blockRange, i);
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        partition.Compaction();
        partition.Cleanup();

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
            // Shouldn't increase compactionRangeCountPerRun due to period
            UNIT_ASSERT_VALUES_EQUAL(2, resultCompactionRangeCountPerRun);
        }
    }

    Y_UNIT_TEST(ShouldSubtractCleanupBlobsWhenCheckingForBlobsCountCompaction)
    {
        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(999999999);
        config.SetCompactionRangeGarbageThreshold(999999999);
        config.SetAllocationUnitSSD(4);
        config.SetAllocationUnitHDD(4);
        config.SetSSDMaxBlobsPerUnit(7);
        config.SetHDDMaxBlobsPerUnit(7);

        ui32 blockCount = 1024 * 1024;

        auto runtime = PrepareTestActorRuntime(config, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByBlobCount = 0;
        ui64 compactionRequestObserved = 0;
        runtime->SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        ++compactionRequestObserved;
                        break;
                    }
                    case TEvPartitionPrivate::EvCleanupRequest: {
                        return true;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg = event->template Get<
                            TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByBlobCount =
                            cc.CompactionByBlobCountPerDisk.Value;
                        break;
                    }
                }
                return false;
            });

        for (size_t i = 0; i < 4; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(i * 1024, 1024), i);
        }

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_EQUAL(0, compactionByBlobCount);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // blob count is less than 4 * 2 => no compaction
        UNIT_ASSERT(!compactionRequestObserved);

        for (size_t i = 0; i < 4; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(i * 1024, 1024), i);
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // blob count is greater than threshold on disk => only one compaction
        // because two blobs go to cleanup queue and just one is created
        UNIT_ASSERT_VALUES_EQUAL(1, compactionRequestObserved);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(1, compactionByBlobCount);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // no more compactions generated because blobs in cleanup queue are
        // considered to be already removed
        UNIT_ASSERT_VALUES_EQUAL(1, compactionRequestObserved);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(0, compactionByBlobCount);
    }

    Y_UNIT_TEST(ShouldRunCompactionIfBlocksCountIsGreaterThanThreshold)
    {
        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(20);
        config.SetCompactionRangeGarbageThreshold(999999999);
        config.SetSSDMaxBlobsPerUnit(999999999);
        config.SetHDDMaxBlobsPerUnit(999999999);

        ui32 blockCount = 10 * 1024;

        auto runtime = PrepareTestActorRuntime(config, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByBlockCount = 0;
        bool compactionRequestObserved = false;
        runtime->SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        compactionRequestObserved = true;
                        break;
                    }
                    case TEvPartitionPrivate::EvCleanupRequest: {
                        return true;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg = event->template Get<
                            TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByBlockCount =
                            cc.CompactionByGarbageBlocksPerDisk.Value;
                    }
                }
                return false;
            });

        for (size_t i = 0; i < 10; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(i * 1024, 1024), i);
        }

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_EQUAL(0, compactionByBlockCount);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // garbage block count is less than 20%  => no compaction
        UNIT_ASSERT(!compactionRequestObserved);

        for (size_t i = 0; i < 2; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(i * 1024, 1024), i);
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // garbage block count is greater than threshold => compaction
        UNIT_ASSERT(compactionRequestObserved);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(1, compactionByBlockCount);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // no more compactions generated because blocks in cleanup queue are
        // considered to be already removed
        UNIT_ASSERT_VALUES_EQUAL(1, compactionRequestObserved);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(0, compactionByBlockCount);
    }

    Y_UNIT_TEST(ShouldProcessMultipleRangesUponGarbageCompaction)
    {
        auto config = DefaultConfig();
        config.SetBatchCompactionEnabled(true);
        config.SetGarbageCompactionRangeCountPerRun(3);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(20);
        config.SetCompactionRangeGarbageThreshold(999999);

        auto runtime = PrepareTestActorRuntime(config, MaxPartitionBlocksCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        TAutoPtr<IEventHandle> compactionRequest;
        const auto interceptCompactionRequest =
            [&compactionRequest](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvPartitionPrivate::EvCompactionRequest)
            {
                auto* msg =
                    event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                if (msg->Mode == TEvPartitionPrivate::GarbageCompaction) {
                    compactionRequest = event.Release();
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime->SetObserverFunc(interceptCompactionRequest);

        const auto blockRange1 = TBlockRange32::WithLength(0, 1024);
        const auto blockRange2 = TBlockRange32::WithLength(1024 * 1024, 1024);
        const auto blockRange3 =
            TBlockRange32::WithLength(2 * 1024 * 1024, 1024);

        partition.WriteBlocks(blockRange1, 1);
        partition.WriteBlocks(blockRange1, 2);

        partition.WriteBlocks(blockRange2, 3);
        partition.WriteBlocks(blockRange2, 4);
        partition.WriteBlocks(blockRange2, 5);

        partition.WriteBlocks(blockRange3, 6);
        partition.WriteBlocks(blockRange3, 7);
        partition.WriteBlocks(blockRange3, 8);

        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(8, stats.GetMergedBlobsCount());
        }

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(compactionRequest);
        runtime->Send(compactionRequest.Release());

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.Cleanup();

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

        // checking that we now have 1 blob in each of the ranges
        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(
        ShouldRunGarbageCompactionWhenUsedBlocksCountGreaterThanBlockCount)
    {
        DoShouldRunCompactionWhenUsedBlocksCountGreaterThanBlockCount(false);
    }

    Y_UNIT_TEST(
        ShouldRunIgnoringZeroedCompactionWhenUsedBlocksCountGreaterThanBlockCount)
    {
        DoShouldRunCompactionWhenUsedBlocksCountGreaterThanBlockCount(true);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
