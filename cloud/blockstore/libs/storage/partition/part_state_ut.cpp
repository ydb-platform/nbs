#include "part_state.h"

#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/partition/part_schema.h>
#include <cloud/blockstore/libs/storage/partition_common/part_thread_safe_state.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

const ui32 DefaultBlockCount = 1000;

////////////////////////////////////////////////////////////////////////////////

NProto::TPartitionMeta DefaultConfig(size_t channelCount, size_t blockCount)
{
    NProto::TPartitionMeta meta;

    auto& config = *meta.MutableConfig();
    config.SetBlockSize(DefaultBlockSize);
    config.SetBlocksCount(blockCount);

    auto cps = config.MutableExplicitChannelProfiles();
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));

    for (ui32 i = 0; i < channelCount; ++i) {
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
    }

    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Fresh));

    return meta;
}

TBackpressureFeaturesConfig DefaultBPConfig()
{
    return {
        {
            30,   // compaction score limit
            10,   // compaction score threshold
            10,   // compaction score feature max value
        },
        {
            1600_KB,   // fresh byte count limit
            400_KB,    // fresh byte count threshold
            10,        // fresh byte count feature max value
        },
        {
            8_MB,   // cleanup queue size limit
            4_MB,   // cleanup queue size threshold
            10,     // cleanup queue size feature max value
        },
    };
}

TFreeSpaceConfig DefaultFreeSpaceConfig()
{
    return {
        0.25,   // free space threshold
        0.15,   // min free space
    };
}

////////////////////////////////////////////////////////////////////////////////

struct TNoBackpressurePolicy: ICompactionPolicy
{
    TCompactionScore CalculateScore(const TRangeStat& stat) const override
    {
        return stat.BlobCount;
    }

    bool BackpressureEnabled() const override
    {
        return false;
    }

    ui64 GetUsedBlocksThresholdForMixedBlocksCompaction() const override
    {
        return 0;
    }
};

struct TPartitionStateOptions
{
    bool CheckpointAwareCleanupEnabled = false;
    bool CompactionStatsTrackerEnabled = false;
    bool ExtendedBlobCountersEnabled = false;
    ICompactionPolicyPtr CompactionPolicy =
        BuildDefaultCompactionPolicy(5, 0, false);
    ui32 MixedIndexCacheSize = 0;
    ui64 AllocationUnit = 10000;
    ui32 MaxBlobsPerUnit = 100;
};

TPartitionState MakeState(
    NProto::TPartitionMeta meta,
    TPartitionStateOptions options = {})
{
    const auto channelCount = meta.GetConfig().ExplicitChannelProfilesSize();
    auto threadSafeState = std::make_shared<TPartitionThreadSafeState>();
    return TPartitionState(
        std::move(meta),
        std::move(options.CompactionPolicy),
        0,   // compactionScoreHistorySize
        0,   // cleanupScoreHistorySize
        DefaultBPConfig(),
        DefaultFreeSpaceConfig(),
        Max(),   // maxIORequestsInFlight
        0,       // reassignChannelsPercentageThreshold
        100,     // reassignFreshChannelsPercentageThreshold
        100,     // reassignMixedChannelsPercentageThreshold
        false,   // reassignSystemChannelsImmediately
        channelCount,
        options.MixedIndexCacheSize,
        options.AllocationUnit,
        options.MaxBlobsPerUnit,
        10,   // maxBlobsPerRange,
        1,    // compactionRangeCountPerRun
        std::move(threadSafeState),
        0,              // tabletId
        std::nullopt,   // mixedBlocksFilterConfig
        options.CheckpointAwareCleanupEnabled,
        options.CompactionStatsTrackerEnabled,
        options.ExtendedBlobCountersEnabled);
}

TPartitionState MakeState(
    size_t blockCount = DefaultBlockCount,
    TPartitionStateOptions options = {})
{
    return MakeState(DefaultConfig(1, blockCount), std::move(options));
}

void CheckBlobCountersAfterUpgrade(bool extendedBlobCountersEnabled)
{
    auto meta = DefaultConfig(1, DefaultBlockCount);
    auto& stats = *meta.MutableStats();
    // With UseBlobChannelDataKindForCounters enabled, these legacy counters
    // describe one mixed-index blob stored on a merged channel.
    stats.SetMixedBlobsCount(0);
    stats.SetMergedBlobsCount(1);
    stats.SetMixedBlocksCount(0);
    stats.SetMergedBlocksCount(3);
    stats.SetMixedIndexBlobsCount(1);
    stats.SetMergedIndexBlobsCount(0);
    stats.SetMixedIndexBlocksCount(3);
    stats.SetMergedIndexBlocksCount(0);

    auto state = MakeState(
        meta,
        {.ExtendedBlobCountersEnabled = extendedBlobCountersEnabled});

    // Compact the existing blob into a merged-index blob, then clean up.
    state.UpdateBlobCounters(
        TPartialBlobId(2, 1, 3, 3 * DefaultBlockSize, 0, 0),
        EChannelDataKind::Merged,
        3,
        true);
    state.UpdateBlobCounters(
        TPartialBlobId(1, 1, 3, 3 * DefaultBlockSize, 0, 0),
        EChannelDataKind::Mixed,
        3,
        false);

    UNIT_ASSERT_VALUES_EQUAL(
        3,
        state.GetMixedBlocksCount() + state.GetMergedBlocksCount());
    UNIT_ASSERT_VALUES_EQUAL(1, state.GetTotalBlobsCount());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionStateTest)
{
    Y_UNIT_TEST(ShouldAccountForBlobCountersByChannelAndIndex)
    {
        auto meta = DefaultConfig(1, DefaultBlockCount);
        meta.MutableConfig()->AddExplicitChannelProfiles()->SetDataKind(
            static_cast<ui32>(EChannelDataKind::Mixed));
        auto state = MakeState(meta, {.ExtendedBlobCountersEnabled = true});
        const TPartialBlobId mixedBlob(1, 1, 3, 123, 0, 0);
        const TPartialBlobId mergedBlob(1, 2, 5, 456, 0, 0);
        const TPartialBlobId deletionMarker(1, 3, 0, 0, 0, 0);

        state.UpdateBlobCounters(mixedBlob, EChannelDataKind::Mixed, 3, true);
        state.UpdateBlobCounters(mergedBlob, EChannelDataKind::Merged, 7, true);
        state.UpdateBlobCounters(
            deletionMarker,
            EChannelDataKind::Mixed,
            5,
            true);
        UNIT_ASSERT_VALUES_EQUAL(2, state.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(1, state.GetMergedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(3, state.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(7, state.GetMergedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(2, state.GetMixedIndexBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(1, state.GetMergedIndexBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(3, state.GetMixedIndexBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(7, state.GetMergedIndexBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(1, state.GetMixedChannelBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(1, state.GetMergedChannelBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(7, state.GetMixedChannelBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(3, state.GetMergedChannelBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(456, state.GetMixedBytesCount());
        UNIT_ASSERT_VALUES_EQUAL(123, state.GetMergedBytesCount());
        UNIT_ASSERT_VALUES_EQUAL(
            123,
            state.GetStats().GetBlobByteCounters().GetMixedIndexBytesCount());
        UNIT_ASSERT_VALUES_EQUAL(
            456,
            state.GetStats().GetBlobByteCounters().GetMergedIndexBytesCount());

        auto restored =
            MakeState(state.GetMeta(), {.ExtendedBlobCountersEnabled = true});
        UNIT_ASSERT_VALUES_EQUAL(456, restored.GetMixedBytesCount());
        UNIT_ASSERT_VALUES_EQUAL(123, restored.GetMergedBytesCount());
        restored
            .UpdateBlobCounters(mixedBlob, EChannelDataKind::Mixed, 3, false);
        restored
            .UpdateBlobCounters(mergedBlob, EChannelDataKind::Merged, 7, false);
        UNIT_ASSERT_VALUES_EQUAL(1, restored.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(1, restored.GetMixedIndexBlobsCount());
        restored.UpdateBlobCounters(
            deletionMarker,
            EChannelDataKind::Mixed,
            5,
            false);
        UNIT_ASSERT_VALUES_EQUAL(0, restored.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(0, restored.GetMergedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(0, restored.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, restored.GetMergedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, restored.GetMixedIndexBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(0, restored.GetMergedIndexBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(0, restored.GetMixedIndexBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, restored.GetMergedIndexBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, restored.GetMixedChannelBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(0, restored.GetMergedChannelBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(0, restored.GetMixedChannelBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, restored.GetMergedChannelBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, restored.GetMixedBytesCount());
        UNIT_ASSERT_VALUES_EQUAL(0, restored.GetMergedBytesCount());
    }

    Y_UNIT_TEST(ShouldPreserveInitializedBlobCountersIncludingZero)
    {
        for (const ui64 count: {0ULL, 123ULL}) {
            auto meta = DefaultConfig(1, DefaultBlockCount);
            auto& stats = *meta.MutableStats();
            stats.SetMixedBlocksCount(20);
            stats.SetMergedBlocksCount(60);
            stats.SetMixedBlobsCount(2);
            stats.SetMergedBlobsCount(3);
            stats.SetMixedChannelBlobsCount(count);
            stats.SetMergedChannelBlobsCount(count);
            stats.SetMixedChannelBlocksCount(count);
            stats.SetMergedChannelBlocksCount(count);
            auto& counters = *stats.MutableBlobByteCounters();
            counters.SetMixedBytesCount(count);
            counters.SetMergedBytesCount(count);
            counters.SetMixedIndexBytesCount(count);
            counters.SetMergedIndexBytesCount(count);

            auto state = MakeState(meta, {.ExtendedBlobCountersEnabled = true});
            const auto& restored = state.GetStats().GetBlobByteCounters();
            UNIT_ASSERT_VALUES_EQUAL(count, restored.GetMixedBytesCount());
            UNIT_ASSERT_VALUES_EQUAL(count, restored.GetMergedBytesCount());
            UNIT_ASSERT_VALUES_EQUAL(count, restored.GetMixedIndexBytesCount());
            UNIT_ASSERT_VALUES_EQUAL(
                count,
                restored.GetMergedIndexBytesCount());
            UNIT_ASSERT_VALUES_EQUAL(count, state.GetMixedChannelBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                count,
                state.GetMergedChannelBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(count, state.GetMixedChannelBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(count, state.GetMergedChannelBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(2, state.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(3, state.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(20, state.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(60, state.GetMergedBlocksCount());
        }
    }

    Y_UNIT_TEST(ShouldOnlyUpdateLegacyBlobCountersWhenDisabled)
    {
        const TPartialBlobId blob(1, 1, 3, 123, 0, 0);
        auto state = MakeState();
        state.UpdateBlobCounters(blob, EChannelDataKind::Mixed, 3, true);
        UNIT_ASSERT_VALUES_EQUAL(1, state.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(3, state.GetMixedBlocksCount());
        state.UpdateBlobCounters(blob, EChannelDataKind::Mixed, 3, false);
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetMixedIndexBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetMixedIndexBlocksCount());
        UNIT_ASSERT(!state.GetStats().HasBlobByteCounters());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetMixedChannelBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetMergedChannelBlocksCount());
    }

    Y_UNIT_TEST(ShouldPreserveBlobCountersOnUpgradeWithExtendedCountersDisabled)
    {
        CheckBlobCountersAfterUpgrade(false);
    }

    Y_UNIT_TEST(ShouldPreserveBlobCountersOnUpgradeWithExtendedCountersEnabled)
    {
        CheckBlobCountersAfterUpgrade(true);
    }

    Y_UNIT_TEST(ShouldReinitializeBlobCountersAfterDisabledWrites)
    {
        auto meta = DefaultConfig(1, DefaultBlockCount);
        auto state = MakeState(meta, {.ExtendedBlobCountersEnabled = true});
        state.UpdateBlobCounters(
            TPartialBlobId(1, 1, 3, 123, 0, 0),
            EChannelDataKind::Mixed,
            1,
            true);

        auto disabled = MakeState(state.GetMeta());
        UNIT_ASSERT(!disabled.GetStats().HasBlobByteCounters());
        UNIT_ASSERT_VALUES_EQUAL(0, disabled.GetMixedChannelBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(0, disabled.GetMergedChannelBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(0, disabled.GetMixedChannelBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, disabled.GetMergedChannelBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(1, disabled.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(1, disabled.GetMixedBlobsCount());
        disabled.IncrementMixedBlocksCount(1);
        disabled.IncrementMergedBlocksCount(3);
        disabled.IncrementMergedBlobsCount(1);

        // Repeated disabled restarts must preserve writes and cleared counters.
        auto restarted = MakeState(disabled.GetMeta());
        auto restored = MakeState(
            restarted.GetMeta(),
            {.ExtendedBlobCountersEnabled = true});
        UNIT_ASSERT_VALUES_EQUAL(1, restored.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(1, restored.GetMergedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(0, restored.GetMixedChannelBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(2, restored.GetMergedChannelBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(2, restored.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(3, restored.GetMergedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, restored.GetMixedChannelBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(5, restored.GetMergedChannelBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, restored.GetMixedBytesCount());
        UNIT_ASSERT_VALUES_EQUAL(
            5 * DefaultBlockSize,
            restored.GetMergedBytesCount());
        UNIT_ASSERT_VALUES_EQUAL(
            2 * DefaultBlockSize,
            restored.GetStats()
                .GetBlobByteCounters()
                .GetMixedIndexBytesCount());
        UNIT_ASSERT_VALUES_EQUAL(
            3 * DefaultBlockSize,
            restored.GetStats()
                .GetBlobByteCounters()
                .GetMergedIndexBytesCount());
    }

    Y_UNIT_TEST(ShouldOnlyCreateCompactionStatsTrackerWhenEnabled)
    {
        auto disabledState = MakeState();
        UNIT_ASSERT(!disabledState.AccessCompactionStatsTracker());

        auto enabledState = MakeState(
            DefaultBlockCount,
            {.CompactionStatsTrackerEnabled = true});
        UNIT_ASSERT(enabledState.AccessCompactionStatsTracker());
    }

    Y_UNIT_TEST(ShouldInitializeAndClearChannelCountersAcrossRestarts)
    {
        for (bool hasMixedChannel: {false, true}) {
            auto meta = DefaultConfig(1, DefaultBlockCount);
            if (hasMixedChannel) {
                meta.MutableConfig()->AddExplicitChannelProfiles()->SetDataKind(
                    static_cast<ui32>(EChannelDataKind::Mixed));
            }
            auto& stats = *meta.MutableStats();
            stats.SetMixedBlobsCount(2);
            stats.SetMergedBlobsCount(3);
            stats.SetMixedBlocksCount(20);
            stats.SetMergedBlocksCount(60);

            for (bool enabled: {true, true, false, false, true, true}) {
                auto state =
                    MakeState(meta, {.ExtendedBlobCountersEnabled = enabled});
                UNIT_ASSERT_VALUES_EQUAL(20, state.GetMixedBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(60, state.GetMergedBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled && hasMixedChannel ? 20 : 0,
                    state.GetMixedChannelBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled ? (hasMixedChannel ? 60 : 80) : 0,
                    state.GetMergedChannelBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(2, state.GetMixedBlobsCount());
                UNIT_ASSERT_VALUES_EQUAL(3, state.GetMergedBlobsCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled && hasMixedChannel ? 2 : 0,
                    state.GetMixedChannelBlobsCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled ? (hasMixedChannel ? 3 : 5) : 0,
                    state.GetMergedChannelBlobsCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled ? 20 : 0,
                    state.GetMixedIndexBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled ? 60 : 0,
                    state.GetMergedIndexBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled ? 2 : 0,
                    state.GetMixedIndexBlobsCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled ? 3 : 0,
                    state.GetMergedIndexBlobsCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled,
                    state.GetStats().HasBlobByteCounters());
                UNIT_ASSERT_VALUES_EQUAL(
                    state.GetMixedChannelBlocksCount() * DefaultBlockSize,
                    state.GetMixedBytesCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    state.GetMergedChannelBlocksCount() * DefaultBlockSize,
                    state.GetMergedBytesCount());
                meta = state.GetMeta();
            }
        }
    }

    Y_UNIT_TEST(ShouldUpdateChannelBlockCountersAfterRebuildOnlyWhenEnabled)
    {
        for (bool enabled: {false, true}) {
            auto state = MakeState(
                DefaultBlockCount,
                {.ExtendedBlobCountersEnabled = enabled});
            state.UpdateBlocksCountersAfterMetadataRebuild(20, 60, 10, 70);
            UNIT_ASSERT_VALUES_EQUAL(20, state.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(60, state.GetMergedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                enabled ? 20 : 0,
                state.GetMixedIndexBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                enabled ? 60 : 0,
                state.GetMergedIndexBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                enabled ? 10 : 0,
                state.GetMixedChannelBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                enabled ? 70 : 0,
                state.GetMergedChannelBlocksCount());
        }
    }

    Y_UNIT_TEST(ShouldUseIndexBlobTotalsRegardlessOfCounterMode)
    {
        for (bool useChannelCounters: {false, true}) {
            auto state = MakeState(
                DefaultBlockCount,
                {.ExtendedBlobCountersEnabled = useChannelCounters});
            auto& stats = state.AccessStats();
            stats.SetMixedBlobsCount(1);
            stats.SetMergedBlobsCount(7);
            stats.SetMixedChannelBlobsCount(2);
            stats.SetMergedChannelBlobsCount(3);

            const ui64 expectedTotal = 8;
            UNIT_ASSERT_VALUES_EQUAL(expectedTotal, state.GetTotalBlobsCount());
            state.StartRebuildBlockCount();
            UNIT_ASSERT_VALUES_EQUAL(
                expectedTotal,
                state.GetMetadataRebuildProgress().Total);
            state.StartScanDisk();
            UNIT_ASSERT_VALUES_EQUAL(
                expectedTotal,
                state.GetScanDiskProgress().TotalBlobs);
        }
    }

    Y_UNIT_TEST(ShouldTrackLegacyCompactionScoreByBlobCount)
    {
        auto state = MakeState(4096);
        auto& map = state.GetCompactionMap();
        const ui32 secondRange = map.GetRangeSize();

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetLegacyCompactionScore());

        // Blob counts matter even below the compaction policy threshold.
        map.Update(0, 2, 100, 100, 0, 0, false);
        map.Update(secondRange, 3, 100, 100, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(3, state.GetLegacyCompactionScore());

        map.Update(0, 10, 100, 100, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(10, state.GetLegacyCompactionScore());

        // Compacted ranges no longer contribute to the legacy score.
        map.Update(0, 10, 100, 100, 0, 0, true);
        UNIT_ASSERT_VALUES_EQUAL(3, state.GetLegacyCompactionScore());

        map.Update(secondRange, 0, 0, 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetLegacyCompactionScore());
    }

    Y_UNIT_TEST(ShouldCalculateCompactionBackpressureByBlobCountWithLoadPolicy)
    {
        auto state = MakeState(
            4096,
            {.CompactionPolicy = BuildLoadOptimizationCompactionPolicy(
                 {.MaxBlobSize = 4_MB,
                  .BlockSize = DefaultBlockSize,
                  .MaxReadIops = 400,
                  .MaxReadBandwidth = 15_MB,
                  .MaxWriteIops = 1000,
                  .MaxWriteBandwidth = 15_MB,
                  .MaxBlobsPerRange = 100},
                 0)});
        auto& map = state.GetCompactionMap();
        const ui32 hotRange = map.GetRangeSize();

        map.Update(0, 30, 1024, 1024, 0, 0, false);
        map.Update(hotRange, 2, 1024, 1024, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(30, state.GetLegacyCompactionScore());
        UNIT_ASSERT_DOUBLES_EQUAL(
            10,
            state.CalculateCurrentBackpressure().CompactionScore,
            1e-5);

        // Reads make the range with fewer blobs the policy's top candidate.
        map.RegisterRead(hotRange, 1000, 1024);
        UNIT_ASSERT_VALUES_EQUAL(hotRange, map.GetTop().BlockIndex);
        UNIT_ASSERT(state.GetCompactionScore() > 0);
        UNIT_ASSERT_VALUES_EQUAL(30, state.GetLegacyCompactionScore());
        UNIT_ASSERT_DOUBLES_EQUAL(
            10,
            state.CalculateCurrentBackpressure().CompactionScore,
            1e-5);

        map.Update(0, 0, 0, 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(2, state.GetLegacyCompactionScore());
        UNIT_ASSERT_DOUBLES_EQUAL(
            1,
            state.CalculateCurrentBackpressure().CompactionScore,
            1e-5);
    }

    Y_UNIT_TEST(CalculateCurrentBackpressure)
    {
        auto state = MakeState(1000);

        const auto initialBackpressure = state.CalculateCurrentBackpressure();
        UNIT_ASSERT_VALUES_EQUAL(1, initialBackpressure.FreshIndexScore);
        UNIT_ASSERT_VALUES_EQUAL(1, initialBackpressure.CompactionScore);
        UNIT_ASSERT_VALUES_EQUAL(1, initialBackpressure.DiskSpaceScore);
        UNIT_ASSERT_VALUES_EQUAL(1, initialBackpressure.CleanupScore);

        state.AddFreshBlob(1, 400_KB, TInstant::Zero());
        state.GetCompactionMap().Update(0, 10, 10, 10, 0, 0, false);
        state.GetCleanupQueue().Add({{1, 1, 4, 4_MB, 0, 0}, 111, {}});

        const auto marginalBackpressure = state.CalculateCurrentBackpressure();
        UNIT_ASSERT_DOUBLES_EQUAL(
            1,
            marginalBackpressure.FreshIndexScore,
            1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(
            1,
            marginalBackpressure.CompactionScore,
            1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(1, marginalBackpressure.CleanupScore, 1e-5);

        // Backpressure caused by increased FreshBlobByteCount
        {
            state.AddFreshBlob(2, 50 * 4096, TInstant::Zero());

            const auto bp = state.CalculateCurrentBackpressure();
            UNIT_ASSERT_DOUBLES_EQUAL(2.5, bp.FreshIndexScore, 1e-5);
        }

        state.AddFreshBlob(3, 300 * 4_KB, TInstant::Zero());
        state.GetCompactionMap().Update(0, 30, 30, 30, 0, 0, false);
        state.GetCleanupQueue().Add({{1, 2, 4, 4_MB, 0, 0}, 111, {}});

        const auto maxBackpressure = state.CalculateCurrentBackpressure();
        UNIT_ASSERT_DOUBLES_EQUAL(10, maxBackpressure.FreshIndexScore, 1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(10, maxBackpressure.CompactionScore, 1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(10, maxBackpressure.CleanupScore, 1e-5);

        state.GetCompactionMap().Update(0, 100, 100, 100, 0, 0, false);

        const auto maxBackpressure2 = state.CalculateCurrentBackpressure();
        UNIT_ASSERT_DOUBLES_EQUAL(10, maxBackpressure2.CompactionScore, 1e-5);

        state.AccessCheckpoints().Add({"c1", 3, "idemp", Now(), {}});

        const auto maxBackpressure3 = state.CalculateCurrentBackpressure();
        UNIT_ASSERT_DOUBLES_EQUAL(10, maxBackpressure3.FreshIndexScore, 1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(10, maxBackpressure3.CompactionScore, 1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(0, maxBackpressure3.CleanupScore, 1e-5);
    }

    Y_UNIT_TEST(CompactionBackpressureShouldBeZeroIfNotRequiredByPolicy)
    {
        auto state = MakeState(
            1000,
            {.CompactionPolicy = std::make_shared<TNoBackpressurePolicy>()});

        state.GetCompactionMap().Update(0, 30, 30, 30, 0, 0, false);

        const auto bp = state.CalculateCurrentBackpressure();
        UNIT_ASSERT_VALUES_EQUAL(0, bp.CompactionScore);
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateUsedBlocksCount)
    {
        auto config = DefaultConfig(1, DefaultBlockCount);

        config.MutableConfig()->SetBaseDiskId("baseDiskID");
        config.MutableConfig()->SetBaseDiskCheckpointId("baseDiskCheckpointId");

        auto state = MakeState(std::move(config));

        state.GetLogicalUsedBlocks().Set(0, 9);
        state.IncrementLogicalUsedBlocksCount(10);

        TTestExecutor executor;
        executor.WriteTx([&](TPartitionDatabase db) { db.InitSchema(); });

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                state.SetUsedBlocks(
                    db,
                    TBlockRange32::MakeClosedInterval(100, 110),
                    0);
            });
        UNIT_ASSERT_EQUAL(11, state.GetUsedBlocksCount());
        UNIT_ASSERT_EQUAL(21, state.GetLogicalUsedBlocksCount());

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                state.SetUsedBlocks(
                    db,
                    TBlockRange32::MakeClosedInterval(105, 130),
                    0);
            });
        UNIT_ASSERT_EQUAL(31, state.GetUsedBlocksCount());
        UNIT_ASSERT_EQUAL(41, state.GetLogicalUsedBlocksCount());

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                state.UnsetUsedBlocks(
                    db,
                    TBlockRange32::MakeClosedInterval(106, 115));
            });
        UNIT_ASSERT_EQUAL(21, state.GetUsedBlocksCount());
        UNIT_ASSERT_EQUAL(31, state.GetLogicalUsedBlocksCount());

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                state.UnsetUsedBlocks(
                    db,
                    TBlockRange32::MakeClosedInterval(109, 110));
            });
        UNIT_ASSERT_EQUAL(21, state.GetUsedBlocksCount());
        UNIT_ASSERT_EQUAL(31, state.GetLogicalUsedBlocksCount());

        executor.WriteTx(
            [&](TPartitionDatabase db)
            { state.SetUsedBlocks(db, {101, 102, 103, 106, 108}); });
        UNIT_ASSERT_EQUAL(23, state.GetUsedBlocksCount());
        UNIT_ASSERT_EQUAL(33, state.GetLogicalUsedBlocksCount());

        executor.WriteTx([&](TPartitionDatabase db)
                         { state.UnsetUsedBlocks(db, {108, 120, 250}); });
        UNIT_ASSERT_EQUAL(21, state.GetUsedBlocksCount());
        UNIT_ASSERT_EQUAL(31, state.GetLogicalUsedBlocksCount());
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateCheckpointBytes)
    {
        auto state = MakeState(10_GB / DefaultBlockSize);

        state.IncrementMergedBlocksCount(5_GB / DefaultBlockSize);
        TCheckpoint checkpoint;
        checkpoint.CheckpointId = "c1";
        checkpoint.CommitId = 1;
        checkpoint.Stats.CopyFrom(state.GetStats());
        state.AccessCheckpoints().Add(checkpoint);

        state.IncrementMixedBlocksCount(2_GB / DefaultBlockSize);

        checkpoint.CheckpointId = "c2";
        checkpoint.CommitId = 2;
        checkpoint.Stats.CopyFrom(state.GetStats());
        state.AccessCheckpoints().Add(checkpoint);

        UNIT_ASSERT_VALUES_EQUAL(7_GB, state.CalculateCheckpointBytes());
    }

    Y_UNIT_TEST(ShouldStoreBlocksInMixedCache)
    {
        auto config = DefaultConfig(1, 10_GB / DefaultBlockSize);

        auto state = MakeState(std::move(config), {.MixedIndexCacheSize = 1});

        TTestExecutor executor;
        executor.WriteTx([&](TPartitionDatabase db) { db.InitSchema(); });

        constexpr ui32 rangeIdx = 0;
        TVector<TMixedBlock> blocks = {
            {{1, 1}, 1, 1, 1, 1},
            {{2, 2}, 2, 2, 2, 2},
            {{3, 3}, 3, 3, 3, 3},
            {{4, 4}, 4, 4, 4, 4},
            {{5, 5}, 5, 5, 5, 5}};

        auto mixedBlocksCompatator = [](const auto& lhs, const auto& rhs)
        {
            return lhs.BlockIndex < rhs.BlockIndex;
        };

        // range is warm now: mixed blocks are not cached
        state.RaiseRangeTemperature(rangeIdx);

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                state.WriteMixedBlock(db, blocks[0]);
                state.WriteMixedBlock(db, blocks[1]);
            });

        TVector<TMixedBlock> actual;

        struct TVisitor final: public IMixedBlocksIndexVisitor
        {
            TVector<TMixedBlock>& Blocks;

            TVisitor(TVector<TMixedBlock>& blocks)
                : Blocks(blocks)
            {}

            bool VisitBlock(
                ui32 blockIndex,
                ui64 commitId,
                const TPartialBlobId& blobId,
                ui16 blobOffset,
                ui8 compactionRangeCount) override
            {
                Blocks.emplace_back(
                    blobId,
                    commitId,
                    blockIndex,
                    blobOffset,
                    compactionRangeCount);
                return true;
            }

        } visitor{actual};

        // should read mixed blocks from db and place them into cache
        executor.WriteTx(
            [&](TPartitionDatabase db)
            { state.FindMixedBlocksForCompaction(db, visitor, rangeIdx); });

        Sort(actual, mixedBlocksCompatator);
        ASSERT_VECTORS_EQUAL(
            TVector<TMixedBlock>({blocks[0], blocks[1]}),
            actual);

        // range is hot now
        state.RaiseRangeTemperature(rangeIdx);

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                state.DeleteMixedBlock(
                    db,
                    blocks[1].BlockIndex,
                    blocks[1].CommitId);
                state.WriteMixedBlock(db, blocks[2]);
                state.WriteMixedBlock(db, blocks[3]);
            });

        actual.clear();

        executor.WriteTx(
            [&](TPartitionDatabase db)
            { state.FindMixedBlocksForCompaction(db, visitor, rangeIdx); });

        Sort(actual, mixedBlocksCompatator);
        ASSERT_VECTORS_EQUAL(
            TVector<TMixedBlock>({blocks[0], blocks[2], blocks[3]}),
            actual);

        // kick range from cache
        state.RaiseRangeTemperature(rangeIdx + 1);

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                state.DeleteMixedBlock(
                    db,
                    blocks[2].BlockIndex,
                    blocks[2].CommitId);
                state.WriteMixedBlock(db, blocks[4]);
            });

        actual.clear();

        // should read from db
        executor.WriteTx(
            [&](TPartitionDatabase db)
            { state.FindMixedBlocksForCompaction(db, visitor, rangeIdx); });

        Sort(actual, mixedBlocksCompatator);
        ASSERT_VECTORS_EQUAL(
            TVector<TMixedBlock>({blocks[0], blocks[3], blocks[4]}),
            actual);
    }

    void CheckMaxBlobsPerDisk(
        ui64 diskSize,
        ui64 allocationUnit,
        ui32 maxBlobsPerUnit,
        ui32 maxBlobsPerDisk,
        ui32 blockSize = DefaultBlockSize)
    {
        auto config = DefaultConfig(1, diskSize / blockSize);
        config.MutableConfig()->SetBlockSize(blockSize);

        auto state = MakeState(
            std::move(config),
            {
                .MixedIndexCacheSize = 1,
                .AllocationUnit = allocationUnit,
                .MaxBlobsPerUnit = maxBlobsPerUnit,
            });
        UNIT_ASSERT_VALUES_EQUAL(maxBlobsPerDisk, state.GetMaxBlobsPerDisk());
    }

    Y_UNIT_TEST(CheckMaxBlobsPerDisk)
    {
        CheckMaxBlobsPerDisk(320_GB, 32_GB, 100, 1000);
        CheckMaxBlobsPerDisk(32_GB, 32_GB, 100, 100);
        CheckMaxBlobsPerDisk(48_GB, 32_GB, 100, 150);
        CheckMaxBlobsPerDisk(16_GB, 32_GB, 100, 50);
        CheckMaxBlobsPerDisk(10_GB, 32_GB, 100, 32);
        CheckMaxBlobsPerDisk(32_GB, 256_GB, 800, 100);
        CheckMaxBlobsPerDisk(48_GB, 256_GB, 800, 150);
        CheckMaxBlobsPerDisk(48_GB, 32_GB, 100, 150, 16_KB);
    }

    Y_UNIT_TEST(ShouldRoundMaxBlobsPerDiskUp)
    {
        CheckMaxBlobsPerDisk(48_GB, 32_GB, 1, 2);
        CheckMaxBlobsPerDisk(DefaultBlockSize, 32_GB, 1, 1);
    }

    Y_UNIT_TEST(ShouldKeepMaxBlobsPerDiskDisabled)
    {
        CheckMaxBlobsPerDisk(320_GB, 32_GB, 0, 0);
        CheckMaxBlobsPerDisk(48_GB, 32_GB, 0, 0);
        CheckMaxBlobsPerDisk(10_GB, 32_GB, 0, 0);
    }

    Y_UNIT_TEST(ShouldCalculateMaxBlobsPerDiskWithoutOverflow)
    {
        CheckMaxBlobsPerDisk(32_GB, 32_GB, 1'000'000'000, 1'000'000'000);
        CheckMaxBlobsPerDisk(48_GB, 32_GB, 1'000'000'000, 1'500'000'000);
        CheckMaxBlobsPerDisk(32_GB, 32_GB, Max<ui32>(), Max<ui32>());
        CheckMaxBlobsPerDisk(48_GB, 32_GB, Max<ui32>(), Max<ui32>());
    }

    Y_UNIT_TEST(ShouldTreatAllocationUnitSmallerThanBlockAsOneBlock)
    {
        // 1_MB disk with 4_KB blocks has 256 blocks, each block is a unit.
        CheckMaxBlobsPerDisk(1_MB, 0, 1, 256);
        CheckMaxBlobsPerDisk(1_MB, 1, 2, 512);
        CheckMaxBlobsPerDisk(1_MB, DefaultBlockSize - 1, 3, 768);
        CheckMaxBlobsPerDisk(1_MB, 4_KB, 1, 64, 16_KB);
        CheckMaxBlobsPerDisk(32_GB, 1, Max<ui32>(), Max<ui32>());
    }

    Y_UNIT_TEST(ShouldTrackCleanupQueueBlockCount)
    {
        auto state = MakeState(1000);

        TCleanupQueueItem b1{{1, 1, 4, 4_MB, 0, 0}, 111, {}};
        TCleanupQueueItem b2{{1, 2, 4, 4096, 0, 0}, 112, {}};

        state.GetCleanupQueue().Add(b2);
        state.GetCleanupQueue().Add(b1);

        UNIT_ASSERT_VALUES_EQUAL(
            1025,
            state.GetCleanupQueue().GetQueueBlocks());

        state.GetCleanupQueue().Remove(b1);
        UNIT_ASSERT_VALUES_EQUAL(1, state.GetCleanupQueue().GetQueueBlocks());

        state.GetCleanupQueue().Remove(b2);
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetCleanupQueue().GetQueueBlocks());
    }

    Y_UNIT_TEST(ShouldCalculateNewlyZeroedBlocks)
    {
        auto state = MakeState();

        const ui32 blockIndex = 0;

        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            state.CalculateNewlyZeroedBlocks(blockIndex, 0));

        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            state.CalculateNewlyZeroedBlocks(blockIndex, 10));

        state.GetCompactionMap().Update(
            blockIndex,
            1 /*blobCount=*/,
            15 /*blockCount=*/,
            10 /*usedBlockCount=*/,
            5 /*newlyZeroedBlocks=*/,
            0 /*mixedBlockCount=*/,
            false /*compacted=*/);

        UNIT_ASSERT_VALUES_EQUAL(
            5u,
            state.CalculateNewlyZeroedBlocks(blockIndex, 10));

        UNIT_ASSERT_VALUES_EQUAL(
            2u,
            state.CalculateNewlyZeroedBlocks(blockIndex, 13));

        UNIT_ASSERT_VALUES_EQUAL(
            7u,
            state.CalculateNewlyZeroedBlocks(blockIndex, 8));

        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            state.CalculateNewlyZeroedBlocks(blockIndex, 30));
    }

    Y_UNIT_TEST(ShouldGetMinAndMaxCheckpointCommitId)
    {
        auto state = MakeState();

        UNIT_ASSERT_VALUES_EQUAL(
            InvalidCommitId,
            state.GetMinCheckpointCommitId());
        UNIT_ASSERT_VALUES_EQUAL(0u, state.GetMaxCheckpointCommitId());

        state.AccessCheckpoints().Add({"c1", 10, "idemp1", Now(), {}});
        state.AccessCheckpoints().Add({"c2", 30, "idemp2", Now(), {}});

        UNIT_ASSERT_VALUES_EQUAL(10u, state.GetMinCheckpointCommitId());
        UNIT_ASSERT_VALUES_EQUAL(30u, state.GetMaxCheckpointCommitId());

        UNIT_ASSERT(state.AccessCheckpointsInFlight()->AddTx("c3", nullptr, 5));
        UNIT_ASSERT(
            state.AccessCheckpointsInFlight()->AddTx("c4", nullptr, 40));

        UNIT_ASSERT_VALUES_EQUAL(5u, state.GetMinCheckpointCommitId());
        UNIT_ASSERT_VALUES_EQUAL(40u, state.GetMaxCheckpointCommitId());

        state.AccessCheckpointsInFlight()->PopTx("c3");
        state.AccessCheckpointsInFlight()->PopTx("c4");

        UNIT_ASSERT_VALUES_EQUAL(10u, state.GetMinCheckpointCommitId());
        UNIT_ASSERT_VALUES_EQUAL(30u, state.GetMaxCheckpointCommitId());
    }

    Y_UNIT_TEST(ShouldGetCleanupCommitId)
    {
        auto generateCommitIds = [](TPartitionState& state)
        {
            for (ui32 i = 0; i < 100; ++i) {
                state.GenerateCommitId();
            }
        };

        auto disabled = MakeState();
        auto enabled = MakeState(
            DefaultBlockCount,
            {.CheckpointAwareCleanupEnabled = true});
        generateCommitIds(disabled);
        generateCommitIds(enabled);

        const ui64 lastCommitId = disabled.GetLastCommitId();
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(0, 100), lastCommitId);

        UNIT_ASSERT_VALUES_EQUAL(lastCommitId, disabled.GetCleanupCommitId());
        UNIT_ASSERT_VALUES_EQUAL(lastCommitId, enabled.GetCleanupCommitId());

        const ui64 barrierCommitId = MakeCommitId(0, 60);
        disabled.GetCleanupQueue().AcquireBarrier(barrierCommitId);
        enabled.GetCleanupQueue().AcquireBarrier(barrierCommitId);

        UNIT_ASSERT_VALUES_EQUAL(
            barrierCommitId - 1,
            disabled.GetCleanupCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            barrierCommitId - 1,
            enabled.GetCleanupCommitId());

        const ui64 checkpointCommitId = MakeCommitId(0, 40);
        disabled.AccessCheckpoints().Add(
            {"c1", checkpointCommitId, "idemp", Now(), {}});
        enabled.AccessCheckpoints().Add(
            {"c1", checkpointCommitId, "idemp", Now(), {}});

        UNIT_ASSERT_VALUES_EQUAL(
            checkpointCommitId - 1,
            disabled.GetCleanupCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            barrierCommitId - 1,
            enabled.GetCleanupCommitId());
    }

    Y_UNIT_TEST(ShouldDetectWhenBlobCountToCleanupReachedThreshold)
    {
        auto addBlobs = [](TPartitionState& state)
        {
            state.GetCleanupQueue().Add(
                {{1, 1, 4, 4_KB, 0, 0}, MakeCommitId(0, 10), {}});
            state.GetCleanupQueue().Add(
                {{1, 2, 4, 4_KB, 0, 0}, MakeCommitId(0, 20), {}});
            state.GetCleanupQueue().Add(
                {{1, 3, 4, 4_KB, 0, 0}, MakeCommitId(0, 30), {}});
        };

        auto enabled = MakeState(
            DefaultBlockCount,
            {.CheckpointAwareCleanupEnabled = true});
        auto disabled = MakeState();

        const ui64 cleanupCommitId = MakeCommitId(0, 100);
        UNIT_ASSERT(
            !enabled.HasBlobCountToCleanupReachedThreshold(cleanupCommitId, 1));
        UNIT_ASSERT(!disabled.HasBlobCountToCleanupReachedThreshold(
            cleanupCommitId,
            1));

        addBlobs(enabled);
        addBlobs(disabled);

        UNIT_ASSERT(!enabled.HasBlobCountToCleanupReachedThreshold(
            MakeCommitId(0, 15),
            2));
        UNIT_ASSERT(
            enabled.HasBlobCountToCleanupReachedThreshold(cleanupCommitId, 3));
        UNIT_ASSERT(
            disabled.HasBlobCountToCleanupReachedThreshold(cleanupCommitId, 3));

        // Default milestone bounds are (0, 0), so the update is applied.
        const TPartialBlobId milestoneBlobId(1, 2, 4, 4_KB, 0, 0);
        enabled.UpdateCleanupMilestoneIfNeeded(
            MakeCommitId(0, 20),
            milestoneBlobId,
            0,
            0);
        disabled.UpdateCleanupMilestoneIfNeeded(
            MakeCommitId(0, 20),
            milestoneBlobId,
            0,
            0);

        // Checkpoint-aware cleanup respects the milestone.
        UNIT_ASSERT(
            !enabled.HasBlobCountToCleanupReachedThreshold(cleanupCommitId, 2));
        UNIT_ASSERT(
            enabled.HasBlobCountToCleanupReachedThreshold(cleanupCommitId, 1));

        // Non-checkpoint-aware cleanup ignores the milestone and still sees
        // all blobs.
        UNIT_ASSERT(
            disabled.HasBlobCountToCleanupReachedThreshold(cleanupCommitId, 3));
    }

    Y_UNIT_TEST(ShouldDetectWhenBlobCountToCleanupDroppedBelowThreshold)
    {
        const ui64 deletionCommitId = MakeCommitId(0, 10);
        const ui64 cleanupCommitId = MakeCommitId(0, 100);

        for (bool checkpointAware: {false, true}) {
            auto state = MakeState(
                DefaultBlockCount,
                {.CheckpointAwareCleanupEnabled = checkpointAware});

            TVector<TCleanupQueueItem> items;
            for (ui32 i = 1; i <= 3; ++i) {
                items.push_back({{1, i, 4, 4_KB, 0, 0}, deletionCommitId, {}});
                state.GetCleanupQueue().Add(items.back());
            }

            UNIT_ASSERT(state.HasBlobCountToCleanupReachedThreshold(
                cleanupCommitId,
                3));

            state.RemoveCleanupQueueItem(items[0]);

            UNIT_ASSERT(!state.HasBlobCountToCleanupReachedThreshold(
                cleanupCommitId,
                3));
            UNIT_ASSERT(state.HasBlobCountToCleanupReachedThreshold(
                cleanupCommitId,
                2));

            // A lower threshold is reached regardless of the queue size.
            UNIT_ASSERT(state.HasBlobCountToCleanupReachedThreshold(
                cleanupCommitId,
                1));

            // Blobs deleted after cleanupCommitId are not counted.
            UNIT_ASSERT(!state.HasBlobCountToCleanupReachedThreshold(
                deletionCommitId - 1,
                1));
        }
    }

    Y_UNIT_TEST(ShouldUpdateCleanupMilestoneIfNeeded)
    {
        auto state = MakeState(
            DefaultBlockCount,
            {.CheckpointAwareCleanupEnabled = true});
        auto disabled = MakeState();

        const ui64 minCheckpointCommitId = MakeCommitId(0, 10);
        const ui64 maxCheckpointCommitId = MakeCommitId(0, 20);
        const ui64 milestoneCommitId = MakeCommitId(0, 15);
        const TPartialBlobId milestoneBlobId(1, 7);

        // Stale checkpoint bounds: milestone is not updated.
        state.UpdateCleanupMilestoneIfNeeded(
            milestoneCommitId,
            milestoneBlobId,
            minCheckpointCommitId,
            maxCheckpointCommitId);

        UNIT_ASSERT_VALUES_EQUAL(0u, state.GetCleanupMilestoneCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            TPartialBlobId(),
            state.GetCleanupMilestoneBlobId());
        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            state.GetMeta().GetCleanupMilestone().GetMinCheckpointCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            state.GetMeta().GetCleanupMilestone().GetMaxCheckpointCommitId());

        state.AccessCheckpoints().Add(
            {"c1", minCheckpointCommitId, "idemp1", Now(), {}});
        state.AccessCheckpoints().Add(
            {"c2", maxCheckpointCommitId, "idemp2", Now(), {}});
        state.ResetCleanupMilestoneIfNeeded();

        UNIT_ASSERT_VALUES_EQUAL(0u, state.GetCleanupMilestoneCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            minCheckpointCommitId,
            state.GetMeta().GetCleanupMilestone().GetMinCheckpointCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            maxCheckpointCommitId,
            state.GetMeta().GetCleanupMilestone().GetMaxCheckpointCommitId());

        // Matching checkpoint bounds: milestone position is updated.
        state.UpdateCleanupMilestoneIfNeeded(
            milestoneCommitId,
            milestoneBlobId,
            minCheckpointCommitId,
            maxCheckpointCommitId);

        UNIT_ASSERT_VALUES_EQUAL(
            milestoneCommitId,
            state.GetCleanupMilestoneCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            milestoneBlobId,
            state.GetCleanupMilestoneBlobId());

        disabled.UpdateCleanupMilestoneIfNeeded(
            milestoneCommitId,
            milestoneBlobId,
            0,
            0);
        // Non-checkpoint-aware getters always return an empty milestone.
        UNIT_ASSERT_VALUES_EQUAL(0u, disabled.GetCleanupMilestoneCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            TPartialBlobId(),
            disabled.GetCleanupMilestoneBlobId());

        const ui64 advancedCommitId = MakeCommitId(0, 18);
        const TPartialBlobId advancedBlobId(1, 9);
        state.UpdateCleanupMilestoneIfNeeded(
            advancedCommitId,
            advancedBlobId,
            minCheckpointCommitId,
            maxCheckpointCommitId);

        UNIT_ASSERT_VALUES_EQUAL(
            advancedCommitId,
            state.GetCleanupMilestoneCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            advancedBlobId,
            state.GetCleanupMilestoneBlobId());

        // Checkpoint set changed, but milestone bounds were not reset yet:
        // update is ignored and the previous position is kept.
        const ui64 newMaxCheckpointCommitId = MakeCommitId(0, 30);
        state.AccessCheckpoints().Add(
            {"c3", newMaxCheckpointCommitId, "idemp3", Now(), {}});
        state.UpdateCleanupMilestoneIfNeeded(
            MakeCommitId(0, 19),
            TPartialBlobId(1, 11),
            minCheckpointCommitId,
            newMaxCheckpointCommitId);

        UNIT_ASSERT_VALUES_EQUAL(
            advancedCommitId,
            state.GetCleanupMilestoneCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            advancedBlobId,
            state.GetCleanupMilestoneBlobId());
        UNIT_ASSERT_VALUES_EQUAL(
            minCheckpointCommitId,
            state.GetMeta().GetCleanupMilestone().GetMinCheckpointCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            maxCheckpointCommitId,
            state.GetMeta().GetCleanupMilestone().GetMaxCheckpointCommitId());
    }

    Y_UNIT_TEST(ShouldResetCleanupMilestoneIfNeeded)
    {
        auto disabled = MakeState();
        disabled.AccessCheckpoints().Add(
            {"c1", MakeCommitId(0, 10), "idemp", Now(), {}});
        disabled.ResetCleanupMilestoneIfNeeded();
        // Flag is disabled: milestone bounds stay at the default.
        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            disabled.GetMeta()
                .GetCleanupMilestone()
                .GetMinCheckpointCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            disabled.GetMeta()
                .GetCleanupMilestone()
                .GetMaxCheckpointCommitId());

        auto state = MakeState(
            DefaultBlockCount,
            {.CheckpointAwareCleanupEnabled = true});

        const ui64 checkpointCommitId = MakeCommitId(0, 10);
        state.AccessCheckpoints().Add(
            {"c1", checkpointCommitId, "idemp", Now(), {}});

        const ui64 milestoneCommitId = MakeCommitId(0, 5);
        const TPartialBlobId milestoneBlobId(1, 3);

        // Align milestone checkpoint bounds with the current checkpoints,
        // then set the milestone position with the same bounds.
        state.ResetCleanupMilestoneIfNeeded();
        state.UpdateCleanupMilestoneIfNeeded(
            milestoneCommitId,
            milestoneBlobId,
            checkpointCommitId,
            checkpointCommitId);

        UNIT_ASSERT_VALUES_EQUAL(
            milestoneCommitId,
            state.GetCleanupMilestoneCommitId());

        // Bounds still match: milestone is preserved.
        state.ResetCleanupMilestoneIfNeeded();
        UNIT_ASSERT_VALUES_EQUAL(
            milestoneCommitId,
            state.GetCleanupMilestoneCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            milestoneBlobId,
            state.GetCleanupMilestoneBlobId());

        const ui64 checkpointCommitId2 = MakeCommitId(0, 20);
        state.AccessCheckpoints().Add(
            {"c2", checkpointCommitId2, "idemp2", Now(), {}});
        state.ResetCleanupMilestoneIfNeeded();

        // Bounds changed: milestone is reset.
        UNIT_ASSERT_VALUES_EQUAL(0u, state.GetCleanupMilestoneCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            TPartialBlobId(),
            state.GetCleanupMilestoneBlobId());
        UNIT_ASSERT_VALUES_EQUAL(
            checkpointCommitId,
            state.GetMeta().GetCleanupMilestone().GetMinCheckpointCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            checkpointCommitId2,
            state.GetMeta().GetCleanupMilestone().GetMaxCheckpointCommitId());

        state.UpdateCleanupMilestoneIfNeeded(
            milestoneCommitId,
            milestoneBlobId,
            checkpointCommitId,
            checkpointCommitId2);

        state.ResetCleanupMilestoneIfNeeded();
        UNIT_ASSERT_VALUES_EQUAL(
            milestoneCommitId,
            state.GetCleanupMilestoneCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            milestoneBlobId,
            state.GetCleanupMilestoneBlobId());

        state.AccessCheckpoints().Delete("c1");
        state.AccessCheckpoints().Delete("c2");
        state.ResetCleanupMilestoneIfNeeded();

        UNIT_ASSERT_VALUES_EQUAL(0u, state.GetCleanupMilestoneCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            InvalidCommitId,
            state.GetMeta().GetCleanupMilestone().GetMinCheckpointCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            state.GetMeta().GetCleanupMilestone().GetMaxCheckpointCommitId());
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition

template <>
inline void Out<NCloud::NBlockStore::NStorage::NPartition::TMixedBlock>(
    IOutputStream& out,
    const NCloud::NBlockStore::NStorage::NPartition::TMixedBlock& b)
{
    out << "[" << b.BlockIndex << ", " << b.CommitId << ", " << b.BlobId << ", "
        << b.BlobOffset << "]";
}
