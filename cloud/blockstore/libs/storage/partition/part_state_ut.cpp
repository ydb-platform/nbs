#include "part_state.h"

#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/partition_common/part_thread_safe_state.h>
#include <cloud/blockstore/libs/storage/partition/part_schema.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

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
            30,     // compaction score limit
            10,     // compaction score threshold
            10,     // compaction score feature max value
        },
        {
            1600_KB,// fresh byte count limit
            400_KB, // fresh byte count threshold
            10,     // fresh byte count feature max value
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

struct TNoBackpressurePolicy
    : ICompactionPolicy
{
    TCompactionScore CalculateScore(const TRangeStat& stat) const override
    {
        return stat.BlobCount;
    }

    bool BackpressureEnabled() const override
    {
        return false;
    }
};

// TODO: use this function in other tests.
TPartitionState MakeState(
    size_t blockCount = DefaultBlockCount,
    bool checkpointAwareCleanupEnabled = false)
{
    auto threadSafeState = std::make_shared<TPartitionThreadSafeState>();
    return TPartitionState(
        DefaultConfig(1, blockCount),
        BuildDefaultCompactionPolicy(5),
        0,   // compactionScoreHistorySize
        0,   // cleanupScoreHistorySize
        DefaultBPConfig(),
        DefaultFreeSpaceConfig(),
        Max(),   // maxIORequestsInFlight
        0,       // reassignChannelsPercentageThreshold
        100,     // reassignFreshChannelsPercentageThreshold
        100,     // reassignMixedChannelsPercentageThreshold
        false,   // reassignSystemChannelsImmediately
        5,       // channelCount
        0,       // mixedIndexCacheSize
        10000,   // allocationUnit
        100,     // maxBlobsPerUnit
        10,      // maxBlobsPerRange,
        1,       // compactionRangeCountPerRun
        std::move(threadSafeState),
        0,             // tabletId
        std::nullopt,  // mixedBlocksFilterConfig
        checkpointAwareCleanupEnabled
    );
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionStateTest)
{
    Y_UNIT_TEST(CalculateCurrentBackpressure)
    {
        auto threadSafeState =
            std::make_shared<TPartitionThreadSafeState>();
        TPartitionState state(
            DefaultConfig(1, 1000),
            BuildDefaultCompactionPolicy(5),
            0,   // compactionScoreHistorySize
            0,   // cleanupScoreHistorySize
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            Max(),   // maxIORequestsInFlight
            0,       // reassignChannelsPercentageThreshold
            100,     // reassignFreshChannelsPercentageThreshold
            100,     // reassignMixedChannelsPercentageThreshold
            false,   // reassignSystemChannelsImmediately
            5,       // channelCount
            0,       // mixedIndexCacheSize
            10000,   // allocationUnit
            100,     // maxBlobsPerUnit
            10,      // maxBlobsPerRange,
            1,       // compactionRangeCountPerRun
            threadSafeState,
            0,             // tabletId
            std::nullopt,  // mixedBlocksFilterConfig
            false          // checkpointAwareCleanupEnabled
        );

        const auto initialBackpressure = state.CalculateCurrentBackpressure();
        UNIT_ASSERT_VALUES_EQUAL(1, initialBackpressure.FreshIndexScore);
        UNIT_ASSERT_VALUES_EQUAL(1, initialBackpressure.CompactionScore);
        UNIT_ASSERT_VALUES_EQUAL(1, initialBackpressure.DiskSpaceScore);
        UNIT_ASSERT_VALUES_EQUAL(1, initialBackpressure.CleanupScore);

        state.AddFreshBlob(1, 400_KB, TInstant::Zero());
        state.GetCompactionMap().Update(0, 10, 10, 10, 0, false);
        state.GetCleanupQueue().Add({{1, 1, 4, 4_MB, 0, 0}, 111, {}});

        const auto marginalBackpressure = state.CalculateCurrentBackpressure();
        UNIT_ASSERT_DOUBLES_EQUAL(1, marginalBackpressure.FreshIndexScore, 1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(1, marginalBackpressure.CompactionScore, 1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(1, marginalBackpressure.CleanupScore, 1e-5);

        // Backpressure caused by increased FreshBlobByteCount
        {
            state.AddFreshBlob(2, 50 * 4096, TInstant::Zero());

            const auto bp = state.CalculateCurrentBackpressure();
            UNIT_ASSERT_DOUBLES_EQUAL(2.5, bp.FreshIndexScore, 1e-5);
        }

        state.AddFreshBlob(3, 300 * 4_KB, TInstant::Zero());
        state.GetCompactionMap().Update(0, 30, 30, 30, 0, false);
        state.GetCleanupQueue().Add({{1, 2, 4, 4_MB, 0, 0}, 111, {}});

        const auto maxBackpressure = state.CalculateCurrentBackpressure();
        UNIT_ASSERT_DOUBLES_EQUAL(10, maxBackpressure.FreshIndexScore, 1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(10, maxBackpressure.CompactionScore, 1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(10, maxBackpressure.CleanupScore, 1e-5);

        state.GetCompactionMap().Update(0, 100, 100, 100, 0, false);

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
        auto threadSafeState =
            std::make_shared<TPartitionThreadSafeState>();
        TPartitionState state(
            DefaultConfig(1, 1000),
            std::make_shared<TNoBackpressurePolicy>(),
            0,   // compactionScoreHistorySize
            0,   // cleanupScoreHistorySize
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            Max(),   // maxIORequestsInFlight
            0,       // reassignChannelsPercentageThreshold
            100,     // reassignFreshChannelsPercentageThreshold
            100,     // reassignMixedChannelsPercentageThreshold
            false,   // reassignSystemChannelsImmediately
            5,       // channelCount
            0,       // mixedIndexCacheSize
            10000,   // allocationUnit
            100,     // maxBlobsPerUnit
            10,      // maxBlobsPerRange,
            1,       // compactionRangeCountPerRun
            threadSafeState,
            0,             // tabletId
            std::nullopt,  // mixedBlocksFilterConfig
            false          // checkpointAwareCleanupEnabled
        );

        state.GetCompactionMap().Update(0, 30, 30, 30, 0, false);

        const auto bp = state.CalculateCurrentBackpressure();
        UNIT_ASSERT_VALUES_EQUAL(0, bp.CompactionScore);
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateUsedBlocksCount)
    {
        auto config = DefaultConfig(1, DefaultBlockCount);

        config.MutableConfig()->SetBaseDiskId("baseDiskID");
        config.MutableConfig()->SetBaseDiskCheckpointId("baseDiskCheckpointId");

        auto threadSafeState =
            std::make_shared<TPartitionThreadSafeState>();
        TPartitionState state(
            config,
            BuildDefaultCompactionPolicy(5),
            0,   // compactionScoreHistorySize
            0,   // cleanupScoreHistorySize
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            Max(),   // maxIORequestsInFlight
            0,       // reassignChannelsPercentageThreshold
            100,     // reassignFreshChannelsPercentageThreshold
            100,     // reassignMixedChannelsPercentageThreshold
            false,   // reassignSystemChannelsImmediately
            5,       // channelCount
            0,       // mixedIndexCacheSize
            10000,   // allocationUnit
            100,     // maxBlobsPerUnit
            10,      // maxBlobsPerRange,
            1,       // compactionRangeCountPerRun
            threadSafeState,
            0,             // tabletId
            std::nullopt,  // mixedBlocksFilterConfig
            false          // checkpointAwareCleanupEnabled
        );

        state.GetLogicalUsedBlocks().Set(0, 9);
        state.IncrementLogicalUsedBlocksCount(10);

        TTestExecutor executor;
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx(
            [&](TPartitionDatabase db) {
                state.SetUsedBlocks(
                    db,
                    TBlockRange32::MakeClosedInterval(100, 110),
                    0);
            });
        UNIT_ASSERT_EQUAL(11, state.GetUsedBlocksCount());
        UNIT_ASSERT_EQUAL(21, state.GetLogicalUsedBlocksCount());

        executor.WriteTx(
            [&](TPartitionDatabase db) {
                state.SetUsedBlocks(
                    db,
                    TBlockRange32::MakeClosedInterval(105, 130),
                    0);
            });
        UNIT_ASSERT_EQUAL(31, state.GetUsedBlocksCount());
        UNIT_ASSERT_EQUAL(41, state.GetLogicalUsedBlocksCount());

        executor.WriteTx(
            [&](TPartitionDatabase db) {
                state.UnsetUsedBlocks(
                    db,
                    TBlockRange32::MakeClosedInterval(106, 115));
            });
        UNIT_ASSERT_EQUAL(21, state.GetUsedBlocksCount());
        UNIT_ASSERT_EQUAL(31, state.GetLogicalUsedBlocksCount());

        executor.WriteTx(
            [&](TPartitionDatabase db) {
                state.UnsetUsedBlocks(
                    db,
                    TBlockRange32::MakeClosedInterval(109, 110));
            });
        UNIT_ASSERT_EQUAL(21, state.GetUsedBlocksCount());
        UNIT_ASSERT_EQUAL(31, state.GetLogicalUsedBlocksCount());

        executor.WriteTx([&] (TPartitionDatabase db) {
            state.SetUsedBlocks(db, {101, 102, 103, 106, 108});
        });
        UNIT_ASSERT_EQUAL(23, state.GetUsedBlocksCount());
        UNIT_ASSERT_EQUAL(33, state.GetLogicalUsedBlocksCount());

        executor.WriteTx([&] (TPartitionDatabase db) {
            state.UnsetUsedBlocks(db, {108, 120, 250});
        });
        UNIT_ASSERT_EQUAL(21, state.GetUsedBlocksCount());
        UNIT_ASSERT_EQUAL(31, state.GetLogicalUsedBlocksCount());
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateCheckpointBytes)
    {
        auto config = DefaultConfig(1, 10_GB / DefaultBlockSize);

        auto threadSafeState =
            std::make_shared<TPartitionThreadSafeState>();
        TPartitionState state(
            config,
            BuildDefaultCompactionPolicy(5),
            0,   // compactionScoreHistorySize
            0,   // cleanupScoreHistorySize
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            Max(),   // maxIORequestsInFlight
            0,       // reassignChannelsPercentageThreshold
            100,     // reassignFreshChannelsPercentageThreshold
            100,     // reassignMixedChannelsPercentageThreshold
            false,   // reassignSystemChannelsImmediately
            1,       // channelCount
            0,       // mixedIndexCacheSize
            10000,   // allocationUnit
            100,     // maxBlobsPerUnit
            10,      // maxBlobsPerRange,
            1,       // compactionRangeCountPerRun
            threadSafeState,
            0,             // tabletId
            std::nullopt,  // mixedBlocksFilterConfig
            false          // checkpointAwareCleanupEnabled
        );

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

        auto threadSafeState =
            std::make_shared<TPartitionThreadSafeState>();
        TPartitionState state(
            config,
            BuildDefaultCompactionPolicy(5),
            0,   // compactionScoreHistorySize
            0,   // cleanupScoreHistorySize
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            Max(),   // maxIORequestsInFlight
            0,       // reassignChannelsPercentageThreshold
            100,     // reassignFreshChannelsPercentageThreshold
            100,     // reassignMixedChannelsPercentageThreshold
            false,   // reassignSystemChannelsImmediately
            5,       // channelCount
            1,       // mixedIndexCacheSize
            10000,   // allocationUnit
            100,     // maxBlobsPerUnit
            10,      // maxBlobsPerRange,
            1,       // compactionRangeCountPerRun
            threadSafeState,
            0,             // tabletId
            std::nullopt,  // mixedBlocksFilterConfig
            false          // checkpointAwareCleanupEnabled
        );

        TTestExecutor executor;
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });

        constexpr ui32 rangeIdx = 0;
        TVector<TMixedBlock> blocks = {
            { {1, 1}, 1, 1, 1, 1},
            { {2, 2}, 2, 2, 2, 2},
            { {3, 3}, 3, 3, 3, 3},
            { {4, 4}, 4, 4, 4, 4},
            { {5, 5}, 5, 5, 5, 5}
        };

        auto mixedBlocksCompatator = [](const auto& lhs, const auto& rhs) {
            return lhs.BlockIndex < rhs.BlockIndex;
        };

        // range is warm now: mixed blocks are not cached
        state.RaiseRangeTemperature(rangeIdx);

        executor.WriteTx([&] (TPartitionDatabase db) {
            state.WriteMixedBlock(db, blocks[0]);
            state.WriteMixedBlock(db, blocks[1]);
        });

        TVector<TMixedBlock> actual;

        struct TVisitor final
            : public IMixedBlocksIndexVisitor
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
        executor.WriteTx([&] (TPartitionDatabase db) {
            state.FindMixedBlocksForCompaction(db, visitor, rangeIdx);
        });

        Sort(actual, mixedBlocksCompatator);
        ASSERT_VECTORS_EQUAL(
            TVector<TMixedBlock>({blocks[0], blocks[1]}),
            actual
        );

        // range is hot now
        state.RaiseRangeTemperature(rangeIdx);

        executor.WriteTx([&] (TPartitionDatabase db) {
            state.DeleteMixedBlock(db, blocks[1].BlockIndex, blocks[1].CommitId);
            state.WriteMixedBlock(db, blocks[2]);
            state.WriteMixedBlock(db, blocks[3]);
        });

        actual.clear();

        executor.WriteTx([&] (TPartitionDatabase db) {
            state.FindMixedBlocksForCompaction(db, visitor, rangeIdx);
        });

        Sort(actual, mixedBlocksCompatator);
        ASSERT_VECTORS_EQUAL(
            TVector<TMixedBlock>({blocks[0], blocks[2], blocks[3]}),
            actual
        );

        // kick range from cache
        state.RaiseRangeTemperature(rangeIdx + 1);

        executor.WriteTx([&] (TPartitionDatabase db) {
            state.DeleteMixedBlock(db, blocks[2].BlockIndex, blocks[2].CommitId);
            state.WriteMixedBlock(db, blocks[4]);
        });

        actual.clear();

        // should read from db
        executor.WriteTx([&] (TPartitionDatabase db) {
            state.FindMixedBlocksForCompaction(db, visitor, rangeIdx);
        });

        Sort(actual, mixedBlocksCompatator);
        ASSERT_VECTORS_EQUAL(
            TVector<TMixedBlock>({blocks[0], blocks[3], blocks[4]}),
            actual
        );
    }

    void CheckMaxBlobsPerDisk(
        ui64 diskSize,
        ui64 allocationUnit,
        ui32 maxBlobsPerUnit,
        ui32 maxBlobsPerDisk)
    {
        auto config = DefaultConfig(1, diskSize / DefaultBlockSize);

        auto threadSafeState =
            std::make_shared<TPartitionThreadSafeState>();
        TPartitionState state(
            config,
            BuildDefaultCompactionPolicy(5),
            0,   // compactionScoreHistorySize
            0,   // cleanupScoreHistorySize
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            Max(),             // maxIORequestsInFlight
            0,                 // reassignChannelsPercentageThreshold
            100,               // reassignFreshChannelsPercentageThreshold
            100,               // reassignMixedChannelsPercentageThreshold
            false,             // reassignSystemChannelsImmediately
            5,                 // channelCount
            1,                 // mixedIndexCacheSize
            allocationUnit,    // allocationUnit
            maxBlobsPerUnit,   // maxBlobsPerUnit
            10,                // maxBlobsPerRange,
            1,                 // compactionRangeCountPerRun
            threadSafeState,
            0,             // tabletId
            std::nullopt,  // mixedBlocksFilterConfig
            false          // checkpointAwareCleanupEnabled
        );
        UNIT_ASSERT_VALUES_EQUAL(maxBlobsPerDisk, state.GetMaxBlobsPerDisk());
    }

    Y_UNIT_TEST(CheckMaxBlobsPerDisk)
    {
        CheckMaxBlobsPerDisk(320_GB, 32_GB, 100, 1000);
        CheckMaxBlobsPerDisk(320_GB, 32_GB, 0, 0);
        CheckMaxBlobsPerDisk(10_GB, 32_GB, 100, 100);
    }

    Y_UNIT_TEST(ShouldTrackCleanupQueueBlockCount)
    {
        auto threadSafeState =
            std::make_shared<TPartitionThreadSafeState>();
        TPartitionState state(
            DefaultConfig(1, 1000),
            BuildDefaultCompactionPolicy(5),
            0,   // compactionScoreHistorySize
            0,   // cleanupScoreHistorySize
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            Max(),   // maxIORequestsInFlight
            0,       // reassignChannelsPercentageThreshold
            100,     // reassignFreshChannelsPercentageThreshold
            100,     // reassignMixedChannelsPercentageThreshold
            false,   // reassignSystemChannelsImmediately
            5,       // channelCount
            0,       // mixedIndexCacheSize
            10000,   // allocationUnit
            100,     // maxBlobsPerUnit
            10,      // maxBlobsPerRange,
            1,       // compactionRangeCountPerRun
            threadSafeState,
            0,             // tabletId
            std::nullopt,  // mixedBlocksFilterConfig
            false          // checkpointAwareCleanupEnabled
        );

        TCleanupQueueItem b1 {{1, 1, 4, 4_MB, 0, 0}, 111, {}};
        TCleanupQueueItem b2 {{1, 2, 4, 4096, 0, 0}, 112, {}};

        state.GetCleanupQueue().Add(b2);
        state.GetCleanupQueue().Add(b1);

        UNIT_ASSERT_VALUES_EQUAL(
            1025,
            state.GetCleanupQueue().GetQueueBlocks());

        state.GetCleanupQueue().Remove(b1);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            state.GetCleanupQueue().GetQueueBlocks());

        state.GetCleanupQueue().Remove(b2);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            state.GetCleanupQueue().GetQueueBlocks());
    }

    Y_UNIT_TEST(ShouldCalculateNewlyZeroedBlocks)
    {
        auto threadSafeState =
            std::make_shared<TPartitionThreadSafeState>();
        TPartitionState state(
            DefaultConfig(1, DefaultBlockCount),
            BuildDefaultCompactionPolicy(5),
            0,   // compactionScoreHistorySize
            0,   // cleanupScoreHistorySize
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            Max(),   // maxIORequestsInFlight
            0,       // reassignChannelsPercentageThreshold
            100,     // reassignFreshChannelsPercentageThreshold
            100,     // reassignMixedChannelsPercentageThreshold
            false,   // reassignSystemChannelsImmediately
            5,       // channelCount
            0,       // mixedIndexCacheSize
            10000,   // allocationUnit
            100,     // maxBlobsPerUnit
            10,      // maxBlobsPerRange,
            1,       // compactionRangeCountPerRun
            threadSafeState,
            0,             // tabletId
            std::nullopt,  // mixedBlocksFilterConfig
            false          // checkpointAwareCleanupEnabled
        );

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

        auto disabled = MakeState(DefaultBlockCount, false);
        auto enabled = MakeState(DefaultBlockCount, true);
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

        auto enabled = MakeState(DefaultBlockCount, true);
        auto disabled = MakeState();

        const ui64 cleanupCommitId = MakeCommitId(0, 100);
        UNIT_ASSERT(!enabled.HasBlobCountToCleanupReachedThreshold(
            cleanupCommitId,
            1));
        UNIT_ASSERT(!disabled.HasBlobCountToCleanupReachedThreshold(
            cleanupCommitId,
            1));

        addBlobs(enabled);
        addBlobs(disabled);

        UNIT_ASSERT(!enabled.HasBlobCountToCleanupReachedThreshold(
            MakeCommitId(0, 15),
            2));
        UNIT_ASSERT(enabled.HasBlobCountToCleanupReachedThreshold(
            cleanupCommitId,
            3));
        UNIT_ASSERT(disabled.HasBlobCountToCleanupReachedThreshold(
            cleanupCommitId,
            3));

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
        UNIT_ASSERT(!enabled.HasBlobCountToCleanupReachedThreshold(
            cleanupCommitId,
            2));
        UNIT_ASSERT(enabled.HasBlobCountToCleanupReachedThreshold(
            cleanupCommitId,
            1));

        // Non-checkpoint-aware cleanup ignores the milestone and still sees
        // all blobs.
        UNIT_ASSERT(disabled.HasBlobCountToCleanupReachedThreshold(
            cleanupCommitId,
            3));
    }

    Y_UNIT_TEST(ShouldUpdateCleanupMilestoneIfNeeded)
    {
        auto state = MakeState(DefaultBlockCount, true);
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
            disabled.GetMeta().GetCleanupMilestone().GetMinCheckpointCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            disabled.GetMeta().GetCleanupMilestone().GetMaxCheckpointCommitId());

        auto state = MakeState(DefaultBlockCount, true);

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
    out << "[" << b.BlockIndex << ", " << b.CommitId << ", "
        << b.BlobId << ", " << b.BlobOffset << "]";
}
