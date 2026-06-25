#include "part_cleanup_logic.h"

#include "part_database.h"
#include "part_state.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/partition_common/part_thread_safe_state.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

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
        {30, 10, 10},
        {1600_KB, 400_KB, 10},
        {8_MB, 4_MB, 10},
    };
}

TFreeSpaceConfig DefaultFreeSpaceConfig()
{
    return {0.25, 0.15};
}

TPartitionState MakeState(size_t blockCount = 2048)
{
    auto threadSafeState = std::make_shared<TPartitionThreadSafeState>();
    return TPartitionState(
        DefaultConfig(1, blockCount),
        BuildDefaultCompactionPolicy(5),
        0,      // compactionScoreHistorySize
        0,      // cleanupScoreHistorySize
        DefaultBPConfig(),
        DefaultFreeSpaceConfig(),
        Max<ui32>(),    // maxIORequestsInFlight
        0,      // reassignChannelsPercentageThreshold
        100,    // reassignFreshChannelsPercentageThreshold
        100,    // reassignMixedChannelsPercentageThreshold
        false,  // reassignSystemChannelsImmediately
        5,      // channelCount (System + Log + Index + 1 Merged + Fresh)
        0,      // mixedIndexCacheSize
        10000,  // allocationUnit
        100,    // maxBlobsPerUnit
        10,     // maxBlobsPerRange
        1,      // compactionRangeCountPerRun
        std::move(threadSafeState));
}

NProto::TBlobMeta MakeMixedBlobMeta(
    const TVector<ui32>& blocks,
    const TVector<ui64>& commitIds = {})
{
    NProto::TBlobMeta meta;
    auto& mixedBlocks = *meta.MutableMixedBlocks();
    for (ui32 blockIndex: blocks) {
        mixedBlocks.AddBlocks(blockIndex);
    }
    for (ui64 commitId: commitIds) {
        mixedBlocks.AddCommitIds(commitId);
    }
    return meta;
}

NProto::TBlobMeta MakeMergedBlobMeta(ui32 start, ui32 end, ui32 skipped = 0)
{
    NProto::TBlobMeta meta;
    auto& mergedBlocks = *meta.MutableMergedBlocks();
    mergedBlocks.SetStart(start);
    mergedBlocks.SetEnd(end);
    mergedBlocks.SetSkipped(skipped);
    return meta;
}

struct TMixedAndMergedBlobsSetup
{
    TPartialBlobId MixedBlobId;
    TPartialBlobId MergedBlobId;
    ui64 DeletionCommitId = 0;
    NProto::TBlobMeta MixedBlobMeta;
    NProto::TBlobMeta MergedBlobMeta;
};

TMixedAndMergedBlobsSetup SetupMixedAndMergedBlobs(
    TTestExecutor& executor,
    TPartitionState& state,
    ui64 deletionCommitId)
{
    TMixedAndMergedBlobsSetup setup;
    setup.DeletionCommitId = deletionCommitId;
    setup.MixedBlobMeta = MakeMixedBlobMeta({0, 1, 2});
    setup.MergedBlobMeta = MakeMergedBlobMeta(10, 13);

    executor.WriteTx([&](TPartitionDatabase db) {
        setup.MixedBlobId = executor.MakeBlobId(3);
        state.WriteMixedBlocks(db, setup.MixedBlobId, {0, 1, 2}, 1);
        db.WriteBlobMeta(setup.MixedBlobId, setup.MixedBlobMeta);
        db.WriteCleanupQueue(setup.MixedBlobId, deletionCommitId);

        setup.MergedBlobId = executor.MakeBlobId(4);
        db.WriteMergedBlocks(
            setup.MergedBlobId,
            TBlockRange32::MakeClosedInterval(10, 13),
            TBlockMask{});
        db.WriteBlobMeta(setup.MergedBlobId, setup.MergedBlobMeta);
        db.WriteCleanupQueue(setup.MergedBlobId, deletionCommitId);
    });

    state.GetCleanupQueue().Add(
        {setup.MixedBlobId, deletionCommitId, {}});
    state.GetCleanupQueue().Add(
        {setup.MergedBlobId, deletionCommitId, {}});

    state.IncrementMixedBlocksCount(3);
    state.IncrementMixedBlobsCount(1);
    state.IncrementMergedBlocksCount(4);
    state.IncrementMergedBlobsCount(1);

    return setup;
}

struct TMergedBlobVisitor final
    : public IBlocksIndexVisitor
    , public IBlobsVisitor
{
    TPartialBlobId BlobId;
    bool Found = false;

    bool Visit(
        TBlockRange32 blockRange,
        const TPartialBlobId& blobId,
        ui32 skippedBlocksCount) override
    {
        Y_UNUSED(blockRange);
        Y_UNUSED(skippedBlocksCount);

        if (blobId == BlobId) {
            Found = true;
            return false;
        }

        return true;
    }

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        Y_UNUSED(blockIndex);
        Y_UNUSED(commitId);
        Y_UNUSED(blobId);
        Y_UNUSED(blobOffset);
        return true;
    }
};

struct TMixedBlockVisitor final: public IMixedBlocksIndexVisitor
{
    bool Found = false;

    bool VisitBlock(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset,
        ui8 compactionRangeCount) override
    {
        Y_UNUSED(blockIndex);
        Y_UNUSED(commitId);
        Y_UNUSED(blobId);
        Y_UNUSED(blobOffset);
        Y_UNUSED(compactionRangeCount);

        Found = true;
        return false;
    }
};

bool HasMixedBlock(
    TPartitionDatabase& db,
    ui32 blockIndex,
    ui64 commitId)
{
    TMixedBlockVisitor visitor;
    const TVector<TBlock> blocks = {{blockIndex, commitId, false}};
    db.FindMixedBlocks(visitor, blocks);
    return visitor.Found;
}

bool HasMergedBlob(
    TPartitionDatabase& db,
    const TPartialBlobId& blobId,
    ui32 start,
    ui32 end)
{
    TMergedBlobVisitor visitor;
    visitor.BlobId = blobId;
    db.FindMergedBlocks(
        visitor,
        visitor,
        TBlockRange32::MakeClosedInterval(start, end),
        false,
        MaxBlocksCount);
    return visitor.Found;
}

bool HasGarbageBlob(TPartitionDatabase& db, const TPartialBlobId& blobId)
{
    TVector<TPartialBlobId> garbageBlobs;
    if (!db.ReadGarbageBlobs(garbageBlobs)) {
        return false;
    }

    return std::ranges::any_of(
        garbageBlobs,
        [&](const auto& id) { return id == blobId; });
}

TTxPartition::TCleanup MakeCleanupArgs(
    const TVector<TCleanupQueueItem>& cleanupQueue,
    ui64 cleanupCommitId)
{
    return TTxPartition::TCleanup(
        MakeIntrusive<TRequestInfo>(),
        cleanupCommitId,
        cleanupQueue);
}

void RunPrepareAndExecute(
    TTestExecutor& executor,
    TPartitionState& state,
    TTxPartition::TCleanup& args,
    bool verifyRecreatedBlobMetasOnCleanup)
{
    executor.ReadTx([&](TPartitionDatabase db) {
        const bool ready = PrepareCleanupTransaction(
            verifyRecreatedBlobMetasOnCleanup,
            TTestExecutor::TabletId,
            "test-disk",
            db,
            args);
        UNIT_ASSERT(ready);
    });

    executor.WriteTx([&](TPartitionDatabase db) {
        ExecuteCleanupTransaction(db, args, state);
    });
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVerifyRecreatedBlobMetaTest)
{
    Y_UNIT_TEST(ShouldRejectMismatchedBlobMetaTypes)
    {
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        const auto blobMeta = MakeMixedBlobMeta({0, 1});
        const auto recreatedBlobMeta = MakeMergedBlobMeta(0, 3);
        const auto blobId = TPartialBlobId(1, 0);

        executor.ReadTx([&](TPartitionDatabase db) {
            const auto result = VerifyRecreatedBlobMeta(
                db,
                blobId,
                blobMeta,
                recreatedBlobMeta);

            UNIT_ASSERT(result.Ready);
            UNIT_ASSERT(HasError(result.Error));
            UNIT_ASSERT_STRING_CONTAINS(
                result.Error.GetMessage(),
                "Mismatched blob meta types");
        });
    }

    Y_UNIT_TEST(ShouldAcceptMatchingMergedBlocks)
    {
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        const auto blobMeta = MakeMergedBlobMeta(10, 20, 2);
        const auto recreatedBlobMeta = MakeMergedBlobMeta(10, 20, 2);
        const auto blobId = TPartialBlobId(1, 0);

        executor.ReadTx([&](TPartitionDatabase db) {
            const auto result = VerifyRecreatedBlobMeta(
                db,
                blobId,
                blobMeta,
                recreatedBlobMeta);

            UNIT_ASSERT(result.Ready);
            UNIT_ASSERT(!HasError(result.Error));
        });
    }

    Y_UNIT_TEST(ShouldRejectMismatchedMergedBlocks)
    {
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        const auto blobMeta = MakeMergedBlobMeta(10, 20, 2);
        const auto recreatedBlobMeta = MakeMergedBlobMeta(10, 21, 2);
        const auto blobId = TPartialBlobId(1, 0);

        executor.ReadTx([&](TPartitionDatabase db) {
            const auto result = VerifyRecreatedBlobMeta(
                db,
                blobId,
                blobMeta,
                recreatedBlobMeta);

            UNIT_ASSERT(result.Ready);
            UNIT_ASSERT(HasError(result.Error));
            UNIT_ASSERT_STRING_CONTAINS(
                result.Error.GetMessage(),
                "Mismatched merged blocks");
        });
    }

    Y_UNIT_TEST(ShouldAcceptMatchingMixedBlocks)
    {
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        TPartialBlobId blobId;
        executor.WriteTx([&](TPartitionDatabase db) {
            blobId = executor.MakeBlobId(3);
            db.WriteMixedBlocks(blobId, {0, 1, 2}, 1);
        });

        const auto blobMeta = MakeMixedBlobMeta({0, 1, 2}, {50, 60, 70});
        const auto recreatedBlobMeta = MakeMixedBlobMeta({0, 1, 2}, {50, 60, 70});

        executor.ReadTx([&](TPartitionDatabase db) {
            const auto result = VerifyRecreatedBlobMeta(
                db,
                blobId,
                blobMeta,
                recreatedBlobMeta);

            UNIT_ASSERT(result.Ready);
            UNIT_ASSERT(!HasError(result.Error));
        });
    }

    Y_UNIT_TEST(ShouldUseBlobCommitIdWhenMixedCommitIdsAbsent)
    {
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        TPartialBlobId blobId;
        executor.WriteTx([&](TPartitionDatabase db) {
            blobId = executor.MakeBlobId(2);
            db.WriteMixedBlocks(blobId, {5, 7}, 1);
        });

        const auto blobMeta = MakeMixedBlobMeta({5, 7});
        const auto recreatedBlobMeta = MakeMixedBlobMeta({5, 7});

        executor.ReadTx([&](TPartitionDatabase db) {
            const auto result = VerifyRecreatedBlobMeta(
                db,
                blobId,
                blobMeta,
                recreatedBlobMeta);

            UNIT_ASSERT(result.Ready);
            UNIT_ASSERT(!HasError(result.Error));
        });
    }

    Y_UNIT_TEST(ShouldAcceptMixedBlocksSubsetWhenMissingBlocksNotInIndex)
    {
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        TPartialBlobId blobId;
        executor.WriteTx([&](TPartitionDatabase db) {
            blobId = executor.MakeBlobId(3);
            db.WriteMixedBlocks(blobId, {0, 1, 2}, 1);
            db.DeleteMixedBlock(1, blobId.CommitId());
        });

        const auto blobMeta = MakeMixedBlobMeta({0, 1, 2});
        const auto recreatedBlobMeta = MakeMixedBlobMeta({0, 2});

        executor.ReadTx([&](TPartitionDatabase db) {
            const auto result = VerifyRecreatedBlobMeta(
                db,
                blobId,
                blobMeta,
                recreatedBlobMeta);

            UNIT_ASSERT(result.Ready);
            UNIT_ASSERT(!HasError(result.Error));
        });
    }

    Y_UNIT_TEST(ShouldRejectLeakedBlocksInRecreatedMixedMeta)
    {
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        TPartialBlobId blobId;
        executor.WriteTx([&](TPartitionDatabase db) {
            blobId = executor.MakeBlobId(3);
            db.WriteMixedBlocks(blobId, {0, 1, 2}, 1);
        });

        const auto blobMeta = MakeMixedBlobMeta({0, 1, 2});
        const auto recreatedBlobMeta = MakeMixedBlobMeta({0, 2});

        executor.ReadTx([&](TPartitionDatabase db) {
            const auto result = VerifyRecreatedBlobMeta(
                db,
                blobId,
                blobMeta,
                recreatedBlobMeta);

            UNIT_ASSERT(result.Ready);
            UNIT_ASSERT(HasError(result.Error));
            UNIT_ASSERT_STRING_CONTAINS(
                result.Error.GetMessage(),
                "Leaked blocks in recreated blob meta");
            UNIT_ASSERT_STRING_CONTAINS(result.Error.GetMessage(), "BlockIndex: 1");
        });
    }

    Y_UNIT_TEST(ShouldRejectExtraBlocksInRecreatedMixedMeta)
    {
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        TPartialBlobId blobId;
        executor.WriteTx([&](TPartitionDatabase db) {
            blobId = executor.MakeBlobId(3);
            db.WriteMixedBlocks(blobId, {0, 1, 2}, 1);
        });

        const auto blobMeta = MakeMixedBlobMeta({0, 1});
        const auto recreatedBlobMeta = MakeMixedBlobMeta({0, 1, 2});

        executor.ReadTx([&](TPartitionDatabase db) {
            const auto result = VerifyRecreatedBlobMeta(
                db,
                blobId,
                blobMeta,
                recreatedBlobMeta);

            UNIT_ASSERT(result.Ready);
            UNIT_ASSERT(HasError(result.Error));
            UNIT_ASSERT_STRING_CONTAINS(
                result.Error.GetMessage(),
                "there are blocks that are not present in the original blob "
                "meta");
        });
    }
}

Y_UNIT_TEST_SUITE(TCleanupTransactionTest)
{
    Y_UNIT_TEST(ShouldCleanupMixedAndMergedBlobsWithoutVerify)
    {
        auto state = MakeState();
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        const ui64 deletionCommitId = MakeCommitId(0, 50);
        const ui64 cleanupCommitId = MakeCommitId(0, 100);
        const auto setup =
            SetupMixedAndMergedBlobs(executor, state, deletionCommitId);

        auto args = MakeCleanupArgs(
            state.GetCleanupQueue().GetItems(cleanupCommitId),
            cleanupCommitId);

        executor.ReadTx([&](TPartitionDatabase db) {
            UNIT_ASSERT(HasMixedBlock(db, 0, setup.MixedBlobId.CommitId()));
            UNIT_ASSERT(HasMixedBlock(db, 1, setup.MixedBlobId.CommitId()));
            UNIT_ASSERT(HasMixedBlock(db, 2, setup.MixedBlobId.CommitId()));
            UNIT_ASSERT(HasMergedBlob(db, setup.MergedBlobId, 10, 13));
        });

        RunPrepareAndExecute(executor, state, args, false);

        UNIT_ASSERT_VALUES_EQUAL(2, args.CleanupQueue.size());
        UNIT_ASSERT_VALUES_EQUAL(2, args.BlobsMeta.size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetCleanupQueue().GetCount());

        executor.ReadTx([&](TPartitionDatabase db) {
            UNIT_ASSERT(!HasMixedBlock(db, 0, setup.MixedBlobId.CommitId()));
            UNIT_ASSERT(!HasMixedBlock(db, 1, setup.MixedBlobId.CommitId()));
            UNIT_ASSERT(!HasMixedBlock(db, 2, setup.MixedBlobId.CommitId()));
            UNIT_ASSERT(!HasMergedBlob(db, setup.MergedBlobId, 10, 13));

            TMaybe<NProto::TBlobMeta> mixedBlobMeta;
            UNIT_ASSERT(db.ReadBlobMeta(setup.MixedBlobId, mixedBlobMeta));
            UNIT_ASSERT(!mixedBlobMeta.Defined());

            TMaybe<NProto::TBlobMeta> mergedBlobMeta;
            UNIT_ASSERT(db.ReadBlobMeta(setup.MergedBlobId, mergedBlobMeta));
            UNIT_ASSERT(!mergedBlobMeta.Defined());

            UNIT_ASSERT(HasGarbageBlob(db, setup.MixedBlobId));
            UNIT_ASSERT(HasGarbageBlob(db, setup.MergedBlobId));

            TVector<TCleanupQueueItem> cleanupQueueItems;
            UNIT_ASSERT(db.ReadCleanupQueue(cleanupQueueItems));
            UNIT_ASSERT(cleanupQueueItems.empty());
        });
    }

    Y_UNIT_TEST(ShouldCleanupMixedAndMergedBlobsWhenVerifySucceeds)
    {
        auto state = MakeState();
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        const ui64 deletionCommitId = MakeCommitId(0, 50);
        const ui64 cleanupCommitId = MakeCommitId(0, 100);
        const auto setup =
            SetupMixedAndMergedBlobs(executor, state, deletionCommitId);

        TVector<TCleanupQueueItem> cleanupQueue;
        cleanupQueue.emplace_back(
            setup.MixedBlobId,
            deletionCommitId,
            setup.MixedBlobMeta);
        cleanupQueue.emplace_back(
            setup.MergedBlobId,
            deletionCommitId,
            setup.MergedBlobMeta);

        auto args = MakeCleanupArgs(cleanupQueue, cleanupCommitId);
        RunPrepareAndExecute(executor, state, args, true);

        UNIT_ASSERT_VALUES_EQUAL(2, args.CleanupQueue.size());
        UNIT_ASSERT_VALUES_EQUAL(2, args.BlobsMeta.size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetCleanupQueue().GetCount());

        executor.ReadTx([&](TPartitionDatabase db) {
            UNIT_ASSERT(!HasMixedBlock(db, 0, setup.MixedBlobId.CommitId()));
            UNIT_ASSERT(!HasMergedBlob(db, setup.MergedBlobId, 10, 13));
            UNIT_ASSERT(HasGarbageBlob(db, setup.MixedBlobId));
            UNIT_ASSERT(HasGarbageBlob(db, setup.MergedBlobId));
        });
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
