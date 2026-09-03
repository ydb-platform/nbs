#include "part_cleanup_logic.h"

#include "part_database.h"
#include "part_state.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/partition_common/part_thread_safe_state.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>
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
        BuildDefaultCompactionPolicy(5, 0),
        0,   // compactionScoreHistorySize
        0,   // cleanupScoreHistorySize
        DefaultBPConfig(),
        DefaultFreeSpaceConfig(),
        Max<ui32>(),   // maxIORequestsInFlight
        0,             // reassignChannelsPercentageThreshold
        100,           // reassignFreshChannelsPercentageThreshold
        100,           // reassignMixedChannelsPercentageThreshold
        false,         // reassignSystemChannelsImmediately
        5,             // channelCount (System + Log + Index + 1 Merged + Fresh)
        0,             // mixedIndexCacheSize
        10000,         // allocationUnit
        100,           // maxBlobsPerUnit
        10,            // maxBlobsPerRange
        1,             // compactionRangeCountPerRun
        std::move(threadSafeState),
        TTestExecutor::TabletId,
        std::nullopt,  // mixedBlocksFilterConfig
        false,         // checkpointAwareCleanupEnabled
        true           // useBlobChannelDataKindForCounters
    );
}

TPartialBlobId MoveToDataChannel(TPartialBlobId blobId)
{
    return TPartialBlobId(
        blobId.Generation(),
        blobId.Step(),
        3,
        blobId.BlobSize(),
        blobId.Cookie(),
        blobId.PartId());
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

    executor.WriteTx(
        [&](TPartitionDatabase db)
        {
            setup.MixedBlobId = MoveToDataChannel(executor.MakeBlobId(3));
            state.WriteMixedBlocks(db, setup.MixedBlobId, {0, 1, 2}, 1);
            db.WriteBlobMeta(setup.MixedBlobId, setup.MixedBlobMeta);
            db.WriteCleanupQueue(setup.MixedBlobId, deletionCommitId);

            setup.MergedBlobId = MoveToDataChannel(executor.MakeBlobId(4));
            db.WriteMergedBlocks(
                setup.MergedBlobId,
                TBlockRange32::MakeClosedInterval(10, 13),
                TBlockMask{});
            db.WriteBlobMeta(setup.MergedBlobId, setup.MergedBlobMeta);
            db.WriteCleanupQueue(setup.MergedBlobId, deletionCommitId);
        });

    state.GetCleanupQueue().Add({setup.MixedBlobId, deletionCommitId, {}});
    state.GetCleanupQueue().Add({setup.MergedBlobId, deletionCommitId, {}});

    state.IncrementMergedBlocksCount(7);
    state.IncrementMergedBlobsCount(2);
    state.IncrementMixedIndexBlocksCount(3);
    state.IncrementMixedIndexBlobsCount(1);
    state.IncrementMergedIndexBlocksCount(4);
    state.IncrementMergedIndexBlobsCount(1);

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
        const TBlockMask& skipMask) override
    {
        Y_UNUSED(blockRange);
        Y_UNUSED(skipMask);

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

bool HasMixedBlock(TPartitionDatabase& db, ui32 blockIndex, ui64 commitId)
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
    ui64 cleanupCommitId,
    bool useRecreatedBlobMeta,
    bool verifyRecreatedBlobMetasOnCleanup,
    bool checkpointAware,
    ui64 minCheckpointCommitId,
    ui64 maxCheckpointCommitId)
{
    return TTxPartition::TCleanup(
        MakeIntrusive<TRequestInfo>(),
        cleanupCommitId,
        useRecreatedBlobMeta,
        verifyRecreatedBlobMetasOnCleanup,
        cleanupQueue,
        checkpointAware,
        minCheckpointCommitId,
        maxCheckpointCommitId);
}

void RunPrepareAndExecute(
    TTestExecutor& executor,
    TTestEnv& env,
    TPartitionState& state,
    TTxPartition::TCleanup& args)
{
    executor.ReadTx(
        [&](TPartitionDatabase db)
        {
            const bool ready = PrepareCleanupTransaction(
                TTestExecutor::TabletId,
                "test-disk",
                db,
                args);
            UNIT_ASSERT(ready);
        });

    executor.WriteTx(
        [&](TPartitionDatabase db)
        {
            ExecuteCleanupTransaction(
                env.GetRuntime().GetActorSystem(0),
                TLogTitle(
                    GetCycleCount(),
                    TLogTitle::TPartition{
                        TTestExecutor::TabletId,
                        "test",
                        0,
                        1,
                        0}),
                TTestExecutor::TabletId,
                db,
                args,
                state);
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

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
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

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
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

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
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
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                blobId = MoveToDataChannel(executor.MakeBlobId(3));
                db.WriteMixedBlocks(blobId, {0, 1, 2}, 1);
            });

        const auto blobMeta = MakeMixedBlobMeta({0, 1, 2}, {50, 60, 70});
        const auto recreatedBlobMeta =
            MakeMixedBlobMeta({0, 1, 2}, {50, 60, 70});

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
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
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                blobId = MoveToDataChannel(executor.MakeBlobId(2));
                db.WriteMixedBlocks(blobId, {5, 7}, 1);
            });

        const auto blobMeta = MakeMixedBlobMeta({5, 7});
        const auto recreatedBlobMeta = MakeMixedBlobMeta({5, 7});

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
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
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                blobId = MoveToDataChannel(executor.MakeBlobId(3));
                db.WriteMixedBlocks(blobId, {0, 1, 2}, 1);
                db.DeleteMixedBlock(1, blobId.CommitId());
            });

        const auto blobMeta = MakeMixedBlobMeta({0, 1, 2});
        const auto recreatedBlobMeta = MakeMixedBlobMeta({0, 2});

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
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
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                blobId = MoveToDataChannel(executor.MakeBlobId(3));
                db.WriteMixedBlocks(blobId, {0, 1, 2}, 1);
            });

        const auto blobMeta = MakeMixedBlobMeta({0, 1, 2});
        const auto recreatedBlobMeta = MakeMixedBlobMeta({0, 2});

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
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
                UNIT_ASSERT_STRING_CONTAINS(
                    result.Error.GetMessage(),
                    "BlockIndex: 1");
            });
    }

    Y_UNIT_TEST(ShouldRejectLeakedBlocksInRecreatedMixedMetaOnlyIfBlobIdMatches)
    {
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        TPartialBlobId blobId;
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                blobId = MoveToDataChannel(executor.MakeBlobId(1));

                auto anotherBlobId = MoveToDataChannel(executor.MakeBlobId(1));
                db.WriteMixedBlock({anotherBlobId, 1, 0, 0, 0});
            });

        const auto blobMeta = MakeMixedBlobMeta({0}, {1});
        const auto recreatedBlobMeta = MakeMixedBlobMeta({}, {});

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                const auto result = VerifyRecreatedBlobMeta(
                    db,
                    blobId,
                    blobMeta,
                    recreatedBlobMeta);

                UNIT_ASSERT(result.Ready);
                UNIT_ASSERT(!HasError(result.Error));
            });
    }

    Y_UNIT_TEST(ShouldRejectExtraBlocksInRecreatedMixedMeta)
    {
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        TPartialBlobId blobId;
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                blobId = MoveToDataChannel(executor.MakeBlobId(3));
                db.WriteMixedBlocks(blobId, {0, 1, 2}, 1);
            });

        const auto blobMeta = MakeMixedBlobMeta({0, 1});
        const auto recreatedBlobMeta = MakeMixedBlobMeta({0, 1, 2});

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                const auto result = VerifyRecreatedBlobMeta(
                    db,
                    blobId,
                    blobMeta,
                    recreatedBlobMeta);

                UNIT_ASSERT(result.Ready);
                UNIT_ASSERT(HasError(result.Error));
                UNIT_ASSERT_STRING_CONTAINS(
                    result.Error.GetMessage(),
                    "there are blocks that are not present in the original "
                    "blob "
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
        TTestEnv env;

        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        const ui64 deletionCommitId = MakeCommitId(0, 50);
        const ui64 cleanupCommitId = MakeCommitId(0, 100);
        const auto setup =
            SetupMixedAndMergedBlobs(executor, state, deletionCommitId);

        auto args = MakeCleanupArgs(
            state.GetCleanupQueue().GetItems(cleanupCommitId),
            cleanupCommitId,
            false,   // useRecreatedBlobMeta
            false,   // verifyRecreatedBlobMetasOnCleanup
            false,   // checkpointAware
            InvalidCommitId,
            InvalidCommitId);

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                UNIT_ASSERT(HasMixedBlock(db, 0, setup.MixedBlobId.CommitId()));
                UNIT_ASSERT(HasMixedBlock(db, 1, setup.MixedBlobId.CommitId()));
                UNIT_ASSERT(HasMixedBlock(db, 2, setup.MixedBlobId.CommitId()));
                UNIT_ASSERT(HasMergedBlob(db, setup.MergedBlobId, 10, 13));
            });

        RunPrepareAndExecute(executor, env, state, args);

        UNIT_ASSERT_VALUES_EQUAL(2, args.CleanupQueue.size());
        UNIT_ASSERT_VALUES_EQUAL(2, args.BlobsMeta.size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetCleanupQueue().GetCount());

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                UNIT_ASSERT(
                    !HasMixedBlock(db, 0, setup.MixedBlobId.CommitId()));
                UNIT_ASSERT(
                    !HasMixedBlock(db, 1, setup.MixedBlobId.CommitId()));
                UNIT_ASSERT(
                    !HasMixedBlock(db, 2, setup.MixedBlobId.CommitId()));
                UNIT_ASSERT(!HasMergedBlob(db, setup.MergedBlobId, 10, 13));

                TMaybe<NProto::TBlobMeta> mixedBlobMeta;
                UNIT_ASSERT(db.ReadBlobMeta(setup.MixedBlobId, mixedBlobMeta));
                UNIT_ASSERT(!mixedBlobMeta.Defined());

                TMaybe<NProto::TBlobMeta> mergedBlobMeta;
                UNIT_ASSERT(
                    db.ReadBlobMeta(setup.MergedBlobId, mergedBlobMeta));
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
        TTestEnv env;
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

        auto args = MakeCleanupArgs(
            cleanupQueue,
            cleanupCommitId,
            false,   // useRecreatedBlobMeta
            true,    // verifyRecreatedBlobMetasOnCleanup
            false,   // checkpointAware
            InvalidCommitId,
            InvalidCommitId);

        RunPrepareAndExecute(executor, env, state, args);

        UNIT_ASSERT_VALUES_EQUAL(2, args.CleanupQueue.size());
        UNIT_ASSERT_VALUES_EQUAL(2, args.BlobsMeta.size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetCleanupQueue().GetCount());

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                UNIT_ASSERT(
                    !HasMixedBlock(db, 0, setup.MixedBlobId.CommitId()));
                UNIT_ASSERT(!HasMergedBlob(db, setup.MergedBlobId, 10, 13));
                UNIT_ASSERT(HasGarbageBlob(db, setup.MixedBlobId));
                UNIT_ASSERT(HasGarbageBlob(db, setup.MergedBlobId));
            });
    }

    Y_UNIT_TEST(ShouldCleanupMixedAndMergedBlobsWhenUseRecreatedBlobMeta)
    {
        auto state = MakeState();
        TTestExecutor executor;
        TTestEnv env;
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

        auto args = MakeCleanupArgs(
            cleanupQueue,
            cleanupCommitId,
            true,    // useRecreatedBlobMeta
            false,   // verifyRecreatedBlobMetasOnCleanup
            false,   // checkpointAware
            InvalidCommitId,
            InvalidCommitId);
        RunPrepareAndExecute(executor, env, state, args);

        UNIT_ASSERT_VALUES_EQUAL(2, args.CleanupQueue.size());
        UNIT_ASSERT_VALUES_EQUAL(2, args.BlobsMeta.size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetCleanupQueue().GetCount());

        // No blob metas were read from the database
        UNIT_ASSERT_VALUES_EQUAL(0, args.ReadBlobMetasCount);

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                UNIT_ASSERT(
                    !HasMixedBlock(db, 0, setup.MixedBlobId.CommitId()));
                UNIT_ASSERT(!HasMergedBlob(db, setup.MergedBlobId, 10, 13));
                UNIT_ASSERT(HasGarbageBlob(db, setup.MixedBlobId));
                UNIT_ASSERT(HasGarbageBlob(db, setup.MergedBlobId));
            });
    }

    Y_UNIT_TEST(ShouldCorrectlyDecrementMixedAndMergedBytesCountWhenUseRecreatedBlobMeta)
    {
        auto state = MakeState();
        TTestExecutor executor;
        TTestEnv env;

        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        const ui64 deletionCommitId = MakeCommitId(0, 50);
        const ui64 cleanupCommitId = MakeCommitId(0, 100);
        const auto setup =
            SetupMixedAndMergedBlobs(executor, state, deletionCommitId);

        executor.WriteTx(
            [&](TPartitionDatabase db)
            { db.DeleteMixedBlock(2, setup.MixedBlobId.CommitId()); });

        TVector<TCleanupQueueItem> cleanupQueue;
        cleanupQueue.emplace_back(
            setup.MixedBlobId,
            deletionCommitId,
            MakeMixedBlobMeta({0, 1}));
        cleanupQueue.emplace_back(
            setup.MergedBlobId,
            deletionCommitId,
            setup.MergedBlobMeta);

        auto args = MakeCleanupArgs(
            cleanupQueue,
            cleanupCommitId,
            true,    // useRecreatedBlobMeta
            false,   // verifyRecreatedBlobMetasOnCleanup
            false,   // checkpointAware
            InvalidCommitId,
            InvalidCommitId);
        RunPrepareAndExecute(executor, env, state, args);

        UNIT_ASSERT_VALUES_EQUAL(2, args.CleanupQueue.size());
        UNIT_ASSERT_VALUES_EQUAL(2, args.BlobsMeta.size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetCleanupQueue().GetCount());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetMixedBlocksCount());

        // No blob metas were read from the database
        UNIT_ASSERT_VALUES_EQUAL(0, args.ReadBlobMetasCount);

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                UNIT_ASSERT(
                    !HasMixedBlock(db, 0, setup.MixedBlobId.CommitId()));
                UNIT_ASSERT(!HasMergedBlob(db, setup.MergedBlobId, 10, 13));
                UNIT_ASSERT(HasGarbageBlob(db, setup.MixedBlobId));
                UNIT_ASSERT(HasGarbageBlob(db, setup.MergedBlobId));
            });
    }

    Y_UNIT_TEST(ShouldRespectCheckpointCommitIdBoundsDuringCleanup)
    {
        auto state = MakeState();
        TTestExecutor executor;
        TTestEnv env;

        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        const ui64 minCheckpointCommitId = MakeCommitId(0, 40);
        const ui64 maxCheckpointCommitId = MakeCommitId(0, 60);
        const ui64 cleanupCommitId = MakeCommitId(0, 100);

        // deletionCommitId < minCheckpointCommitId => blobs with such
        // deletion commit id should be cleaned up.
        const ui64 beforeCheckpointsDeletionCommitId = MakeCommitId(0, 30);
        // deletionCommitId > minCheckpointCommitId => blobs with such deletion
        // commit id might be still needed. We should check blob's commit id.
        const ui64 afterCheckpointsdeletionCommitId = MakeCommitId(0, 80);
        const ui64 betweenCheckpointsdeletionCommitId = MakeCommitId(0, 50);

        struct TMergedTestCase
        {
            ui64 DeletionCommitId = 0;
            ui64 BlobCommitId = 0;
            ui32 StartIndex = 0;
            ui32 EndIndex = 0;
            bool ShouldBeCleanedUp = false;
        };

        struct TMixedTestCase
        {
            ui64 DeletionCommitId = 0;
            ui64 BlobCommitId = 0;
            TVector<ui64> CommitIds;
            TVector<ui32> BlockIndices;
            bool ShouldBeCleanedUp = false;
        };

        const TVector<TMergedTestCase> mergedTestCases = {
            // DeletionCommitId < minCheckpointCommitId
            {
                .DeletionCommitId = beforeCheckpointsDeletionCommitId,
                .BlobCommitId = MakeCommitId(0, 20),
                .StartIndex = 0,
                .EndIndex = 3,
                .ShouldBeCleanedUp = true,
            },
            // BlobCommitId > maxCheckpointCommitId
            {
                .DeletionCommitId = afterCheckpointsdeletionCommitId,
                .BlobCommitId = MakeCommitId(0, 70),
                .StartIndex = 10,
                .EndIndex = 13,
                .ShouldBeCleanedUp = true,
            },
            // BlobCommitId < maxCheckpointCommitId
            {
                .DeletionCommitId = afterCheckpointsdeletionCommitId,
                .BlobCommitId = MakeCommitId(0, 50),
                .StartIndex = 20,
                .EndIndex = 23,
                .ShouldBeCleanedUp = false,
            },
            // This blob is not needed neither for minCheckpoint nor for
            // maxCheckpoint. But it might be needed if some other checkpoints
            // exist.
            {
                .DeletionCommitId = betweenCheckpointsdeletionCommitId,
                .BlobCommitId = MakeCommitId(0, 45),
                .StartIndex = 30,
                .EndIndex = 33,
                .ShouldBeCleanedUp = false,
            },
        };

        const TVector<TMixedTestCase> mixedTestCases = {
            // DeletionCommitId < minCheckpointCommitId.
            {
                .DeletionCommitId = beforeCheckpointsDeletionCommitId,
                .BlobCommitId = MakeCommitId(0, 25),
                .CommitIds = {MakeCommitId(0, 20), MakeCommitId(0, 21)},
                .BlockIndices = {4, 5},
                .ShouldBeCleanedUp = true,
            },
            // BlobCommitId > maxCheckpointCommitId.
            {
                .DeletionCommitId = afterCheckpointsdeletionCommitId,
                .BlobCommitId = MakeCommitId(0, 75),
                .CommitIds = {},
                .BlockIndices = {14, 15},
                .ShouldBeCleanedUp = true,
            },
            // BlobCommitId < maxCheckpointCommitId.
            {
                .DeletionCommitId = afterCheckpointsdeletionCommitId,
                .BlobCommitId = MakeCommitId(0, 55),
                .CommitIds = {},
                .BlockIndices = {24, 25},
                .ShouldBeCleanedUp = false,
            },
            // Commit ids of all blocks > maxCheckpointCommitId.
            {
                .DeletionCommitId = afterCheckpointsdeletionCommitId,
                .BlobCommitId = MakeCommitId(0, 76),
                .CommitIds = {MakeCommitId(0, 70), MakeCommitId(0, 71)},
                .BlockIndices = {16, 17},
                .ShouldBeCleanedUp = true,
            },
            // Commit id of some block < maxCheckpointCommitId.
            {
                .DeletionCommitId = afterCheckpointsdeletionCommitId,
                .BlobCommitId = MakeCommitId(0, 77),
                .CommitIds = {MakeCommitId(0, 50), MakeCommitId(0, 70)},
                .BlockIndices = {26, 27},
                .ShouldBeCleanedUp = false,
            },
        };

        TVector<TPartialBlobId> mergedBlobIds;
        TVector<TPartialBlobId> mixedBlobIds;

        for (const auto& tc: mergedTestCases) {
            mergedBlobIds.push_back(
                MoveToDataChannel(executor.MakeBlobIdWithCommitId(
                    tc.BlobCommitId, tc.EndIndex - tc.StartIndex + 1)));
        }
        for (const auto& tc: mixedTestCases) {
            mixedBlobIds.push_back(
                MoveToDataChannel(executor.MakeBlobIdWithCommitId(
                    tc.BlobCommitId, tc.BlockIndices.size())));
        }

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                for (size_t i = 0; i < mergedTestCases.size(); ++i) {
                    const auto& tc = mergedTestCases[i];
                    const auto& blobId = mergedBlobIds[i];
                    db.WriteMergedBlocks(
                        blobId,
                        TBlockRange32::MakeClosedInterval(
                            tc.StartIndex,
                            tc.EndIndex),
                        TBlockMask{});
                    db.WriteBlobMeta(
                        blobId,
                        MakeMergedBlobMeta(tc.StartIndex, tc.EndIndex));
                    db.WriteCleanupQueue(blobId, tc.DeletionCommitId);
                }

                for (size_t i = 0; i < mixedTestCases.size(); ++i) {
                    const auto& tc = mixedTestCases[i];
                    const auto& blobId = mixedBlobIds[i];
                    if (tc.CommitIds.empty()) {
                        state.WriteMixedBlocks(db, blobId, tc.BlockIndices, 1);
                    } else {
                        Y_ABORT_UNLESS(
                            tc.CommitIds.size() == tc.BlockIndices.size());
                        for (size_t j = 0; j < tc.BlockIndices.size(); ++j) {
                            db.WriteMixedBlock(TMixedBlock(
                                blobId,
                                tc.CommitIds[j],
                                tc.BlockIndices[j],
                                j,
                                1));
                        }
                    }
                    db.WriteBlobMeta(
                        blobId,
                        MakeMixedBlobMeta(tc.BlockIndices, tc.CommitIds));
                    db.WriteCleanupQueue(blobId, tc.DeletionCommitId);
                }
            });

        ui64 mergedBlocksCount = 0;
        ui64 mixedBlocksCount = 0;
        size_t cleanedUpCount = 0;
        size_t remainingMergedBlobs = 0;
        size_t remainingMixedBlobs = 0;

        for (size_t i = 0; i < mergedTestCases.size(); ++i) {
            const auto& tc = mergedTestCases[i];
            state.GetCleanupQueue().Add(
                {mergedBlobIds[i], tc.DeletionCommitId, {}});
            mergedBlocksCount += tc.EndIndex - tc.StartIndex + 1;
            if (tc.ShouldBeCleanedUp) {
                ++cleanedUpCount;
            } else {
                ++remainingMergedBlobs;
            }
        }
        for (size_t i = 0; i < mixedTestCases.size(); ++i) {
            const auto& tc = mixedTestCases[i];
            state.GetCleanupQueue().Add(
                {mixedBlobIds[i], tc.DeletionCommitId, {}});
            mixedBlocksCount += tc.BlockIndices.size();
            if (tc.ShouldBeCleanedUp) {
                ++cleanedUpCount;
            } else {
                ++remainingMixedBlobs;
            }
        }

        state.IncrementMergedBlocksCount(mergedBlocksCount + mixedBlocksCount);
        state.IncrementMergedBlobsCount(
            mergedTestCases.size() + mixedTestCases.size());
        state.IncrementMergedIndexBlocksCount(mergedBlocksCount);
        state.IncrementMergedIndexBlobsCount(mergedTestCases.size());
        state.IncrementMixedIndexBlocksCount(mixedBlocksCount);
        state.IncrementMixedIndexBlobsCount(mixedTestCases.size());

        auto args = MakeCleanupArgs(
            state.GetCleanupQueue().GetItems(cleanupCommitId),
            cleanupCommitId,
            false,   // useRecreatedBlobMeta
            false,   // verifyRecreatedBlobMetasOnCleanup
            true,    // checkpointAware
            minCheckpointCommitId,
            maxCheckpointCommitId);

        RunPrepareAndExecute(executor, env, state, args);

        const size_t remainingCount =
            remainingMergedBlobs + remainingMixedBlobs;
        UNIT_ASSERT_VALUES_EQUAL(cleanedUpCount, args.CleanupQueue.size());
        UNIT_ASSERT_VALUES_EQUAL(remainingCount, args.BlobsSkipped);
        UNIT_ASSERT_VALUES_EQUAL(
            remainingCount,
            state.GetCleanupQueue().GetCount());
        UNIT_ASSERT_VALUES_EQUAL(remainingCount, state.GetMergedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(
            remainingMergedBlobs, state.GetMergedIndexBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(
            remainingMixedBlobs, state.GetMixedIndexBlobsCount());

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                for (size_t i = 0; i < mergedTestCases.size(); ++i) {
                    const auto& tc = mergedTestCases[i];
                    const auto& blobId = mergedBlobIds[i];
                    if (tc.ShouldBeCleanedUp) {
                        UNIT_ASSERT(!HasMergedBlob(
                            db,
                            blobId,
                            tc.StartIndex,
                            tc.EndIndex));
                        UNIT_ASSERT(HasGarbageBlob(db, blobId));
                    } else {
                        UNIT_ASSERT(HasMergedBlob(
                            db,
                            blobId,
                            tc.StartIndex,
                            tc.EndIndex));
                        UNIT_ASSERT(!HasGarbageBlob(db, blobId));

                        TMaybe<NProto::TBlobMeta> blobMeta;
                        UNIT_ASSERT(db.ReadBlobMeta(blobId, blobMeta));
                        UNIT_ASSERT(blobMeta.Defined());
                    }
                }

                for (size_t i = 0; i < mixedTestCases.size(); ++i) {
                    const auto& tc = mixedTestCases[i];
                    const auto& blobId = mixedBlobIds[i];
                    const ui64 commitId = tc.CommitIds.empty()
                                              ? blobId.CommitId()
                                              : tc.CommitIds[0];
                    if (tc.ShouldBeCleanedUp) {
                        UNIT_ASSERT(
                            !HasMixedBlock(db, tc.BlockIndices[0], commitId));
                        UNIT_ASSERT(HasGarbageBlob(db, blobId));
                    } else {
                        UNIT_ASSERT(
                            HasMixedBlock(db, tc.BlockIndices[0], commitId));
                        UNIT_ASSERT(!HasGarbageBlob(db, blobId));

                        TMaybe<NProto::TBlobMeta> blobMeta;
                        UNIT_ASSERT(db.ReadBlobMeta(blobId, blobMeta));
                        UNIT_ASSERT(blobMeta.Defined());
                    }
                }

                TVector<TCleanupQueueItem> cleanupQueueItems;
                UNIT_ASSERT(db.ReadCleanupQueue(cleanupQueueItems));
                UNIT_ASSERT_VALUES_EQUAL(
                    remainingCount,
                    cleanupQueueItems.size());
            });
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
