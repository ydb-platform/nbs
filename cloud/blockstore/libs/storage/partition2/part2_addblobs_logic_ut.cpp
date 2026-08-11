#include "part2_addblobs_logic.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/partition_common/part_thread_safe_state.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>

#include <cloud/storage/core/libs/features/features_config.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MaxBlocksInBlob = 8;

NProto::TPartitionMeta DefaultConfig(size_t blockCount)
{
    NProto::TPartitionMeta meta;

    auto& config = *meta.MutableConfig();
    config.SetDiskId("test-disk");
    config.SetBlockSize(DefaultBlockSize);
    config.SetBlocksCount(blockCount);
    config.SetMaxBlocksInBlob(MaxBlocksInBlob);
    meta.SetL0RangeSize(MaxBlocksInBlob * DefaultBlockSize);
    meta.SetL1RangeSize(2 * MaxBlocksInBlob * DefaultBlockSize);

    auto cps = config.MutableExplicitChannelProfiles();
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
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

TPartitionState MakeState(size_t blockCount = 64)
{
    auto threadSafeState = std::make_shared<TPartitionThreadSafeState>();
    return TPartitionState(
        DefaultConfig(blockCount),
        BuildDefaultCompactionPolicy(5),
        0,   // compactionScoreHistorySize
        0,   // cleanupScoreHistorySize
        DefaultBPConfig(),
        DefaultFreeSpaceConfig(),
        Max<ui32>(),   // maxIORequestsInFlight
        0,             // reassignChannelsPercentageThreshold
        100,           // reassignFreshChannelsPercentageThreshold
        100,           // reassignMixedChannelsPercentageThreshold
        false,         // reassignSystemChannelsImmediately
        5,             // channelCount
        0,             // mixedIndexCacheSize
        10000,         // allocationUnit
        100,           // maxBlobsPerUnit
        10,            // maxBlobsPerRange
        1,             // compactionRangeCountPerRun
        std::move(threadSafeState));
}

TTxPartition::TAddBlobs MakeArgs(
    ui64 commitId,
    TVector<TAddMixedBlob> mixedBlobs = {},
    TVector<TAddMergedBlob> mergedBlobs = {},
    TVector<TAddFreshBlob> freshBlobs = {},
    TVector<TAddL0Blob> l0Blobs = {}, EAddBlobMode mode = ADD_WRITE_RESULT,
    TAffectedBlobs affectedBlobs = {}, TAffectedBlocks affectedBlocks = {},
    TVector<TBlobCompactionInfo> mixedBlobCompactionInfos = {},
    TVector<TBlobCompactionInfo> mergedBlobCompactionInfos = {},
    ui64 fromLevel = 0,
    ui64 toLevel = 0,
    ui64 rangeIndex = 0)
{
    return TTxPartition::TAddBlobs(
        MakeIntrusive<TRequestInfo>(), commitId, std::move(mixedBlobs),
        std::move(mergedBlobs), std::move(freshBlobs), std::move(l0Blobs), mode,
        std::move(affectedBlobs), std::move(affectedBlocks),
        std::move(mixedBlobCompactionInfos),
        std::move(mergedBlobCompactionInfos), fromLevel, toLevel, rangeIndex);
}

void RunExecute(TTestExecutor& executor, TPartitionState& state,
                TTxPartition::TAddBlobs& args, ui64 deletionCommitId)
{
    executor.WriteTx(
        [&](TPartitionDatabase db)
        {
            TLogTitle logTitle(GetCycleCount(),
                               TLogTitle::TPartition{TTestExecutor::TabletId,
                                                     "test-disk", 0, 1, 0});

            ExecuteAddBlobsTransaction(
                nullptr, logTitle.GetChild(GetCycleCount()),
                TTestExecutor::TabletId, "test-disk", deletionCommitId,
                state.GetMaxBlocksInBlob(), db, args, state);
        });
}

struct TBlockRecord
{
    ui32 BlockIndex = 0;
    ui64 CommitId = 0;
    TPartialBlobId BlobId;
    ui16 BlobOffset = 0;
};

struct TBlockVisitor final
    : public IBlocksIndexVisitor
    , public IMixedBlocksIndexVisitor
{
    TVector<TBlockRecord> Records;

    bool Visit(ui32 blockIndex, ui64 commitId, const TPartialBlobId& blobId,
               ui16 blobOffset) override
    {
        Records.push_back({blockIndex, commitId, blobId, blobOffset});
        return true;
    }

    bool VisitBlock(ui32 blockIndex, ui64 commitId,
                    const TPartialBlobId& blobId, ui16 blobOffset,
                    ui8 compactionRangeCount) override
    {
        Y_UNUSED(compactionRangeCount);
        Records.push_back({blockIndex, commitId, blobId, blobOffset});
        return true;
    }
};

TMaybe<NProto::TBlobMeta> ReadBlobMeta(TTestExecutor& executor,
                                       const TPartialBlobId& blobId)
{
    TMaybe<NProto::TBlobMeta> blobMeta;
    executor.ReadTx([&](TPartitionDatabase db)
                    { UNIT_ASSERT(db.ReadBlobMeta(blobId, blobMeta)); });
    return blobMeta;
}

TMaybe<TBlockMask> ReadBlockMask(TTestExecutor& executor,
                                 const TPartialBlobId& blobId)
{
    TMaybe<TBlockMask> blockMask;
    executor.ReadTx([&](TPartitionDatabase db)
                    { UNIT_ASSERT(db.ReadBlockMask(blobId, blockMask)); });
    return blockMask;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TAddBlobsLogicTest)
{
    Y_UNIT_TEST(ShouldAddMixedAndMergedWriteResults)
    {
        auto state = MakeState();
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        const ui64 commitId = executor.CommitId();
        const auto mixedBlobId = executor.MakeBlobId(3);
        const auto mergedBlobId = executor.MakeBlobId(3);

        TBlockMask skipMask;
        skipMask.Set(1);

        auto args = MakeArgs(
            commitId,
            {{mixedBlobId, {1, 3, 5}, {11, 22, 33}, 1}},
            {{mergedBlobId, TBlockRange32::MakeClosedInterval(8, 11), skipMask,
              {44, 55, 66}}});

        RunExecute(executor, state, args, MakeCommitId(0, 50));

        const auto mixedBlobMeta = ReadBlobMeta(executor, mixedBlobId);
        UNIT_ASSERT(mixedBlobMeta.Defined());
        UNIT_ASSERT(mixedBlobMeta->HasMixedBlocks());
        UNIT_ASSERT_VALUES_EQUAL(3,
                                 mixedBlobMeta->GetMixedBlocks().BlocksSize());
        UNIT_ASSERT_VALUES_EQUAL(11, mixedBlobMeta->GetBlockChecksums(0));
        UNIT_ASSERT_VALUES_EQUAL(22, mixedBlobMeta->GetBlockChecksums(1));
        UNIT_ASSERT_VALUES_EQUAL(33, mixedBlobMeta->GetBlockChecksums(2));

        const auto mergedBlobMeta = ReadBlobMeta(executor, mergedBlobId);
        UNIT_ASSERT(mergedBlobMeta.Defined());
        UNIT_ASSERT(mergedBlobMeta->HasMergedBlocks());
        UNIT_ASSERT_VALUES_EQUAL(8,
                                 mergedBlobMeta->GetMergedBlocks().GetStart());
        UNIT_ASSERT_VALUES_EQUAL(11,
                                 mergedBlobMeta->GetMergedBlocks().GetEnd());
        UNIT_ASSERT_VALUES_EQUAL(
            1, mergedBlobMeta->GetMergedBlocks().GetSkipped());

        const auto mixedBlockMask = ReadBlockMask(executor, mixedBlobId);
        UNIT_ASSERT(mixedBlockMask.Defined());
        UNIT_ASSERT(!mixedBlockMask->Get(0));
        UNIT_ASSERT(!mixedBlockMask->Get(1));
        UNIT_ASSERT(!mixedBlockMask->Get(2));
        UNIT_ASSERT(mixedBlockMask->Get(3));

        const auto mergedBlockMask = ReadBlockMask(executor, mergedBlobId);
        UNIT_ASSERT(mergedBlockMask.Defined());
        UNIT_ASSERT(!mergedBlockMask->Get(0));
        UNIT_ASSERT(!mergedBlockMask->Get(1));
        UNIT_ASSERT(!mergedBlockMask->Get(2));
        UNIT_ASSERT(mergedBlockMask->Get(3));

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                TBlockVisitor mixedVisitor;
                UNIT_ASSERT(db.FindMixedBlocks(
                    mixedVisitor,
                    TVector<TBlock>{{1, commitId, false}, {3, commitId, false},
                                    {5, commitId, false}}));
                UNIT_ASSERT_VALUES_EQUAL(3, mixedVisitor.Records.size());
                for (const auto& record: mixedVisitor.Records) {
                    UNIT_ASSERT_VALUES_EQUAL(mixedBlobId, record.BlobId);
                }

                TBlockVisitor mergedVisitor;
                UNIT_ASSERT(db.FindMergedBlocks(
                    mergedVisitor, TBlockRange32::MakeClosedInterval(8, 11),
                    false, MaxBlocksInBlob));
                UNIT_ASSERT_VALUES_EQUAL(3, mergedVisitor.Records.size());
                UNIT_ASSERT_VALUES_EQUAL(8,
                                         mergedVisitor.Records[0].BlockIndex);
                UNIT_ASSERT_VALUES_EQUAL(10,
                                         mergedVisitor.Records[1].BlockIndex);
                UNIT_ASSERT_VALUES_EQUAL(11,
                                         mergedVisitor.Records[2].BlockIndex);
                for (const auto& record: mergedVisitor.Records) {
                    UNIT_ASSERT_VALUES_EQUAL(mergedBlobId, record.BlobId);
                }
            });

        UNIT_ASSERT_VALUES_EQUAL(1, state.GetStats().GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(3, state.GetStats().GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(1, state.GetStats().GetMergedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(3, state.GetStats().GetMergedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(6, state.GetStats().GetUsedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(2, state.GetGarbageQueue().GetNewBlobsCount());

        const auto mixedRangeStat = state.GetCompactionMap().Get(1);
        UNIT_ASSERT_VALUES_EQUAL(1, mixedRangeStat.BlobCount);
        UNIT_ASSERT_VALUES_EQUAL(3, mixedRangeStat.BlockCount);

        const auto mergedRangeStat = state.GetCompactionMap().Get(8);
        UNIT_ASSERT_VALUES_EQUAL(1, mergedRangeStat.BlobCount);
        UNIT_ASSERT_VALUES_EQUAL(3, mergedRangeStat.BlockCount);
    }

    Y_UNIT_TEST(ShouldMoveFreshAndL0BlobsIntoIndexes)
    {
        auto state = MakeState();
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        const ui64 commitId = MakeCommitId(0, 10);
        const auto freshBlobId =
            TPartialBlobId(0, 10, 0, 2 * DefaultBlockSize, 0, 0);
        const auto zeroBlobId = TPartialBlobId(0, 10, 0, 0, 1, 0);
        const auto l0BlobId =
            TPartialBlobId(0, 10, 0, 2 * DefaultBlockSize, 2, 0);

        const TVector<TOwningFreshBlock> freshBlocks = {
            {{2, MakeCommitId(0, 5), true}, "five", {}},
            {{2, MakeCommitId(0, 7), true}, "seven", {}},
            {{2, MakeCommitId(0, 6), true}, {}, {}},
            {{16, MakeCommitId(0, 4), false}, "sixteen", {}},
            {{17, MakeCommitId(0, 4), false}, "seventeen", {}},
        };
        state.InitFreshBlocks(freshBlocks);
        state.IncrementUnflushedFreshBlocksFromDbCount(3);
        state.IncrementUnflushedFreshBlocksFromChannelCount(2);

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                for (const auto& block: freshBlocks) {
                    if (block.Meta.IsStoredInDb) {
                        db.WriteFreshBlock(
                            block.Meta.BlockIndex,
                            block.Meta.CommitId,
                            block.Content ? TBlockDataRef(block.Content.data(),
                                                          block.Content.size())
                                          : TBlockDataRef());
                    }
                }
            });

        auto args =
            MakeArgs(commitId, {}, {},
                     {{freshBlobId, {{2, MakeCommitId(0, 5), true},
                                     {2, MakeCommitId(0, 7), true}},
                       {51, 71}, 1},
                      {zeroBlobId, {{2, MakeCommitId(0, 6), true}}, {}, 1}},
                     {{l0BlobId, {{16, MakeCommitId(0, 4), false},
                                  {17, MakeCommitId(0, 4), false}},
                       {161, 171}}}, ADD_FLUSH_RESULT);

        RunExecute(executor, state, args, MakeCommitId(0, 50));

        const auto freshBlockMask = ReadBlockMask(executor, freshBlobId);
        UNIT_ASSERT(freshBlockMask.Defined());
        UNIT_ASSERT(freshBlockMask->Get(0));
        UNIT_ASSERT(!freshBlockMask->Get(1));

        const auto zeroBlockMask = ReadBlockMask(executor, zeroBlobId);
        UNIT_ASSERT(zeroBlockMask.Defined());
        UNIT_ASSERT(IsBlockMaskFull(*zeroBlockMask, MaxBlocksInBlob));

        UNIT_ASSERT_VALUES_EQUAL(1, state.GetStats().GetUsedBlocksCount());
        UNIT_ASSERT(state.GetUsedBlocks().Test(2));
        UNIT_ASSERT_VALUES_EQUAL(2, state.GetStats().GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(2, state.GetStats().GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetUnflushedFreshBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(1, state.GetCleanupQueue().GetCount());
        UNIT_ASSERT_VALUES_EQUAL(2, state.GetGarbageQueue().GetNewBlobsCount());

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& database)
            {
                TPartitionDatabase db(database, MaxBlocksInBlob,
                                      MaxBlocksInBlob);
                TBlockVisitor visitor;
                UNIT_ASSERT(db.FindBlocksInL0Index(
                    visitor, TBlockRange32::MakeClosedInterval(16, 17)));
                UNIT_ASSERT_VALUES_EQUAL(2, visitor.Records.size());
                UNIT_ASSERT_VALUES_EQUAL(16, visitor.Records[0].BlockIndex);
                UNIT_ASSERT_VALUES_EQUAL(17, visitor.Records[1].BlockIndex);
                UNIT_ASSERT_VALUES_EQUAL(l0BlobId, visitor.Records[0].BlobId);

                TVector<TOwningFreshBlock> blocks;
                UNIT_ASSERT(db.ReadFreshBlocks(blocks));
                UNIT_ASSERT(blocks.empty());
            });
    }

    Y_UNIT_TEST(ShouldUpdateCompactionMapCountersWhenAddingL0Blobs)
    {
        auto state = MakeState();
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        const ui64 commitId = MakeCommitId(0, 10);
        const auto firstBlobId =
            TPartialBlobId(0, 10, 0, 2 * DefaultBlockSize, 0, 0);
        const auto secondBlobId =
            TPartialBlobId(0, 10, 0, 3 * DefaultBlockSize, 1, 0);

        const TVector<TOwningFreshBlock> freshBlocks = {
            {{1, MakeCommitId(0, 5), false}, "one", {}},
            {{2, MakeCommitId(0, 5), false}, "two", {}},
            {{2, MakeCommitId(0, 7), false}, "two-new", {}},
            {{3, MakeCommitId(0, 7), false}, "three", {}},
            {{4, MakeCommitId(0, 7), false}, "four", {}},
        };
        state.InitFreshBlocks(freshBlocks);
        state.IncrementUnflushedFreshBlocksFromChannelCount(
            freshBlocks.size());

        auto args = MakeArgs(
            commitId,
            {},
            {},
            {},
            {{firstBlobId,
              {{1, MakeCommitId(0, 5), false},
               {2, MakeCommitId(0, 5), false}},
              {11, 22}},
             {secondBlobId,
              {{2, MakeCommitId(0, 7), false},
               {3, MakeCommitId(0, 7), false},
               {4, MakeCommitId(0, 7), false}},
              {23, 33, 44}}},
            ADD_FLUSH_RESULT);

        RunExecute(executor, state, args, MakeCommitId(0, 50));

        const auto rangeStat =
            state.GetCompactionMapL0().GetCompactionMap().Get(1);
        UNIT_ASSERT_VALUES_EQUAL(2, rangeStat.BlobCount);
        UNIT_ASSERT_VALUES_EQUAL(5, rangeStat.BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(4, rangeStat.UsedBlockCount);
    }

    Y_UNIT_TEST(ShouldApplyCompactionResultsAndQueueAffectedBlobs)
    {
        auto state = MakeState();
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        const ui64 commitId = MakeCommitId(0, 20);
        const auto newBlobId =
            TPartialBlobId(0, 20, 0, 2 * DefaultBlockSize, 0, 0);
        const auto affectedBlobId =
            TPartialBlobId(0, 10, 0, 2 * DefaultBlockSize, 0, 0);

        TAffectedBlob affectedBlob;
        affectedBlob.BlockMask = GetFullBlockMask(MaxBlocksInBlob);

        TAffectedBlobs affectedBlobs;
        affectedBlobs.emplace(affectedBlobId, std::move(affectedBlob));

        auto args =
            MakeArgs(commitId, {{newBlobId, {1, 2}, {11, 22}, 1}}, {}, {}, {},
                     ADD_COMPACTION_RESULT, std::move(affectedBlobs),
                     {},
                     {{3, 4}});

        const ui64 deletionCommitId = MakeCommitId(0, 50);
        RunExecute(executor, state, args, deletionCommitId);

        const auto rangeStat = state.GetCompactionMap().Get(1);
        UNIT_ASSERT_VALUES_EQUAL(4, rangeStat.BlobCount);
        UNIT_ASSERT_VALUES_EQUAL(6, rangeStat.BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetStats().GetUsedBlocksCount());

        UNIT_ASSERT_VALUES_EQUAL(1, state.GetCleanupQueue().GetCount());
        const auto cleanupItems =
            state.GetCleanupQueue().GetItems(deletionCommitId);
        UNIT_ASSERT_VALUES_EQUAL(1, cleanupItems.size());
        UNIT_ASSERT_VALUES_EQUAL(affectedBlobId, cleanupItems[0].BlobId);

        const auto affectedBlockMask = ReadBlockMask(executor, affectedBlobId);
        UNIT_ASSERT(affectedBlockMask.Defined());
        UNIT_ASSERT(IsBlockMaskFull(*affectedBlockMask, MaxBlocksInBlob));

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                TVector<TCompactionCounter> compactionMap;
                UNIT_ASSERT(db.ReadCompactionMap(compactionMap));
                UNIT_ASSERT_VALUES_EQUAL(1, compactionMap.size());
                UNIT_ASSERT_VALUES_EQUAL(0, compactionMap[0].BlockIndex);
                UNIT_ASSERT_VALUES_EQUAL(4, compactionMap[0].Stat.BlobCount);
                UNIT_ASSERT_VALUES_EQUAL(6, compactionMap[0].Stat.BlockCount);

                TVector<TCleanupQueueItem> cleanupQueue;
                UNIT_ASSERT(db.ReadCleanupQueue(cleanupQueue));
                UNIT_ASSERT_VALUES_EQUAL(1, cleanupQueue.size());
                UNIT_ASSERT_VALUES_EQUAL(affectedBlobId,
                                         cleanupQueue[0].BlobId);
                UNIT_ASSERT_VALUES_EQUAL(deletionCommitId,
                                         cleanupQueue[0].CommitId);
            });
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
