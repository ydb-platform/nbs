#include "part_compaction_logic.h"

#include "part_database.h"
#include "part_state.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/partition_common/part_thread_safe_state.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <cloud/storage/core/libs/features/features_config.h>
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
        std::move(threadSafeState));
}

std::shared_ptr<TStorageConfig> MakeStorageConfig(ui64 diskPrefixLength = 0)
{
    NProto::TStorageServiceConfig proto;
    proto.SetDiskPrefixLengthWithBlockChecksumsInBlobs(diskPrefixLength);
    return std::make_shared<TStorageConfig>(
        std::move(proto),
        std::make_shared<NFeatures::TFeaturesConfig>());
}

NKikimr::TTabletStorageInfo MakeStorageInfo(ui32 numChannels)
{
    NKikimr::TTabletStorageInfo info;
    info.TabletID = TTestExecutor::TabletId;
    for (ui32 ch = 0; ch < numChannels; ++ch) {
        NKikimr::TTabletChannelInfo channelInfo(
            ch,
            NKikimr::TBlobStorageGroupType::ErasureNone);
        channelInfo.History.emplace_back(0, 1);
        info.Channels.push_back(std::move(channelInfo));
    }
    return info;
}

constexpr ui64 CommitId = 100;

TTxPartition::TRangeCompaction MakeArgs()
{
    return TTxPartition::TRangeCompaction(
        0,
        TBlockRange32::MakeClosedInterval(0, 7));
}

TAffectedBlob MakeFullyAvailableMixedBlob(
    TVector<TAffectedBlock> affectedBlocks)
{
    TAffectedBlob ab;
    ab.CompactionRangeCount = 1;
    ab.MaxCommitIdInCompactionRange = CommitId;
    ab.AffectedBlocks = std::move(affectedBlocks);
    return ab;
}

struct TPrepareCompleteResult
{
    bool Ready = false;
    TVector<TRangeCompactionInfo> RangeCompactionInfos;
    TVector<TBlobCompactionRequest> Requests;
};

TPrepareCompleteResult RunPrepareAndComplete(
    TPartitionState& state,
    TTestExecutor& executor,
    ui32 rangeIdx,
    ui64 compactionCommitId,
    bool recreateBlobMetasEnabled)
{
    const auto compactionRange =
        state.GetCompactionMap().GetBlockRange(rangeIdx);

    TTxPartition::TRangeCompaction args(rangeIdx, compactionRange);
    THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlockMasks;
    THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlobMetas;
    bool ready = true;

    executor.ReadTx(
        [&](TPartitionDatabase db)
        {
            PrepareRangeCompaction(
                *MakeStorageConfig(),
                0,
                compactionCommitId,
                TTestExecutor::TabletId,
                true,    // readBlockMaskOnCompactionOptimizationEnabled
                false,   // useRecreatedBlobMetasOnCleanup
                ready,
                db,
                state,
                args,
                blobsToReadBlockMasks,
                blobsToReadBlobMetas);
        });

    TVector<TBlobCompactionRequest> requests;
    TVector<TRangeCompactionInfo> rangeCompactionInfos;
    auto storageInfo = MakeStorageInfo(1);

    CompleteRangeCompaction(
        false,
        0,
        compactionCommitId,
        TTestExecutor::TabletId,
        recreateBlobMetasEnabled,
        storageInfo,
        state,
        args,
        requests,
        rangeCompactionInfos,
        0);

    return {
        ready,
        std::move(rangeCompactionInfos),
        std::move(requests),
    };
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRangeCompactionLogicTest)
{
    // PrepareRangeCompaction: blob goes into blobsToReadBlockMasks when
    // read-block-mask optimization is disabled, regardless of blob range count.
    Y_UNIT_TEST(PrepareAddsToBlockMasksWhenOptimizationDisabled)
    {
        auto state = MakeState();
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        TPartialBlobId blobId;
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                blobId = executor.MakeBlobId(4);
                db.WriteMergedBlocks(
                    blobId,
                    TBlockRange32::MakeClosedInterval(0, 3),
                    TBlockMask{});
            });

        TTxPartition::TRangeCompaction args(
            0,
            TBlockRange32::MakeClosedInterval(0, MaxBlocksCount - 1));
        THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlockMasks;
        THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlobMetas;
        bool ready = true;

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                PrepareRangeCompaction(
                    *MakeStorageConfig(),
                    0,
                    MakeCommitId(0, 100),
                    TTestExecutor::TabletId,
                    false,   // readBlockMaskOnCompactionOptimizationEnabled
                    false,   // useRecreatedBlobMetasOnCleanup
                    ready,
                    db,
                    state,
                    args,
                    blobsToReadBlockMasks,
                    blobsToReadBlobMetas);
            });

        UNIT_ASSERT(ready);
        UNIT_ASSERT_VALUES_EQUAL(1, blobsToReadBlockMasks.size());
        UNIT_ASSERT(blobsToReadBlockMasks.contains(blobId));
    }

    // PrepareRangeCompaction: a fully-available single-range blob is NOT added
    // to blobsToReadBlockMasks when optimization is enabled.
    Y_UNIT_TEST(PrepareSkipsBlockMasksWhenOptimizationEnabledAndSingleRange)
    {
        auto state = MakeState();
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        TPartialBlobId blobId;
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                blobId = executor.MakeBlobId(4);
                db.WriteMergedBlocks(
                    blobId,
                    TBlockRange32::MakeClosedInterval(0, 3),
                    TBlockMask{});
            });

        TTxPartition::TRangeCompaction args(
            0,
            TBlockRange32::MakeClosedInterval(0, MaxBlocksCount - 1));
        THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlockMasks;
        THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlobMetas;
        bool ready = true;

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                PrepareRangeCompaction(
                    *MakeStorageConfig(),
                    0,
                    MakeCommitId(0, 100),
                    TTestExecutor::TabletId,
                    true,    // readBlockMaskOnCompactionOptimizationEnabled
                    false,   // useRecreatedBlobMetasOnCleanup
                    ready,
                    db,
                    state,
                    args,
                    blobsToReadBlockMasks,
                    blobsToReadBlobMetas);
            });

        UNIT_ASSERT(ready);
        UNIT_ASSERT(blobsToReadBlockMasks.empty());
    }

    // PrepareRangeCompaction: a blob spanning two compaction ranges has
    // CompactionRangeCount > 1 and must go into blobsToReadBlockMasks even
    // when optimization is enabled.
    Y_UNIT_TEST(PrepareAddsToBlockMasksForMultiRangeBlob)
    {
        auto state = MakeState(2048);
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        // Blob [1023..1024] crosses the boundary between range 0 and range 1.
        TPartialBlobId blobId;
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                blobId = executor.MakeBlobId(2);
                db.WriteMergedBlocks(
                    blobId,
                    TBlockRange32::MakeClosedInterval(1023, 1024),
                    TBlockMask{});
            });

        TTxPartition::TRangeCompaction args(
            0,
            TBlockRange32::MakeClosedInterval(0, MaxBlocksCount - 1));
        THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlockMasks;
        THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlobMetas;
        bool ready = true;

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                PrepareRangeCompaction(
                    *MakeStorageConfig(),
                    0,
                    MakeCommitId(0, 100),
                    TTestExecutor::TabletId,
                    true,    // readBlockMaskOnCompactionOptimizationEnabled
                    false,   // useRecreatedBlobMetasOnCleanup
                    ready,
                    db,
                    state,
                    args,
                    blobsToReadBlockMasks,
                    blobsToReadBlobMetas);
            });

        UNIT_ASSERT(ready);
        UNIT_ASSERT_VALUES_EQUAL(1, blobsToReadBlockMasks.size());
        UNIT_ASSERT(blobsToReadBlockMasks.contains(blobId));
    }

    // PrepareRangeCompaction: a blob already in the cleanup queue gets a full
    // block mask assigned in-place and is NOT put into blobsToReadBlockMasks.
    Y_UNIT_TEST(PrepareSkipsBlockMasksForCleanupQueueBlob)
    {
        auto state = MakeState();
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        TPartialBlobId blobId;
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                blobId = executor.MakeBlobId(4);
                db.WriteMergedBlocks(
                    blobId,
                    TBlockRange32::MakeClosedInterval(0, 3),
                    TBlockMask{});
            });

        state.GetCleanupQueue().Add({blobId, executor.CommitId(), {}});

        TTxPartition::TRangeCompaction args(
            0,
            TBlockRange32::MakeClosedInterval(0, MaxBlocksCount - 1));
        THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlockMasks;
        THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlobMetas;
        bool ready = true;

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                PrepareRangeCompaction(
                    *MakeStorageConfig(),
                    0,
                    MakeCommitId(0, 100),
                    TTestExecutor::TabletId,
                    false,   // readBlockMaskOnCompactionOptimizationEnabled
                    false,   // useRecreatedBlobMetasOnCleanup
                    ready,
                    db,
                    state,
                    args,
                    blobsToReadBlockMasks,
                    blobsToReadBlobMetas);
            });

        UNIT_ASSERT(ready);
        UNIT_ASSERT(blobsToReadBlockMasks.empty());
    }

    // PrepareRangeCompaction: when checksums are enabled and the compacted
    // range starts before the checksum boundary, every non-fresh merged block
    // puts its blob into blobsToReadBlobMetas.
    Y_UNIT_TEST(PrepareAddsToBlobMetasWhenChecksumsEnabled)
    {
        auto state = MakeState();
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        TPartialBlobId blobId;
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                blobId = executor.MakeBlobId(4);
                db.WriteMergedBlocks(
                    blobId,
                    TBlockRange32::MakeClosedInterval(0, 3),
                    TBlockMask{});
            });

        auto config = MakeStorageConfig(1000 * DefaultBlockSize);

        TTxPartition::TRangeCompaction args(
            0,
            TBlockRange32::MakeClosedInterval(0, MaxBlocksCount - 1));
        THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlockMasks;
        THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlobMetas;
        bool ready = true;

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                PrepareRangeCompaction(
                    *config,
                    0,
                    MakeCommitId(0, 100),
                    TTestExecutor::TabletId,
                    true,
                    false,   // useRecreatedBlobMetasOnCleanup
                    ready,
                    db,
                    state,
                    args,
                    blobsToReadBlockMasks,
                    blobsToReadBlobMetas);
            });

        UNIT_ASSERT(ready);
        UNIT_ASSERT_VALUES_EQUAL(1, blobsToReadBlobMetas.size());
        UNIT_ASSERT(blobsToReadBlobMetas.contains(blobId));
    }

    // PrepareRangeCompaction: blobsToReadBlobMetas stays empty when checksums
    // are disabled (DiskPrefixLengthWithBlockChecksumsInBlobs == 0).
    Y_UNIT_TEST(PrepareEmptyBlobMetasWhenChecksumsDisabled)
    {
        auto state = MakeState();
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        TPartialBlobId blobId;
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                blobId = executor.MakeBlobId(4);
                db.WriteMergedBlocks(
                    blobId,
                    TBlockRange32::MakeClosedInterval(0, 3),
                    TBlockMask{});
            });

        TTxPartition::TRangeCompaction args(
            0,
            TBlockRange32::MakeClosedInterval(0, MaxBlocksCount - 1));
        THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlockMasks;
        THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlobMetas;
        bool ready = true;

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                PrepareRangeCompaction(
                    *MakeStorageConfig(),   // DiskPrefixLength == 0 → checksums
                                            // off
                    0,
                    MakeCommitId(0, 100),
                    TTestExecutor::TabletId,
                    true,    // readBlockMaskOnCompactionOptimizationEnabled
                    false,   // useRecreatedBlobMetasOnCleanup
                    ready,
                    db,
                    state,
                    args,
                    blobsToReadBlockMasks,
                    blobsToReadBlobMetas);
            });

        UNIT_ASSERT(ready);
        UNIT_ASSERT(blobsToReadBlobMetas.empty());
    }

    // PrepareRangeCompaction: when useRecreatedBlobMetasOnCleanup is enabled,
    // every blob in blobsToReadBlockMasks is also added to
    // blobsToReadBlobMetas.
    Y_UNIT_TEST(
        PrepareAddsToBlobMetasForEveryBlockMaskBlobWhenUseRecreatedBlobMetasOnCleanupEnabled)
    {
        auto state = MakeState();
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        TPartialBlobId blobId1;
        TPartialBlobId blobId2;
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                blobId1 = executor.MakeBlobId(4);
                db.WriteMergedBlocks(
                    blobId1,
                    TBlockRange32::MakeClosedInterval(0, 3),
                    TBlockMask{});

                blobId2 = executor.MakeBlobId(4);
                db.WriteMergedBlocks(
                    blobId2,
                    TBlockRange32::MakeClosedInterval(4, 7),
                    TBlockMask{});
            });

        TTxPartition::TRangeCompaction args(
            0,
            TBlockRange32::MakeClosedInterval(0, MaxBlocksCount - 1));
        THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlockMasks;
        THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlobMetas;
        bool ready = true;

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                PrepareRangeCompaction(
                    *MakeStorageConfig(),   // checksums off
                    0,
                    MakeCommitId(0, 100),
                    TTestExecutor::TabletId,
                    false,   // readBlockMaskOnCompactionOptimizationEnabled
                    true,    // useRecreatedBlobMetasOnCleanup
                    ready,
                    db,
                    state,
                    args,
                    blobsToReadBlockMasks,
                    blobsToReadBlobMetas);
            });

        UNIT_ASSERT(ready);
        UNIT_ASSERT_VALUES_EQUAL(2, blobsToReadBlockMasks.size());
        UNIT_ASSERT(blobsToReadBlockMasks.contains(blobId1));
        UNIT_ASSERT(blobsToReadBlockMasks.contains(blobId2));
        UNIT_ASSERT_VALUES_EQUAL(
            blobsToReadBlockMasks.size(),
            blobsToReadBlobMetas.size());
        for (const auto& blobId: blobsToReadBlockMasks) {
            UNIT_ASSERT(blobsToReadBlobMetas.contains(blobId));
        }
    }

    // CompleteRangeCompaction: when ChecksumsEnabled is true, every non-fresh
    // merged block produces a TChecksumFixup entry with the correct index,
    // source BlobId and BlobOffset.
    Y_UNIT_TEST(CompleteCreatesChecksumFixupsWhenEnabled)
    {
        auto state = MakeState();
        auto storageInfo = MakeStorageInfo(1);   // channel 0 configured

        // Source blob lives on channel 0, generation 0.
        const TPartialBlobId sourceBlobId(0, 1, 0, 4 * DefaultBlockSize, 0, 0);
        const ui16 sourceBlobOffset = 7;

        const auto compactionCommitId = MakeCommitId(0, 100);
        TTxPartition::TRangeCompaction args(
            0,
            TBlockRange32::MakeClosedInterval(0, 3));
        args.ChecksumsEnabled = true;

        auto& mark = args.GetBlockMark(0);
        mark.CommitId = MakeCommitId(0, 1);
        mark.BlobId = sourceBlobId;
        mark.BlobOffset = sourceBlobOffset;

        args.AffectedBlobs[sourceBlobId].Offsets.push_back(sourceBlobOffset);

        TVector<TBlobCompactionRequest> requests;
        TVector<TRangeCompactionInfo> rangeCompactionInfos;

        CompleteRangeCompaction(
            false,   // blobPatchingEnabled
            0,       // mergedBlobThreshold
            compactionCommitId,
            TTestExecutor::TabletId,
            false,   // shouldRecreateBlobMetas
            storageInfo,
            state,
            args,
            requests,
            rangeCompactionInfos,
            0);

        UNIT_ASSERT_VALUES_EQUAL(1, rangeCompactionInfos.size());
        const auto& result = rangeCompactionInfos[0];
        UNIT_ASSERT_VALUES_EQUAL(1, result.ChecksumFixups.size());
        UNIT_ASSERT_VALUES_EQUAL(0, result.ChecksumFixups[0].ChecksumIndex);
        UNIT_ASSERT_VALUES_EQUAL(sourceBlobId, result.ChecksumFixups[0].BlobId);
        UNIT_ASSERT_VALUES_EQUAL(
            sourceBlobOffset,
            result.ChecksumFixups[0].BlobOffset);
    }

    // CompleteRangeCompaction: when ChecksumsEnabled is false, no checksum
    // fixup entries are produced regardless of the block marks present.
    Y_UNIT_TEST(CompleteEmptyChecksumFixupsWhenDisabled)
    {
        auto state = MakeState();
        auto storageInfo = MakeStorageInfo(1);

        const TPartialBlobId sourceBlobId(0, 1, 0, 4 * DefaultBlockSize, 0, 0);

        const auto compactionCommitId = MakeCommitId(0, 100);
        TTxPartition::TRangeCompaction args(
            0,
            TBlockRange32::MakeClosedInterval(0, 3));
        args.ChecksumsEnabled = false;

        auto& mark = args.GetBlockMark(0);
        mark.CommitId = MakeCommitId(0, 1);
        mark.BlobId = sourceBlobId;
        mark.BlobOffset = 0;

        args.AffectedBlobs[sourceBlobId].Offsets.push_back(0);

        TVector<TBlobCompactionRequest> requests;
        TVector<TRangeCompactionInfo> rangeCompactionInfos;

        CompleteRangeCompaction(
            false,
            0,
            compactionCommitId,
            TTestExecutor::TabletId,
            false,   // shouldRecreateBlobMetas
            storageInfo,
            state,
            args,
            requests,
            rangeCompactionInfos,
            0);

        UNIT_ASSERT_VALUES_EQUAL(1, rangeCompactionInfos.size());
        UNIT_ASSERT(rangeCompactionInfos[0].ChecksumFixups.empty());
    }

    // PrepareRangeCompaction + CompleteRangeCompaction: merged blobs get
    // RecreatedBlobMeta with MergedBlocks populated from the index.
    Y_UNIT_TEST(CompleteRecreatesMergedBlobMetaAfterPrepare)
    {
        auto state = MakeState();
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        const auto blockRange = TBlockRange32::MakeClosedInterval(0, 3);
        TBlockMask skipMask;
        skipMask.Set(1);

        TPartialBlobId blobId;
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                blobId = executor.MakeBlobId(4);
                db.WriteMergedBlocks(blobId, blockRange, skipMask);
            });

        const auto compactionCommitId = MakeCommitId(0, 100);
        const auto result =
            RunPrepareAndComplete(state, executor, 0, compactionCommitId, true);

        UNIT_ASSERT(result.Ready);
        UNIT_ASSERT_VALUES_EQUAL(1, result.RangeCompactionInfos.size());

        const auto& affectedBlobs =
            result.RangeCompactionInfos[0].AffectedBlobs;
        UNIT_ASSERT(affectedBlobs.contains(blobId));

        const auto& recreatedMeta = affectedBlobs.at(blobId).RecreatedBlobMeta;
        UNIT_ASSERT(recreatedMeta);
        UNIT_ASSERT(recreatedMeta->HasMergedBlocks());
        UNIT_ASSERT(!recreatedMeta->HasMixedBlocks());

        const auto& mergedBlocks = recreatedMeta->GetMergedBlocks();
        UNIT_ASSERT_VALUES_EQUAL(blockRange.Start, mergedBlocks.GetStart());
        UNIT_ASSERT_VALUES_EQUAL(blockRange.End, mergedBlocks.GetEnd());
        UNIT_ASSERT_VALUES_EQUAL(skipMask.Count(), mergedBlocks.GetSkipped());
    }

    // PrepareRangeCompaction + CompleteRangeCompaction: fully-available mixed
    // blobs (CompactionRangeCount == 1) get RecreatedBlobMeta with MixedBlocks.
    Y_UNIT_TEST(CompleteRecreatesMixedBlobMetaWhenFullyAvailable)
    {
        auto state = MakeState();
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        TPartialBlobId blobId;
        const TVector<ui32> blockIndices = {0, 1, 2};
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                blobId = executor.MakeBlobId(blockIndices.size());
                db.WriteMixedBlocks(blobId, blockIndices, 1);
            });

        const auto compactionCommitId = MakeCommitId(0, 100);
        const auto result =
            RunPrepareAndComplete(state, executor, 0, compactionCommitId, true);

        UNIT_ASSERT(result.Ready);
        UNIT_ASSERT_VALUES_EQUAL(1, result.RangeCompactionInfos.size());

        const auto& affectedBlobs =
            result.RangeCompactionInfos[0].AffectedBlobs;
        UNIT_ASSERT(affectedBlobs.contains(blobId));

        const auto& ab = affectedBlobs.at(blobId);
        UNIT_ASSERT_VALUES_EQUAL(1, ab.CompactionRangeCount);

        const auto& recreatedMeta = ab.RecreatedBlobMeta;
        UNIT_ASSERT(recreatedMeta);
        UNIT_ASSERT(recreatedMeta->HasMixedBlocks());
        UNIT_ASSERT(!recreatedMeta->HasMergedBlocks());

        const auto& mixedBlocks = recreatedMeta->GetMixedBlocks();
        UNIT_ASSERT_VALUES_EQUAL(blockIndices.size(), mixedBlocks.BlocksSize());
        UNIT_ASSERT_VALUES_EQUAL(
            blockIndices.size(),
            mixedBlocks.CommitIdsSize());

        for (size_t i = 0; i < blockIndices.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(blockIndices[i], mixedBlocks.GetBlocks(i));
            UNIT_ASSERT_VALUES_EQUAL(
                blobId.CommitId(),
                mixedBlocks.GetCommitIds(i));
        }
    }

    // PrepareRangeCompaction + CompleteRangeCompaction: mixed blobs that span
    // multiple compaction ranges do not get RecreatedBlobMeta.
    Y_UNIT_TEST(CompleteSkipsMixedBlobMetaWhenNotFullyAvailable)
    {
        auto state = MakeState();
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        TPartialBlobId blobId;
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                blobId = executor.MakeBlobId(3);
                db.WriteMixedBlocks(blobId, {0, 1, 2}, 2);
            });

        const auto compactionCommitId = MakeCommitId(0, 100);
        const auto result =
            RunPrepareAndComplete(state, executor, 0, compactionCommitId, true);

        UNIT_ASSERT(result.Ready);
        UNIT_ASSERT_VALUES_EQUAL(1, result.RangeCompactionInfos.size());

        const auto& affectedBlobs =
            result.RangeCompactionInfos[0].AffectedBlobs;
        UNIT_ASSERT(affectedBlobs.contains(blobId));
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            affectedBlobs.at(blobId).CompactionRangeCount);
        UNIT_ASSERT(!affectedBlobs.at(blobId).RecreatedBlobMeta);
    }

    // PrepareRangeCompaction + CompleteRangeCompaction: mixed blobs that have
    // at least one block with commit id above the compaction commit id do not
    // get RecreatedBlobMeta, even if other blocks are visible to compaction.
    Y_UNIT_TEST(
        CompleteSkipsMixedBlobMetaWhenBlockCommitIdExceedsCompactionCommitId)
    {
        auto state = MakeState();
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        const ui64 lowCommitId = MakeCommitId(0, 50);
        const ui64 highCommitId = MakeCommitId(0, 150);
        const auto compactionCommitId = MakeCommitId(0, 100);

        TPartialBlobId blobId;
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                blobId = executor.MakeBlobId(2);
                state.WriteMixedBlock(
                    db,
                    TMixedBlock(blobId, lowCommitId, 0, 0, 1));
                state.WriteMixedBlock(
                    db,
                    TMixedBlock(blobId, highCommitId, 1, 1, 1));
            });

        const auto result =
            RunPrepareAndComplete(state, executor, 0, compactionCommitId, true);

        UNIT_ASSERT(result.Ready);
        UNIT_ASSERT_VALUES_EQUAL(1, result.RangeCompactionInfos.size());

        const auto& affectedBlobs =
            result.RangeCompactionInfos[0].AffectedBlobs;
        UNIT_ASSERT(affectedBlobs.contains(blobId));

        const auto& ab = affectedBlobs.at(blobId);
        UNIT_ASSERT_VALUES_EQUAL(1, ab.CompactionRangeCount);
        UNIT_ASSERT(ab.MaxCommitIdInCompactionRange > compactionCommitId);
        UNIT_ASSERT(!ab.RecreatedBlobMeta);
    }
}

Y_UNIT_TEST_SUITE(TRecreateBlobMetasTest)
{
    Y_UNIT_TEST(ShouldRecreateMergedBlobMeta)
    {
        auto args = MakeArgs();
        const auto blobId = TPartialBlobId(1, 0);

        TAffectedBlob ab;
        ab.MergedBlobsSpecificInfo.ConstructInPlace();
        ab.MergedBlobsSpecificInfo->BlockRange =
            TBlockRange32::MakeClosedInterval(10, 20);
        ab.MergedBlobsSpecificInfo->SkippedBlocksCount = 11;
        args.AffectedBlobs.emplace(blobId, std::move(ab));

        RecreateBlobMetas(args, CommitId);

        const auto& recreatedMeta =
            args.AffectedBlobs.at(blobId).RecreatedBlobMeta;
        UNIT_ASSERT(recreatedMeta);
        UNIT_ASSERT(recreatedMeta->HasMergedBlocks());
        UNIT_ASSERT(!recreatedMeta->HasMixedBlocks());

        const auto& mergedBlocks = recreatedMeta->GetMergedBlocks();
        UNIT_ASSERT_VALUES_EQUAL(10u, mergedBlocks.GetStart());
        UNIT_ASSERT_VALUES_EQUAL(20u, mergedBlocks.GetEnd());
        UNIT_ASSERT_VALUES_EQUAL(11u, mergedBlocks.GetSkipped());
    }

    Y_UNIT_TEST(ShouldRecreateMixedBlobMetaWhenFullyAvailable)
    {
        auto args = MakeArgs();
        const auto blobId = TPartialBlobId(2, 0);

        args.AffectedBlobs.emplace(
            blobId,
            MakeFullyAvailableMixedBlob({
                {.BlockIndex = 5, .CommitId = 50},
                {.BlockIndex = 7, .CommitId = 80},
            }));

        RecreateBlobMetas(args, CommitId);

        const auto& recreatedMeta =
            args.AffectedBlobs.at(blobId).RecreatedBlobMeta;
        UNIT_ASSERT(recreatedMeta);
        UNIT_ASSERT(recreatedMeta->HasMixedBlocks());
        UNIT_ASSERT(!recreatedMeta->HasMergedBlocks());

        const auto& mixedBlocks = recreatedMeta->GetMixedBlocks();
        UNIT_ASSERT_VALUES_EQUAL(2, mixedBlocks.BlocksSize());
        UNIT_ASSERT_VALUES_EQUAL(2, mixedBlocks.CommitIdsSize());
        UNIT_ASSERT_VALUES_EQUAL(5u, mixedBlocks.GetBlocks(0));
        UNIT_ASSERT_VALUES_EQUAL(50u, mixedBlocks.GetCommitIds(0));
        UNIT_ASSERT_VALUES_EQUAL(7u, mixedBlocks.GetBlocks(1));
        UNIT_ASSERT_VALUES_EQUAL(80u, mixedBlocks.GetCommitIds(1));
    }

    Y_UNIT_TEST(ShouldSkipMixedBlobMetaWhenSpanningMultipleCompactionRanges)
    {
        auto args = MakeArgs();
        const auto blobId = TPartialBlobId(3, 0);

        auto ab = MakeFullyAvailableMixedBlob({
            {.BlockIndex = 1, .CommitId = 10},
        });
        ab.CompactionRangeCount = 2;
        args.AffectedBlobs.emplace(blobId, std::move(ab));

        RecreateBlobMetas(args, CommitId);

        UNIT_ASSERT(!args.AffectedBlobs.at(blobId).RecreatedBlobMeta);
    }

    Y_UNIT_TEST(ShouldSkipMixedBlobMetaWhenMaxCommitIdExceedsCompactionCommitId)
    {
        auto args = MakeArgs();
        const auto blobId = TPartialBlobId(4, 0);

        auto ab = MakeFullyAvailableMixedBlob({
            {.BlockIndex = 2, .CommitId = 10},
        });
        ab.MaxCommitIdInCompactionRange = CommitId + 1;
        args.AffectedBlobs.emplace(blobId, std::move(ab));

        RecreateBlobMetas(args, CommitId);

        UNIT_ASSERT(!args.AffectedBlobs.at(blobId).RecreatedBlobMeta);
    }

    Y_UNIT_TEST(
        ShouldRecreateMixedBlobMetaWhenMaxCommitIdEqualsCompactionCommitId)
    {
        auto args = MakeArgs();
        const auto blobId = TPartialBlobId(5, 0);

        args.AffectedBlobs.emplace(
            blobId,
            MakeFullyAvailableMixedBlob({
                {.BlockIndex = 3, .CommitId = CommitId},
            }));

        RecreateBlobMetas(args, CommitId);

        const auto& recreatedMeta =
            args.AffectedBlobs.at(blobId).RecreatedBlobMeta;
        UNIT_ASSERT(recreatedMeta);
        UNIT_ASSERT(recreatedMeta->HasMixedBlocks());

        const auto& mixedBlocks = recreatedMeta->GetMixedBlocks();
        UNIT_ASSERT_VALUES_EQUAL(1, mixedBlocks.BlocksSize());
        UNIT_ASSERT_VALUES_EQUAL(3u, mixedBlocks.GetBlocks(0));
        UNIT_ASSERT_VALUES_EQUAL(CommitId, mixedBlocks.GetCommitIds(0));
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
