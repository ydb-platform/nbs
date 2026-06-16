#include "part_compaction_logic.h"

#include "part_database.h"
#include "part_state.h"

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
        executor.WriteTx([&](TPartitionDatabase db) {
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

        executor.ReadTx([&](TPartitionDatabase db) {
            PrepareRangeCompaction(
                *MakeStorageConfig(),
                0,
                MakeCommitId(0, 100),
                TTestExecutor::TabletId,
                false,  // optimization disabled
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
        executor.WriteTx([&](TPartitionDatabase db) {
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

        executor.ReadTx([&](TPartitionDatabase db) {
            PrepareRangeCompaction(
                *MakeStorageConfig(),
                0,
                MakeCommitId(0, 100),
                TTestExecutor::TabletId,
                true,   // optimization enabled
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
        executor.WriteTx([&](TPartitionDatabase db) {
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

        executor.ReadTx([&](TPartitionDatabase db) {
            PrepareRangeCompaction(
                *MakeStorageConfig(),
                0,
                MakeCommitId(0, 100),
                TTestExecutor::TabletId,
                true,   // optimization enabled
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
        executor.WriteTx([&](TPartitionDatabase db) {
            blobId = executor.MakeBlobId(4);
            db.WriteMergedBlocks(
                blobId,
                TBlockRange32::MakeClosedInterval(0, 3),
                TBlockMask{});
        });

        state.GetCleanupQueue().Add({blobId, executor.CommitId()});

        TTxPartition::TRangeCompaction args(
            0,
            TBlockRange32::MakeClosedInterval(0, MaxBlocksCount - 1));
        THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlockMasks;
        THashSet<TPartialBlobId, TPartialBlobIdHash> blobsToReadBlobMetas;
        bool ready = true;

        executor.ReadTx([&](TPartitionDatabase db) {
            PrepareRangeCompaction(
                *MakeStorageConfig(),
                0,
                MakeCommitId(0, 100),
                TTestExecutor::TabletId,
                false,
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

    // PrepareRangeCompaction: when checksums are enabled and the compacted range
    // starts before the checksum boundary, every non-fresh merged block puts its
    // blob into blobsToReadBlobMetas.
    Y_UNIT_TEST(PrepareAddsToBlobMetasWhenChecksumsEnabled)
    {
        auto state = MakeState();
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });

        TPartialBlobId blobId;
        executor.WriteTx([&](TPartitionDatabase db) {
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

        executor.ReadTx([&](TPartitionDatabase db) {
            PrepareRangeCompaction(
                *config,
                0,
                MakeCommitId(0, 100),
                TTestExecutor::TabletId,
                true,
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
        executor.WriteTx([&](TPartitionDatabase db) {
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

        executor.ReadTx([&](TPartitionDatabase db) {
            PrepareRangeCompaction(
                *MakeStorageConfig(),   // DiskPrefixLength == 0 → checksums off
                0,
                MakeCommitId(0, 100),
                TTestExecutor::TabletId,
                true,
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

    // CompleteRangeCompaction: when ChecksumsEnabled is true, every non-fresh
    // merged block produces a TChecksumFixup entry with the correct index,
    // source BlobId and BlobOffset.
    Y_UNIT_TEST(CompleteCreatesChecksumFixupsWhenEnabled)
    {
        auto state = MakeState();
        auto storageInfo = MakeStorageInfo(1);  // channel 0 configured

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
            false,  // blobPatchingEnabled
            0,      // mergedBlobThreshold
            compactionCommitId,
            TTestExecutor::TabletId,
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
            storageInfo,
            state,
            args,
            requests,
            rangeCompactionInfos,
            0);

        UNIT_ASSERT_VALUES_EQUAL(1, rangeCompactionInfos.size());
        UNIT_ASSERT(rangeCompactionInfos[0].ChecksumFixups.empty());
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
