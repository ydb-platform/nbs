#include "part_compaction_logic.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRecreateBlobMetasTest)
{
    Y_UNIT_TEST(ShouldRecreateMergedBlobMeta)
    {
        auto args = MakeArgs();
        const auto blobId = TPartialBlobId(1, 0);

        TAffectedBlob ab;
        ab.MergedBlockRange = TBlockRange32::MakeClosedInterval(10, 20);
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
