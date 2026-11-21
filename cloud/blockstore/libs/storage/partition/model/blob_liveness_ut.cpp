#include "blob_liveness.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

TPartialBlobId MakeBlob(ui64 commitId, ui32 cookie)
{
    return TPartialBlobId(0, commitId, 0, 4096, cookie, 0);
}

TVector<TPartialBlobId> SortedLiveBlobs(
    TVector<TBlockVersion> versions,
    TVector<ui64> checkpointCommitIds = {},
    ui64 commitId = Max<ui64>())
{
    const auto liveBlobs = FindLiveBlobs(
        std::move(versions),
        commitId,
        std::move(checkpointCommitIds));
    TVector<TPartialBlobId> result(liveBlobs.begin(), liveBlobs.end());
    Sort(result);
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlobLivenessTest)
{
    Y_UNIT_TEST(ShouldTreatFullyOverwrittenBlobAsDead)
    {
        const auto oldBlob = MakeBlob(10, 1);
        const auto newBlob = MakeBlob(20, 1);

        const auto live = SortedLiveBlobs({
            {.BlockIndex = 1, .CommitId = 10, .BlobId = oldBlob},
            {.BlockIndex = 2, .CommitId = 10, .BlobId = oldBlob},
            {.BlockIndex = 1, .CommitId = 20, .BlobId = newBlob},
            {.BlockIndex = 2, .CommitId = 20, .BlobId = newBlob},
        });

        UNIT_ASSERT_VALUES_EQUAL(TVector<TPartialBlobId>{newBlob}, live);
    }

    Y_UNIT_TEST(ShouldKeepPartiallyOverwrittenBlobLive)
    {
        const auto oldBlob = MakeBlob(10, 1);
        const auto newBlob = MakeBlob(20, 1);

        const auto live = SortedLiveBlobs({
            {.BlockIndex = 1, .CommitId = 10, .BlobId = oldBlob},
            {.BlockIndex = 2, .CommitId = 10, .BlobId = oldBlob},
            {.BlockIndex = 1, .CommitId = 20, .BlobId = newBlob},
        });

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<TPartialBlobId>{oldBlob, newBlob}),
            live);
    }

    Y_UNIT_TEST(ShouldKeepBlobLiveForCheckpoint)
    {
        const auto oldBlob = MakeBlob(10, 1);
        const auto newBlob = MakeBlob(20, 1);
        const TVector<TBlockVersion> versions = {
            {.BlockIndex = 1, .CommitId = 10, .BlobId = oldBlob},
            {.BlockIndex = 1, .CommitId = 20, .BlobId = newBlob},
        };

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<TPartialBlobId>{oldBlob, newBlob}),
            SortedLiveBlobs(versions, {15}));

        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TPartialBlobId>{newBlob},
            SortedLiveBlobs(versions, {20, 25}));

        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TPartialBlobId>{newBlob},
            SortedLiveBlobs(versions, {5}));
    }

    Y_UNIT_TEST(ShouldIgnoreVersionsAfterCommit)
    {
        const auto oldBlob = MakeBlob(10, 1);
        const auto newBlob = MakeBlob(20, 1);
        const TVector<TBlockVersion> versions = {
            {.BlockIndex = 1, .CommitId = 10, .BlobId = oldBlob},
            {.BlockIndex = 1, .CommitId = 20, .BlobId = newBlob},
        };

        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TPartialBlobId>{oldBlob},
            SortedLiveBlobs(versions, {}, 15));

        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TPartialBlobId>{newBlob},
            SortedLiveBlobs(versions, {}, 20));
    }

    Y_UNIT_TEST(ShouldCompareVersionsInsideOneBlob)
    {
        // A flushed fresh blob holds two versions of block 1, and a merged
        // blob was written between them.
        const auto flushedBlob = MakeBlob(30, 1);
        const auto mergedBlob = MakeBlob(6, 1);
        const TVector<TBlockVersion> versions = {
            {.BlockIndex = 1, .CommitId = 5, .BlobId = flushedBlob},
            {.BlockIndex = 1, .CommitId = 6, .BlobId = mergedBlob},
            {.BlockIndex = 1, .CommitId = 7, .BlobId = flushedBlob},
        };

        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TPartialBlobId>{flushedBlob},
            SortedLiveBlobs(versions));

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<TPartialBlobId>{mergedBlob, flushedBlob}),
            SortedLiveBlobs(versions, {6}));

        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TPartialBlobId>{flushedBlob},
            SortedLiveBlobs(versions, {5}));
    }

    Y_UNIT_TEST(ShouldKeepAllVersionsTiedOnCommitLive)
    {
        const auto freshBlob = MakeBlob(10, 1);
        const auto mixedBlob = MakeBlob(10, 2);

        const auto live = SortedLiveBlobs({
            {.BlockIndex = 1, .CommitId = 10, .BlobId = freshBlob},
            {.BlockIndex = 1, .CommitId = 10, .BlobId = mixedBlob},
        });

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<TPartialBlobId>{freshBlob, mixedBlob}),
            live);
    }

    Y_UNIT_TEST(ShouldLetDeletionMarkerKillOlderBlob)
    {
        const auto blob = MakeBlob(10, 1);
        const auto deletionMarker = TPartialBlobId(0, 20, 0, 0, 0, 0);

        const auto live = SortedLiveBlobs({
            {.BlockIndex = 1, .CommitId = 10, .BlobId = blob},
            {.BlockIndex = 1, .CommitId = 20, .BlobId = deletionMarker},
        });

        UNIT_ASSERT_VALUES_EQUAL(TVector<TPartialBlobId>{deletionMarker}, live);
    }

    Y_UNIT_TEST(ShouldAppendMixedAndMergedBlobBlocks)
    {
        NProto::TBlobMeta mixed;
        mixed.MutableMixedBlocks()->AddBlocks(7);
        mixed.MutableMixedBlocks()->AddBlocks(3);
        mixed.MutableMixedBlocks()->AddBlocks(7);

        NProto::TBlobMeta merged;
        merged.MutableMergedBlocks()->SetStart(4);
        merged.MutableMergedBlocks()->SetEnd(6);

        TVector<ui32> blocks;
        AppendBlobBlocks(mixed, blocks);
        AppendBlobBlocks(merged, blocks);
        AppendBlobBlocks(NProto::TBlobMeta(), blocks);

        UNIT_ASSERT_VALUES_EQUAL((TVector<ui32>{7, 3, 7, 4, 5, 6}), blocks);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
