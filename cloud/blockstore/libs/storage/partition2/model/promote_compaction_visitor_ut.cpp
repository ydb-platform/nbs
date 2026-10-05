#include "promote_compaction_visitor.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 BlockSize = 4;

bool VisitFreshBlock(
    TPromoteCompactionVisitor& visitor,
    ui32 blockIndex,
    ui64 commitId,
    TStringBuf content,
    TPartialBlobId blobId = {})
{
    return visitor.Visit(
        TFreshBlock(TBlock(blockIndex, commitId, false), content, blobId));
}

const TPromoteCompactionVisitor::TBlockMark& GetMark(
    const TPromoteCompactionVisitor::TBlob& blob,
    size_t index,
    ui32 expectedBlockIndex,
    ui64 expectedCommitId)
{
    UNIT_ASSERT_C(
        index < blob.BlockIndexToMark.size(),
        "Missing block mark at index " << index);

    const auto& [blockIndex, mark] = blob.BlockIndexToMark[index];
    UNIT_ASSERT_VALUES_EQUAL(expectedBlockIndex, blockIndex);
    UNIT_ASSERT_VALUES_EQUAL(expectedCommitId, mark.CommitId);
    return mark;
}

void AssertBlockIndices(
    const TPromoteCompactionVisitor::TBlob& blob,
    const TVector<ui32>& expectedBlockIndices,
    ui64 expectedCommitId)
{
    UNIT_ASSERT_VALUES_EQUAL(
        expectedBlockIndices.size(),
        blob.BlockIndexToMark.size());
    UNIT_ASSERT_VALUES_EQUAL(
        expectedBlockIndices.size(),
        blob.BlobContent.GetBlocksCount());

    for (size_t i = 0; i < expectedBlockIndices.size(); ++i) {
        GetMark(blob, i, expectedBlockIndices[i], expectedCommitId);
    }
}

void AssertFreshMark(
    const TPromoteCompactionVisitor::TBlockMark& mark,
    const TPartialBlobId& expectedBlobId,
    TStringBuf expectedContent)
{
    UNIT_ASSERT(
        std::holds_alternative<
            TPromoteCompactionVisitor::TFreshBlockMark>(
                mark.IndexSpecificMark));

    const auto& freshMark =
        std::get<TPromoteCompactionVisitor::TFreshBlockMark>(
            mark.IndexSpecificMark);
    UNIT_ASSERT_VALUES_EQUAL(expectedBlobId, freshMark.BlobId);
    UNIT_ASSERT_VALUES_EQUAL(expectedContent, freshMark.Content);
}

void AssertBlobMark(
    const TPromoteCompactionVisitor::TBlockMark& mark,
    const TPartialBlobId& expectedBlobId,
    ui16 expectedBlobOffset)
{
    UNIT_ASSERT(
        std::holds_alternative<
            TPromoteCompactionVisitor::TBlobBlockMark>(
                mark.IndexSpecificMark));

    const auto& blobMark =
        std::get<TPromoteCompactionVisitor::TBlobBlockMark>(
            mark.IndexSpecificMark);
    UNIT_ASSERT_VALUES_EQUAL(expectedBlobId, blobMark.BlobId);
    UNIT_ASSERT_VALUES_EQUAL(expectedBlobOffset, blobMark.BlobOffset);
}

void FillRequest(
    TPromoteCompactionVisitor::TReadBlobRequest& request,
    TStringBuf content)
{
    UNIT_ASSERT_VALUES_EQUAL(
        content.size(),
        SgListCopy(
            TBlockDataRef(content.data(), content.size()),
            request.Sglist));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPromoteCompactionVisitorTest)
{
    Y_UNIT_TEST(ShouldReturnNothingForEmptyVisitor)
    {
        TCleanupQueue cleanupQueue(BlockSize);
        TPromoteCompactionVisitor visitor(
            /*targetRangeBlocksCount*/ {4},
            /*targetBlobSizesForPromote*/ {0},
            BlockSize,
            /*maxBlocksInBlob*/ 2,
            /*allowBlockDuplicates*/ false,
            cleanupQueue);

        auto blobs = visitor.Finish().ResultedBlobs;
        UNIT_ASSERT(blobs.empty());
        UNIT_ASSERT(
            TPromoteCompactionVisitor::CollectReadBlobRequests(blobs).empty());
    }

    Y_UNIT_TEST(ShouldExcludeBlobsAlreadyInCleanupQueue)
    {
        for (bool l0: {true, false}) {
            const TPartialBlobId queuedBlobId(10, Max<ui64>());
            const TPartialBlobId liveBlobId(20, Max<ui64>());

            NProto::TBlobMeta2 blobMeta;
            auto* blocks = l0 ? blobMeta.MutableL0Blocks()
                              : blobMeta.MutableL1Blocks();
            blocks->AddBlocks(0);
            blocks->AddCommitIds(10);

            TCleanupQueue cleanupQueue(BlockSize);
            UNIT_ASSERT(cleanupQueue.Add({queuedBlobId, 30, blobMeta}));

            TPromoteCompactionVisitor visitor(
                /*targetRangeBlocksCount*/ {4},
                /*targetBlobSizesForPromote*/ {0},
                BlockSize,
                /*maxBlocksInBlob*/ 2,
                /*allowBlockDuplicates*/ false,
                cleanupQueue);

            UNIT_ASSERT(visitor.Visit(queuedBlobId, blobMeta));
            UNIT_ASSERT(visitor.Visit(liveBlobId, blobMeta));

            auto result = visitor.Finish();
            UNIT_ASSERT_VALUES_EQUAL(1, result.AffectedBlobs.size());
            UNIT_ASSERT(!result.AffectedBlobs.contains(queuedBlobId));
            const auto* affectedBlob = result.AffectedBlobs.FindPtr(liveBlobId);
            UNIT_ASSERT(affectedBlob);
            UNIT_ASSERT_VALUES_EQUAL(
                blobMeta.SerializeAsString(),
                affectedBlob->SerializeAsString());
        }
    }

    Y_UNIT_TEST(ShouldOrderBlocksAndSplitBlobsAtRangeAndSizeBoundaries)
    {
        const TPartialBlobId firstSourceBlobId(10, Max<ui64>());
        const TPartialBlobId secondSourceBlobId(20, Max<ui64>());
        const TPartialBlobId freshBlobId(30, Max<ui64>());

        TCleanupQueue cleanupQueue(BlockSize);
        TPromoteCompactionVisitor visitor(
            /*targetRangeBlocksCount*/ {4},
            /*targetBlobSizesForPromote*/ {0},
            BlockSize,
            /*maxBlocksInBlob*/ 2,
            /*allowBlockDuplicates*/ false,
            cleanupQueue);

        UNIT_ASSERT(VisitFreshBlock(visitor, 5, 50, "5555", freshBlobId));
        UNIT_ASSERT(VisitFreshBlock(visitor, 2, 20, "2222"));
        UNIT_ASSERT(visitor.Visit(0, 10, firstSourceBlobId, 6));
        UNIT_ASSERT(VisitFreshBlock(visitor, 1, 11, {}));
        UNIT_ASSERT(visitor.Visit(4, 40, secondSourceBlobId, 8));

        auto blobs = visitor.Finish().ResultedBlobs;
        UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].BlockIndexToMark.size());
        UNIT_ASSERT_VALUES_EQUAL(
            TString(2 * BlockSize, 0),
            blobs[0].BlobContent.AsString());
        AssertBlobMark(GetMark(blobs[0], 0, 0, 10), firstSourceBlobId, 6);
        AssertFreshMark(GetMark(blobs[0], 1, 1, 11), {}, {});

        UNIT_ASSERT_VALUES_EQUAL(1, blobs[1].BlockIndexToMark.size());
        UNIT_ASSERT_VALUES_EQUAL("2222", blobs[1].BlobContent.AsString());
        AssertFreshMark(GetMark(blobs[1], 0, 2, 20), {}, "2222");

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[2].BlockIndexToMark.size());
        UNIT_ASSERT_VALUES_EQUAL(
            TString(BlockSize, 0) + "5555",
            blobs[2].BlobContent.AsString());
        AssertBlobMark(GetMark(blobs[2], 0, 4, 40), secondSourceBlobId, 8);
        AssertFreshMark(
            GetMark(blobs[2], 1, 5, 50),
            freshBlobId,
            "5555");
    }

    Y_UNIT_TEST(ShouldBuildBlobsForSmallerRangesBeforeLargestRange)
    {
        TCleanupQueue cleanupQueue(BlockSize);
        TPromoteCompactionVisitor visitor(
            /*targetRangeBlocksCount*/ {4, 8, 16},
            /*targetBlobSizesForPromote*/ {3, 4, 100},
            BlockSize,
            /*maxBlocksInBlob*/ 8,
            /*allowBlockDuplicates*/ false,
            cleanupQueue);

        // Visit in reverse order to also check ordering inside each blob.
        for (ui32 blockIndex: {30, 29, 28, 20, 16, 13, 12, 9, 8, 5, 4, 2, 1, 0})
        {
            UNIT_ASSERT(VisitFreshBlock(visitor, blockIndex, 20, "live"));
        }

        auto result = visitor.Finish();
        const auto& blobs = result.ResultedBlobs;
        UNIT_ASSERT_VALUES_EQUAL(5, blobs.size());
        UNIT_ASSERT(result.AlreadyOverwrittenBlobs.empty());

        // Blobs exactly at the threshold win, including the last range.
        AssertBlockIndices(blobs[0], {0, 1, 2}, 20);
        AssertBlockIndices(blobs[1], {28, 29, 30}, 20);
        // Two sparse four-block ranges together fill an eight-block range.
        AssertBlockIndices(blobs[2], {8, 9, 12, 13}, 20);
        // A blob below the promotion threshold falls back, and remaining
        // sparse blocks are combined only within the largest range boundaries.
        AssertBlockIndices(blobs[3], {4, 5}, 20);
        AssertBlockIndices(blobs[4], {16, 20}, 20);

        for (const auto& blob: blobs) {
            for (size_t i = 0; i < blob.BlockIndexToMark.size(); ++i) {
                AssertFreshMark(blob.BlockIndexToMark[i].second, {}, "live");
                UNIT_ASSERT_VALUES_EQUAL(
                    "live",
                    blob.BlobContent.GetBlock(i).AsStringBuf());
            }
        }
    }

    Y_UNIT_TEST(ShouldKeepSparseTailsWhenSplittingPromotedBlobs)
    {
        TCleanupQueue cleanupQueue(BlockSize);
        TPromoteCompactionVisitor visitor(
            /*targetRangeBlocksCount*/ {16, 64},
            /*targetBlobSizesForPromote*/ {3, 100},
            BlockSize,
            /*maxBlocksInBlob*/ 4,
            /*allowBlockDuplicates*/ false,
            cleanupQueue);

        for (ui32 blockIndex: {0, 1, 2, 3, 4, 5, 16, 17, 18, 60, 61, 62, 63}) {
            UNIT_ASSERT(VisitFreshBlock(visitor, blockIndex, 20, "live"));
        }

        auto result = visitor.Finish();
        const auto& blobs = result.ResultedBlobs;
        UNIT_ASSERT_VALUES_EQUAL(4, blobs.size());
        UNIT_ASSERT(result.AlreadyOverwrittenBlobs.empty());

        AssertBlockIndices(blobs[0], {0, 1, 2, 3}, 20);
        AssertBlockIndices(blobs[1], {16, 17, 18}, 20);
        AssertBlockIndices(blobs[2], {60, 61, 62, 63}, 20);
        // The three-block range is promoted at the exact threshold, while the
        // two-block tail falls back to the largest range without being lost.
        AssertBlockIndices(blobs[3], {4, 5}, 20);
    }

    Y_UNIT_TEST(ShouldPackOverwrittenBlocksOnlyInLargestRange)
    {
        const TPartialBlobId overwrittenBlobId(10, Max<ui64>());

        TCleanupQueue cleanupQueue(BlockSize);
        TPromoteCompactionVisitor visitor(
            /*targetRangeBlocksCount*/ {4, 8, 16},
            /*targetBlobSizesForPromote*/ {2, 3, 100},
            BlockSize,
            /*maxBlocksInBlob*/ 5,
            /*allowBlockDuplicates*/ true,
            cleanupQueue);

        for (ui32 blockIndex: {0, 1, 2, 4, 5, 6, 16, 17, 18}) {
            UNIT_ASSERT(VisitFreshBlock(visitor, blockIndex, 20, "live"));
            UNIT_ASSERT(
                visitor.Visit(blockIndex, 10, overwrittenBlobId, blockIndex));
        }

        auto result = visitor.Finish();
        const auto& blobs = result.ResultedBlobs;
        UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());
        AssertBlockIndices(blobs[0], {0, 1, 2}, 20);
        AssertBlockIndices(blobs[1], {4, 5, 6}, 20);
        AssertBlockIndices(blobs[2], {16, 17, 18}, 20);

        // Even dense overwritten ranges bypass promotion. Pack them using
        // only the largest range boundaries and the maximum blob size.
        const auto& overwrittenBlobs = result.AlreadyOverwrittenBlobs;
        UNIT_ASSERT_VALUES_EQUAL(3, overwrittenBlobs.size());
        AssertBlockIndices(overwrittenBlobs[0], {0, 1, 2, 4, 5}, 10);
        AssertBlockIndices(overwrittenBlobs[1], {6}, 10);
        AssertBlockIndices(overwrittenBlobs[2], {16, 17, 18}, 10);

        for (const auto& blob: overwrittenBlobs) {
            UNIT_ASSERT_VALUES_EQUAL(
                TString(blob.BlockIndexToMark.size() * BlockSize, 0),
                blob.BlobContent.AsString());
            for (const auto& [blockIndex, mark]: blob.BlockIndexToMark) {
                AssertBlobMark(mark, overwrittenBlobId, blockIndex);
            }
        }
    }

    Y_UNIT_TEST(ShouldCountOnlyLiveBlocksForPromotion)
    {
        const TPartialBlobId overwrittenBlobId(10, Max<ui64>());

        TCleanupQueue cleanupQueue(BlockSize);
        TPromoteCompactionVisitor visitor(
            /*targetRangeBlocksCount*/ {4, 8},
            /*targetBlobSizesForPromote*/ {3, 100},
            BlockSize,
            /*maxBlocksInBlob*/ 8,
            /*allowBlockDuplicates*/ true,
            cleanupQueue);

        for (ui32 blockIndex: {0, 1}) {
            UNIT_ASSERT(
                visitor.Visit(blockIndex, 10, overwrittenBlobId, blockIndex));
            UNIT_ASSERT(VisitFreshBlock(visitor, blockIndex, 20, "live"));
        }
        UNIT_ASSERT(VisitFreshBlock(visitor, 4, 20, "live"));

        auto result = visitor.Finish();
        UNIT_ASSERT_VALUES_EQUAL(1, result.ResultedBlobs.size());
        AssertBlockIndices(result.ResultedBlobs[0], {0, 1, 4}, 20);
        UNIT_ASSERT_VALUES_EQUAL(1, result.AlreadyOverwrittenBlobs.size());
        AssertBlockIndices(result.AlreadyOverwrittenBlobs[0], {0, 1}, 10);
    }

    Y_UNIT_TEST(ShouldKeepMarkWithNewestCommitId)
    {
        const TPartialBlobId blobId1(1, Max<ui64>());
        const TPartialBlobId blobId2(2, Max<ui64>());
        const TPartialBlobId blobId3(3, Max<ui64>());
        const TPartialBlobId blobId4(4, Max<ui64>());
        const TPartialBlobId blobId5(5, Max<ui64>());

        TCleanupQueue cleanupQueue(BlockSize);
        TPromoteCompactionVisitor visitor(
            /*targetRangeBlocksCount*/ {100},
            /*targetBlobSizesForPromote*/ {0},
            BlockSize,
            /*maxBlocksInBlob*/ 10,
            /*allowBlockDuplicates*/ false,
            cleanupQueue);

        UNIT_ASSERT(VisitFreshBlock(visitor, 0, 10, "aaaa"));
        UNIT_ASSERT(visitor.Visit(0, 11, blobId1, 1));

        UNIT_ASSERT(visitor.Visit(1, 10, blobId2, 2));
        UNIT_ASSERT(VisitFreshBlock(visitor, 1, 11, "bbbb"));

        UNIT_ASSERT(VisitFreshBlock(visitor, 2, 10, "cccc"));
        UNIT_ASSERT(visitor.Visit(2, 9, blobId3, 3));

        UNIT_ASSERT(visitor.Visit(3, 10, blobId4, 4));
        UNIT_ASSERT(VisitFreshBlock(visitor, 3, 9, "dddd"));

        UNIT_ASSERT(visitor.Visit(4, 10, blobId5, 5));
        UNIT_ASSERT(VisitFreshBlock(visitor, 4, 10, "eeee"));

        UNIT_ASSERT(VisitFreshBlock(visitor, 5, 10, "ffff"));
        UNIT_ASSERT(visitor.Visit(5, 10, blobId5, 6));

        auto blobs = visitor.Finish().ResultedBlobs;
        UNIT_ASSERT_VALUES_EQUAL(1, blobs.size());
        UNIT_ASSERT_VALUES_EQUAL(6, blobs[0].BlockIndexToMark.size());
        UNIT_ASSERT_VALUES_EQUAL(
            TString(BlockSize, 0) + "bbbbcccc" + TString(2 * BlockSize, 0) +
                "ffff",
            blobs[0].BlobContent.AsString());

        AssertBlobMark(GetMark(blobs[0], 0, 0, 11), blobId1, 1);
        AssertFreshMark(GetMark(blobs[0], 1, 1, 11), {}, "bbbb");
        AssertFreshMark(GetMark(blobs[0], 2, 2, 10), {}, "cccc");
        AssertBlobMark(GetMark(blobs[0], 3, 3, 10), blobId4, 4);
        AssertBlobMark(GetMark(blobs[0], 4, 4, 10), blobId5, 5);
        AssertFreshMark(GetMark(blobs[0], 5, 5, 10), {}, "ffff");
    }

    Y_UNIT_TEST(ShouldKeepAllMarksWhenBlockDuplicatesAreAllowed)
    {
        const TPartialBlobId blobId1(1, Max<ui64>());
        const TPartialBlobId blobId2(2, Max<ui64>());

        TCleanupQueue cleanupQueue(BlockSize);
        TPromoteCompactionVisitor visitor(
            /*targetRangeBlocksCount*/ {100},
            /*targetBlobSizesForPromote*/ {0},
            BlockSize,
            /*maxBlocksInBlob*/ 10,
            /*allowBlockDuplicates*/ true,
            cleanupQueue);

        UNIT_ASSERT(visitor.Visit(0, 12, blobId2, 2));
        UNIT_ASSERT(VisitFreshBlock(visitor, 0, 10, "aaaa"));
        UNIT_ASSERT(visitor.Visit(0, 11, blobId1, 1));

        auto result = visitor.Finish();
        const auto& blobs = result.ResultedBlobs;
        const auto& overwrittenBlobs = result.AlreadyOverwrittenBlobs;
        UNIT_ASSERT_VALUES_EQUAL(1, blobs.size());
        UNIT_ASSERT_VALUES_EQUAL(1, overwrittenBlobs.size());
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[0].BlockIndexToMark.size());
        UNIT_ASSERT_VALUES_EQUAL(
            TString(BlockSize, 0),
            blobs[0].BlobContent.AsString());
        AssertBlobMark(GetMark(blobs[0], 0, 0, 12), blobId2, 2);

        const auto& overwrittenBlob = overwrittenBlobs[0];
        UNIT_ASSERT_VALUES_EQUAL(2, overwrittenBlob.BlockIndexToMark.size());
        UNIT_ASSERT_VALUES_EQUAL(2, overwrittenBlob.BlobContent.GetBlocksCount());
        const size_t freshMarkIndex =
            overwrittenBlob.BlockIndexToMark[0].second.CommitId == 10 ? 0 : 1;
        const size_t blobMarkIndex = 1 - freshMarkIndex;
        AssertFreshMark(
            GetMark(overwrittenBlob, freshMarkIndex, 0, 10),
            {},
            "aaaa");
        AssertBlobMark(
            GetMark(overwrittenBlob, blobMarkIndex, 0, 11),
            blobId1,
            1);
        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa",
            overwrittenBlob.BlobContent.GetBlock(freshMarkIndex).AsStringBuf());
        UNIT_ASSERT_VALUES_EQUAL(
            TString(BlockSize, 0),
            overwrittenBlob.BlobContent.GetBlock(blobMarkIndex).AsStringBuf());
    }

    Y_UNIT_TEST(ShouldCollectReadRequestsBySourceBlob)
    {
        const TPartialBlobId firstSourceBlobId(10, Max<ui64>());
        const TPartialBlobId secondSourceBlobId(20, Max<ui64>());

        TCleanupQueue cleanupQueue(BlockSize);
        TPromoteCompactionVisitor visitor(
            /*targetRangeBlocksCount*/ {3},
            /*targetBlobSizesForPromote*/ {0},
            BlockSize,
            /*maxBlocksInBlob*/ 2,
            /*allowBlockDuplicates*/ false,
            cleanupQueue);

        UNIT_ASSERT(visitor.Visit(4, 14, firstSourceBlobId, 9));
        UNIT_ASSERT(visitor.Visit(1, 11, secondSourceBlobId, 7));
        UNIT_ASSERT(visitor.Visit(0, 10, firstSourceBlobId, 3));
        UNIT_ASSERT(VisitFreshBlock(visitor, 2, 12, "F222"));

        auto blobs = visitor.Finish().ResultedBlobs;
        UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

        auto requests =
            TPromoteCompactionVisitor::CollectReadBlobRequests(blobs);
        UNIT_ASSERT_VALUES_EQUAL(2, requests.size());

        UNIT_ASSERT_VALUES_EQUAL(firstSourceBlobId, requests[0].BlobId);
        UNIT_ASSERT_VALUES_EQUAL(2, requests[0].BlobOffsets.size());
        UNIT_ASSERT_VALUES_EQUAL(3, requests[0].BlobOffsets[0]);
        UNIT_ASSERT_VALUES_EQUAL(9, requests[0].BlobOffsets[1]);
        FillRequest(requests[0], "aaaabbbb");

        UNIT_ASSERT_VALUES_EQUAL(secondSourceBlobId, requests[1].BlobId);
        UNIT_ASSERT_VALUES_EQUAL(1, requests[1].BlobOffsets.size());
        UNIT_ASSERT_VALUES_EQUAL(7, requests[1].BlobOffsets[0]);
        FillRequest(requests[1], "cccc");

        UNIT_ASSERT_VALUES_EQUAL("aaaacccc", blobs[0].BlobContent.AsString());
        UNIT_ASSERT_VALUES_EQUAL("F222", blobs[1].BlobContent.AsString());
        UNIT_ASSERT_VALUES_EQUAL("bbbb", blobs[2].BlobContent.AsString());
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
