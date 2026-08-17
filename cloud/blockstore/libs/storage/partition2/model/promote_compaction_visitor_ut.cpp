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
        TPromoteCompactionVisitor visitor(
            /*targetRangeBlocksCount*/ 4,
            BlockSize,
            /*maxBlocksInBlob*/ 2,
            /*allowBlockDuplicates*/ false);

        auto blobs = visitor.Finish().ResultedBlobs;
        UNIT_ASSERT(blobs.empty());
        UNIT_ASSERT(
            TPromoteCompactionVisitor::CollectReadBlobRequests(blobs).empty());
    }

    Y_UNIT_TEST(ShouldOrderBlocksAndSplitBlobsAtRangeAndSizeBoundaries)
    {
        const TPartialBlobId firstSourceBlobId(10, Max<ui64>());
        const TPartialBlobId secondSourceBlobId(20, Max<ui64>());
        const TPartialBlobId freshBlobId(30, Max<ui64>());

        TPromoteCompactionVisitor visitor(
            /*targetRangeBlocksCount*/ 4,
            BlockSize,
            /*maxBlocksInBlob*/ 2,
            /*allowBlockDuplicates*/ false);

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

    Y_UNIT_TEST(ShouldKeepMarkWithNewestCommitId)
    {
        const TPartialBlobId blobId1(1, Max<ui64>());
        const TPartialBlobId blobId2(2, Max<ui64>());
        const TPartialBlobId blobId3(3, Max<ui64>());
        const TPartialBlobId blobId4(4, Max<ui64>());
        const TPartialBlobId blobId5(5, Max<ui64>());

        TPromoteCompactionVisitor visitor(
            /*targetRangeBlocksCount*/ 100,
            BlockSize,
            /*maxBlocksInBlob*/ 10,
            /*allowBlockDuplicates*/ false);

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

        TPromoteCompactionVisitor visitor(
            /*targetRangeBlocksCount*/ 100,
            BlockSize,
            /*maxBlocksInBlob*/ 10,
            /*allowBlockDuplicates*/ true);

        UNIT_ASSERT(visitor.Visit(0, 12, blobId2, 2));
        UNIT_ASSERT(VisitFreshBlock(visitor, 0, 10, "aaaa"));
        UNIT_ASSERT(visitor.Visit(0, 11, blobId1, 1));

        auto blobs = visitor.Finish().ResultedBlobs;
        UNIT_ASSERT_VALUES_EQUAL(1, blobs.size());
        UNIT_ASSERT_VALUES_EQUAL(3, blobs[0].BlockIndexToMark.size());
        UNIT_ASSERT_VALUES_EQUAL(
            TString("aaaa") + TString(2 * BlockSize, 0),
            blobs[0].BlobContent.AsString());

        AssertFreshMark(GetMark(blobs[0], 0, 0, 10), {}, "aaaa");
        AssertBlobMark(GetMark(blobs[0], 1, 0, 11), blobId1, 1);
        AssertBlobMark(GetMark(blobs[0], 2, 0, 12), blobId2, 2);
    }

    Y_UNIT_TEST(ShouldCollectReadRequestsBySourceBlob)
    {
        const TPartialBlobId firstSourceBlobId(10, Max<ui64>());
        const TPartialBlobId secondSourceBlobId(20, Max<ui64>());

        TPromoteCompactionVisitor visitor(
            /*targetRangeBlocksCount*/ 3,
            BlockSize,
            /*maxBlocksInBlob*/ 2,
            /*allowBlockDuplicates*/ false);

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
