#include "flush_blocks_visitor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>

#include <cloud/storage/core/libs/common/block_data_ref.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 BlockSize = 4;
constexpr ui32 CompactionRangeSize = 8;
constexpr ui32 CompactionThreshold = 8;
constexpr ui32 MaxBlobRangeSize = 32 * CompactionRangeSize;
constexpr ui32 MaxBlocksInBlob = MaxBlobRangeSize;
constexpr ui64 TabletId = 1;

TString MakeBlockContent(size_t visitOrderIndex)
{
    const char fill = static_cast<char>('a' + (visitOrderIndex % 26));
    return TString(BlockSize, fill);
}

ui32 ExpectedChecksum(size_t visitOrderIndex)
{
    const TString content = MakeBlockContent(visitOrderIndex);
    return ComputeDefaultDigest(TBlockDataRef(content.data(), content.size()));
}

void VisitDataBlock(
    TFlushBlocksVisitor& visitor,
    ui32 blockIndex,
    const TString& content)
{
    const TFreshBlock block{
        TBlock(blockIndex, 1, false),
        content,
        {}
    };

    visitor.Visit(block);
}

TVector<TFlushBlocksVisitor::TBlob> BuildBlobs(
    const TVector<ui32>& blockIndices,
    bool readBlockMaskOnCompactionOptimizationEnabled,
    ui64 splitByCompactionRangeMaxBlobCount,
    ui64 diskPrefixLengthWithBlockChecksumsInBlobs = 0,
    ui64 writeBlobSizeThreshold = 1)
{
    TCompactionMap compactionMap(
        CompactionRangeSize,
        BuildDefaultCompactionPolicy(CompactionThreshold));

    TVector<TFlushBlocksVisitor::TBlob> blobs;
    TFlushBlocksVisitor visitor(
        BlockSize,
        /*flushBlobSizeThreshold*/ 1,
        MaxBlobRangeSize,
        MaxBlocksInBlob,
        diskPrefixLengthWithBlockChecksumsInBlobs,
        compactionMap,
        readBlockMaskOnCompactionOptimizationEnabled,
        splitByCompactionRangeMaxBlobCount,
        TabletId,
        writeBlobSizeThreshold,
        blobs);

    for (size_t i = 0; i < blockIndices.size(); ++i) {
        const TString content = MakeBlockContent(i);
        VisitDataBlock(visitor, blockIndices[i], content);
    }

    visitor.Finish();
    return blobs;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFlushBlocksVisitorTest)
{
    Y_UNIT_TEST(ShouldCalculateCompactionRangeCount)
    {
        {
            const auto blobs = BuildBlobs(
                {1, 2, 10, 11, 20},
                /*readBlockMaskOnCompactionOptimizationEnabled*/ true,
                /*splitByCompactionRangeMaxBlobCount*/ 0);

            UNIT_ASSERT_VALUES_EQUAL(1, blobs.size());
            UNIT_ASSERT_VALUES_EQUAL(5, blobs[0].Blocks.size());
            UNIT_ASSERT_VALUES_EQUAL(3, blobs[0].CompactionRangeCount);
        }

        {
            const auto blobs = BuildBlobs(
                {1,
                 2,
                 MaxBlobRangeSize + 1,
                 MaxBlobRangeSize + 2,
                 2 * MaxBlobRangeSize,
                 2 * MaxBlobRangeSize + CompactionRangeSize,
                 2 * MaxBlobRangeSize + 2 * CompactionRangeSize},
                /*readBlockMaskOnCompactionOptimizationEnabled*/ true,
                /*splitByCompactionRangeMaxBlobCount*/ 0);

            UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

            UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, blobs[0].CompactionRangeCount);

            UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Blocks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, blobs[1].CompactionRangeCount);

            UNIT_ASSERT_VALUES_EQUAL(3, blobs[2].Blocks.size());
            UNIT_ASSERT_VALUES_EQUAL(3, blobs[2].CompactionRangeCount);
        }

        {
            const auto blobs = BuildBlobs(
                {1,
                 2,
                 MaxBlobRangeSize + 1,
                 MaxBlobRangeSize + 2,
                 2 * MaxBlobRangeSize,
                 2 * MaxBlobRangeSize + CompactionRangeSize,
                 2 * MaxBlobRangeSize + 2 * CompactionRangeSize},
                /*readBlockMaskOnCompactionOptimizationEnabled*/ false,
                /*splitByCompactionRangeMaxBlobCount*/ 0);

            UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

            UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, blobs[0].CompactionRangeCount);

            UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Blocks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, blobs[1].CompactionRangeCount);

            UNIT_ASSERT_VALUES_EQUAL(3, blobs[2].Blocks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, blobs[2].CompactionRangeCount);
        }
    }

    Y_UNIT_TEST(ShouldSplitBlobByCompactionRangeBorders)
    {
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 3);

        UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[0].Blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks[1].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[0].CompactionRangeCount);

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(10, blobs[1].Blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(11, blobs[1].Blocks[1].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[1].CompactionRangeCount);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs[2].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(20, blobs[2].Blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[2].CompactionRangeCount);
    }

    Y_UNIT_TEST(ShouldNotSplitBlobIfRangeCountIsGreaterThanLimit)
    {
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 2);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs.size());
        UNIT_ASSERT_VALUES_EQUAL(5, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(3, blobs[0].CompactionRangeCount);
    }

    Y_UNIT_TEST(ShouldNotSplitBlobIfOptimizationIsDisabled)
    {
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ false,
            /*splitByCompactionRangeMaxBlobCount*/ 3);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs.size());
        UNIT_ASSERT_VALUES_EQUAL(5, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, blobs[0].CompactionRangeCount);
    }

    Y_UNIT_TEST(ShouldSplitChecksumsByCompactionRangeBorders)
    {
        // All 5 blocks are below the checksum boundary => Checksums.size() ==
        // Blocks.size(). After the split into 3 compaction-range pieces the
        // checksums of each block must remain attached to that block.
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 3,
            /*diskPrefixLengthWithBlockChecksumsInBlobs*/ 100 * BlockSize);

        UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Checksums.size());
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(0), blobs[0].Checksums[0]);
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(1), blobs[0].Checksums[1]);

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Checksums.size());
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(2), blobs[1].Checksums[0]);
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(3), blobs[1].Checksums[1]);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs[2].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[2].Checksums.size());
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(4), blobs[2].Checksums[0]);
    }

    Y_UNIT_TEST(ShouldSplitBlobWithPartialChecksumsAcrossRangeBoundary)
    {
        // Checksum boundary = 15 blocks => blocks {1, 2, 10, 11} are
        // checksummed and Checksums.size() == 4, but block 20 is above the
        // boundary so the trailing piece must end up with an empty Checksums
        // vector while still preserving the checksums for the earlier pieces.
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 3,
            /*diskPrefixLengthWithBlockChecksumsInBlobs*/ 15 * BlockSize);

        UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Checksums.size());
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(0), blobs[0].Checksums[0]);
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(1), blobs[0].Checksums[1]);

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Checksums.size());
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(2), blobs[1].Checksums[0]);
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(3), blobs[1].Checksums[1]);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs[2].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, blobs[2].Checksums.size());
    }

    Y_UNIT_TEST(ShouldSplitBlobWithChecksumBoundaryInsidePiece)
    {
        // Checksum boundary = 11 blocks => blocks {1, 2, 10} are checksummed
        // (Checksums.size() == 3), block 11 lies inside the second
        // compaction-range piece but is above the boundary, and block 20 is
        // also above the boundary. The middle piece must therefore receive
        // exactly one checksum (for block 10) and the last piece must end up
        // with no checksums.
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 3,
            /*diskPrefixLengthWithBlockChecksumsInBlobs*/ 11 * BlockSize);

        UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Checksums.size());
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(0), blobs[0].Checksums[0]);
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(1), blobs[0].Checksums[1]);

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[1].Checksums.size());
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(2), blobs[1].Checksums[0]);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs[2].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, blobs[2].Checksums.size());
    }

    Y_UNIT_TEST(ShouldSplitWhenOriginalBlobIsSmallerThanWriteBlobThreshold)
    {
        // BlockSize = 4, blocks {1, 2, 10, 11, 20} => original blob is
        // 5 * 4 = 20 bytes which is below the WriteBlobSizeThreshold (100).
        // The split pieces are 8, 8 and 4 bytes — also all below the
        // threshold. Hugeness of the original blob matches hugeness of every
        // piece (none of them is huge), so the blob must be split.
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 3,
            /*diskPrefixLengthWithBlockChecksumsInBlobs*/ 0,
            /*writeBlobSizeThreshold*/ 100);

        UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[0].Blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks[1].BlockIndex);

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(10, blobs[1].Blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(11, blobs[1].Blocks[1].BlockIndex);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs[2].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(20, blobs[2].Blocks[0].BlockIndex);
    }

    Y_UNIT_TEST(ShouldSplitWhenOriginalAndAllSplitBlobsAreHuge)
    {
        // BlockSize = 4, blocks {1, 2, 10, 11, 20} => original blob is 20
        // bytes (huge w.r.t. threshold 4). Pieces are 8, 8 and 4 bytes — all
        // >= 4, i.e. all huge. Hugeness matches between the original and
        // every piece, so the blob must be split.
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 3,
            /*diskPrefixLengthWithBlockChecksumsInBlobs*/ 0,
            /*writeBlobSizeThreshold*/ 4);

        UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[0].Blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks[1].BlockIndex);

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(10, blobs[1].Blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(11, blobs[1].Blocks[1].BlockIndex);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs[2].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(20, blobs[2].Blocks[0].BlockIndex);
    }

    Y_UNIT_TEST(ShouldNotSplitWhenOriginalIsHugeButSomeSplitBlobsAreNotHuge)
    {
        // BlockSize = 4, blocks {1, 2, 10, 11, 20} => original blob is 20
        // bytes (huge w.r.t. threshold 8). Pieces are 8, 8 and 4 bytes —
        // the last piece (4 bytes) is below the threshold, so it is NOT
        // huge while the original blob IS huge. Splitting would turn a huge
        // blob into a mix of huge and small pieces, so it must be skipped.
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 3,
            /*diskPrefixLengthWithBlockChecksumsInBlobs*/ 0,
            /*writeBlobSizeThreshold*/ 8);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs.size());
        UNIT_ASSERT_VALUES_EQUAL(5, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(3, blobs[0].CompactionRangeCount);
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[0].Blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks[1].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(10, blobs[0].Blocks[2].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(11, blobs[0].Blocks[3].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(20, blobs[0].Blocks[4].BlockIndex);
    }

    Y_UNIT_TEST(ShouldKeepChecksumsTogetherWhenBlobIsNotSplit)
    {
        // splitByCompactionRangeMaxBlobCount = 2 is below the 3 compaction
        // ranges spanned by the visited blocks, so the blob must NOT be split.
        // All checksums should remain in a single blob in visit order.
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 2,
            /*diskPrefixLengthWithBlockChecksumsInBlobs*/ 100 * BlockSize);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs.size());
        UNIT_ASSERT_VALUES_EQUAL(5, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(5, blobs[0].Checksums.size());
        for (size_t i = 0; i < 5; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(i), blobs[0].Checksums[i]);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
