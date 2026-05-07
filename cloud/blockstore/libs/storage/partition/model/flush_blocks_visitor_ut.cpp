#include "flush_blocks_visitor.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 BlockSize = 4;
constexpr ui32 CompactionRangeSize = 8;
constexpr ui32 CompactionThreshold = 5;
constexpr ui32 MaxBlobRangeSize = 32 * CompactionRangeSize;
constexpr ui32 MaxBlocksInBlob = MaxBlobRangeSize;
constexpr ui64 TabletId = 1;

void VisitDataBlock(TFlushBlocksVisitor& visitor, ui32 blockIndex, char fill)
{
    TString content(BlockSize, fill);
    const TFreshBlock block{
        TBlock(blockIndex, 1, false),
        TStringBuf(content)
    };

    visitor.Visit(block);
}

TVector<TFlushBlocksVisitor::TBlob> BuildBlobs(
    const TVector<ui32>& blockIndices,
    bool splitOptimizationEnabled,
    ui64 splitByCompactionRangeMaxBlobCount)
{
    TCompactionMap compactionMap(
        CompactionRangeSize,
        BuildDefaultCompactionPolicy(CompactionThreshold));

    TVector<TFlushBlocksVisitor::TBlob> blobs;
    TFlushBlocksVisitor visitor(
        blobs,
        BlockSize,
        /*flushBlobSizeThreshold*/ 1,
        MaxBlobRangeSize,
        MaxBlocksInBlob,
        /*diskPrefixLengthWithBlockChecksumsInBlobs*/ 0,
        compactionMap,
        splitOptimizationEnabled,
        splitByCompactionRangeMaxBlobCount,
        TabletId);

    for (size_t i = 0; i < blockIndices.size(); ++i) {
        const char fill = static_cast<char>('a' + (i % 26));
        VisitDataBlock(visitor, blockIndices[i], fill);
    }

    visitor.Finish();
    return blobs;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFlushBlocksVisitorTest)
{
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

    Y_UNIT_TEST(ShouldCalculateCompactionRangeCount)
    {
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 0);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs.size());
        UNIT_ASSERT_VALUES_EQUAL(5, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(3, blobs[0].CompactionRangeCount);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
