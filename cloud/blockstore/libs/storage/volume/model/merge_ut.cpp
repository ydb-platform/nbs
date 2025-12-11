#include "merge.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMergeTest)
{
    Y_UNIT_TEST(ShouldCorrectlyMergeStripedBitMaskSinglePartition)
    {
        const auto originalRange = TBlockRange64::WithLength(0, 6);
        const ui32 blocksPerStripe = 2;
        const ui32 partitionsCount = 1;

        TString dstMask;
        MergeStripedBitMask(
            originalRange,
            TBlockRange64::WithLength(0, 6),
            blocksPerStripe,
            partitionsCount,
            0,   // partitionId
            TString(1, char(0b110111)),
            dstMask);
        UNIT_ASSERT_VALUES_EQUAL(1, dstMask.size());
        UNIT_ASSERT_VALUES_EQUAL(0b00110111, ui8(dstMask[0]));
    }

    Y_UNIT_TEST(ShouldCorrectlyMergeStripedBitMaskMultiPartition)
    {
        const auto originalRange = TBlockRange64::WithLength(0, 11);
        const ui32 blocksPerStripe = 2;
        const ui32 partitionsCount = 3;

        /*
         block#  | 0 1 | 2 3 | 4 5 | 6 7 | 8 9 | 10 |
         part#   |  0  |  1  |  2  |  0  |  1  |  2 |
         byte    |        0              |     1    |
         */
        {
            // from part 0
            TString dstMask;
            MergeStripedBitMask(
                originalRange,
                TBlockRange64::WithLength(0, 4),
                blocksPerStripe,
                partitionsCount,
                0,   // partitionId
                TString(1, char(0b1101)),
                dstMask);
            UNIT_ASSERT_VALUES_EQUAL(2, dstMask.size());
            UNIT_ASSERT_VALUES_EQUAL(0b11000001, ui8(dstMask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000000, ui8(dstMask[1]));
        }

        {
            // from part 1
            TString dstMask;
            MergeStripedBitMask(
                originalRange,
                TBlockRange64::WithLength(0, 4),
                blocksPerStripe,
                partitionsCount,
                1,   // partitionId
                TString(1, char(0b1101)),
                dstMask);
            UNIT_ASSERT_VALUES_EQUAL(2, dstMask.size());
            UNIT_ASSERT_VALUES_EQUAL(0b00000100, ui8(dstMask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000011, ui8(dstMask[1]));
        }

        {
            // from part 2
            TString dstMask;
            MergeStripedBitMask(
                originalRange,
                TBlockRange64::WithLength(0, 3),
                blocksPerStripe,
                partitionsCount,
                2,   // partitionId
                TString(1, char(0b111)),
                dstMask);
            UNIT_ASSERT_VALUES_EQUAL(2, dstMask.size());
            UNIT_ASSERT_VALUES_EQUAL(0b00110000, ui8(dstMask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000100, ui8(dstMask[1]));
        }

        {
            // from part 0
            TString dstMask;
            MergeStripedBitMask(
                originalRange,
                TBlockRange64::WithLength(0, 4),
                blocksPerStripe,
                partitionsCount,
                0,   // partitionId
                TString(1, char(0b1111)),
                dstMask);

            // from part 1
            MergeStripedBitMask(
                originalRange,
                TBlockRange64::WithLength(0, 4),
                blocksPerStripe,
                partitionsCount,
                1,   // partitionId
                TString(1, char(0b1111)),
                dstMask);

            // from part 2
            MergeStripedBitMask(
                originalRange,
                TBlockRange64::WithLength(0, 3),
                blocksPerStripe,
                partitionsCount,
                2,   // partitionId
                TString(1, char(0b111)),
                dstMask);
            UNIT_ASSERT_VALUES_EQUAL(2, dstMask.size());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(dstMask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000111, ui8(dstMask[1]));
        }
    }

    Y_UNIT_TEST(ShouldSplitFreshBlockRangeFromRelativeToGlobalIndices)
    {
        NProto::TFreshBlockRange freshData;
        freshData.SetStartIndex(5);
        freshData.SetBlocksCount(8);
        freshData.MutableBlocksContent()->append(TString(1024, 'X'));

        NProto::TDescribeBlocksResponse dst;
        SplitFreshBlockRangeFromRelativeToGlobalIndices(
            freshData,
            4,     // blocksPerStripe
            128,   // blockSize
            2,     // partitionsCount
            0,     // partitionId
            &dst);

        UNIT_ASSERT_VALUES_EQUAL(3, dst.FreshBlockRangesSize());

        const auto& range1 = dst.GetFreshBlockRanges(0);
        UNIT_ASSERT_VALUES_EQUAL(9, range1.GetStartIndex());
        UNIT_ASSERT_VALUES_EQUAL(3, range1.GetBlocksCount());

        const auto& range2 = dst.GetFreshBlockRanges(1);
        UNIT_ASSERT_VALUES_EQUAL(16, range2.GetStartIndex());
        UNIT_ASSERT_VALUES_EQUAL(4, range2.GetBlocksCount());

        TString actualContent;
        for (size_t i = 0; i < dst.FreshBlockRangesSize(); ++i) {
            const auto& freshRange = dst.GetFreshBlockRanges(i);
            actualContent += freshRange.GetBlocksContent();
        }

        UNIT_ASSERT_VALUES_EQUAL(1024, actualContent.size());
        for (size_t i = 0; i < actualContent.size(); i++) {
            UNIT_ASSERT_VALUES_EQUAL('X', actualContent[i]);
        }
    }

    Y_UNIT_TEST(ShouldSplitBlobPieceRangeFromRelativeToGlobalIndices)
    {
        NProto::TRangeInBlob rangeInBlob;
        rangeInBlob.SetBlobOffset(10);
        rangeInBlob.SetBlockIndex(13);
        rangeInBlob.SetBlocksCount(1024);

        NProto::TBlobPiece dst;
        SplitBlobPieceRangeFromRelativeToGlobalIndices(
            rangeInBlob,
            4,   // blocksPerStripe
            2,   // partitionsCount
            0,   // partitionId
            &dst);

        UNIT_ASSERT_VALUES_EQUAL(257, dst.RangesSize());

        const auto& range1 = dst.GetRanges(0);
        UNIT_ASSERT_VALUES_EQUAL(10, range1.GetBlobOffset());
        UNIT_ASSERT_VALUES_EQUAL(25, range1.GetBlockIndex());
        UNIT_ASSERT_VALUES_EQUAL(3, range1.GetBlocksCount());

        const auto& range2 = dst.GetRanges(1);
        UNIT_ASSERT_VALUES_EQUAL(13, range2.GetBlobOffset());
        UNIT_ASSERT_VALUES_EQUAL(32, range2.GetBlockIndex());
        UNIT_ASSERT_VALUES_EQUAL(4, range2.GetBlocksCount());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
