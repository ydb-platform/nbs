#include "merge.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMergeTest)
{
    Y_UNIT_TEST(ShouldCorrectlyMergeStripedBitMaskSinglePartition)
    {
        const TBlockRange64 originalRange(0, 5);
        const ui32 blocksPerStripe = 2;
        const ui32 partitionsCount = 1;

        TString dstMask;
        MergeStripedBitMask(
            originalRange,
            TBlockRange64(0, 5),
            blocksPerStripe,
            partitionsCount,
            0,  // partitionId
            TString(1, char(0b110111)),
            dstMask
        );
        UNIT_ASSERT_VALUES_EQUAL(1, dstMask.size());
        UNIT_ASSERT_VALUES_EQUAL(0b00110111, ui8(dstMask[0]));
    }

    Y_UNIT_TEST(ShouldCorrectlyMergeStripedBitMaskMultiPartition)
    {
        const TBlockRange64 originalRange(0, 10);
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
                TBlockRange64(0, 3),
                blocksPerStripe,
                partitionsCount,
                0,  // partitionId
                TString(1, char(0b1101)),
                dstMask
            );
            UNIT_ASSERT_VALUES_EQUAL(2, dstMask.size());
            UNIT_ASSERT_VALUES_EQUAL(0b11000001, ui8(dstMask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000000, ui8(dstMask[1]));
        }

        {
            // from part 1
            TString dstMask;
            MergeStripedBitMask(
                originalRange,
                TBlockRange64(0, 3),
                blocksPerStripe,
                partitionsCount,
                1,  // partitionId
                TString(1, char(0b1101)),
                dstMask
            );
            UNIT_ASSERT_VALUES_EQUAL(2, dstMask.size());
            UNIT_ASSERT_VALUES_EQUAL(0b00000100, ui8(dstMask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000011, ui8(dstMask[1]));
        }

        {
            // from part 2
            TString dstMask;
            MergeStripedBitMask(
                originalRange,
                TBlockRange64(0, 2),
                blocksPerStripe,
                partitionsCount,
                2,  // partitionId
                TString(1, char(0b111)),
                dstMask
            );
            UNIT_ASSERT_VALUES_EQUAL(2, dstMask.size());
            UNIT_ASSERT_VALUES_EQUAL(0b00110000, ui8(dstMask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000100, ui8(dstMask[1]));
        }

        {
            // from part 0
            TString dstMask;
            MergeStripedBitMask(
                originalRange,
                TBlockRange64(0, 3),
                blocksPerStripe,
                partitionsCount,
                0,  // partitionId
                TString(1, char(0b1111)),
                dstMask
            );

            // from part 1
            MergeStripedBitMask(
                originalRange,
                TBlockRange64(0, 3),
                blocksPerStripe,
                partitionsCount,
                1,  // partitionId
                TString(1, char(0b1111)),
                dstMask
            );

            // from part 2
            MergeStripedBitMask(
                originalRange,
                TBlockRange64(0, 2),
                blocksPerStripe,
                partitionsCount,
                2,  // partitionId
                TString(1, char(0b111)),
                dstMask
            );
            UNIT_ASSERT_VALUES_EQUAL(2, dstMask.size());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(dstMask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000111, ui8(dstMask[1]));
        }
    }

}

}   // namespace NCloud::NBlockStore::NStorage
