#include "block_mask.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockMaskTest)
{
    Y_UNIT_TEST(ShouldCorrectlyDetermineFullness)
    {
        TBlockMask mask;
        UNIT_ASSERT(IsBlockMaskFull(mask, 0));
        UNIT_ASSERT(!IsBlockMaskFull(mask, 4));

        mask.Set(0);
        mask.Set(1);
        mask.Set(2);

        UNIT_ASSERT(IsBlockMaskFull(mask, 3));
        UNIT_ASSERT(!IsBlockMaskFull(mask, 4));

        mask.Set(3);
        UNIT_ASSERT(IsBlockMaskFull(mask, 4));

        // testing multiple bitmap 'chunks'
        UNIT_ASSERT(!IsBlockMaskFull(mask, 256));

        for (ui32 i = 4; i <= 254; ++i) {
            mask.Set(i);
        }

        UNIT_ASSERT(!IsBlockMaskFull(mask, 256));

        mask.Set(255);
        UNIT_ASSERT(IsBlockMaskFull(mask, 256));

        mask.Set(0, MaxBlocksCount);
        UNIT_ASSERT(IsBlockMaskFull(mask, MaxBlocksCount));
    }

    Y_UNIT_TEST(ShouldConstructFullMask)
    {
        TBlockMask mask = GetFullBlockMask(256);
        for (ui32 i = 1; i <= 256; ++i) {
            UNIT_ASSERT(IsBlockMaskFull(mask, i));
        }

        mask = GetFullBlockMask(MaxBlocksCount);
        for (ui32 i = 1; i <= MaxBlocksCount; ++i) {
            UNIT_ASSERT(IsBlockMaskFull(mask, i));
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
