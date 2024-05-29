#include "changed_ranges_map.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TChangedRangesMapTest)
{
    Y_UNIT_TEST(ShouldAssumeThatAllBlocksInitiallyAreChanged)
    {
        TChangedRangesMap rangesMap(1024, 512, 4096);

        auto changedBlocks =
            rangesMap.GetChangedBlocks(TBlockRange64::WithLength(0, 1024));
        UNIT_ASSERT_VALUES_EQUAL(1024 / 8, changedBlocks.size());
        for (auto b: changedBlocks) {
            UNIT_ASSERT_VALUES_EQUAL(255, static_cast<ui8>(b));
        }
    }

    Y_UNIT_TEST(ShouldMarkCleanRangeAligned)
    {
        const ui32 blockSize = 2;
        const ui32 rangeSize = 8;
        TChangedRangesMap rangesMap(1024, blockSize, rangeSize);

        // Mark second range
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(4, 4));
        auto changed =
            rangesMap.GetChangedBlocks(TBlockRange64::WithLength(0, 32));
        UNIT_ASSERT_VALUES_EQUAL(4, changed.size());
        UNIT_ASSERT_VALUES_EQUAL(0b00001111, static_cast<ui8>(changed[0]));
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, static_cast<ui8>(changed[1]));
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, static_cast<ui8>(changed[2]));
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, static_cast<ui8>(changed[3]));

        // Mark first range
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(0, 4));
        changed = rangesMap.GetChangedBlocks(TBlockRange64::WithLength(0, 32));
        UNIT_ASSERT_VALUES_EQUAL(4, changed.size());
        UNIT_ASSERT_VALUES_EQUAL(0b00000000, static_cast<ui8>(changed[0]));
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, static_cast<ui8>(changed[1]));
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, static_cast<ui8>(changed[2]));
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, static_cast<ui8>(changed[3]));

        // Mark two ranges at once
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(8, 8));
        changed = rangesMap.GetChangedBlocks(TBlockRange64::WithLength(0, 32));
        UNIT_ASSERT_VALUES_EQUAL(4, changed.size());
        UNIT_ASSERT_VALUES_EQUAL(0b00000000, static_cast<ui8>(changed[0]));
        UNIT_ASSERT_VALUES_EQUAL(0b00000000, static_cast<ui8>(changed[1]));
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, static_cast<ui8>(changed[2]));
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, static_cast<ui8>(changed[3]));
    }

    Y_UNIT_TEST(ShouldMarkCleanRangeUnaligned)
    {
        const ui32 blockSize = 2;
        const ui32 rangeSize = blockSize * 4;
        TChangedRangesMap rangesMap(1024, blockSize, rangeSize);

        // Mark a part of range shouldn't clear range
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(0, 1));
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(0, 2));
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(0, 3));
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(1, 1));
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(1, 2));
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(1, 3));
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(1, 4));
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(1, 5));
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(1, 6));
        auto changed =
            rangesMap.GetChangedBlocks(TBlockRange64::WithLength(0, 8));
        UNIT_ASSERT_VALUES_EQUAL(1, changed.size());
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, static_cast<ui8>(changed[0]));

        // Unaligned range should be cut off at the beginning
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(1, 7));
        changed = rangesMap.GetChangedBlocks(TBlockRange64::WithLength(0, 16));
        UNIT_ASSERT_VALUES_EQUAL(0b00001111, static_cast<ui8>(changed[0]));
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, static_cast<ui8>(changed[1]));

        // Unaligned range should be cut off at the end
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(12, 6));
        changed = rangesMap.GetChangedBlocks(TBlockRange64::WithLength(0, 24));
        UNIT_ASSERT_VALUES_EQUAL(0b00001111, static_cast<ui8>(changed[0]));
        UNIT_ASSERT_VALUES_EQUAL(0b00001111, static_cast<ui8>(changed[1]));
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, static_cast<ui8>(changed[2]));

        // Unaligned range should be cut off at the beginning and end.
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(17, 14));
        changed = rangesMap.GetChangedBlocks(TBlockRange64::WithLength(0, 32));
        UNIT_ASSERT_VALUES_EQUAL(0b00001111, static_cast<ui8>(changed[0]));
        UNIT_ASSERT_VALUES_EQUAL(0b00001111, static_cast<ui8>(changed[1]));
        UNIT_ASSERT_VALUES_EQUAL(0b00001111, static_cast<ui8>(changed[2]));
        UNIT_ASSERT_VALUES_EQUAL(0b11110000, static_cast<ui8>(changed[3]));
    }

    Y_UNIT_TEST(ShouldTrimRange)
    {
        const ui32 blockSize = 2;
        const ui32 rangeSize = 4;
        TChangedRangesMap rangesMap(16, blockSize, rangeSize);

        // It is safe to pass sizes larger than the data size
        auto changed =
            rangesMap.GetChangedBlocks(TBlockRange64::WithLength(0, 16));
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(1024, 2048));
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, static_cast<ui8>(changed[0]));
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, static_cast<ui8>(changed[1]));

        rangesMap.MarkNotChanged(TBlockRange64::WithLength(12, 1024));
        changed = rangesMap.GetChangedBlocks(TBlockRange64::WithLength(0, 16));
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, static_cast<ui8>(changed[0]));
        UNIT_ASSERT_VALUES_EQUAL(0b00001111, static_cast<ui8>(changed[1]));
    }

    Y_UNIT_TEST(ShouldMarkRangeAsChanged)
    {
        const ui32 blockSize = 2;
        const ui32 rangeSize = 4;
        TChangedRangesMap rangesMap(1024, blockSize, rangeSize);

        // Clean all blocks.
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(0, 1024));
        auto changed =
            rangesMap.GetChangedBlocks(TBlockRange64::WithLength(0, 16));
        UNIT_ASSERT_VALUES_EQUAL(0b00000000, static_cast<ui8>(changed[0]));
        UNIT_ASSERT_VALUES_EQUAL(0b00000000, static_cast<ui8>(changed[1]));

        // Mark as changed first block in range
        rangesMap.MarkChanged(TBlockRange64::MakeOneBlock(0));
        changed = rangesMap.GetChangedBlocks(TBlockRange64::WithLength(0, 8));
        UNIT_ASSERT_VALUES_EQUAL(0b00000011, static_cast<ui8>(changed[0]));

        // Mark as changed last block in range
        rangesMap.MarkChanged(TBlockRange64::MakeOneBlock(3));
        changed = rangesMap.GetChangedBlocks(TBlockRange64::WithLength(0, 8));
        UNIT_ASSERT_VALUES_EQUAL(0b00001111, static_cast<ui8>(changed[0]));

        // Mark as changed two ranges at once aligned
        rangesMap.MarkChanged(TBlockRange64::WithLength(6, 4));
        changed = rangesMap.GetChangedBlocks(TBlockRange64::WithLength(0, 16));
        UNIT_ASSERT_VALUES_EQUAL(0b11001111, static_cast<ui8>(changed[0]));
        UNIT_ASSERT_VALUES_EQUAL(0b00000011, static_cast<ui8>(changed[1]));

        // Mark as changed two ranges at once unaligned
        rangesMap.MarkChanged(TBlockRange64::WithLength(13, 2));
        changed = rangesMap.GetChangedBlocks(TBlockRange64::WithLength(0, 16));
        UNIT_ASSERT_VALUES_EQUAL(0b11001111, static_cast<ui8>(changed[0]));
        UNIT_ASSERT_VALUES_EQUAL(0b11110011, static_cast<ui8>(changed[1]));
    }

    Y_UNIT_TEST(ShouldGetChangedBlocks)
    {
        const ui32 blockSize = 1;
        const ui32 rangeSize = 1;
        TChangedRangesMap rangesMap(16, blockSize, rangeSize);

        // Mark three blocks.
        rangesMap.MarkNotChanged(TBlockRange64::WithLength(0, 16));
        rangesMap.MarkChanged(TBlockRange64::WithLength(3, 3));

        // Get aligned blocks
        auto changed =
            rangesMap.GetChangedBlocks(TBlockRange64::WithLength(0, 8));
        UNIT_ASSERT_VALUES_EQUAL(0b00111000, static_cast<ui8>(changed[0]));

        // Get unaligned blocks
        changed = rangesMap.GetChangedBlocks(TBlockRange64::WithLength(1, 8));
        UNIT_ASSERT_VALUES_EQUAL(0b00011100, static_cast<ui8>(changed[0]));

        // Get one block. Upper bits should be set.
        changed = rangesMap.GetChangedBlocks(TBlockRange64::WithLength(1, 1));
        UNIT_ASSERT_VALUES_EQUAL(0b11111110, static_cast<ui8>(changed[0]));
        changed = rangesMap.GetChangedBlocks(TBlockRange64::WithLength(3, 1));
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, static_cast<ui8>(changed[0]));

        // Get blocks out-of-range. The out-of-range bits must be set
        changed = rangesMap.GetChangedBlocks(TBlockRange64::WithLength(2, 32));
        UNIT_ASSERT_VALUES_EQUAL(4, changed.size());
        UNIT_ASSERT_VALUES_EQUAL(0b00001110, static_cast<ui8>(changed[0]));
        UNIT_ASSERT_VALUES_EQUAL(0b11000000, static_cast<ui8>(changed[1]));
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, static_cast<ui8>(changed[2]));
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, static_cast<ui8>(changed[3]));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
