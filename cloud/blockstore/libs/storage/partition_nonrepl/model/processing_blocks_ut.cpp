#include "processing_blocks.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TProcessingBlocksTest)
{
    Y_UNIT_TEST(ShouldStartProcessingFromInitialProcessingIndex)
    {
        TProcessingBlocks blocks(
            4096, // blockCount
            4096, // blockSize
            1024  // initialProcessingIndex
        );

        auto range = blocks.BuildProcessingRange();
        UNIT_ASSERT_VALUES_EQUAL(1024, range.Start);
        UNIT_ASSERT_VALUES_EQUAL(2047, range.End);

        UNIT_ASSERT_VALUES_EQUAL(1024, blocks.GetProcessedBlockCount());
    }

    Y_UNIT_TEST(ShouldProperlyAccountForProcessedPrefix)
    {
        TProcessingBlocks blocks(
            4096, // blockCount
            4096, // blockSize
            0     // initialProcessingIndex
        );

        blocks.MarkProcessed(TBlockRange64(0, 1024));
        blocks.SkipProcessedRanges();

        UNIT_ASSERT(blocks.IsProcessingStarted());
        auto range = blocks.BuildProcessingRange();
        UNIT_ASSERT_VALUES_EQUAL(1025, range.Start);
        UNIT_ASSERT_VALUES_EQUAL(2048, range.End);

        UNIT_ASSERT(blocks.AdvanceProcessingIndex());
        UNIT_ASSERT(blocks.IsProcessingStarted());
        range = blocks.BuildProcessingRange();
        UNIT_ASSERT_VALUES_EQUAL(2049, range.Start);
        UNIT_ASSERT_VALUES_EQUAL(3072, range.End);

        UNIT_ASSERT(blocks.AdvanceProcessingIndex());
        UNIT_ASSERT(blocks.IsProcessingStarted());
        range = blocks.BuildProcessingRange();
        UNIT_ASSERT_VALUES_EQUAL(3073, range.Start);
        UNIT_ASSERT_VALUES_EQUAL(4095, range.End);

        UNIT_ASSERT(!blocks.AdvanceProcessingIndex());
        UNIT_ASSERT(!blocks.IsProcessingStarted());
    }

    Y_UNIT_TEST(ShouldTestIfRangeProcessed)
    {
        TProcessingBlocks blocks(
            4096, // blockCount
            4096, // blockSize
            0     // initialProcessingIndex
        );

        TBlockRange64 range(100, 1199);
        UNIT_ASSERT(!blocks.IsProcessed(range));

        UNIT_ASSERT(blocks.AdvanceProcessingIndex());
        UNIT_ASSERT(!blocks.IsProcessed(range));

        UNIT_ASSERT(blocks.AdvanceProcessingIndex());
        UNIT_ASSERT(blocks.IsProcessed(range));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
