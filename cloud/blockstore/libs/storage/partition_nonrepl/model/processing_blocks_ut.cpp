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

        blocks.MarkProcessed(TBlockRange64::WithLength(0, 1024));
        blocks.SkipProcessedRanges();

        UNIT_ASSERT(blocks.IsProcessing());
        auto range = blocks.BuildProcessingRange();
        UNIT_ASSERT_VALUES_EQUAL(1024, range.Start);
        UNIT_ASSERT_VALUES_EQUAL(2047, range.End);

        UNIT_ASSERT(blocks.AdvanceProcessingIndex());
        UNIT_ASSERT(blocks.IsProcessing());
        range = blocks.BuildProcessingRange();
        UNIT_ASSERT_VALUES_EQUAL(2048, range.Start);
        UNIT_ASSERT_VALUES_EQUAL(3071, range.End);

        UNIT_ASSERT(blocks.AdvanceProcessingIndex());
        UNIT_ASSERT(blocks.IsProcessing());
        range = blocks.BuildProcessingRange();
        UNIT_ASSERT_VALUES_EQUAL(3072, range.Start);
        UNIT_ASSERT_VALUES_EQUAL(4095, range.End);

        UNIT_ASSERT(!blocks.AdvanceProcessingIndex());
        UNIT_ASSERT(!blocks.IsProcessing());
    }

    Y_UNIT_TEST(ShouldTestIfRangeProcessed)
    {
        TProcessingBlocks blocks(
            4096, // blockCount
            4096, // blockSize
            0     // initialProcessingIndex
        );

        auto range = TBlockRange64::MakeClosedInterval(100, 1199);
        UNIT_ASSERT(!blocks.IsProcessed(range));

        UNIT_ASSERT(blocks.AdvanceProcessingIndex());
        UNIT_ASSERT(!blocks.IsProcessed(range));

        UNIT_ASSERT(blocks.AdvanceProcessingIndex());
        UNIT_ASSERT(blocks.IsProcessed(range));
    }

    Y_UNIT_TEST(ShouldSkipProcessedRanges)
    {
        TProcessingBlocks blocks(
            1024 * 1024, // blockCount
            4096,        // blockSize
            0            // initialProcessingIndex
        );

        blocks.MarkProcessed(TBlockRange64::WithLength(1000, 500));
        blocks.MarkProcessed(TBlockRange64::WithLength(3000, 30000));
        blocks.MarkProcessed(
            TBlockRange64::MakeClosedInterval(35048, 1024 * 1024 - 1));
        blocks.SkipProcessedRanges();

        UNIT_ASSERT(blocks.IsProcessing());
        auto range = blocks.BuildProcessingRange();
        UNIT_ASSERT_VALUES_EQUAL(0, range.Start);
        UNIT_ASSERT_VALUES_EQUAL(1023, range.End);

        UNIT_ASSERT(blocks.AdvanceProcessingIndex());
        UNIT_ASSERT(blocks.IsProcessing());
        range = blocks.BuildProcessingRange();
        UNIT_ASSERT_VALUES_EQUAL(1500, range.Start);
        UNIT_ASSERT_VALUES_EQUAL(2523, range.End);

        UNIT_ASSERT(blocks.AdvanceProcessingIndex());
        UNIT_ASSERT(blocks.IsProcessing());
        range = blocks.BuildProcessingRange();
        UNIT_ASSERT_VALUES_EQUAL(2524, range.Start);
        UNIT_ASSERT_VALUES_EQUAL(3547, range.End);

        UNIT_ASSERT(blocks.AdvanceProcessingIndex());
        UNIT_ASSERT(blocks.IsProcessing());
        range = blocks.BuildProcessingRange();
        UNIT_ASSERT_VALUES_EQUAL(33000, range.Start);
        UNIT_ASSERT_VALUES_EQUAL(34023, range.End);

        UNIT_ASSERT(blocks.AdvanceProcessingIndex());
        UNIT_ASSERT(blocks.IsProcessing());
        range = blocks.BuildProcessingRange();
        UNIT_ASSERT_VALUES_EQUAL(34024, range.Start);
        UNIT_ASSERT_VALUES_EQUAL(35047, range.End);

        UNIT_ASSERT(!blocks.AdvanceProcessingIndex());
        UNIT_ASSERT(!blocks.IsProcessing());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
