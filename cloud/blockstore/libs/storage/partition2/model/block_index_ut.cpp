#include "block_index.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockIndexTest)
{
    Y_UNIT_TEST(ShouldStoreBlocks)
    {
        ui64 commitId = 0;

        TBlockIndex index;
        index.AddBlock(0, "x", ++commitId);

        const auto* block = index.FindBlock(0, commitId);
        UNIT_ASSERT(block);
        UNIT_ASSERT_VALUES_EQUAL(block->Meta.MinCommitId, 1);
        UNIT_ASSERT_VALUES_EQUAL(block->Content, "x");

        block = index.FindBlock(1, commitId);
        UNIT_ASSERT(!block);
    }

    Y_UNIT_TEST(ShouldOverwriteBlocks)
    {
        ui64 commitId = 0;

        TBlockIndex index;
        index.AddBlock(0, "x", ++commitId);
        index.AddBlock(0, "y", ++commitId);

        const auto* block = index.FindBlock(0, commitId);
        UNIT_ASSERT(block);
        UNIT_ASSERT_VALUES_EQUAL(block->Meta.MinCommitId, 2);
        UNIT_ASSERT_VALUES_EQUAL(block->Content, "y");

        block = index.FindBlock(0, 1);
        UNIT_ASSERT(block);
        UNIT_ASSERT_VALUES_EQUAL(block->Meta.MinCommitId, 1);
        UNIT_ASSERT_VALUES_EQUAL(block->Content, "x");
    }

    Y_UNIT_TEST(ShouldAddDeletedBlock)
    {
        ui64 commitId = 0;

        TBlockIndex index;
        index.AddBlock(0, "x", ++commitId);
        UNIT_ASSERT_EQUAL(index.AddDeletedBlock(0, ++commitId), 1);
        index.AddBlock(0, "y", ++commitId);

        const auto* block = index.FindBlock(0, commitId);
        UNIT_ASSERT(block);
        UNIT_ASSERT_EQUAL(block->Meta.MinCommitId, 3);
        UNIT_ASSERT_EQUAL(block->Content, "y");

        block = index.FindBlock(0, 1);
        UNIT_ASSERT(block);
        UNIT_ASSERT_EQUAL(block->Meta.MinCommitId, 1);
        UNIT_ASSERT_EQUAL(block->Content, "x");

        block = index.FindBlock(0, 2);
        UNIT_ASSERT(!block);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
