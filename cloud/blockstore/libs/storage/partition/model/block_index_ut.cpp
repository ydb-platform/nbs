#include "block_index.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

const ui32 InvalidBlockIndex = Max<ui32>();

TFreshBlock FindBlock(const TBlockIndex& index, ui32 blockIndex, ui64 commitId)
{
    struct TVisitor final
        : IFreshBlocksIndexVisitor
    {
        TFreshBlock Block;

        TVisitor()
            : Block({InvalidBlockIndex, 0, true}, 0, {})
        {
        }

        bool Visit(const TFreshBlock& block) override
        {
            UNIT_ASSERT_VALUES_EQUAL(InvalidBlockIndex, Block.Meta.BlockIndex);
            Block = block;

            return false;
        }
    } visitor;

    index.FindBlocks(
        visitor,
        TBlockRange32::MakeOneBlock(blockIndex),
        commitId);

    return visitor.Block;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartition1BlockIndexTest)
{
    Y_UNIT_TEST(ShouldStoreBlocks)
    {
        ui64 commitId = 0;

        TBlockIndex index;
        index.AddBlock(0, ++commitId, true, "x", {});

        auto block = FindBlock(index, 0, commitId);
        UNIT_ASSERT_VALUES_EQUAL(block.Meta.CommitId, 1);
        UNIT_ASSERT_VALUES_EQUAL(block.Meta.IsStoredInDb, true);
        UNIT_ASSERT_VALUES_EQUAL(block.Content, "x");

        block = FindBlock(index, 1, commitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidBlockIndex, block.Meta.BlockIndex);
    }

    Y_UNIT_TEST(ShouldOverwriteBlocks)
    {
        ui64 commitId = 0;

        TBlockIndex index;
        index.AddBlock(0, ++commitId, true, "x", {});
        index.AddBlock(0, ++commitId, false, "y", {});

        auto block = FindBlock(index, 0, commitId);
        UNIT_ASSERT_VALUES_EQUAL(block.Meta.CommitId, 2);
        UNIT_ASSERT_VALUES_EQUAL(block.Meta.IsStoredInDb, false);
        UNIT_ASSERT_VALUES_EQUAL(block.Content, "y");

        block = FindBlock(index, 0, 1);
        UNIT_ASSERT_VALUES_EQUAL(block.Meta.CommitId, 1);
        UNIT_ASSERT_VALUES_EQUAL(block.Meta.IsStoredInDb, true);
        UNIT_ASSERT_VALUES_EQUAL(block.Content, "x");
    }

    Y_UNIT_TEST(ShouldCountZeroBlocks)
    {
        TBlockIndex index;
        UNIT_ASSERT_VALUES_EQUAL(0, index.GetZeroBlockCount());

        index.AddBlock(0, 1, true, {}, {});
        UNIT_ASSERT_VALUES_EQUAL(1, index.GetZeroBlockCount());

        index.AddBlock(1, 1, true, "x", {});
        UNIT_ASSERT_VALUES_EQUAL(1, index.GetZeroBlockCount());

        index.AddBlock(2, 1, false, {}, {});
        UNIT_ASSERT_VALUES_EQUAL(2, index.GetZeroBlockCount());

        UNIT_ASSERT(index.RemoveBlock(0, 1, true));
        UNIT_ASSERT_VALUES_EQUAL(1, index.GetZeroBlockCount());

        UNIT_ASSERT(index.RemoveBlock(1, 1, true));
        UNIT_ASSERT_VALUES_EQUAL(1, index.GetZeroBlockCount());

        UNIT_ASSERT(index.RemoveBlock(2, 1, false));
        UNIT_ASSERT_VALUES_EQUAL(0, index.GetZeroBlockCount());
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
