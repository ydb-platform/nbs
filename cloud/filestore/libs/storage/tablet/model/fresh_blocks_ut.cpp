#include "fresh_blocks.h"

#include <cloud/filestore/libs/storage/core/public.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFreshBlockVisitor final
    : public IFreshBlockVisitor
{
private:
    TVector<TBlock> Blocks;

public:
    void Accept(const TBlock& block, TStringBuf blockData) override
    {
        Y_UNUSED(blockData);
        Blocks.push_back(block);
    }

    TVector<TBlock> Finish()
    {
        return std::move(Blocks);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFreshBlocksTest)
{
    Y_UNIT_TEST(ShouldStoreBlocks)
    {
        ui64 nodeId = 1;
        ui32 blockIndex = 123;

        TFreshBlocks freshBlocks(TDefaultAllocator::Instance());
        freshBlocks.AddBlock(nodeId, blockIndex, "x", 1, 1);

        auto block = freshBlocks.FindBlock(nodeId, blockIndex, 1);
        UNIT_ASSERT(block);

        UNIT_ASSERT_VALUES_EQUAL(block->NodeId, nodeId);
        UNIT_ASSERT_VALUES_EQUAL(block->BlockIndex, blockIndex);
        UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, 1);
        UNIT_ASSERT_VALUES_EQUAL(block->MaxCommitId, InvalidCommitId);
        UNIT_ASSERT_VALUES_EQUAL(block->BlockData, "x");

        block = freshBlocks.FindBlock(nodeId, blockIndex + 1, 1);
        UNIT_ASSERT(!block);

        freshBlocks.AddBlock(nodeId + 2, blockIndex, "y", 1, 1);
        block = freshBlocks.FindBlock(nodeId + 1, blockIndex, 1);
        UNIT_ASSERT(!block);

        UNIT_ASSERT(!freshBlocks.FindBlock(nodeId + 1, blockIndex));

        block = freshBlocks.FindBlock(nodeId + 2, blockIndex, 1);
        UNIT_ASSERT(block);

        UNIT_ASSERT_VALUES_EQUAL(block->NodeId, nodeId + 2);
        UNIT_ASSERT_VALUES_EQUAL(block->BlockIndex, blockIndex);
        UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, 1);
        UNIT_ASSERT_VALUES_EQUAL(block->MaxCommitId, InvalidCommitId);
        UNIT_ASSERT_VALUES_EQUAL(block->BlockData, "y");

        UNIT_ASSERT(freshBlocks.FindBlock(nodeId + 2, blockIndex));
    }

    Y_UNIT_TEST(ShouldOverwriteBlocks)
    {
        ui64 nodeId = 1;
        ui32 blockIndex = 123;

        TFreshBlocks freshBlocks(TDefaultAllocator::Instance());
        freshBlocks.AddBlock(nodeId, blockIndex, "x", 1, 1);

        auto deleted = freshBlocks.MarkBlocksDeleted(
            nodeId,
            blockIndex,
            1,
            2);
        UNIT_ASSERT_VALUES_EQUAL(deleted.size(), 1);
        ui64 minCommitId = deleted[0].second;
        UNIT_ASSERT_VALUES_EQUAL(minCommitId, 1);

        freshBlocks.AddBlock(nodeId, blockIndex, "y", 1, 2);

        auto block = freshBlocks.FindBlock(nodeId, blockIndex, 1);
        UNIT_ASSERT(block);

        UNIT_ASSERT_VALUES_EQUAL(block->NodeId, nodeId);
        UNIT_ASSERT_VALUES_EQUAL(block->BlockIndex, blockIndex);
        UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, 1);
        UNIT_ASSERT_VALUES_EQUAL(block->MaxCommitId, 2);
        UNIT_ASSERT_VALUES_EQUAL(block->BlockData, "x");

        block = freshBlocks.FindBlock(nodeId, blockIndex, 2);
        UNIT_ASSERT(block);

        UNIT_ASSERT_VALUES_EQUAL(block->NodeId, nodeId);
        UNIT_ASSERT_VALUES_EQUAL(block->BlockIndex, blockIndex);
        UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, 2);
        UNIT_ASSERT_VALUES_EQUAL(block->MaxCommitId, InvalidCommitId);
        UNIT_ASSERT_VALUES_EQUAL(block->BlockData, "y");
    }

    Y_UNIT_TEST(ShouldOverwriteBlocksOutOfOrder)
    {
        ui64 nodeId = 1;
        ui32 blockIndex = 123;

        TFreshBlocks freshBlocks(TDefaultAllocator::Instance());

        freshBlocks.AddBlock(nodeId, blockIndex + 0, "a", 1, 1);
        freshBlocks.AddBlock(nodeId, blockIndex + 1, "b", 1, 1);
        freshBlocks.AddBlock(nodeId, blockIndex + 2, "c", 1, 1);

        auto deleted = freshBlocks.MarkBlocksDeleted(
            nodeId,
            blockIndex,
            3,
            10);
        UNIT_ASSERT_VALUES_EQUAL(deleted.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(deleted[0].first, blockIndex);
        UNIT_ASSERT_VALUES_EQUAL(deleted[0].second, 1);
        UNIT_ASSERT_VALUES_EQUAL(deleted[1].first, blockIndex + 1);
        UNIT_ASSERT_VALUES_EQUAL(deleted[1].second, 1);
        UNIT_ASSERT_VALUES_EQUAL(deleted[2].first, blockIndex + 2);
        UNIT_ASSERT_VALUES_EQUAL(deleted[2].second, 1);

        freshBlocks.AddBlock(nodeId, blockIndex + 0, "A", 1, 10);
        freshBlocks.AddBlock(nodeId, blockIndex + 1, "B", 1, 10);
        freshBlocks.AddBlock(nodeId, blockIndex + 2, "C", 1, 10);

        deleted = freshBlocks.MarkBlocksDeleted(
            nodeId,
            blockIndex + 1,
            2,
            5);
        UNIT_ASSERT_VALUES_EQUAL(deleted.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(deleted[0].first, blockIndex + 1);
        UNIT_ASSERT_VALUES_EQUAL(deleted[0].second, 1);
        UNIT_ASSERT_VALUES_EQUAL(deleted[1].first, blockIndex + 2);
        UNIT_ASSERT_VALUES_EQUAL(deleted[1].second, 1);

        auto block = freshBlocks.FindBlock(nodeId, blockIndex + 0, 6);
        UNIT_ASSERT(block);
        UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, 1);
        UNIT_ASSERT_VALUES_EQUAL(block->MaxCommitId, 10);
        UNIT_ASSERT_VALUES_EQUAL(block->BlockData, "a");

        block = freshBlocks.FindBlock(nodeId, blockIndex + 1, 4);
        UNIT_ASSERT(block);
        UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, 1);
        UNIT_ASSERT_VALUES_EQUAL(block->MaxCommitId, 5);
        UNIT_ASSERT_VALUES_EQUAL(block->BlockData, "b");

        block = freshBlocks.FindBlock(nodeId, blockIndex + 1, 6);
        UNIT_ASSERT(!block);

        block = freshBlocks.FindBlock(nodeId, blockIndex + 2, 6);
        UNIT_ASSERT(!block);

        block = freshBlocks.FindBlock(nodeId, blockIndex + 0, 10);
        UNIT_ASSERT(block);
        UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, 10);
        UNIT_ASSERT_VALUES_EQUAL(block->MaxCommitId, InvalidCommitId);
        UNIT_ASSERT_VALUES_EQUAL(block->BlockData, "A");

        block = freshBlocks.FindBlock(nodeId, blockIndex + 1, 10);
        UNIT_ASSERT(block);
        UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, 10);
        UNIT_ASSERT_VALUES_EQUAL(block->MaxCommitId, InvalidCommitId);
        UNIT_ASSERT_VALUES_EQUAL(block->BlockData, "B");

        block = freshBlocks.FindBlock(nodeId, blockIndex + 2, 10);
        UNIT_ASSERT(block);
        UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, 10);
        UNIT_ASSERT_VALUES_EQUAL(block->MaxCommitId, InvalidCommitId);
        UNIT_ASSERT_VALUES_EQUAL(block->BlockData, "C");
    }

    Y_UNIT_TEST(ShouldFindBlocks)
    {
        ui64 nodeId = 1;
        ui32 blockIndex = 123;

        TFreshBlocks freshBlocks(TDefaultAllocator::Instance());
        for (size_t i = 0; i < 10; ++i) {
            freshBlocks.AddBlock(nodeId, blockIndex + i, "x", 1, 1 + i);
        }

        {
            TFreshBlockVisitor visitor;
            freshBlocks.FindBlocks(visitor, nodeId, blockIndex, 10);

            auto blocks = visitor.Finish();
            UNIT_ASSERT_VALUES_EQUAL(blocks.size(), 10);
        }

        {
            TFreshBlockVisitor visitor;
            freshBlocks.FindBlocks(visitor, nodeId, blockIndex, 10, 5);

            auto blocks = visitor.Finish();
            UNIT_ASSERT_VALUES_EQUAL(blocks.size(), 5);
        }
    }

    Y_UNIT_TEST(ShouldHandlePartialBlocksData)
    {
        ui64 nodeId = 1;
        ui32 blockIndex = 123;

        TFreshBlocks freshBlocks(TDefaultAllocator::Instance());
        freshBlocks.AddBlock(nodeId, blockIndex, "x", DefaultBlockSize, 1);

        auto block = freshBlocks.FindBlock(nodeId, blockIndex, 1);
        UNIT_ASSERT(block);

        UNIT_ASSERT_VALUES_EQUAL(block->NodeId, nodeId);
        UNIT_ASSERT_VALUES_EQUAL(block->BlockIndex, blockIndex);
        UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, 1);
        UNIT_ASSERT_VALUES_EQUAL(block->MaxCommitId, InvalidCommitId);
        UNIT_ASSERT_VALUES_EQUAL(block->BlockData, "x" + TString(DefaultBlockSize - 1, 0));
        UNIT_ASSERT_VALUES_EQUAL(block->BlockData, "x" + TString(DefaultBlockSize - 1, 0));

        block = freshBlocks.FindBlock(nodeId, blockIndex + 1, 1);
        UNIT_ASSERT(!block);

        freshBlocks.AddBlock(nodeId + 2, blockIndex, "y", DefaultBlockSize, 1);
        block = freshBlocks.FindBlock(nodeId + 1, blockIndex, 1);
        UNIT_ASSERT(!block);

        UNIT_ASSERT(!freshBlocks.FindBlock(nodeId + 1, blockIndex));

        block = freshBlocks.FindBlock(nodeId + 2, blockIndex, 1);
        UNIT_ASSERT(block);

        UNIT_ASSERT_VALUES_EQUAL(block->NodeId, nodeId + 2);
        UNIT_ASSERT_VALUES_EQUAL(block->BlockIndex, blockIndex);
        UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, 1);
        UNIT_ASSERT_VALUES_EQUAL(block->MaxCommitId, InvalidCommitId);
        UNIT_ASSERT_VALUES_EQUAL(block->BlockData, "y" + TString(DefaultBlockSize - 1, 0));

        UNIT_ASSERT(freshBlocks.FindBlock(nodeId + 2, blockIndex));
    }
}

}   // namespace NCloud::NFileStore::NStorage
