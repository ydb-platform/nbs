#include "mixed_blocks.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMixedBlockVisitor final
    : public IMixedBlockVisitor
{
private:
    TVector<TBlockDataRef> Blocks;

public:
    void Accept(
        const TBlock& block,
        const TPartialBlobId& blobId,
        ui32 blobOffset,
        ui32 blocksCount) override
    {
        Blocks.emplace_back(block, blobId, blobOffset);

        if (blocksCount > 1) {
            auto b = block;
            while (--blocksCount > 0) {
                b.BlockIndex++;
                blobOffset++;

                Blocks.emplace_back(b, blobId, blobOffset);
            }
        }
    }

    TVector<TBlockDataRef> Finish()
    {
        return std::move(Blocks);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMixedBlocksTest)
{
    Y_UNIT_TEST(ShouldTrackBlocks)
    {
        constexpr ui32 rangeId = 0;

        constexpr ui64 nodeId = 1;
        constexpr ui64 minCommitId = MakeCommitId(12, 345);
        constexpr ui64 maxCommitId = InvalidCommitId;

        constexpr ui32 blockIndex = 123456;
        constexpr size_t blocksCount = 100;

        TBlock block(nodeId, blockIndex, minCommitId, maxCommitId);

        auto list = TBlockList::EncodeBlocks(block, blocksCount, TDefaultAllocator::Instance());

        TMixedBlocks mixedBlocks(TDefaultAllocator::Instance());
        mixedBlocks.RefRange(rangeId);
        mixedBlocks.AddBlocks(rangeId, TPartialBlobId(), std::move(list));

        {
            TMixedBlockVisitor visitor;
            mixedBlocks.FindBlocks(
                visitor,
                rangeId,
                nodeId,
                minCommitId + 1,
                blockIndex,
                blocksCount);

            auto blocks = visitor.Finish();
            UNIT_ASSERT_VALUES_EQUAL(blocks.size(), blocksCount);

            for (size_t i = 0; i < blocksCount; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(blocks[i].NodeId, nodeId);
                UNIT_ASSERT_VALUES_EQUAL(blocks[i].MinCommitId, minCommitId);
                UNIT_ASSERT_VALUES_EQUAL(blocks[i].MaxCommitId, maxCommitId);
                UNIT_ASSERT_VALUES_EQUAL(blocks[i].BlockIndex, blockIndex + i);
            }
        }
    }

    Y_UNIT_TEST(ShouldTrackDeletionMarkers)
    {
        constexpr ui32 rangeId = 0;

        constexpr ui64 nodeId = 1;
        constexpr ui64 minCommitId = MakeCommitId(12, 345);
        constexpr ui64 maxCommitId = InvalidCommitId;

        constexpr ui32 blockIndex = 123456;
        constexpr size_t blocksCount = 100;

        TBlock block(nodeId, blockIndex, minCommitId, maxCommitId);

        auto list = TBlockList::EncodeBlocks(block, blocksCount, TDefaultAllocator::Instance());

        TMixedBlocks mixedBlocks(TDefaultAllocator::Instance());
        mixedBlocks.RefRange(rangeId);
        mixedBlocks.AddBlocks(rangeId, TPartialBlobId(), std::move(list));

        mixedBlocks.AddDeletionMarker(
            rangeId,
            {nodeId, minCommitId + 2, blockIndex, blocksCount}
        );

        {
            TMixedBlockVisitor visitor;
            mixedBlocks.FindBlocks(
                visitor,
                rangeId,
                nodeId,
                minCommitId + 1,
                blockIndex,
                blocksCount);

            auto blocks = visitor.Finish();
            UNIT_ASSERT_VALUES_EQUAL(blocks.size(), blocksCount);

            for (size_t i = 0; i < blocksCount; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(blocks[i].NodeId, nodeId);
                UNIT_ASSERT_VALUES_EQUAL(blocks[i].MinCommitId, minCommitId);
                UNIT_ASSERT_VALUES_EQUAL(blocks[i].MaxCommitId, minCommitId + 2);
                UNIT_ASSERT_VALUES_EQUAL(blocks[i].BlockIndex, blockIndex + i);
            }
        }
    }

    Y_UNIT_TEST(ShouldRefCountRanges)
    {
        constexpr ui32 rangeId = 0;

        constexpr ui64 nodeId = 1;
        constexpr ui64 minCommitId = MakeCommitId(12, 345);
        constexpr ui64 maxCommitId = InvalidCommitId;

        constexpr ui32 blockIndex = 123456;
        constexpr size_t blocksCount = 100;

        TBlock block(nodeId, blockIndex, minCommitId, maxCommitId);

        auto list = TBlockList::EncodeBlocks(block, blocksCount, TDefaultAllocator::Instance());

        TMixedBlocks mixedBlocks(TDefaultAllocator::Instance());
        mixedBlocks.RefRange(rangeId);
        mixedBlocks.AddBlocks(rangeId, TPartialBlobId(), std::move(list));
        UNIT_ASSERT(mixedBlocks.IsLoaded(rangeId));

        mixedBlocks.RefRange(rangeId);
        UNIT_ASSERT(mixedBlocks.IsLoaded(rangeId));

        mixedBlocks.UnRefRange(rangeId);
        UNIT_ASSERT(mixedBlocks.IsLoaded(rangeId));

        mixedBlocks.UnRefRange(rangeId);
        UNIT_ASSERT(!mixedBlocks.IsLoaded(rangeId));
    }

    Y_UNIT_TEST(ShouldEvictLeastRecentlyUsedRanges)
    {
        constexpr ui32 rangeId1 = 0;
        constexpr ui32 rangeId2 = 1;
        constexpr ui32 rangeId3 = 2;

        constexpr ui64 nodeId = 1;
        constexpr ui64 minCommitId = MakeCommitId(12, 345);
        constexpr ui64 maxCommitId = InvalidCommitId;

        constexpr ui32 blockIndex = 123456;
        constexpr size_t blocksCount = 100;

        TBlock block(nodeId, blockIndex, minCommitId, maxCommitId);

        auto list = TBlockList::EncodeBlocks(
            block,
            blocksCount,
            TDefaultAllocator::Instance());

        TMixedBlocks mixedBlocks(TDefaultAllocator::Instance());
        mixedBlocks.Reset(1);
        mixedBlocks.RefRange(rangeId1);
        UNIT_ASSERT(mixedBlocks.IsLoaded(rangeId1));

        mixedBlocks.RefRange(rangeId2);
        mixedBlocks.AddBlocks(rangeId2, TPartialBlobId(), list);
        UNIT_ASSERT(mixedBlocks.IsLoaded(rangeId1));
        UNIT_ASSERT(mixedBlocks.IsLoaded(rangeId2));

        mixedBlocks.AddBlocks(rangeId1, TPartialBlobId(), list);
        mixedBlocks.UnRefRange(rangeId2);
        // So now the least recently used range is rangeId2. It should be added
        // to the offloaded list

        // The rangeId2 is not evicted, because it fits into the capacity of
        // offloaded ranges
        UNIT_ASSERT(mixedBlocks.IsLoaded(rangeId2));
        mixedBlocks.RefRange(rangeId3);
        mixedBlocks.UnRefRange(rangeId3);

        // Now the least recently used range is rangeId2, and it is evicted from
        // the offloaded ranges. It is replaced by rangeId3
        UNIT_ASSERT(!mixedBlocks.IsLoaded(rangeId2));
        UNIT_ASSERT(mixedBlocks.IsLoaded(rangeId3));

        mixedBlocks.UnRefRange(rangeId1);
        UNIT_ASSERT(mixedBlocks.IsLoaded(rangeId1));

        mixedBlocks.RefRange(rangeId1);
        // The range is moved from offloaded ranges to active ranges and its
        // data should be preserved
        UNIT_ASSERT(mixedBlocks.IsLoaded(rangeId1));
        UNIT_ASSERT_VALUES_EQUAL(
            blocksCount,
            mixedBlocks.FindBlob(rangeId1, TPartialBlobId()).Blocks.size());
        {
            TMixedBlockVisitor visitor;
            mixedBlocks.FindBlocks(
                visitor,
                rangeId1,
                nodeId,
                minCommitId + 1,
                blockIndex,
                blocksCount);

            auto blocks = visitor.Finish();
            UNIT_ASSERT_VALUES_EQUAL(blocksCount, blocks.size());
        }

        // And this can not be said about rangeId2
        mixedBlocks.RefRange(rangeId2);
        {
            TMixedBlockVisitor visitor;
            mixedBlocks.FindBlocks(
                visitor,
                rangeId2,
                nodeId,
                minCommitId + 1,
                blockIndex,
                blocksCount);

            auto blocks = visitor.Finish();
            UNIT_ASSERT_VALUES_EQUAL(0, blocks.size());
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
