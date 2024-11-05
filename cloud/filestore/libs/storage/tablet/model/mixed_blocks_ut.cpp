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
        ui32 blobOffset) override
    {
        Blocks.push_back({ block, blobId, blobOffset });
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
        mixedBlocks.AddBlocks(rangeId, TPartialBlobId(), std::move(list), {});

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
        mixedBlocks.AddBlocks(rangeId, TPartialBlobId(), std::move(list), {});

        mixedBlocks.AddDeletionMarker(
            rangeId,
            {nodeId, minCommitId + 2, blockIndex,blocksCount}
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
        mixedBlocks.AddBlocks(rangeId, TPartialBlobId(), std::move(list), {});
        UNIT_ASSERT(mixedBlocks.IsLoaded(rangeId));

        mixedBlocks.RefRange(rangeId);
        UNIT_ASSERT(mixedBlocks.IsLoaded(rangeId));

        mixedBlocks.UnRefRange(rangeId);
        UNIT_ASSERT(mixedBlocks.IsLoaded(rangeId));

        mixedBlocks.UnRefRange(rangeId);
        UNIT_ASSERT(!mixedBlocks.IsLoaded(rangeId));
    }
}

}   // namespace NCloud::NFileStore::NStorage
