#include <cloud/filestore/libs/storage/tablet/model/mixed_blocks.h>

#include <library/cpp/testing/benchmark/bench.h>

#include <util/generic/vector.h>

#include <memory>

using namespace NCloud;
using namespace NCloud::NFileStore;
using namespace NCloud::NFileStore::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TMixedBlockVisitor final
    : public IMixedBlockVisitor
{
    TVector<TBlockDataRef> Blocks;

    TMixedBlockVisitor()
        : Blocks(Reserve(MaxBlocksCount))
    {}

    void Accept(
        const TBlock& block,
        const TPartialBlobId& blobId,
        ui32 blobOffset) override
    {
        Blocks.emplace_back(block, blobId, blobOffset);
    }
};

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 RangeId = 1234;

constexpr ui64 NodeId = 456;
constexpr ui64 MinCommitId = MakeCommitId(12, 345);

constexpr ui32 BlockIndex = 123456;
constexpr size_t BlocksCount = MaxBlocksCount;

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TMixedBlocks> MakeMixedBlocks(TVector<TBlock> blocks)
{
    auto blockList = TBlockList::EncodeBlocks(
        std::move(blocks),
        TDefaultAllocator::Instance());

    auto mixedBlocks = std::make_unique<TMixedBlocks>(
        TDefaultAllocator::Instance());
    mixedBlocks->RefRange(RangeId);
    mixedBlocks->AddBlocks(RangeId, TPartialBlobId(), std::move(blockList));
    return mixedBlocks;
}

std::unique_ptr<TMixedBlocks> GenerateMixedBlocks(
    size_t deletionMarkers,
    bool mixedDeletionMarkers = false,
    bool startDeletionMarkersAtTheEnd = false)
{
    TVector<TBlock> blocks;
    for (size_t i = 0; i < BlocksCount; i++) {
        blocks.emplace_back(
            NodeId,
            BlockIndex + i,
            MinCommitId,
            InvalidCommitId);
    }

    for (size_t i = 0; i < blocks.size(); i++) {
        TBlock* block = nullptr;
        if (startDeletionMarkersAtTheEnd) {
            block = &blocks[blocks.size() - i - 1];
        } else {
            block = &blocks[i];
        }

        // Generate marker for blocks with even block indices only
        if (mixedDeletionMarkers && (i % 2 != 0)) {
            continue;
        }

        if (deletionMarkers == 0) {
            break;
        }
        deletionMarkers--;

        block->MaxCommitId = MinCommitId + 2;
    }

    return MakeMixedBlocks(std::move(blocks));
}

void FindBlocks(
    TMixedBlocks& mixedBlocks,
    ui32 blockIndex = BlockIndex,
    size_t blocksCount = BlocksCount)
{
    TMixedBlockVisitor visitor;
    mixedBlocks.FindBlocks(
        visitor,
        RangeId,
        NodeId,
        MinCommitId,
        blockIndex,
        blocksCount);
    Y_ABORT_UNLESS(
        blocksCount == visitor.Blocks.size(),
        "expected %lu but got %lu blocks",
        blocksCount,
        visitor.Blocks.size());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

const auto MixedBlocksWithoutDeletionMarkers = GenerateMixedBlocks(0);
const auto MixedBlocksWithOneDeletionMarker = GenerateMixedBlocks(1);
const auto MixedBlocksWithOneDeletionMarkerAtTheEnd = GenerateMixedBlocks(
    1,
    false,  // mixedDeletionMarkers
    true);  // startDeletionMarkersAtTheEnd
const auto MixedBlocksWithMergedDeletionMarkers = GenerateMixedBlocks(
    BlocksCount / 2);
const auto MixedBlocksWithMergedDeletionMarkersAtTheEnd = GenerateMixedBlocks(
    BlocksCount / 2,
    false,  // mixedDeletionMarkers
    true);  // startDeletionMarkersAtTheEnd
const auto MixedBlocksWithMixedDeletionMarkers = GenerateMixedBlocks(
    BlocksCount / 2,
    true);  // mixedDeletionMarkers

Y_CPU_BENCHMARK(TMixedBlocks_FindBlocksWithoutDeletionMarkers, iface)
{
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        FindBlocks(*MixedBlocksWithoutDeletionMarkers);
    }
}

Y_CPU_BENCHMARK(TMixedBlocks_FindBlocksWithOneDeletionMarker, iface)
{
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        FindBlocks(*MixedBlocksWithOneDeletionMarker);
    }
}

Y_CPU_BENCHMARK(TMixedBlocks_FindBlocksWithOneDeletionMarkerAtTheEnd, iface)
{
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        FindBlocks(*MixedBlocksWithOneDeletionMarkerAtTheEnd);
    }
}

Y_CPU_BENCHMARK(TMixedBlocks_FindBlocksWithMergedDeletionMarkers, iface)
{
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        FindBlocks(*MixedBlocksWithMergedDeletionMarkers);
    }
}

Y_CPU_BENCHMARK(TMixedBlocks_FindBlocksWithMergedDeletionMarkersAtTheEnd, iface)
{
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        FindBlocks(*MixedBlocksWithMergedDeletionMarkersAtTheEnd);
    }
}

Y_CPU_BENCHMARK(TMixedBlocks_FindBlocksWithMixedDeletionMarkers, iface)
{
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        FindBlocks(*MixedBlocksWithMixedDeletionMarkers);
    }
}

Y_CPU_BENCHMARK(TMixedBlocks_FindSingleBlockWithoutDeletionMarkers, iface)
{
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        FindBlocks(*MixedBlocksWithoutDeletionMarkers, BlockIndex, 1);
    }
}

Y_CPU_BENCHMARK(TMixedBlocks_FindSingleBlockWithMergedDeletionMarkers, iface)
{
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        FindBlocks(
            *MixedBlocksWithMergedDeletionMarkers,
            BlockIndex + BlocksCount/2,
            1);
    }
}
