#include "forward_helpers.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NBlobMarkers;

namespace {

////////////////////////////////////////////////////////////////////////////////

void ZeroBlock(TString& block)
{
    memset(&block[0], 0, block.size());
}

void ZeroBlock(TBlockDataRef block)
{
    memset(const_cast<char*>(block.Data()), 0, block.Size());
}

template <typename TBlocks>
void ZeroBlocks(const TBlockMarks& usedBlocks, TBlocks& blocks)
{
    const size_t blockCount = Min<size_t>(usedBlocks.size(), blocks.size());
    for (size_t i = 0; i < blockCount; ++i) {
        auto& block = blocks[i];
        if (block && std::holds_alternative<TEmptyMark>(usedBlocks[i])) {
            ZeroBlock(block);
        }
    }
}

}   // namespace

void ClearEmptyBlocks(
    const TBlockMarks& usedBlocks,
    NProto::TReadBlocksResponse& response)
{
    NProto::TIOVector& blocks = *response.MutableBlocks();
    ZeroBlocks(usedBlocks, *blocks.MutableBuffers());
}

void ClearEmptyBlocks(
    const TBlockMarks& usedBlocks,
    const TGuardedSgList& sglist)
{
    if (auto guard = sglist.Acquire()) {
        ZeroBlocks(usedBlocks, guard.Get());
    }
}

////////////////////////////////////////////////////////////////////////////////

TBlockMarks MakeUsedBlockMarks(
    const TCompressedBitmap& usedBlocks,
    TBlockRange64 range)
{
    TBlockMarks blockMarks(range.Size(), TEmptyMark{});

    for (ui64 i = 0; i < range.Size(); ++i) {
        if (usedBlocks.Test(i + range.Start)) {
            blockMarks[i] = TUsedMark{};
        }
    }

    return blockMarks;
}

}   // namespace NCloud::NBlockStore::NStorage
