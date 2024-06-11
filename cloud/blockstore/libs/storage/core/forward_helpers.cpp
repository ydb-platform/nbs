#include "forward_helpers.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NBlobMarkers;

////////////////////////////////////////////////////////////////////////////////

void ClearEmptyBlocks(
    const TBlockMarks& usedBlocks,
    NProto::TReadBlocksResponse& response)
{
    auto& buffers = *response.MutableBlocks()->MutableBuffers();
    const size_t blockCount = Min<size_t>(usedBlocks.size(), buffers.size());
    for (size_t i = 0; i < blockCount; ++i) {
        auto& buffer = buffers[i];
        if (buffer.data() && std::holds_alternative<TEmptyMark>(usedBlocks[i]))
        {
            memset(buffer.begin(), 0, buffer.size());
        }
    }
}

void ClearEmptyBlocks(
    const TBlockMarks& usedBlocks,
    const TGuardedSgList& sglist)
{
    auto guard = sglist.Acquire();
    if (!guard) {
        return;
    }

    const auto& buffers = guard.Get();

    const size_t blockCount = Min(usedBlocks.size(), buffers.size());
    for (size_t i = 0; i < blockCount; ++i) {
        const auto& buffer = buffers[i];
        if (buffer.Data() && std::holds_alternative<TEmptyMark>(usedBlocks[i]))
        {
            memset(const_cast<char*>(buffer.Data()), 0, buffer.Size());
        }
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
