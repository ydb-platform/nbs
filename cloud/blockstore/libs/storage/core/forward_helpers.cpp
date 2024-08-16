#include "forward_helpers.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NBlobMarkers;

////////////////////////////////////////////////////////////////////////////////

void ApplyMask(
    const TBlockMarks& blockMarks,
    NProto::TReadBlocksRequest& request)
{
    Y_UNUSED(blockMarks);
    Y_UNUSED(request);
}

void ApplyMask(
    const TBlockMarks& blockMarks,
    NProto::TReadBlocksResponse& response)
{
    auto& buffers = *response.MutableBlocks()->MutableBuffers();
    const size_t minSize = std::min(std::ssize(blockMarks), std::ssize(buffers));
    for (size_t i = 0; i < minSize; ++i) {
        if (buffers[i].data() && std::holds_alternative<TEmptyMark>(blockMarks[i])) {
            memset(buffers[i].begin(), 0, buffers[i].size());
        }
    }
}

void ApplyMask(
    const TBlockMarks& blockMarks,
    NProto::TReadBlocksLocalRequest& request)
{
    auto& sglist = request.Sglist;
    auto guard = sglist.Acquire();
    if (!guard) {
        return;
    }

    auto& blockDatas = guard.Get();

    for (size_t i = 0; i < blockMarks.size() && i < blockDatas.size(); ++i) {
        auto& buffer = blockDatas[i];
        if (buffer.Data() && std::holds_alternative<TEmptyMark>(blockMarks[i])) {
            memset(const_cast<char*>(buffer.Data()), 0, buffer.Size());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void FillUnencryptedBlockMask(
    const TBlockMarks& blockMarks,
    NProto::TReadBlocksResponse& response)
{
    if (blockMarks.empty()) {
        return;
    }

    TDynBitMap bitmap;
    for (size_t i = 0; i < blockMarks.size(); ++i) {
        if (std::holds_alternative<TEmptyMark>(blockMarks[i]) ||
            std::holds_alternative<TFreshMarkOnBaseDisk>(blockMarks[i]) ||
            std::holds_alternative<TBlobMarkOnBaseDisk>(blockMarks[i])) {
            bitmap.Set(i);
        }
    }

    auto& blockMask = *response.MutableUnencryptedBlockMask();
    blockMask.assign(TStringBuf{
        reinterpret_cast<const char*>(bitmap.GetChunks()),
        bitmap.Size() / 8});
}

////////////////////////////////////////////////////////////////////////////////

TBlockMarks MakeBlockMarks(
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
