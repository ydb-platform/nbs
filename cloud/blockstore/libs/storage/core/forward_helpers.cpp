#include "forward_helpers.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void ApplyMask(
    const THashSet<ui64>& unusedIndices,
    const ui64 startIndex,
    NProto::TReadBlocksRequest& request)
{
    Y_UNUSED(unusedIndices);
    Y_UNUSED(startIndex);
    Y_UNUSED(request);
}

void ApplyMask(
    const THashSet<ui64>& unusedIndices,
    const ui64 startIndex,
    NProto::TReadBlocksResponse& response)
{
    auto& buffers = *response.MutableBlocks()->MutableBuffers();
    for (int i = 0; i < buffers.size(); ++i) {
        ui64 b = startIndex + i;
        if (buffers[i].size() && unusedIndices.contains(b)) {
            memset(buffers[i].begin(), 0, buffers[i].size());
        }
    }
}

void ApplyMask(
    const THashSet<ui64>& unusedIndices,
    const ui64 startIndex,
    NProto::TReadBlocksLocalRequest& request)
{
    auto& sglist = request.Sglist;
    auto guard = sglist.Acquire();
    if (!guard) {
        return;
    }

    auto& blockDatas = guard.Get();

    for (ui32 i = 0; i < blockDatas.size(); ++i) {
        auto& buffer = blockDatas[i];
        ui64 b = startIndex + i;
        if (buffer.Size() && unusedIndices.contains(b)) {
            memset(const_cast<char*>(buffer.Data()), 0, buffer.Size());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void FillUnencryptedBlockMask(
    const THashSet<ui64>& unusedIndices,
    const ui64 startIndex,
    NProto::TReadBlocksResponse& response)
{
    if (unusedIndices.empty()) {
        return;
    }

    TDynBitMap bitmap;
    for (auto unusedIndex: unusedIndices) {
        Y_VERIFY(unusedIndex >= startIndex);
        bitmap.Set(unusedIndex - startIndex);
    }

    auto& blockMask = *response.MutableUnencryptedBlockMask();
    blockMask.assign(TStringBuf{
        reinterpret_cast<const char*>(bitmap.GetChunks()),
        bitmap.Size() / 8});
}

////////////////////////////////////////////////////////////////////////////////

void FillUnusedIndices(
    const NProto::TReadBlocksRequest& request,
    const TCompressedBitmap* usedBlocks,
    THashSet<ui64>* unusedIndices)
{
    if (!usedBlocks) {
        for (ui64 i = 0; i < request.GetBlocksCount(); ++i) {
            unusedIndices->insert(request.GetStartIndex() + i);
        }
        return;
    }

    const auto b = request.GetStartIndex();
    const auto e = request.GetStartIndex() + request.GetBlocksCount();
    if (usedBlocks->Count(b, e) < e - b) {
        for (ui64 i = 0; i < request.GetBlocksCount(); ++i) {
            ui64 b = request.GetStartIndex() + i;
            if (!usedBlocks->Test(b)) {
                unusedIndices->insert(b);
            }
        }
    }
}

void FillUnusedIndices(
    const NProto::TReadBlocksLocalRequest& request,
    const TCompressedBitmap* usedBlocks,
    THashSet<ui64>* unusedIndices)
{
    FillUnusedIndices(
        static_cast<const NProto::TReadBlocksRequest&>(request),
        usedBlocks,
        unusedIndices);
}

}   // namespace NCloud::NBlockStore::NStorage
