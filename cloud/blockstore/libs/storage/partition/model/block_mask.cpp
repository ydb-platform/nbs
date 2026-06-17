#include "block_mask.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
T GetIntWithNBits(unsigned n)
{
    T result = 0;
    if (n == 0) {
        return result;
    }
    if (n >= sizeof(T) * 8) {
        result |= ~T{0};
        return result;
    }
    result |= (T{1} << n) - 1;
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBlockMask BlockMaskFromString(TStringBuf s)
{
    TBlockMask mask;

    if (s) {
        Y_ABORT_UNLESS(mask.GetChunkCount() * sizeof(TBlockMask::TChunk) == MaxBlocksCount/8);
        Y_ABORT_UNLESS(s.length() == MaxBlocksCount/8);
        memcpy((char*)mask.GetChunks(), s.data(), s.length());  // TODO
    }

    return mask;
}

TStringBuf BlockMaskAsString(const TBlockMask& mask)
{
    Y_ABORT_UNLESS(mask.GetChunkCount() * sizeof(TBlockMask::TChunk) == MaxBlocksCount/8);
    return { reinterpret_cast<const char*>(mask.GetChunks()), MaxBlocksCount/8 };
}

bool IsBlockMaskFull(const TBlockMask& mask, ui32 blockCount)
{
    const auto blocksInChunk = 8 * sizeof(mask.GetChunks()[0]);

    for (size_t i = 0; i < mask.GetChunkCount(); ++i) {
        const auto chunk = mask.GetChunks()[i];
        if (blockCount < blocksInChunk) {
            const TBitMap<blocksInChunk> actual(chunk);
            const TBitMap<blocksInChunk> expectedMask(
                GetIntWithNBits<TBlockMask::TChunk>(blockCount));
            return (actual & expectedMask) == expectedMask;
        }

        if (chunk != ~TBlockMask::TChunk(0)) {
            return false;
        }

        blockCount -= blocksInChunk;
    }
    return true;
}

TBlockMask GetFullBlockMask(ui32 blockCount)
{
    TBlockMask mask;
    mask.Set(0, blockCount);
    return mask;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
