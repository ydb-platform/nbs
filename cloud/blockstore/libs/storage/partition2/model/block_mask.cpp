#include "block_mask.h"

namespace NCloud::NBlockStore::NStorage::NPartition2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
    requires(
        std::is_integral_v<T> && std::is_unsigned_v<T> &&
        !std::is_same_v<T, bool>)
T GetIntWithNBits(unsigned n)
{
    constexpr unsigned bits = std::numeric_limits<T>::digits;

    if (n == 0) {
        return T{0};
    }

    if (n >= bits) {
        return std::numeric_limits<T>::max();
    }

    return static_cast<T>(std::numeric_limits<T>::max() >> (bits - n));
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

void SetSkippedBlockIds(
    NProto::TBlobMeta2::TMergedBlocks& mergedBlocks,
    const TBlockMask& skipMask)
{
    const auto serialized = BlockMaskAsString(skipMask);
    mergedBlocks.SetSkippedBlockIds(serialized.data(), serialized.size());
}

TBlockMask GetSkippedBlockMask(
    const NProto::TBlobMeta2::TMergedBlocks& mergedBlocks)
{
    return BlockMaskFromString(mergedBlocks.GetSkippedBlockIds());
}

ui32 GetSkippedBlockCount(
    const NProto::TBlobMeta2::TMergedBlocks& mergedBlocks)
{
    if (mergedBlocks.GetSkippedBlockIds()) {
        return GetSkippedBlockMask(mergedBlocks).Count();
    }

    const auto& unknownFields =
        mergedBlocks.GetReflection()->GetUnknownFields(mergedBlocks);
    for (int i = 0; i < unknownFields.field_count(); ++i) {
        const auto& field = unknownFields.field(i);
        if (field.number() == 3 &&
            field.type() == NProtoBuf::UnknownField::TYPE_VARINT)
        {
            Y_ABORT_UNLESS(field.varint() <= MaxBlocksCount);
            return static_cast<ui32>(field.varint());
        }
    }

    return 0;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
