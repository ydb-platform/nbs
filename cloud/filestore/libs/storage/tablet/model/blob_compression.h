#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/block_buffer.h>

#include <contrib/ydb/library/actors/util/rope.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NBlockCodecs {

////////////////////////////////////////////////////////////////////////////////

struct ICodec;

}   // namespace NBlockCodecs

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TUncompressedRange
{
    ui32 Offset = 0;
    ui32 Length = 0;

    TUncompressedRange() = default;

    TUncompressedRange(ui32 offset, ui32 length)
        : Offset(offset)
        , Length(length)
    {}

    ui32 End() const
    {
        return Offset + Length;
    }

    void Extend(ui32 length)
    {
        Length += length;
    }

    void Merge(TUncompressedRange other)
    {
        Offset = Min(Offset, other.Offset);
        auto end = Max(End(), other.End());
        Length = end - Offset;
    }

    bool Overlaps(TUncompressedRange other) const
    {
        auto offset = Max(Offset, other.Offset);
        auto end = Min(End(), other.End());
        return end <= offset;
    }
};

using TCompressedRange = TUncompressedRange;

////////////////////////////////////////////////////////////////////////////////

class TBlobCompressionInfo
{
public:
    bool BlobCompressed() const
    {
        return false;
    }

    TString Encode() const
    {
        return "";
    }

    TCompressedRange CompressedRange(TUncompressedRange range) const
    {
        Y_UNUSED(range);
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

TBlobCompressionInfo TryCompressBlob(
    ui32 chunkSize,
    const NBlockCodecs::ICodec* codec,
    TString* content);

////////////////////////////////////////////////////////////////////////////////

struct TUncompressedBlock
{
    ui32 BlobOffset;
    ui32 BlockOffset;

    TUncompressedBlock(ui32 blobOffset, ui32 blockOffset)
        : BlobOffset(blobOffset)
        , BlockOffset(blockOffset)
    {}
};

struct IBlockBuffer;

void Decompress(
    const TBlobCompressionInfo& blobCompressionInfo,
    ui32 blockSize,
    const TRope& compressedData,
    ui32 compressedDataOffset,
    const TVector<TUncompressedBlock>& blocks,
    IBlockBuffer* out);

}   // namespace NCloud::NFileStore::NStorage
