#pragma once

#include "public.h"

#include "alloc.h"

#include <cloud/storage/core/libs/common/block_buffer.h>
#include <cloud/storage/core/libs/common/byte_vector.h>

#include <contrib/ydb/library/actors/util/rope.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

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

    void Extend(ui32 length)
    {
        Length += length;
    }
};

struct TCompressedRange
{
    ui32 Offset = 0;
    ui32 Length = 0;

    TCompressedRange() = default;

    TCompressedRange(ui32 offset, ui32 length)
        : Offset(offset)
        , Length(length)
    {}

    ui32 End() const
    {
        return Offset + Length;
    }

    void Merge(TCompressedRange other)
    {
        Offset = Min(Offset, other.Offset);
        auto end = Max(End(), other.End());
        Length = end - Offset;
    }

    bool Overlaps(TCompressedRange other) const
    {
        auto offset = Max(Offset, other.Offset);
        auto end = Min(End(), other.End());
        return end <= offset;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBlobCompressionInfo
{
private:
    struct TImpl;
    std::shared_ptr<TImpl> Impl;

public:
    TBlobCompressionInfo() = default;

    TBlobCompressionInfo(ui32 compressedBlobSize, IAllocator* alloc);

    explicit TBlobCompressionInfo(TByteVector bytes);

    bool BlobCompressed() const;

    ui32 CompressedBlobSize() const;

    TCompressedRange CompressedRange(TUncompressedRange range) const;

    const TByteVector& GetEncoded() const;
};

////////////////////////////////////////////////////////////////////////////////

TBlobCompressionInfo TryCompressBlob(
    ui32 chunkSize,
    const NBlockCodecs::ICodec* codec,
    TString* content,
    IAllocator* alloc);

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
