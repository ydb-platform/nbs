#include "blob_compression.h"

#include "binary_reader.h"
#include "binary_writer.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TBlobCompressionInfo::TImpl
{
    TByteVector Bytes;
    ui32 CompressedBlobSize = 0;

    TImpl(ui32 compressedBlobSize, IAllocator* alloc)
        : Bytes(alloc)
        , CompressedBlobSize(compressedBlobSize)
    {
        TBinaryWriter writer(alloc);
        writer.Write<ui32>(CompressedBlobSize);
        Bytes = writer.Finish();
    }

    explicit TImpl(TByteVector bytes)
        : Bytes(std::move(bytes))
    {
        TBinaryReader reader(Bytes);
        CompressedBlobSize = reader.Read<ui32>();
    }

    TCompressedRange CompressedRange(TUncompressedRange range) const
    {
        return TCompressedRange(range.Offset, range.Length);
    }

    const TByteVector& GetEncoded() const
    {
        return Bytes;
    }
};

////////////////////////////////////////////////////////////////////////////////

TBlobCompressionInfo::TBlobCompressionInfo(
        ui32 compressedBlobSize,
        IAllocator* alloc)
    : Impl(new TImpl(compressedBlobSize, alloc))
{}

TBlobCompressionInfo::TBlobCompressionInfo(TByteVector bytes)
    : Impl(new TImpl(std::move(bytes)))
{}

////////////////////////////////////////////////////////////////////////////////

bool TBlobCompressionInfo::BlobCompressed() const
{
    return !!Impl;
}

ui32 TBlobCompressionInfo::CompressedBlobSize() const
{
    Y_ABORT_UNLESS(Impl);
    return Impl->CompressedBlobSize;
}

TCompressedRange TBlobCompressionInfo::CompressedRange(
    TUncompressedRange range) const
{
    Y_ABORT_UNLESS(Impl);
    return Impl->CompressedRange(range);
}

const TByteVector& TBlobCompressionInfo::GetEncoded() const
{
    Y_ABORT_UNLESS(Impl);
    return Impl->GetEncoded();
}

////////////////////////////////////////////////////////////////////////////////

TBlobCompressionInfo TryCompressBlob(
    ui32 chunkSize,
    const NBlockCodecs::ICodec* codec,
    TString* content,
    IAllocator* alloc)
{
    Y_UNUSED(chunkSize);
    Y_UNUSED(codec);

    return TBlobCompressionInfo(static_cast<ui32>(content->size()), alloc);
}

////////////////////////////////////////////////////////////////////////////////

void Decompress(
    const TBlobCompressionInfo& blobCompressionInfo,
    ui32 blockSize,
    const TRope& compressedData,
    ui32 compressedDataOffset,
    const TVector<TUncompressedBlock>& blocks,
    IBlockBuffer* out)
{
    Y_UNUSED(blobCompressionInfo);
    Y_UNUSED(blockSize);
    Y_UNUSED(compressedData);
    Y_UNUSED(compressedDataOffset);
    Y_UNUSED(blocks);
    Y_UNUSED(out);
}

}   // namespace NCloud::NFileStore::NStorage
