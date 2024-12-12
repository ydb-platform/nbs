#include "blob_compression.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TBlobCompressionInfo::TImpl
{
    TByteVector Bytes;

    TImpl(TByteVector bytes)
        : Bytes(std::move(bytes))
    {}

    TCompressedRange CompressedRange(TUncompressedRange range) const
    {
        Y_UNUSED(range);
        return {};
    }

    const TByteVector& GetEncoded() const
    {
        return Bytes;
    }
};

////////////////////////////////////////////////////////////////////////////////

TBlobCompressionInfo::TBlobCompressionInfo(TByteVector bytes)
    : Impl(new TImpl(std::move(bytes)))
{}

////////////////////////////////////////////////////////////////////////////////

bool TBlobCompressionInfo::BlobCompressed() const
{
    return !!Impl;
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
    TString* content)
{
    Y_UNUSED(chunkSize);
    Y_UNUSED(codec);
    Y_UNUSED(content);
    return {};
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
