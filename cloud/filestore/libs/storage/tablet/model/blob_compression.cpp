#include "blob_compression.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TBlobCompressionInfo::TImpl
{
    TCompressedRange CompressedRange(TUncompressedRange range) const
    {
        Y_UNUSED(range);
        return {};
    }

    TString Encode() const
    {
        return "";
    }
};

////////////////////////////////////////////////////////////////////////////////

TBlobCompressionInfo::TBlobCompressionInfo()
{}

TBlobCompressionInfo::TBlobCompressionInfo(const TBlobCompressionInfo& other)
{
    if (other.Impl) {
        Impl.reset(new TImpl(*other.Impl));
    }
}

TBlobCompressionInfo& TBlobCompressionInfo::operator=(
    TBlobCompressionInfo other)
{
    Impl = std::move(other.Impl);
    return *this;
}

TBlobCompressionInfo::~TBlobCompressionInfo()
{}

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

TString TBlobCompressionInfo::Encode() const
{
    Y_ABORT_UNLESS(Impl);
    return Impl->Encode();
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
