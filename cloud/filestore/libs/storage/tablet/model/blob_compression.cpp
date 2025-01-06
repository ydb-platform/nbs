#include "blob_compression.h"

#include "binary_reader.h"
#include "binary_writer.h"

#include <cloud/filestore/libs/storage/model/block_buffer.h>

#include <library/cpp/blockcodecs/codecs.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TBlobCompressionInfo::TImpl
{
    TByteVector Bytes;
    ui32 DecompressedBlobSize = 0;
    ui32 CompressedBlobSize = 0;

    TImpl(
            ui32 decompressedBlobSize,
            ui32 compressedBlobSize,
            IAllocator* alloc)
        : Bytes(alloc)
        , DecompressedBlobSize(decompressedBlobSize)
        , CompressedBlobSize(compressedBlobSize)
    {
        TBinaryWriter writer(alloc);
        writer.Write<ui32>(DecompressedBlobSize);
        writer.Write<ui32>(CompressedBlobSize);
        Bytes = writer.Finish();
    }

    explicit TImpl(TByteVector bytes)
        : Bytes(std::move(bytes))
    {
        TBinaryReader reader(Bytes);
        DecompressedBlobSize = reader.Read<ui32>();
        CompressedBlobSize = reader.Read<ui32>();
    }

    TCompressedRange CompressedRange(TUncompressedRange range) const
    {
        Y_UNUSED(range);
        return TCompressedRange(0, CompressedBlobSize);
    }

    const TByteVector& GetEncoded() const
    {
        return Bytes;
    }
};

////////////////////////////////////////////////////////////////////////////////

TBlobCompressionInfo::TBlobCompressionInfo(
        ui32 decompressedBlobSize,
        ui32 compressedBlobSize,
        IAllocator* alloc)
    : Impl(new TImpl(decompressedBlobSize, compressedBlobSize, alloc))
{}

TBlobCompressionInfo::TBlobCompressionInfo(TByteVector bytes)
    : Impl(new TImpl(std::move(bytes)))
{}

////////////////////////////////////////////////////////////////////////////////

bool TBlobCompressionInfo::BlobCompressed() const
{
    return !!Impl;
}

ui32 TBlobCompressionInfo::DecompressedBlobSize() const
{
    Y_ABORT_UNLESS(Impl);
    return Impl->DecompressedBlobSize;
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
    Y_ABORT_UNLESS(chunkSize);
    Y_ABORT_UNLESS(codec);
    Y_ABORT_UNLESS(content);
    Y_ABORT_UNLESS(alloc);

    const size_t decompressedSize = content->size();

    TString out;
    codec->Encode(*content, out);
    *content = std::move(out);

    const size_t compressedSize = content->size();
    Y_DEBUG_ABORT_UNLESS(decompressedSize >= compressedSize);

    return TBlobCompressionInfo(
        static_cast<ui32>(decompressedSize),
        static_cast<ui32>(compressedSize),
        alloc);
}

////////////////////////////////////////////////////////////////////////////////

void Decompress(
    const NBlockCodecs::ICodec* codec,
    const TBlobCompressionInfo& blobCompressionInfo,
    ui32 blockSize,
    const TRope& compressedData,
    ui32 compressedDataOffset,
    const TVector<TUncompressedBlock>& blocks,
    IBlockBuffer* out)
{
    Y_ABORT_UNLESS(codec);
    Y_ABORT_UNLESS(blobCompressionInfo.BlobCompressed());
    Y_ABORT_UNLESS(blockSize);
    Y_ABORT_UNLESS(
        compressedData.size() == blobCompressionInfo.CompressedBlobSize());
    Y_ABORT_UNLESS(compressedDataOffset == 0);
    Y_ABORT_UNLESS(out);

    TString data = compressedData.ConvertToString();
    Y_ABORT_UNLESS(
        codec->DecompressedLength(data) == blobCompressionInfo.DecompressedBlobSize());

    TString decompressedData;
    codec->Decode(data, decompressedData);

    for (const auto& block: blocks) {
        const ui32 byteOffset = block.BlobOffset * blockSize;
        Y_ABORT_UNLESS(byteOffset < decompressedData.size());

        TStringBuf view(decompressedData.begin() + byteOffset, blockSize);
        out->SetBlock(block.BlockOffset, view);
    }
}

}   // namespace NCloud::NFileStore::NStorage
