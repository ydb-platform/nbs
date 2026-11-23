#include "blob_compression.h"

#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <cstring>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

ui32 GetUncompressedChunkSize(
    const NProto::TBlobCompressionInfo& info,
    size_t chunk)
{
    if (info.UncompressedChunkSizesSize()) {
        return info.GetUncompressedChunkSizes(chunk);
    }
    return chunk + 1 == info.ChunkSizesSize()
        ? info.GetLastChunkSize()
        : info.GetChunkSize();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsWorthStoringCompressed(
    ui64 compressedSize,
    ui64 size,
    ui32 minSavedPercent)
{
    return compressedSize * 100 <= size * (100 - minSavedPercent);
}

TVector<ui32> SplitIntoChunks(TConstArrayRef<ui32> runSizes, ui32 chunkSize)
{
    Y_ABORT_UNLESS(chunkSize);

    TVector<ui32> chunkSizes;
    for (ui32 runSize: runSizes) {
        while (runSize) {
            chunkSizes.push_back(Min(runSize, chunkSize));
            runSize -= chunkSizes.back();
        }
    }
    return chunkSizes;
}

TResultOrError<TCompressedBlob> CompressChunks(
    NProto::ECompressionCodec codec,
    ui32 blockSize,
    ui32 chunkSize,
    ui32 minSavedPercent,
    TStringBuf data,
    TConstArrayRef<ui32> chunkSizes)
{
    const auto codecOrError = GetCompressionCodec(codec);
    if (HasError(codecOrError.GetError())) {
        return codecOrError.GetError();
    }
    const auto* blockCodec = codecOrError.GetResult();

    if (!blockSize || !chunkSize || chunkSize % blockSize != 0) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "invalid chunk size " << chunkSize << " for block size "
            << blockSize);
    }

    TCompressedBlob result;
    auto& info = result.Info;
    info.SetCodec(codec);
    info.SetChunkSize(chunkSize);

    auto& content = result.Content;
    content.reserve(blockCodec->MaxCompressedLength(data));

    size_t offset = 0;
    try {
        for (const ui32 size: chunkSizes) {
            if (!size || size % blockSize != 0 || size > chunkSize
                    || size > data.size() - offset)
            {
                return MakeError(E_ARGUMENT, TStringBuilder()
                    << "invalid chunk size " << size << " at offset "
                    << offset << ", data size " << data.size());
            }

            const auto chunk = data.SubStr(offset, size);
            const size_t storedSize = content.size();
            content.ReserveAndResize(
                storedSize + blockCodec->MaxCompressedLength(chunk));
            char* out = content.begin() + storedSize;

            size_t storedChunkSize = blockCodec->Compress(chunk, out);
            if (!IsWorthStoringCompressed(
                    storedChunkSize,
                    size,
                    minSavedPercent))
            {
                memcpy(out, chunk.data(), size);
                storedChunkSize = size;
            }

            content.resize(storedSize + storedChunkSize);
            info.AddChunkSizes(storedChunkSize);
            offset += size;
        }
    } catch (...) {
        return MakeError(E_FAIL, TStringBuilder()
            << "failed to compress chunk: " << CurrentExceptionMessage());
    }

    if (chunkSizes.empty() || offset != data.size()) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "chunks cover " << offset << " of " << data.size()
            << " bytes");
    }

    const bool fullChunks = AllOf(
        chunkSizes.first(chunkSizes.size() - 1),
        [=] (ui32 size) { return size == chunkSize; });
    if (fullChunks) {
        info.SetLastChunkSize(chunkSizes.back());
    } else {
        info.MutableUncompressedChunkSizes()->Assign(
            chunkSizes.begin(),
            chunkSizes.end());
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool IsCompressed(const NProto::TBlobCompressionInfo& info)
{
    return info.ChunkSizesSize() != 0;
}

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TBlobChunkLayout> TBlobChunkLayout::Create(
    const NProto::TBlobCompressionInfo& info,
    ui64 blobSize,
    ui32 blockSize)
{
    const auto codec = GetCompressionCodec(info.GetCodec());
    if (HasError(codec.GetError())) {
        return codec.GetError();
    }

    const ui32 maxChunkSize = info.GetChunkSize();
    if (!blockSize || !maxChunkSize || maxChunkSize % blockSize != 0) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "invalid chunk size " << maxChunkSize << " for block size "
            << blockSize);
    }

    // either the size of every chunk or only the size of the last one
    const size_t chunkCount = info.ChunkSizesSize();
    const bool validSizes = info.UncompressedChunkSizesSize()
        ? info.UncompressedChunkSizesSize() == chunkCount
            && !info.HasLastChunkSize()
        : info.HasLastChunkSize();
    if (!chunkCount || !validSizes) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "invalid chunk sizes: " << chunkCount << " stored, "
            << info.UncompressedChunkSizesSize() << " uncompressed");
    }

    TBlobChunkLayout layout;
    layout.Codec = codec.GetResult();
    layout.Offsets.reserve(chunkCount + 1);
    layout.StoredOffsets.reserve(chunkCount + 1);

    for (size_t i = 0; i < chunkCount; ++i) {
        const ui32 chunkSize = GetUncompressedChunkSize(info, i);
        const ui32 storedChunkSize = info.GetChunkSizes(i);

        if (!chunkSize || chunkSize % blockSize != 0
                || chunkSize > maxChunkSize
                || !storedChunkSize || storedChunkSize > chunkSize)
        {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "chunk " << i << " has invalid sizes: " << storedChunkSize
                << " stored, " << chunkSize << " uncompressed");
        }

        layout.Offsets.push_back(layout.Offsets.back() + chunkSize);
        layout.StoredOffsets.push_back(
            layout.StoredOffsets.back() + storedChunkSize);
    }

    if (layout.StoredOffsets.back() != blobSize) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "chunks occupy " << layout.StoredOffsets.back()
            << " bytes, blob size is " << blobSize);
    }

    return layout;
}

const NBlockCodecs::ICodec* TBlobChunkLayout::GetCodec() const
{
    return Codec;
}

ui32 TBlobChunkLayout::GetChunkCount() const
{
    return Offsets.size() - 1;
}

ui64 TBlobChunkLayout::GetUncompressedSize() const
{
    return Offsets.back();
}

ui32 TBlobChunkLayout::GetChunkIndex(ui64 offset) const
{
    Y_ABORT_UNLESS(offset < GetUncompressedSize());
    return UpperBound(Offsets.begin(), Offsets.end(), offset)
        - Offsets.begin() - 1;
}

ui64 TBlobChunkLayout::GetChunkOffset(ui32 chunk) const
{
    return Offsets[chunk];
}

ui64 TBlobChunkLayout::GetChunkLength(ui32 chunk) const
{
    return Offsets[chunk + 1] - Offsets[chunk];
}

ui64 TBlobChunkLayout::GetStoredChunkOffset(ui32 chunk) const
{
    return StoredOffsets[chunk];
}

ui64 TBlobChunkLayout::GetStoredChunkLength(ui32 chunk) const
{
    return StoredOffsets[chunk + 1] - StoredOffsets[chunk];
}

ui64 TBlobChunkLayout::GetStoredSpanLength(
    ui32 firstChunk,
    ui32 lastChunk) const
{
    return StoredOffsets[lastChunk + 1] - StoredOffsets[firstChunk];
}

bool TBlobChunkLayout::IsRawChunk(ui32 chunk) const
{
    return GetStoredChunkLength(chunk) == GetChunkLength(chunk);
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError DecompressChunk(
    const TBlobChunkLayout& layout,
    ui32 chunk,
    TStringBuf stored,
    char* out)
{
    if (chunk >= layout.GetChunkCount()
            || stored.size() != layout.GetStoredChunkLength(chunk))
    {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "invalid chunk " << chunk << " of " << layout.GetChunkCount()
            << ", stored size " << stored.size());
    }

    const ui64 expected = layout.GetChunkLength(chunk);

    if (layout.IsRawChunk(chunk)) {
        memcpy(out, stored.data(), expected);
        return {};
    }

    const auto* codec = layout.GetCodec();

    try {
        const ui64 declared = codec->DecompressedLength(stored);
        if (declared != expected || codec->Decompress(stored, out) != expected)
        {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "chunk " << chunk << " declares " << declared
                << " uncompressed bytes, expected " << expected);
        }
    } catch (...) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "chunk " << chunk << " decompression failed: "
            << CurrentExceptionMessage());
    }

    return {};
}

NProto::TError DecompressChunks(
    const TBlobChunkLayout& layout,
    TStringBuf stored,
    ui32 firstChunk,
    ui32 lastChunk,
    TString& out)
{
    if (firstChunk > lastChunk || lastChunk >= layout.GetChunkCount()
            || stored.size()
                != layout.GetStoredSpanLength(firstChunk, lastChunk))
    {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "invalid chunk range [" << firstChunk << ", " << lastChunk
            << "] of " << layout.GetChunkCount() << " chunks, stored size "
            << stored.size());
    }

    const ui64 outOffset = layout.GetChunkOffset(firstChunk);
    const ui64 storedOffset = layout.GetStoredChunkOffset(firstChunk);
    out.ReserveAndResize(
        layout.GetChunkOffset(lastChunk) + layout.GetChunkLength(lastChunk)
            - outOffset);

    for (ui32 chunk = firstChunk; chunk <= lastChunk; ++chunk) {
        auto error = DecompressChunk(
            layout,
            chunk,
            stored.SubStr(
                layout.GetStoredChunkOffset(chunk) - storedOffset,
                layout.GetStoredChunkLength(chunk)),
            out.begin() + (layout.GetChunkOffset(chunk) - outOffset));
        if (HasError(error)) {
            return error;
        }
    }

    return {};
}

}   // namespace NCloud
