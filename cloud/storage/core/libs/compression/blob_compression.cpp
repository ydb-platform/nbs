#include "blob_compression.h"

#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <cstring>
#include <iterator>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

ui32 GetUncompressedChunkSize(
    const NProto::TBlobCompressionInfo& info,
    size_t chunk)
{
    if (info.HasExplicitChunks()) {
        return info.GetExplicitChunks().GetUncompressedChunkSizes(chunk);
    }
    const auto& regular = info.GetRegularChunks();
    return chunk + 1 == info.CompressedChunkSizesSize()
        ? regular.GetLastUncompressedChunkSize()
        : regular.GetMaxUncompressedChunkSize();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsWorthStoringCompressed(
    ui64 compressedSize,
    ui64 size,
    ui32 minSavedPercent)
{
    return compressedSize < size && minSavedPercent <= 100
        && compressedSize * 100 <= size * (100 - minSavedPercent);
}

TVector<ui32> SplitIntoChunks(TConstArrayRef<ui32> runSizes, ui32 chunkSize)
{
    TVector<ui32> chunkSizes;
    if (!chunkSize) {
        return chunkSizes;
    }

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

    auto& content = result.Content;

    size_t offset = 0;
    try {
        content.reserve(blockCodec->MaxCompressedLength(data));

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
            const size_t maxChunkSize =
                Max(chunk.size(), blockCodec->MaxCompressedLength(chunk));
            content.ReserveAndResize(storedSize + maxChunkSize);
            char* out = content.begin() + storedSize;

            const size_t compressedSize = blockCodec->Compress(chunk, out);
            if (IsWorthStoringCompressed(
                    compressedSize,
                    size,
                    minSavedPercent))
            {
                content.resize(storedSize + compressedSize);
                info.AddCompressedChunkSizes(compressedSize);
            } else {
                memcpy(out, chunk.data(), size);
                content.resize(storedSize + size);
                info.AddCompressedChunkSizes(0);
            }
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
        auto* regular = info.MutableRegularChunks();
        regular->SetMaxUncompressedChunkSize(chunkSize);
        regular->SetLastUncompressedChunkSize(chunkSizes.back());
    } else {
        info.MutableExplicitChunks()->MutableUncompressedChunkSizes()->Assign(
            chunkSizes.begin(),
            chunkSizes.end());
    }

    return result;
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

    const size_t chunkCount = info.CompressedChunkSizesSize();
    if (!blockSize || !chunkCount) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "invalid chunk count " << chunkCount << " or block size "
            << blockSize);
    }

    if (info.HasRegularChunks()) {
        const auto& regular = info.GetRegularChunks();
        const ui32 maxSize = regular.GetMaxUncompressedChunkSize();
        const ui32 lastSize = regular.GetLastUncompressedChunkSize();
        if (!maxSize || maxSize % blockSize != 0 || lastSize > maxSize) {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "invalid regular chunks: maximum " << maxSize
                << ", last " << lastSize << ", block size " << blockSize);
        }
    } else if (info.HasExplicitChunks()) {
        const size_t count =
            info.GetExplicitChunks().UncompressedChunkSizesSize();
        if (count != chunkCount) {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "invalid chunk sizes: " << chunkCount << " stored, "
                << count << " uncompressed");
        }
    } else {
        return MakeError(E_ARGUMENT, "missing uncompressed chunk layout");
    }

    TBlobChunkLayout layout;
    layout.Codec = codec.GetResult();
    layout.Offsets.reserve(chunkCount + 1);
    layout.StoredOffsets.reserve(chunkCount + 1);
    layout.RawChunks.reserve(chunkCount);

    layout.Offsets.push_back(0);
    layout.StoredOffsets.push_back(0);
    for (size_t i = 0; i < chunkCount; ++i) {
        const ui32 chunkSize = GetUncompressedChunkSize(info, i);
        const ui32 compressedSize = info.GetCompressedChunkSizes(i);

        if (!chunkSize || chunkSize % blockSize != 0
                || compressedSize > chunkSize)
        {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "chunk " << i << " has invalid sizes: " << compressedSize
                << " compressed, " << chunkSize << " uncompressed");
        }

        const bool raw = !compressedSize;
        layout.Offsets.push_back(layout.Offsets.back() + chunkSize);
        layout.StoredOffsets.push_back(
            layout.StoredOffsets.back() + (raw ? chunkSize : compressedSize));
        layout.RawChunks.push_back(raw);
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
    return RawChunks.size();
}

ui64 TBlobChunkLayout::GetUncompressedSize() const
{
    return Offsets.empty() ? 0 : Offsets.back();
}

ui32 TBlobChunkLayout::GetChunkIndex(ui64 offset) const
{
    Y_ABORT_UNLESS(offset < GetUncompressedSize());
    const auto next = UpperBound(Offsets.begin(), Offsets.end(), offset);
    return std::distance(Offsets.begin(), next) - 1;
}

ui64 TBlobChunkLayout::GetChunkOffset(ui32 chunk) const
{
    return Offsets[chunk];
}

ui64 TBlobChunkLayout::GetChunkLength(ui32 chunk) const
{
    return Offsets[chunk + 1] - Offsets[chunk];
}

ui64 TBlobChunkLayout::GetSpanLength(ui32 firstChunk, ui32 lastChunk) const
{
    return Offsets[lastChunk + 1] - Offsets[firstChunk];
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
    return RawChunks[chunk];
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError DecompressChunk(
    const TBlobChunkLayout& layout,
    ui32 chunk,
    TStringBuf stored,
    TArrayRef<char> out)
{
    if (chunk >= layout.GetChunkCount()
            || stored.size() != layout.GetStoredChunkLength(chunk)
            || out.size() != layout.GetChunkLength(chunk))
    {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "invalid chunk " << chunk << " of " << layout.GetChunkCount()
            << ", stored size " << stored.size() << ", output size "
            << out.size());
    }

    const ui64 expected = out.size();

    if (layout.IsRawChunk(chunk)) {
        memcpy(out.data(), stored.data(), expected);
        return {};
    }

    const auto* codec = layout.GetCodec();

    try {
        const ui64 declared = codec->DecompressedLength(stored);
        if (declared != expected) {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "chunk " << chunk << " declares " << declared
                << " uncompressed bytes, expected " << expected);
        }

        const ui64 decompressed = codec->Decompress(stored, out.data());
        if (decompressed != expected) {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "chunk " << chunk << " decompressed to " << decompressed
                << " bytes, expected " << expected);
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
    TArrayRef<char> out)
{
    if (firstChunk > lastChunk || lastChunk >= layout.GetChunkCount()
            || stored.size()
                != layout.GetStoredSpanLength(firstChunk, lastChunk)
            || out.size() != layout.GetSpanLength(firstChunk, lastChunk))
    {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "invalid chunk range [" << firstChunk << ", " << lastChunk
            << "] of " << layout.GetChunkCount() << " chunks, stored size "
            << stored.size() << ", output size " << out.size());
    }

    const ui64 outOffset = layout.GetChunkOffset(firstChunk);
    const ui64 storedOffset = layout.GetStoredChunkOffset(firstChunk);

    for (ui32 chunk = firstChunk; chunk <= lastChunk; ++chunk) {
        auto error = DecompressChunk(
            layout,
            chunk,
            stored.SubStr(
                layout.GetStoredChunkOffset(chunk) - storedOffset,
                layout.GetStoredChunkLength(chunk)),
            out.subspan(
                layout.GetChunkOffset(chunk) - outOffset,
                layout.GetChunkLength(chunk)));
        if (HasError(error)) {
            return error;
        }
    }

    return {};
}

}   // namespace NCloud
