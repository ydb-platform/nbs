#include "blob_compressor.h"
#include "cloud/blockstore/libs/diagnostics/block_digest.h"

#include <cloud/storage/core/libs/common/verify.h>

#include <library/cpp/blockcodecs/codecs.h>

#include <util/generic/algorithm.h>
#include <util/generic/cast.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <cstring>
#include <utility>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

const NBlockCodecs::ICodec* GetCodec(ui64 tabletId, NProto::TBlobCompression::ECodec codec)
{
    switch (codec) {
        case NProto::TBlobCompression::LZ4:
            return NBlockCodecs::Codec("lz4-fast14-safe");
        case NProto::TBlobCompression::ZSTD:
            return NBlockCodecs::Codec("zstd08_1");
        default:
            STORAGE_VERIFY_C(
                false,
                TWellKnownEntityTypes::TABLET,
                tabletId,
                TStringBuilder() << "Unsupported blob compression codec: "
                                 << static_cast<ui32>(codec));
    }
}

ui32 GetChunkSize(
    ui64 tabletId,
    const NProto::TBlobCompression& compression,
    ui32 blockSize)
{
    const ui32 chunkSize = compression.GetChunkSize();
    STORAGE_VERIFY(
        chunkSize >= blockSize && chunkSize % blockSize == 0,
        TWellKnownEntityTypes::TABLET,
        tabletId);
    return chunkSize;
}

NProto::TError DecodeChunk(
    const NBlockCodecs::ICodec& codec,
    TStringBuf compressed,
    ui32 blockSize,
    ui32 chunkSize,
    bool lastChunk,
    TString& decoded)
{
    size_t size;
    try {
        size = codec.DecompressedLength(compressed);
    } catch (...) {
        return MakeError(
            E_REJECTED,
            TStringBuilder() << "Failed to read decompressed chunk size: "
                             << CurrentExceptionMessage());
    }
    if (!size || size > chunkSize || size % blockSize != 0 ||
        (!lastChunk && size != chunkSize))
    {
        return MakeError(
            E_REJECTED,
            TStringBuilder() << "Invalid decompressed chunk size: " << size);
    }
    try {
        codec.Decode(compressed, decoded);
    } catch (...) {
        return MakeError(
            E_REJECTED,
            TStringBuilder() << "Failed to decompress chunk: "
                             << CurrentExceptionMessage());
    }
    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBlobCompressor::TBlobCompressor(
    ui64 tabletId,
    ui32 blockSize,
    ui32 chunkSize,
    NProto::TBlobCompression::ECodec codec)
    : TabletId(tabletId)
    , BlockSize(blockSize)
    , ChunkSize(chunkSize)
    , CodecId(codec)
    , Codec(GetCodec(tabletId, codec))
{
    STORAGE_VERIFY(BlockSize, TWellKnownEntityTypes::TABLET, TabletId);
    STORAGE_VERIFY(*ChunkSize, TWellKnownEntityTypes::TABLET, TabletId);
    STORAGE_VERIFY(
        *ChunkSize % BlockSize == 0,
        TWellKnownEntityTypes::TABLET,
        TabletId);
    STORAGE_VERIFY(
        BlockSize <= *ChunkSize,
        TWellKnownEntityTypes::TABLET,
        TabletId);
}

TBlobCompressor::TBlobCompressor(ui64 tabletId, ui32 blockSize)
    : TabletId(tabletId)
    , BlockSize(blockSize)
    , ChunkSize(std::nullopt)
    , CodecId(std::nullopt)
    , Codec(nullptr)
{}

TBlobCompressor::TCompressedBlob TBlobCompressor::CompressBlob(
    TStringBuf blobPayload) const
{
    STORAGE_VERIFY(ChunkSize, TWellKnownEntityTypes::TABLET, TabletId);
    STORAGE_VERIFY(CodecId, TWellKnownEntityTypes::TABLET, TabletId);
    STORAGE_VERIFY(Codec, TWellKnownEntityTypes::TABLET, TabletId);

    STORAGE_VERIFY(
        blobPayload.size() % BlockSize == 0,
        TWellKnownEntityTypes::TABLET,
        TabletId);

    TCompressedBlob result;
    result.Compression.SetCodec(*CodecId);
    result.Compression.SetChunkSize(*ChunkSize);

    TString compressedChunk;
    while (blobPayload) {
        const auto chunk =
            blobPayload.Head(Min<size_t>(*ChunkSize, blobPayload.size()));
        try {
            Codec->Encode(chunk, compressedChunk);
        } catch (...) {
            STORAGE_VERIFY_C(
                false,
                TWellKnownEntityTypes::TABLET,
                TabletId,
                CurrentExceptionMessage());
        }
        result.Compression.AddChunkSizes(
            SafeIntegerCast<ui32>(compressedChunk.size()));
        result.Data.append(compressedChunk);
        blobPayload.Skip(chunk.size());
    }

    return result;
}

NProto::TError TBlobCompressor::DecompressBlob(
    const TVector<TRope>& compressedBuffers,
    const TSgList& outputBuffers,
    const NProto::TBlobCompression& compression,
    const TVector<ui16>& blockOffsets,
    TVector<ui32>* blockChecksums) const
{
    STORAGE_VERIFY(
        outputBuffers.size() == blockOffsets.size(),
        TWellKnownEntityTypes::TABLET, TabletId);
    STORAGE_VERIFY(
        IsSorted(blockOffsets.begin(), blockOffsets.end()),
        TWellKnownEntityTypes::TABLET, TabletId);
    for (const auto size: compression.GetChunkSizes()) {
        STORAGE_VERIFY(size, TWellKnownEntityTypes::TABLET, TabletId);
    }
    const ui32 chunkSize = GetChunkSize(TabletId, compression, BlockSize);
    for (size_t i = 0; i < blockOffsets.size(); ++i) {
        STORAGE_VERIFY(
            static_cast<ui64>(blockOffsets[i]) * BlockSize / chunkSize <
                compression.ChunkSizesSize(),
            TWellKnownEntityTypes::TABLET, TabletId);
        STORAGE_VERIFY(
            outputBuffers[i].Size() == BlockSize,
            TWellKnownEntityTypes::TABLET, TabletId);
    }

    size_t bufferIndex = 0;

    if (compressedBuffers.empty()) {
        if (!blockOffsets.empty()) {
            return MakeError(E_REJECTED, "Missing compressed buffers");
        }
        return {};
    }

    const NBlockCodecs::ICodec* codec =
        GetCodec(TabletId, compression.GetCodec());

    TRope::TConstIterator iter = compressedBuffers.front().Begin();
    TRope::TConstIterator end = compressedBuffers.front().End();
    size_t remainingBytes = compressedBuffers.front().GetSize();

    TString compressedChunk;
    TString decoded;

    size_t currentChunk = compression.ChunkSizesSize();
    for (size_t i = 0; i < blockOffsets.size(); ++i) {
        const ui64 blockStart = static_cast<ui64>(blockOffsets[i]) * BlockSize;
        const size_t chunk = blockStart / chunkSize;
        if (chunk != currentChunk) {
            const size_t compressedSize = compression.GetChunkSizes(chunk);

            while (iter == end && bufferIndex < compressedBuffers.size() - 1) {
                ++bufferIndex;
                iter = compressedBuffers[bufferIndex].Begin();
                end = compressedBuffers[bufferIndex].End();
                remainingBytes = compressedBuffers[bufferIndex].GetSize();
            }

            if (compressedSize > remainingBytes) {
                return MakeError(
                    E_REJECTED,
                    TStringBuilder() << "Incomplete compressed chunk " << chunk
                                     << ": expected " << compressedSize
                                     << " bytes, got " << remainingBytes);
            }

            compressedChunk.ReserveAndResize(compressedSize);
            iter.ExtractPlainDataAndAdvance(
                compressedChunk.begin(), compressedSize);
            remainingBytes -= compressedSize;

            auto error = DecodeChunk(
                *codec,
                compressedChunk,
                BlockSize,
                chunkSize, chunk + 1 == compression.ChunkSizesSize(), decoded);
            if (HasError(error)) {
                return error;
            }
            currentChunk = chunk;
        }

        const size_t offset = blockStart % chunkSize;

        if (offset + BlockSize > decoded.size()) {
            return MakeError(
                E_REJECTED,
                "Requested block is outside the decompressed chunk");
        }

        if (blockChecksums) {
            blockChecksums->push_back(
                ComputeDefaultDigest({decoded.data() + offset, BlockSize}));
        }

        std::memcpy(
            const_cast<char*>(outputBuffers[i].Data()),
            decoded.data() + offset,
            BlockSize);
    }

    if (remainingBytes != 0) {
        return MakeError(E_REJECTED, "Unexpected compressed data after chunks");
    }
    for (++bufferIndex; bufferIndex < compressedBuffers.size(); ++bufferIndex) {
        if (compressedBuffers[bufferIndex].GetSize() != 0) {
            return MakeError(
                E_REJECTED,
                "Unexpected compressed buffer after chunks");
        }
    }
    return {};
}

TBlobCompressor::TReadPlan TBlobCompressor::PlanReadBlob(
    const NKikimr::TLogoBlobID& blobId,
    const NProto::TBlobCompression& compression,
    const TVector<ui16>& blockOffsets) const
{
    if (blockOffsets.empty()) {
        return {};
    }

    STORAGE_VERIFY(
        IsSorted(blockOffsets.begin(), blockOffsets.end()),
        TWellKnownEntityTypes::TABLET,
        TabletId);
    const ui32 chunkSize = GetChunkSize(TabletId, compression, BlockSize);
    // Prefix sums translate uncompressed chunk indices to compressed offsets.
    TVector<ui64> chunkOffsets;
    chunkOffsets.reserve(compression.ChunkSizesSize() + 1);
    chunkOffsets.push_back(0);
    for (const auto size: compression.GetChunkSizes()) {
        STORAGE_VERIFY(size, TWellKnownEntityTypes::TABLET, TabletId);
        chunkOffsets.push_back(chunkOffsets.back() + size);
    }

    using TQuery = NKikimr::TEvBlobStorage::TEvGet::TQuery;

    TArrayHolder<TQuery> queries(new TQuery[blockOffsets.size()]);
    size_t queriesCount = 0;
    size_t nextChunk = 0;

    TVector<TVector<ui32>> blockOffsetsForEachRequest;

    for (const auto blockOffset: blockOffsets) {
        const ui64 blockStart = static_cast<ui64>(blockOffset) * BlockSize;
        const size_t chunk = blockStart / chunkSize;
        STORAGE_VERIFY(
            chunk < static_cast<size_t>(compression.ChunkSizesSize()),
            TWellKnownEntityTypes::TABLET,
            TabletId);

        if (chunk < nextChunk) {
            blockOffsetsForEachRequest.back().emplace_back(blockOffset);
            continue;
        }

        const ui64 offset = chunkOffsets[chunk];
        const ui64 size = compression.GetChunkSizes(chunk);
        STORAGE_VERIFY(
            offset + size <= blobId.BlobSize(),
            TWellKnownEntityTypes::TABLET,
            TabletId);
        auto* previousQuery =
            queriesCount ? &queries[queriesCount - 1] : nullptr;
        if (previousQuery &&
            static_cast<ui64>(previousQuery->Shift) + previousQuery->Size ==
                offset)
        {
            previousQuery->Size += SafeIntegerCast<ui32>(size);
        } else {
            queries[queriesCount++].Set(
                blobId,
                SafeIntegerCast<ui32>(offset),
                SafeIntegerCast<ui32>(size));
            blockOffsetsForEachRequest.emplace_back();
        }
        blockOffsetsForEachRequest.back().emplace_back(blockOffset);
        nextChunk = chunk + 1;
    }

    return {
        .Queries = std::move(queries),
        .QueriesCount = queriesCount,
        .BlockOffsetsForEachRequest = std::move(blockOffsetsForEachRequest)};
}

}   // namespace NCloud::NBlockStore::NStorage
