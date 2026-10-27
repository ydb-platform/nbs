#include "blob_compressor.h"
#include "huge_slot_sizes.h"

#include "cloud/blockstore/libs/diagnostics/block_digest.h"

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

NProto::TError MakeInputError(ui64 tabletId, TStringBuf message)
{
    return MakeError(
        E_ARGUMENT,
        TStringBuilder() << "Tablet " << tabletId << ": " << message);
}

TResultOrError<const NBlockCodecs::ICodec*> GetCodec(
    ui64 tabletId,
    NProto::TBlobCompression::ECodec codec)
{
    const char* name = nullptr;
    switch (codec) {
        case NProto::TBlobCompression::LZ4:
            name = "lz4-fast14-safe";
            break;
        case NProto::TBlobCompression::ZSTD:
            name = "zstd08_1";
            break;
        default:
            return MakeInputError(
                tabletId,
                TStringBuilder() << "Unsupported blob compression codec: "
                                 << static_cast<ui32>(codec));
    }
    try {
        return NBlockCodecs::Codec(name);
    } catch (...) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "Tablet " << tabletId
                             << ": Failed to look up blob compression codec: "
                             << CurrentExceptionMessage());
    }
}

NProto::TError ValidateCompressionSettings(
    ui64 tabletId,
    ui32 blockSize,
    ui32 chunkSize)
{
    if (!blockSize) {
        return MakeInputError(tabletId, "Block size must be nonzero");
    }
    if (chunkSize < blockSize || chunkSize % blockSize != 0) {
        return MakeInputError(
            tabletId,
            TStringBuilder() << "Invalid chunk size " << chunkSize
                             << " for block size " << blockSize);
    }
    return {};
}

NProto::TError ValidateCompressBlobInput(
    ui64 tabletId,
    ui32 blockSize,
    const std::optional<ui32>& chunkSize,
    const std::optional<NProto::TBlobCompression::ECodec>& codecId,
    TStringBuf blobPayload)
{
    if (!chunkSize || !codecId) {
        return MakeInputError(tabletId, "Missing blob compression settings");
    }
    if (auto error =
            ValidateCompressionSettings(tabletId, blockSize, *chunkSize);
        HasError(error))
    {
        return error;
    }
    if (blobPayload.size() % blockSize != 0) {
        return MakeInputError(
            tabletId, "Blob payload must contain whole blocks");
    }
    return {};
}

NProto::TError ValidateReadBlobInput(
    ui64 tabletId,
    ui32 blockSize,
    ui32 chunkSize,
    const NProto::TBlobCompression& compression,
    const TVector<ui16>& blockOffsets)
{
    if (auto error =
            ValidateCompressionSettings(tabletId, blockSize, chunkSize);
        HasError(error))
    {
        return error;
    }
    if (!IsSorted(blockOffsets.begin(), blockOffsets.end())) {
        return MakeInputError(tabletId, "Block offsets must be sorted");
    }
    for (const auto size: compression.GetChunkSizes()) {
        if (!size) {
            return MakeInputError(
                tabletId, "Compressed chunk sizes must be nonzero");
        }
    }
    for (const auto blockOffset: blockOffsets) {
        if (static_cast<ui64>(blockOffset) * blockSize / chunkSize >=
            compression.ChunkSizesSize())
        {
            return MakeInputError(
                tabletId,
                TStringBuilder() << "Block offset " << blockOffset
                                 << " is outside the chunk list");
        }
    }
    return {};
}

NProto::TError ValidateDecompressBlobInput(
    ui64 tabletId,
    ui32 blockSize,
    ui32 chunkSize,
    const TVector<TRope>& compressedBuffers,
    const TSgList& outputBuffers,
    const NProto::TBlobCompression& compression,
    const TVector<ui16>& blockOffsets)
{
    if (outputBuffers.size() != blockOffsets.size()) {
        return MakeInputError(
            tabletId, "Output buffer count must match block offset count");
    }
    if (auto error = ValidateReadBlobInput(
            tabletId,
            blockSize,
            chunkSize,
            compression,
            blockOffsets);
        HasError(error))
    {
        return error;
    }
    for (const auto& buffer: outputBuffers) {
        if (buffer.Size() != blockSize) {
            return MakeInputError(
                tabletId, "Output buffer size must match block size");
        }
    }

    if (compressedBuffers.empty()) {
        if (!blockOffsets.empty()) {
            return MakeError(E_REJECTED, "Missing compressed buffers");
        }
        return {};
    }

    size_t bufferIndex = 0;
    size_t remainingBytes = compressedBuffers.front().GetSize();
    size_t currentChunk = compression.ChunkSizesSize();
    for (const auto blockOffset: blockOffsets) {
        const size_t chunk =
            static_cast<ui64>(blockOffset) * blockSize / chunkSize;
        if (chunk == currentChunk) {
            continue;
        }
        const size_t compressedSize = compression.GetChunkSizes(chunk);
        while (!remainingBytes && bufferIndex < compressedBuffers.size() - 1) {
            remainingBytes = compressedBuffers[++bufferIndex].GetSize();
        }
        if (compressedSize > remainingBytes) {
            return MakeError(
                E_REJECTED,
                TStringBuilder() << "Incomplete compressed chunk " << chunk
                                 << ": expected " << compressedSize
                                 << " bytes, got " << remainingBytes);
        }
        remainingBytes -= compressedSize;
        currentChunk = chunk;
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

NProto::TError ValidatePlanReadBlobInput(
    ui64 tabletId,
    ui32 blockSize,
    ui32 chunkSize,
    const NKikimr::TLogoBlobID& blobId,
    const NProto::TBlobCompression& compression,
    const TVector<ui16>& blockOffsets,
    const TVector<ui64>& chunkOffsets)
{
    if (auto error = ValidateReadBlobInput(
            tabletId,
            blockSize,
            chunkSize,
            compression,
            blockOffsets);
        HasError(error))
    {
        return error;
    }
    for (const auto blockOffset: blockOffsets) {
        const size_t chunk =
            static_cast<ui64>(blockOffset) * blockSize / chunkSize;
        if (chunkOffsets[chunk + 1] > blobId.BlobSize()) {
            return MakeInputError(
                tabletId,
                TStringBuilder() << "Compressed chunk " << chunk
                                 << " extends beyond the blob");
        }
    }
    return {};
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
    NProto::TBlobCompression::ECodec codec,
    ui64 minSavingsPercent,
    NProto::EBlobCompressionMode mode,
    const NProto::THugeSlotConfig& hugeSlotConfig)
    : TabletId(tabletId)
    , BlockSize(blockSize)
    , ChunkSize(chunkSize)
    , CodecId(codec)
    , MinSavingsPercent(minSavingsPercent)
    , Mode(mode)
    , BlobHeaderSize(hugeSlotConfig.GetBlobHeaderSize())
    , HugeSlotSizes(
          mode == NProto::BCM_HUGE_SLOT
              ? CalculateHugeSlotSizes(hugeSlotConfig)
              : TResultOrError<TVector<ui32>>(TVector<ui32>{}))
{}

TBlobCompressor::TBlobCompressor(ui64 tabletId, ui32 blockSize)
    : TabletId(tabletId)
    , BlockSize(blockSize)
    , ChunkSize(std::nullopt)
    , CodecId(std::nullopt)
    , MinSavingsPercent(0)
    , Mode(NProto::BCM_MIN_SAVINGS)
    , BlobHeaderSize(0)
    , HugeSlotSizes(TVector<ui32>{})
{}

TResultOrError<TBlobCompressor::TCompressedBlob> TBlobCompressor::CompressBlob(
    TStringBuf blobPayload) const
{
    if (auto error = ValidateCompressBlobInput(
            TabletId,
            BlockSize,
            ChunkSize,
            CodecId,
            blobPayload);
        HasError(error))
    {
        return error;
    }
    auto codecOrError = GetCodec(TabletId, *CodecId);
    if (HasError(codecOrError.GetError())) {
        return codecOrError.GetError();
    }
    const auto* codec = codecOrError.GetResult();

    TCompressedBlob result;
    result.Compression.SetCodec(*CodecId);
    result.Compression.SetChunkSize(*ChunkSize);

    TString compressedChunk;
    while (blobPayload) {
        const auto chunk =
            blobPayload.Head(Min<size_t>(*ChunkSize, blobPayload.size()));
        try {
            codec->Encode(chunk, compressedChunk);
        } catch (...) {
            return MakeError(
                E_FAIL,
                TStringBuilder()
                    << "Tablet " << TabletId << ": Failed to compress chunk: "
                    << CurrentExceptionMessage());
        }
        if (compressedChunk.size() > Max<ui32>()) {
            return MakeError(E_FAIL, "Compressed chunk size exceeds ui32");
        }
        result.Compression.AddChunkSizes(
            static_cast<ui32>(compressedChunk.size()));
        result.Data.append(compressedChunk);
        blobPayload.Skip(chunk.size());
    }

    return result;
}

TResultOrError<TBlobCompressor::TCompressedBlob>
TBlobCompressor::CompressBlobIfNeeded(
    TStringBuf blobPayload,
    const NKikimr::TBlobStorageGroupType* groupType,
    NKikimr::TErasureType::ECrcMode crcMode) const
{
    if (Mode != NProto::BCM_MIN_SAVINGS && Mode != NProto::BCM_HUGE_SLOT) {
        return MakeInputError(TabletId, "Unknown blob compression mode");
    }
    if (HasError(HugeSlotSizes.GetError())) {
        return MakeInputError(TabletId, HugeSlotSizes.GetError().GetMessage());
    }
    if (Mode == NProto::BCM_HUGE_SLOT && !groupType) {
        return MakeInputError(TabletId, "Missing target blob storage group type");
    }
    if (Mode == NProto::BCM_HUGE_SLOT &&
        (blobPayload.size() > Max<ui32>() ||
         (crcMode != NKikimr::TErasureType::CrcModeNone &&
          crcMode != NKikimr::TErasureType::CrcModeWholePart)))
    {
        return MakeInputError(TabletId, "Invalid blob size or CRC mode");
    }

    auto compressedOrError = CompressBlob(blobPayload);
    if (HasError(compressedOrError.GetError())) {
        return compressedOrError.GetError();
    }

    const auto compressedSize = compressedOrError.GetResult().Data.size();
    if (Mode == NProto::BCM_HUGE_SLOT) {
        const auto& sizes = HugeSlotSizes.GetResult();
        const auto findSlot = [&](ui32 payloadSize)
        {
            const ui64 partSize = groupType->MaxPartSize(crcMode, payloadSize);
            return LowerBound(
                sizes.begin(),
                sizes.end(),
                partSize + BlobHeaderSize);
        };
        const auto originalSlot = findSlot(static_cast<ui32>(blobPayload.size()));
        if (originalSlot == sizes.end()) {
            return MakeInputError(TabletId, "Blob exceeds huge-slot size grid");
        }
        if (compressedSize >= blobPayload.size() ||
            findSlot(static_cast<ui32>(compressedSize)) >= originalSlot)
        {
            return MakeError(S_ALREADY);
        }
        return compressedOrError.ExtractResult();
    }

    if (compressedSize >= blobPayload.size() || MinSavingsPercent >= 100) {
        return MakeError(S_ALREADY);
    }

    // Compute floor(payload size * percentage / 100) without overflow.
    const auto minSavings = blobPayload.size() / 100 * MinSavingsPercent +
                            blobPayload.size() % 100 * MinSavingsPercent / 100;
    if (blobPayload.size() - compressedSize <= minSavings) {
        return MakeError(S_ALREADY);
    }

    return compressedOrError.ExtractResult();
}

NProto::TError TBlobCompressor::DecompressBlob(
    const TVector<TRope>& compressedBuffers,
    const TSgList& outputBuffers,
    const NProto::TBlobCompression& compression,
    const TVector<ui16>& blockOffsets,
    TVector<ui32>* blockChecksums) const
{
    const ui32 chunkSize = compression.GetChunkSize();
    if (auto error = ValidateDecompressBlobInput(
            TabletId,
            BlockSize,
            chunkSize,
            compressedBuffers,
            outputBuffers,
            compression,
            blockOffsets);
        HasError(error))
    {
        return error;
    }

    size_t bufferIndex = 0;

    if (compressedBuffers.empty()) {
        return {};
    }

    auto codecOrError =
        GetCodec(TabletId, compression.GetCodec());
    if (HasError(codecOrError.GetError())) {
        return codecOrError.GetError();
    }
    const auto* codec = codecOrError.GetResult();

    TRope::TConstIterator iter = compressedBuffers.front().Begin();
    TRope::TConstIterator end = compressedBuffers.front().End();

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
            }

            compressedChunk.ReserveAndResize(compressedSize);
            iter.ExtractPlainDataAndAdvance(
                compressedChunk.begin(), compressedSize);

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

    return {};
}

TResultOrError<TBlobCompressor::TReadPlan> TBlobCompressor::PlanReadBlob(
    const NKikimr::TLogoBlobID& blobId,
    const NProto::TBlobCompression& compression,
    const TVector<ui16>& blockOffsets) const
{
    if (blockOffsets.empty()) {
        return TReadPlan{};
    }

    const ui32 chunkSize = compression.GetChunkSize();
    // Prefix sums translate uncompressed chunk indices to compressed offsets.
    TVector<ui64> chunkOffsets;
    chunkOffsets.reserve(compression.ChunkSizesSize() + 1);
    chunkOffsets.push_back(0);
    for (const auto size: compression.GetChunkSizes()) {
        chunkOffsets.push_back(chunkOffsets.back() + size);
    }
    if (auto error = ValidatePlanReadBlobInput(
            TabletId,
            BlockSize,
            chunkSize,
            blobId,
            compression,
            blockOffsets,
            chunkOffsets);
        HasError(error))
    {
        return error;
    }

    using TQuery = NKikimr::TEvBlobStorage::TEvGet::TQuery;

    TArrayHolder<TQuery> queries(new TQuery[blockOffsets.size()]);
    size_t queriesCount = 0;
    size_t nextChunk = 0;

    TVector<TVector<ui32>> blockOffsetsForEachRequest;

    for (const auto blockOffset: blockOffsets) {
        const ui64 blockStart = static_cast<ui64>(blockOffset) * BlockSize;
        const size_t chunk = blockStart / chunkSize;

        if (chunk < nextChunk) {
            blockOffsetsForEachRequest.back().emplace_back(blockOffset);
            continue;
        }

        const ui64 offset = chunkOffsets[chunk];
        const ui64 size = compression.GetChunkSizes(chunk);
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

    return TReadPlan{
        .Queries = std::move(queries),
        .QueriesCount = queriesCount,
        .BlockOffsetsForEachRequest = std::move(blockOffsetsForEachRequest)};
}

}   // namespace NCloud::NBlockStore::NStorage
