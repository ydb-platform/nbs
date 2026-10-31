#include "blobs_compressor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>

#include <cloud/storage/core/libs/common/format.h>

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

std::optional<TBlobsCompressor::TMetricsConfig> PrepareMetricsConfig(
    std::optional<TBlobsCompressor::TMetricsConfig> config)
{
    if (config) {
        SortUnique(config->ChunkSizes);
    }
    return config;
}

TResultOrError<const NBlockCodecs::ICodec*> GetCodec(TStringBuf codecName)
{
    try {
        return NBlockCodecs::Codec(codecName);
    } catch (const NBlockCodecs::TNotFound&) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Unsupported blob compression codec: " << codecName);
    } catch (...) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to look up blob compression codec: "
                             << CurrentExceptionMessage());
    }
}

NProto::TError ValidateCompressionSettings(ui32 blockSize, ui32 chunkSize)
{
    if (!blockSize) {
        return MakeError(E_ARGUMENT, "Block size must be nonzero");
    }
    if (chunkSize < blockSize || chunkSize % blockSize != 0) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Invalid chunk size " << chunkSize
                             << " for block size " << blockSize);
    }
    return {};
}

NProto::TError ValidateCompressBlobInput(
    ui32 blockSize,
    const std::optional<ui32>& chunkSize,
    const TSgList& blobPayload)
{
    if (!chunkSize) {
        return MakeError(E_ARGUMENT, "Missing blob compression settings");
    }
    if (auto error = ValidateCompressionSettings(blockSize, *chunkSize);
        HasError(error))
    {
        return error;
    }
    for (const auto& buffer: blobPayload) {
        if (buffer.Size() != blockSize) {
            return MakeError(
                E_ARGUMENT,
                "Input buffer size must match block size");
        }
    }
    return {};
}

ui32 GetCompressedChunkSize(
    const NProto::TCompressionMetadata& compression,
    ui32 blobSize,
    size_t chunk)
{
    const auto& chunks = compression.GetPlainChuncks();
    const ui32 end = chunk < chunks.ChunkOffsetSize()
                         ? chunks.GetChunkOffset(chunk)
                         : blobSize;
    return end - (chunk ? chunks.GetChunkOffset(chunk - 1) : 0);
}

NProto::TError ValidateReadBlobInput(
    ui32 blockSize,
    ui32 chunkSize,
    const NProto::TCompressionMetadata& compression,
    ui32 blobSize,
    const TVector<ui16>& blockOffsets)
{
    if (auto error = ValidateCompressionSettings(blockSize, chunkSize);
        HasError(error))
    {
        return error;
    }
    if (!IsSorted(blockOffsets.begin(), blockOffsets.end())) {
        return MakeError(E_ARGUMENT, "Block offsets must be sorted");
    }
    if (!compression.HasPlainChuncks()) {
        return MakeError(E_ARGUMENT, "Missing plain chunk offsets");
    }
    ui32 previousOffset = 0;
    for (const auto offset: compression.GetPlainChuncks().GetChunkOffset()) {
        if (offset <= previousOffset || offset >= blobSize) {
            return MakeError(
                E_ARGUMENT,
                "Compressed chunk offsets must strictly increase inside the "
                "blob");
        }
        previousOffset = offset;
    }
    for (const auto blockOffset: blockOffsets) {
        if (static_cast<ui64>(blockOffset) * blockSize / chunkSize >=
            (blobSize ? compression.GetPlainChuncks().ChunkOffsetSize() + 1
                      : 0))
        {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Block offset " << blockOffset
                                 << " is outside the chunk list");
        }
    }
    return {};
}

NProto::TError ValidateDecompressBlobInput(
    ui32 blockSize,
    ui32 chunkSize,
    const TVector<TRope>& compressedBuffers,
    const TSgList& outputBuffers,
    const NProto::TCompressionMetadata& compression,
    ui32 blobSize,
    const TVector<ui16>& blockOffsets)
{
    if (outputBuffers.size() != blockOffsets.size()) {
        return MakeError(
            E_ARGUMENT,
            "Output buffer count must match block offset count");
    }
    if (auto error = ValidateReadBlobInput(
            blockSize,
            chunkSize,
            compression,
            blobSize,
            blockOffsets);
        HasError(error))
    {
        return error;
    }
    for (const auto& buffer: outputBuffers) {
        if (!buffer.Data()) {
            return MakeError(E_ARGUMENT, "Output buffer must be non-null");
        }
        if (buffer.Size() != blockSize) {
            return MakeError(
                E_ARGUMENT,
                "Output buffer size must match block size");
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
    size_t currentChunk = compression.GetPlainChuncks().ChunkOffsetSize() + 1;
    for (const auto blockOffset: blockOffsets) {
        const size_t chunk =
            static_cast<ui64>(blockOffset) * blockSize / chunkSize;
        if (chunk == currentChunk) {
            continue;
        }
        const size_t compressedSize =
            GetCompressedChunkSize(compression, blobSize, chunk);
        while (!remainingBytes && bufferIndex < compressedBuffers.size() - 1) {
            remainingBytes = compressedBuffers[++bufferIndex].GetSize();
        }
        if (compressedSize > remainingBytes) {
            return MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "Incomplete compressed chunk " << chunk << ": expected "
                    << compressedSize << " bytes, got " << remainingBytes);
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
            TStringBuilder()
                << "Failed to decompress chunk: " << CurrentExceptionMessage());
    }
    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBlobsCompressor::TBlobsCompressor(
    ui32 blockSize,
    ui32 chunkSize,
    TStringBuf codecName,
    std::optional<TMetricsConfig> metricsConfig)
    : BlobCodec(GetCodec(codecName))
    , MetricsConfig(PrepareMetricsConfig(std::move(metricsConfig)))
    , BlockSize(blockSize)
    , ChunkSize(chunkSize)
{}

TBlobsCompressor::TBlobsCompressor(
    ui32 blockSize,
    TStringBuf codecName,
    std::optional<TMetricsConfig> metricsConfig)
    : BlobCodec(GetCodec(codecName))
    , MetricsConfig(PrepareMetricsConfig(std::move(metricsConfig)))
    , BlockSize(blockSize)
    , ChunkSize(std::nullopt)
{}

TResultOrError<TBlobsCompressor::TCompressedBlob>
TBlobsCompressor::CompressBlob(const TSgList& blobPayload) const
{
    if (auto error =
            ValidateCompressBlobInput(BlockSize, ChunkSize, blobPayload);
        HasError(error))
    {
        return error;
    }
    if (HasError(BlobCodec.GetError())) {
        return BlobCodec.GetError();
    }
    const auto* codec = BlobCodec.GetResult();

    TCompressedBlob result;
    result.Compression.SetCodec(TString(codec->Name()));
    result.Compression.SetChunkSize(*ChunkSize);
    auto* chunks = result.Compression.MutablePlainChuncks();

    const size_t blocksPerChunk = *ChunkSize / BlockSize;
    TString chunk;
    TString compressedChunk;
    for (size_t blockIndex = 0; blockIndex < blobPayload.size();) {
        const size_t blockCount =
            Min(blocksPerChunk, blobPayload.size() - blockIndex);
        chunk.ReserveAndResize(blockCount * BlockSize);
        char* dest = chunk.begin();
        for (size_t i = 0; i < blockCount; ++i) {
            const auto& block = blobPayload[blockIndex + i];
            if (block.Data()) {
                memcpy(dest, block.Data(), BlockSize);
            } else {
                memset(dest, 0, BlockSize);
            }
            dest += BlockSize;
        }
        try {
            codec->Encode(chunk, compressedChunk);
        } catch (...) {
            return MakeError(
                E_FAIL,
                TStringBuilder() << "Failed to compress chunk: "
                                 << CurrentExceptionMessage());
        }
        if (compressedChunk.size() > Max<ui32>() - result.Data.size()) {
            return MakeError(E_FAIL, "Compressed blob size exceeds ui32");
        }
        if (blockIndex) {
            chunks->AddChunkOffset(static_cast<ui32>(result.Data.size()));
        }
        result.Data.append(compressedChunk);
        blockIndex += blockCount;
    }

    return result;
}

NProto::TError TBlobsCompressor::DecompressBlob(
    const TVector<TRope>& compressedBuffers,
    const TSgList& outputBuffers,
    const NProto::TCompressionMetadata& compression,
    ui32 blobSize,
    const TVector<ui16>& blockOffsets,
    TVector<ui32>* blockChecksums) const
{
    const ui32 chunkSize = compression.GetChunkSize();
    if (auto error = ValidateDecompressBlobInput(
            BlockSize,
            chunkSize,
            compressedBuffers,
            outputBuffers,
            compression,
            blobSize,
            blockOffsets);
        HasError(error))
    {
        return error;
    }

    size_t bufferIndex = 0;

    if (compressedBuffers.empty()) {
        return {};
    }

    auto codecOrError = GetCodec(compression.GetCodec());
    if (HasError(codecOrError.GetError())) {
        return codecOrError.GetError();
    }
    const auto* codec = codecOrError.GetResult();

    TRope::TConstIterator iter = compressedBuffers.front().Begin();
    TRope::TConstIterator end = compressedBuffers.front().End();

    TString compressedChunk;
    TString decoded;

    size_t currentChunk = compression.GetPlainChuncks().ChunkOffsetSize() + 1;
    for (size_t i = 0; i < blockOffsets.size(); ++i) {
        const ui64 blockStart = static_cast<ui64>(blockOffsets[i]) * BlockSize;
        const size_t chunk = blockStart / chunkSize;
        if (chunk != currentChunk) {
            const size_t compressedSize =
                GetCompressedChunkSize(compression, blobSize, chunk);

            while (iter == end && bufferIndex < compressedBuffers.size() - 1) {
                ++bufferIndex;
                iter = compressedBuffers[bufferIndex].Begin();
                end = compressedBuffers[bufferIndex].End();
            }

            compressedChunk.ReserveAndResize(compressedSize);
            iter.ExtractPlainDataAndAdvance(
                compressedChunk.begin(),
                compressedSize);

            auto error = DecodeChunk(
                *codec,
                compressedChunk,
                BlockSize,
                chunkSize,
                chunk == compression.GetPlainChuncks().ChunkOffsetSize(),
                decoded);
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

TResultOrError<TBlobsCompressor::TReadPlan> TBlobsCompressor::PlanReadBlob(
    const NKikimr::TLogoBlobID& blobId,
    const NProto::TCompressionMetadata& compression,
    const TVector<ui16>& blockOffsets) const
{
    if (blockOffsets.empty()) {
        return TReadPlan{};
    }

    const ui32 chunkSize = compression.GetChunkSize();
    const ui32 blobSize = blobId.BlobSize();
    if (auto error = ValidateReadBlobInput(
            BlockSize,
            chunkSize,
            compression,
            blobSize,
            blockOffsets);
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

        const ui64 offset =
            chunk ? compression.GetPlainChuncks().GetChunkOffset(chunk - 1) : 0;
        const ui64 size = GetCompressedChunkSize(compression, blobSize, chunk);
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

void TBlobsCompressor::CalculateCompressionMetrics(
    const TPartialBlobId& blobId,
    TStringBuf blobContent) const
{
    if (!MetricsConfig || !MetricsConfig->CompressionRate ||
        HasError(BlobCodec.GetError()) ||
        blobId.GetHash() % MetricsConfig->CompressionRate != 0)
    {
        return;
    }

    const auto size = EstimateCompressedSize(blobContent, blobContent.size());
    MetricsConfig->PartCounters->Access(
        [&](auto& counters)
        {
            counters->Cumulative.UncompressedBytesWritten.Increment(
                blobContent.size());
            counters->Cumulative.CompressedBytesWritten.Increment(size);
        });
    for (const ui64 chunkSize: MetricsConfig->ChunkSizes) {
        const auto size = EstimateCompressedSize(
            blobContent,
            chunkSize ? chunkSize : blobContent.size());
        MetricsConfig->PartCounters->Access(
            [&](auto& counters)
            {
                auto* compression =
                    counters->BlobCompression.FindPtr(chunkSize);
                if (!compression) {
                    ReportCompressionChunkNotFound(
                        TStringBuilder() << "Compression counters not "
                                            "found for chunk size "
                                         << FormatByteSize(chunkSize));
                    return;
                }
                compression->UncompressedBytesWritten.Increment(
                    blobContent.size());
                compression->CompressedBytesWritten.Increment(size);
            });
    }
}

ui64 TBlobsCompressor::EstimateCompressedSize(
    TStringBuf blobContent,
    ui64 chunkSize) const
{
    Y_ABORT_UNLESS(chunkSize > 0);
    const ui64 uncompressedSize = blobContent.size();

    TString out;
    ui64 compressedSize = 0;
    while (blobContent) {
        const auto chunk =
            blobContent.SubStr(0, Min<ui64>(chunkSize, blobContent.size()));
        out.ReserveAndResize(BlobCodec.GetResult()->MaxCompressedLength(chunk));
        compressedSize += BlobCodec.GetResult()->Compress(chunk, out.begin());
        blobContent.Skip(chunk.size());
    }
    return Min(compressedSize, uncompressedSize);
}

}   // namespace NCloud::NBlockStore::NStorage
