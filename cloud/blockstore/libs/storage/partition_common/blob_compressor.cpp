#include "blob_compressor.h"

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

using namespace NProto;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCompressionCodec
{
    TCompressionMetadata::ECompressionType Type;
    TStringBuf Family;
    TStringBuf Name;
};

constexpr TCompressionCodec CompressionCodecs[] = {
    {TCompressionMetadata::COMPRESSION_LZ4_FAST14_SAFE,
     "lz4",
     "lz4-fast14-safe"},
    {TCompressionMetadata::COMPRESSION_SNAPPY, "snappy", "snappy"},
    {TCompressionMetadata::COMPRESSION_ZLIB_6, "zlib", "zlib-6"},
    {TCompressionMetadata::COMPRESSION_LZMA_5, "lzma", "lzma-5"},
    {TCompressionMetadata::COMPRESSION_BZIP2_6, "bzip2", "bzip2-6"},
    {TCompressionMetadata::COMPRESSION_FASTLZ_0, "fastlz", "fastlz-0"},
    {TCompressionMetadata::COMPRESSION_ZSTD_3, "zstd", "zstd_3"},
    {TCompressionMetadata::COMPRESSION_BROTLI_GENERIC_11,
     "brotli",
     "brotli_11"}};

std::optional<TBlobCompressor::TMetricsConfig> PrepareMetricsConfig(
    std::optional<TBlobCompressor::TMetricsConfig> config)
{
    if (config) {
        SortUnique(config->ChunkSizes);
    }
    return config;
}

TResultOrError<const NBlockCodecs::ICodec*> GetCodec(
    TCompressionMetadata::ECompressionType codec)
{
    try {
        for (const auto& entry: CompressionCodecs) {
            if (entry.Type == codec) {
                return NBlockCodecs::Codec(entry.Name);
            }
        }
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Unsupported blob compression type: "
                             << static_cast<int>(codec));
    } catch (const NBlockCodecs::TNotFound&) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Unavailable blob compression type: "
                             << static_cast<int>(codec));
    } catch (...) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to look up blob compression codec: "
                             << CurrentExceptionMessage());
    }
}

TError ValidateCompressionSettings(ui32 blockSize, ui32 chunkSize)
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

TError ValidateCompressBlobInput(
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
    const TCompressionMetadata& compression,
    ui32 lastChunkSize,
    size_t chunk)
{
    const auto& values = compression.GetPlainChunks().GetChunkSize();
    if (chunk == static_cast<size_t>(values.size())) {
        return lastChunkSize;
    }
    return values[chunk];
}

TError ValidateReadBlobInput(
    ui32 blockSize,
    ui32 chunkSize,
    const TCompressionMetadata& compression,
    ui32 blobSize,
    const TVector<ui16>& blockOffsets,
    ui32& lastChunkSize)
{
    if (auto error = ValidateCompressionSettings(blockSize, chunkSize);
        HasError(error))
    {
        return error;
    }
    if (!IsSorted(blockOffsets.begin(), blockOffsets.end())) {
        return MakeError(E_ARGUMENT, "Block offsets must be sorted");
    }
    if (!compression.HasPlainChunks()) {
        return MakeError(E_ARGUMENT, "Missing compressed chunk metadata");
    }
    const auto& values = compression.GetPlainChunks().GetChunkSize();
    ui32 previousOffset = 0;
    for (const auto value: values) {
        const ui64 offset = static_cast<ui64>(previousOffset) + value;
        if (offset <= previousOffset || offset >= blobSize) {
            return MakeError(
                E_ARGUMENT,
                "Compressed chunk sizes must be nonzero and their sum must be "
                "less than the blob size");
        }
        previousOffset = static_cast<ui32>(offset);
    }
    lastChunkSize = blobSize - previousOffset;
    for (const auto blockOffset: blockOffsets) {
        if (static_cast<ui64>(blockOffset) * blockSize / chunkSize >=
            (blobSize ? static_cast<size_t>(values.size()) + 1 : 0))
        {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Block offset " << blockOffset
                                 << " is outside the chunk list");
        }
    }
    return {};
}

TError ValidateDecompressBlobInput(
    ui32 blockSize,
    ui32 chunkSize,
    const TCompressionMetadata& compression,
    ui32 blobSize,
    const TVector<ui16>& blockOffsets,
    const TSgList& outputBuffers,
    ui32& lastChunkSize)
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
            blockOffsets,
            lastChunkSize);
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

    return {};
}

TError DecodeChunk(
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

TResultOrError<TCompressionMetadata::ECompressionType>
ParseBlobCompressionCodec(TStringBuf codecName)
{
    for (const auto& entry: CompressionCodecs) {
        if (entry.Family == codecName || entry.Name == codecName) {
            return entry.Type;
        }
    }
    return MakeError(
        E_ARGUMENT,
        TStringBuilder() << "Unsupported blob compression codec: "
                         << codecName);
}

TBlobCompressor::TBlobCompressor(
    ui32 blockSize,
    ui32 chunkSize,
    ECompressionType codec,
    std::optional<TMetricsConfig> metricsConfig)
    : CompressionType(codec)
    , BlobCodec(
          codec == TCompressionMetadata::COMPRESSION_NONE
              ? MakeError(E_ARGUMENT, "Compression type is not provided")
              : GetCodec(codec))
    , MetricsConfig(PrepareMetricsConfig(std::move(metricsConfig)))
    , BlockSize(blockSize)
    , ChunkSize(chunkSize)
{}

TBlobCompressor::TBlobCompressor(
    ui32 blockSize,
    ECompressionType codec,
    std::optional<TMetricsConfig> metricsConfig)
    : CompressionType(codec)
    , BlobCodec(
          codec == TCompressionMetadata::COMPRESSION_NONE
              ? MakeError(E_ARGUMENT, "Compression type is not provided")
              : GetCodec(codec))
    , MetricsConfig(PrepareMetricsConfig(std::move(metricsConfig)))
    , BlockSize(blockSize)
    , ChunkSize(std::nullopt)
{}

TResultOrError<TBlobCompressor::TCompressedBlob> TBlobCompressor::CompressBlob(
    const TSgList& blobPayload) const
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
    result.Compression.SetCodec(CompressionType);
    result.Compression.SetChunkSize(*ChunkSize);
    auto* chunks = result.Compression.MutablePlainChunks();

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
            return MakeError(E_ARGUMENT, "Compressed blob size exceeds ui32");
        }
        if (blockIndex + blockCount < blobPayload.size()) {
            chunks->AddChunkSize(static_cast<ui32>(compressedChunk.size()));
        }
        result.Data.append(compressedChunk);
        blockIndex += blockCount;
    }

    return result;
}

TError TBlobCompressor::DecompressBlob(
    const TVector<TRope>& compressedBuffers,
    const TCompressionMetadata& compression,
    const ui32 blobSize,
    const TVector<ui16>& blockOffsets,
    const TSgList& outputBuffers,
    TVector<ui32>* blockChecksums) const
{
    const ui32 chunkSize = compression.GetChunkSize();
    ui32 lastChunkSize = 0;
    if (auto error = ValidateDecompressBlobInput(
            BlockSize,
            chunkSize,
            compression,
            blobSize,
            blockOffsets,
            outputBuffers,
            lastChunkSize);
        HasError(error))
    {
        return error;
    }

    if (compressedBuffers.empty()) {
        if (!blockOffsets.empty()) {
            return MakeError(E_REJECTED, "Missing compressed buffers");
        }
        return {};
    }

    auto codecOrError = GetCodec(compression.GetCodec());
    if (HasError(codecOrError.GetError())) {
        return codecOrError.GetError();
    }
    const auto* codec = codecOrError.GetResult();

    TRope compressedData;
    for (const auto& buffer: compressedBuffers) {
        compressedData.Insert(compressedData.End(), TRope(buffer));
    }
    TRope::TConstIterator iter = compressedData.Begin();
    size_t remainingBytes = compressedData.GetSize();

    TString compressedChunk;
    TString decoded;

    const size_t chunkCount =
        compression.GetPlainChunks().GetChunkSize().size() + 1;
    size_t currentChunk = chunkCount;
    for (size_t i = 0; i < blockOffsets.size(); ++i) {
        const ui64 blockStart = static_cast<ui64>(blockOffsets[i]) * BlockSize;
        const size_t chunk = blockStart / chunkSize;
        if (chunk != currentChunk) {
            const size_t compressedSize =
                GetCompressedChunkSize(compression, lastChunkSize, chunk);

            if (compressedSize > remainingBytes) {
                return MakeError(
                    E_REJECTED,
                    TStringBuilder() << "Incomplete compressed chunk " << chunk
                                     << ": expected " << compressedSize
                                     << " bytes, got " << remainingBytes);
            }

            compressedChunk.ReserveAndResize(compressedSize);
            iter.ExtractPlainDataAndAdvance(
                compressedChunk.begin(),
                compressedSize);
            remainingBytes -= compressedSize;

            const bool lastChunk = chunk + 1 == chunkCount;
            auto error = DecodeChunk(
                *codec,
                compressedChunk,
                BlockSize,
                chunkSize,
                lastChunk,
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

    if (remainingBytes != 0) {
        return MakeError(E_REJECTED, "Unexpected compressed data after chunks");
    }
    return {};
}

TResultOrError<TBlobCompressor::TReadPlan> TBlobCompressor::PlanReadBlob(
    const NKikimr::TLogoBlobID& blobId,
    const TCompressionMetadata& compression,
    const TVector<ui16>& blockOffsets) const
{
    if (blockOffsets.empty()) {
        return TReadPlan{};
    }

    const ui32 chunkSize = compression.GetChunkSize();
    const ui32 blobSize = blobId.BlobSize();
    ui32 lastChunkSize = 0;
    if (auto error = ValidateReadBlobInput(
            BlockSize,
            chunkSize,
            compression,
            blobSize,
            blockOffsets,
            lastChunkSize);
        HasError(error))
    {
        return error;
    }

    using TQuery = NKikimr::TEvBlobStorage::TEvGet::TQuery;

    TArrayHolder<TQuery> queries(new TQuery[blockOffsets.size()]);
    size_t queriesCount = 0;
    size_t nextChunk = 0;
    ui64 offset = 0;

    TVector<TVector<ui32>> blockOffsetsForEachRequest;

    for (const auto blockOffset: blockOffsets) {
        const ui64 blockStart = static_cast<ui64>(blockOffset) * BlockSize;
        const size_t chunk = blockStart / chunkSize;

        if (chunk < nextChunk) {
            blockOffsetsForEachRequest.back().emplace_back(blockOffset);
            continue;
        }

        while (nextChunk < chunk) {
            offset +=
                GetCompressedChunkSize(compression, lastChunkSize, nextChunk);
            ++nextChunk;
        }
        const ui64 size =
            GetCompressedChunkSize(compression, lastChunkSize, chunk);
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
        offset += size;
        nextChunk = chunk + 1;
    }

    return TReadPlan{
        .Queries = std::move(queries),
        .QueriesCount = queriesCount,
        .BlockOffsetsForEachRequest = std::move(blockOffsetsForEachRequest)};
}

void TBlobCompressor::CalculateCompressionMetrics(
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

ui64 TBlobCompressor::EstimateCompressedSize(
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
