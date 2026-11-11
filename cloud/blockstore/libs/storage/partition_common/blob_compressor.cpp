#include "blob_compressor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>

#include <cloud/storage/core/libs/common/format.h>

#include <library/cpp/blockcodecs/codecs.h>

#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <algorithm>
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
    {TCompressionMetadata::COMPRESSION_ZSTD_3, "zstd", "zstd_3"}};

static_assert(
    TCompressionMetadata::ECompressionType_ARRAYSIZE - 1 ==
    sizeof(CompressionCodecs) / sizeof(CompressionCodecs[0]));

constexpr bool IsValidCodecList(const TCompressionCodec* codecList, size_t size)
{
    for (size_t i = 0; i < size; ++i) {
        if (codecList[i].Type !=
            static_cast<TCompressionMetadata::ECompressionType>(i + 1))
        {
            return false;
        }
    }
    return true;
}

static_assert(IsValidCodecList(
    CompressionCodecs,
    TCompressionMetadata::ECompressionType_ARRAYSIZE - 1));

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
        if (codec < TCompressionMetadata::ECompressionType_MIN ||
            codec > TCompressionMetadata::ECompressionType_MAX ||
            codec == TCompressionMetadata::COMPRESSION_NONE)
        {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Unsupported blob compression type: "
                                 << static_cast<int>(codec));
        }
        const auto& entry = CompressionCodecs[static_cast<size_t>(codec) - 1];

        return NBlockCodecs::Codec(entry.Name);
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
    ui32 chunkSize,
    const TSgList& blobPayload)
{
    if (auto error = ValidateCompressionSettings(blockSize, chunkSize);
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

bool IsUncompressedChunk(const TCompressionMetadata& compression, size_t chunk)
{
    return compression.GetPlainChunks().GetCompressedChunkSize(chunk) == 0;
}

ui32 GetStoredChunkSize(
    const TCompressionMetadata& compression,
    ui32 lastChunkSize,
    size_t chunk)
{
    const auto& values = compression.GetPlainChunks().GetCompressedChunkSize();
    if (values[chunk]) {
        return values[chunk];
    }
    return chunk + 1 == static_cast<size_t>(values.size())
               ? lastChunkSize
               : compression.GetChunkSize();
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
    if (!compression.HasPlainChunks()) {
        return MakeError(E_ARGUMENT, "Missing compressed chunk metadata");
    }
    const auto& sizes = compression.GetPlainChunks().GetCompressedChunkSize();
    const size_t chunkCount = sizes.size();
    ui32 offset = 0;
    lastChunkSize = 0;
    for (size_t chunk = 0; chunk < chunkCount; ++chunk) {
        const bool lastChunk = chunk + 1 == chunkCount;
        const bool uncompressed = IsUncompressedChunk(compression, chunk);
        if (lastChunk) {
            lastChunkSize = blobSize - offset;
        }
        const ui32 size = GetStoredChunkSize(compression, lastChunkSize, chunk);
        const ui64 end = static_cast<ui64>(offset) + size;
        if (!size || end > blobSize) {
            return MakeError(
                E_ARGUMENT,
                "Chunk sizes do not match the blob size");
        }
        if (uncompressed && (size > chunkSize || size % blockSize)) {
            return MakeError(E_ARGUMENT, "Invalid uncompressed chunk size");
        }
        offset = static_cast<ui32>(end);
    }
    if (offset != blobSize) {
        return MakeError(E_ARGUMENT, "Missing chunk sizes");
    }
    for (const auto blockOffset: blockOffsets) {
        const ui64 blockStart = static_cast<ui64>(blockOffset) * blockSize;
        const size_t chunk = blockStart / chunkSize;
        if (chunk >= chunkCount ||
            (chunk + 1 == chunkCount &&
             IsUncompressedChunk(compression, chunk) &&
             blockStart % chunkSize + blockSize > lastChunkSize))
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

ui64 EstimateCompressedSize(
    const NBlockCodecs::ICodec& codec,
    TStringBuf blobContent,
    ui64 chunkSize)
{
    Y_ABORT_UNLESS(chunkSize > 0);
    const ui64 uncompressedSize = blobContent.size();

    TString out;
    ui64 compressedSize = 0;
    while (blobContent) {
        const auto chunk =
            blobContent.SubStr(0, Min<ui64>(chunkSize, blobContent.size()));
        out.ReserveAndResize(codec.MaxCompressedLength(chunk));
        compressedSize += codec.Compress(chunk, out.begin());
        blobContent.Skip(chunk.size());
    }
    return Min(compressedSize, uncompressedSize);
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

TResultOrError<TCompressedBlob> CompressBlob(
    ui32 blockSize,
    ui32 chunkSize,
    TCompressionMetadata::ECompressionType compressionType,
    const TSgList& blobPayload)
{
    if (auto error =
            ValidateCompressBlobInput(blockSize, chunkSize, blobPayload);
        HasError(error))
    {
        return error;
    }
    auto codecOrError = GetCodec(compressionType);
    if (HasError(codecOrError.GetError())) {
        return codecOrError.GetError();
    }
    const auto* codec = codecOrError.GetResult();

    TCompressedBlob result;
    result.Compression.SetCodec(compressionType);
    result.Compression.SetChunkSize(chunkSize);
    auto* chunks = result.Compression.MutablePlainChunks();

    const size_t blocksPerChunk = chunkSize / blockSize;
    TString chunk;
    TString compressedChunk;
    for (size_t blockIndex = 0; blockIndex < blobPayload.size();) {
        const size_t blockCount =
            Min(blocksPerChunk, blobPayload.size() - blockIndex);

        auto subList = std::span<const TBlockDataRef>(
            &blobPayload[blockIndex],
            blockCount);

        chunk.ReserveAndResize(blockCount * blockSize);
        SgListCopy(subList, TBlockDataRef(chunk.data(), chunk.size()));

        try {
            codec->Encode(chunk, compressedChunk);
        } catch (...) {
            return MakeError(
                E_FAIL,
                TStringBuilder() << "Failed to compress chunk: "
                                 << CurrentExceptionMessage());
        }
        const bool uncompressed = compressedChunk.size() >= chunk.size();
        const TStringBuf storedChunk = uncompressed ? chunk : compressedChunk;
        if (storedChunk.size() > Max<ui32>() - result.Data.size()) {
            return MakeError(E_ARGUMENT, "Compressed blob size exceeds ui32");
        }
        chunks->AddCompressedChunkSize(
            uncompressed ? 0 : static_cast<ui32>(storedChunk.size()));
        result.Data.append(storedChunk);
        blockIndex += blockCount;
    }

    return result;
}

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDecompressChunkIter
{
private:
    const ui64 BlockSize;
    const ui32 ChunkSize;
    const TCompressionMetadata& Compression;
    const NBlockCodecs::ICodec& Codec;
    const size_t ChunkCount;

    TRope::TConstIterator Iter;
    size_t RemainingBytes;
    TString CompressedChunk;
    TString Decoded;
    size_t CurrentChunk;

    TVector<ui32>* BlockChecksums;

public:
    TDecompressChunkIter(
        ui64 blockSize,
        const TCompressionMetadata& compression,
        const NBlockCodecs::ICodec& codec,
        const TRope& compressedData,
        TVector<ui32>* blockChecksums)
        : BlockSize(blockSize)
        , ChunkSize(compression.GetChunkSize())
        , Compression(compression)
        , Codec(codec)
        , ChunkCount(compression.GetPlainChunks().CompressedChunkSizeSize())
        , Iter(compressedData.Begin())
        , RemainingBytes(compressedData.GetSize())
        , CurrentChunk(ChunkCount)
        , BlockChecksums(blockChecksums)
    {}

    NProto::TError OnBlockOffset(ui16 blockOffset, TBlockDataRef output)
    {
        const ui64 blockStart = static_cast<ui64>(blockOffset) * BlockSize;
        const size_t chunk = blockStart / ChunkSize;

        if (IsUncompressedChunk(Compression, chunk)) {
            return OnUncompressedBlock(output);
        }

        if (chunk != CurrentChunk) {
            auto error = ExtractAndDecompressNextChunk(chunk);
            if (HasError(error)) {
                return error;
            }
        }

        const size_t offset = blockStart % ChunkSize;

        if (offset + BlockSize > Decoded.size()) {
            return MakeError(
                E_REJECTED,
                "Requested block is outside the decompressed chunk");
        }

        if (BlockChecksums) {
            BlockChecksums->push_back(
                ComputeDefaultDigest({Decoded.data() + offset, BlockSize}));
        }

        std::memcpy(
            const_cast<char*>(output.Data()),
            Decoded.data() + offset,
            BlockSize);

        return {};
    }

    NProto::TError OnUncompressedBlock(TBlockDataRef output)
    {
        if (RemainingBytes < BlockSize) {
            return MakeError(E_REJECTED, "Incomplete uncompressed block");
        }
        if (BlockChecksums) {
            Decoded.ReserveAndResize(BlockSize);
        }

        auto* bufferToExtract =
            BlockChecksums ? &Decoded[0] : const_cast<char*>(output.Data());

        Iter.ExtractPlainDataAndAdvance(bufferToExtract, BlockSize);
        RemainingBytes -= BlockSize;

        if (BlockChecksums) {
            BlockChecksums->push_back(
                ComputeDefaultDigest({bufferToExtract, BlockSize}));
            Y_ABORT_UNLESS(bufferToExtract != output.Data());
            std::memcpy(
                const_cast<char*>(output.Data()),
                bufferToExtract,
                BlockSize);
        }

        return {};
    }

    NProto::TError ExtractAndDecompressNextChunk(size_t chunk)
    {
        const size_t compressedSize =
            Compression.GetPlainChunks().GetCompressedChunkSize(chunk);

        if (compressedSize > RemainingBytes) {
            return MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "Incomplete compressed chunk " << chunk << ": expected "
                    << compressedSize << " bytes, got " << RemainingBytes);
        }

        CompressedChunk.ReserveAndResize(compressedSize);
        Iter.ExtractPlainDataAndAdvance(
            CompressedChunk.begin(),
            compressedSize);
        RemainingBytes -= compressedSize;

        const bool lastChunk = chunk + 1 == ChunkCount;
        auto error = DecodeChunk(
            Codec,
            CompressedChunk,
            BlockSize,
            ChunkSize,
            lastChunk,
            Decoded);
        if (HasError(error)) {
            return error;
        }
        CurrentChunk = chunk;

        return {};
    }

    size_t GetRemainingBytes() const
    {
        return RemainingBytes;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TError DecompressBlob(
    ui32 blockSize,
    const TVector<TRope>& compressedBuffers,
    const TCompressionMetadata& compression,
    ui32 blobSize,
    const TVector<ui16>& blockOffsets,
    const TSgList& outputBuffers,
    TVector<ui32>* blockChecksums)
{
    const ui32 chunkSize = compression.GetChunkSize();
    ui32 lastChunkSize = 0;
    if (auto error = ValidateDecompressBlobInput(
            blockSize,
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

    TDecompressChunkIter iter(
        blockSize,
        compression,
        *codec,
        compressedData,
        blockChecksums);

    for (size_t i = 0; i < blockOffsets.size(); ++i) {
        auto error = iter.OnBlockOffset(blockOffsets[i], outputBuffers[i]);
        if (HasError(error)) {
            return error;
        }
    }

    if (iter.GetRemainingBytes() != 0) {
        return MakeError(E_REJECTED, "Unexpected compressed data after chunks");
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TReadPlan> PlanReadBlob(
    ui32 blockSize,
    const NKikimr::TLogoBlobID& blobId,
    const TCompressionMetadata& compression,
    const TVector<ui16>& blockOffsets)
{
    if (blockOffsets.empty()) {
        return TReadPlan{};
    }

    const ui32 chunkSize = compression.GetChunkSize();
    const ui32 blobSize = blobId.BlobSize();
    ui32 lastChunkSize = 0;
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

    using TQuery = NKikimr::TEvBlobStorage::TEvGet::TQuery;

    TVector<TQuery> queries;
    TVector<TVector<ui16>> blockOffsetsForEachRequest;
    std::optional<size_t> lastCompressedChunk;
    size_t nextChunk = 0;
    ui32 chunkOffset = 0;

    for (const auto blockOffset: blockOffsets) {
        const ui64 blockStart = static_cast<ui64>(blockOffset) * blockSize;
        const size_t chunk = blockStart / chunkSize;
        while (nextChunk < chunk) {
            chunkOffset +=
                GetStoredChunkSize(compression, lastChunkSize, nextChunk);
            ++nextChunk;
        }
        const bool uncompressed = IsUncompressedChunk(compression, chunk);
        if (!uncompressed && lastCompressedChunk == chunk) {
            blockOffsetsForEachRequest.back().emplace_back(blockOffset);
            continue;
        }
        const ui32 offset =
            chunkOffset + (uncompressed ? blockStart % chunkSize : 0);
        const ui32 size =
            uncompressed
                ? blockSize
                : GetStoredChunkSize(compression, lastChunkSize, chunk);
        auto* previousQuery = queries.empty() ? nullptr : &queries.back();
        if (previousQuery &&
            static_cast<ui64>(previousQuery->Shift) + previousQuery->Size ==
                offset)
        {
            previousQuery->Size += size;
        } else {
            queries.emplace_back().Set(blobId, offset, size);
            blockOffsetsForEachRequest.emplace_back();
        }
        blockOffsetsForEachRequest.back().emplace_back(blockOffset);
        if (!uncompressed) {
            lastCompressedChunk = chunk;
        }
    }

    TArrayHolder<TQuery> queryArray(new TQuery[queries.size()]);
    std::ranges::move(queries, queryArray.Get());

    return TReadPlan{
        .Queries = std::move(queryArray),
        .QueriesCount = queries.size(),
        .BlockOffsetsForEachRequest = std::move(blockOffsetsForEachRequest)};
}

////////////////////////////////////////////////////////////////////////////////

TBlobCompressor::TBlobCompressor(
    TCompressionMetadata::ECompressionType codec,
    std::optional<TMetricsConfig> metricsConfig)
    : BlobCodec(GetCodec(codec))
    , MetricsConfig(PrepareMetricsConfig(std::move(metricsConfig)))
{}

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

    const auto size = EstimateCompressedSize(
        *BlobCodec.GetResult(),
        blobContent,
        blobContent.size());
    MetricsConfig->PartCounters->Access(
        [&](auto& counters)
        {
            counters->Cumulative.UncompressedBytesWritten.Increment(
                blobContent.size());
            counters->Cumulative.CompressedBytesWritten.Increment(size);
        });
    for (const ui64 chunkSize: MetricsConfig->ChunkSizes) {
        const auto size = EstimateCompressedSize(
            *BlobCodec.GetResult(),
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

}   // namespace NCloud::NBlockStore::NStorage
