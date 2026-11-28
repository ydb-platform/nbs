#include "blob_compressor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/zeroes.h>

#include <library/cpp/blockcodecs/codecs.h>

#include <util/generic/algorithm.h>
#include <util/generic/cast.h>
#include <util/string/builder.h>

#include <cstring>
#include <utility>

namespace NCloud::NBlockStore::NStorage {

using namespace NProto;

namespace {

////////////////////////////////////////////////////////////////////////////////

std::optional<TBlobCompressor::TMetricsConfig> PrepareMetricsConfig(
    std::optional<TBlobCompressor::TMetricsConfig> config)
{
    if (config) {
        SortUnique(config->ChunkSizes);
    }
    return config;
}

TError ValidateDecompressionBuffers(
    ui32 blockSize,
    const TRope& compressedData,
    std::span<const ui16> blockOffsets,
    std::span<const TBlockDataRef> outputBuffers)
{
    if (outputBuffers.size() != blockOffsets.size()) {
        return MakeError(
            E_ARGUMENT,
            "Output buffer count must match block offset count");
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

    if (compressedData.empty() && !blockOffsets.empty()) {
        return MakeError(E_REJECTED, "Missing compressed buffers");
    }
    return {};
}

// Finds the chunks of uncompressed offsets visited in increasing order.
struct TChunkCursor
{
    const TBlobChunkLayout& Layout;
    ui32 Chunk = 0;
    ui64 ChunkEnd = 0;

    // Returns true if offset is not in the chunk of the previous offset.
    bool Advance(ui64 offset)
    {
        if (offset < ChunkEnd) {
            return false;
        }
        Chunk = Layout.GetChunkIndex(offset);
        ChunkEnd = Layout.GetChunkOffset(Chunk) + Layout.GetChunkLength(Chunk);
        return true;
    }
};

// Returns the next size bytes of iter, copying them into buffer only if they
// span rope segments.
TStringBuf ExtractContiguous(
    TRope::TConstIterator& iter,
    size_t size,
    TString& buffer)
{
    if (iter.ContiguousSize() >= size) {
        const TStringBuf data(iter.ContiguousData(), size);
        iter += size;
        return data;
    }

    buffer.ReserveAndResize(size);
    iter.ExtractPlainDataAndAdvance(buffer.begin(), size);
    return buffer;
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

TError ValidateCompressionMetadata(
    ui32 blockSize,
    const NProto::TBlobCompressionInfo& compression,
    ui32 blobSize,
    ui32 expectedBlockCount)
{
    const auto layout =
        TBlobChunkLayout::Create(compression, blobSize, blockSize);
    if (HasError(layout.GetError())) {
        return layout.GetError();
    }
    const ui64 blockCount =
        layout.GetResult().GetUncompressedSize() / blockSize;
    if (blockCount != expectedBlockCount) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Compression metadata covers " << blockCount
                             << " blocks, expected " << expectedBlockCount);
    }
    return {};
}

TResultOrError<TCompressedBlob> CompressBlob(
    ui32 blockSize,
    ui32 chunkSize,
    NCloud::NProto::ECompressionCodec compressionType,
    std::span<const TBlockDataRef> blobPayload)
{
    if (!blockSize) {
        return MakeError(E_ARGUMENT, "Block size must be nonzero");
    }
    if (blobPayload.empty()) {
        return MakeError(E_ARGUMENT, "Blob payload must be nonempty");
    }
    for (const auto& buffer: blobPayload) {
        if (buffer.Size() != blockSize) {
            return MakeError(
                E_ARGUMENT,
                "Input buffer size must match block size");
        }
    }
    const ui64 size = static_cast<ui64>(blobPayload.size()) * blockSize;
    if (size > Max<ui32>()) {
        return MakeError(E_ARGUMENT, "Blob size exceeds ui32");
    }

    auto data = TString::Uninitialized(size);
    SgListCopy(blobPayload, TBlockDataRef(data.data(), data.size()));

    return CompressChunks(
        compressionType,
        blockSize,
        chunkSize,
        0,   // minSavedPercent
        data,
        SplitIntoChunks({static_cast<ui32>(size)}, chunkSize));
}

TResultOrError<TValidatedBlobRead> ValidateReadBlobInput(
    ui32 blockSize,
    const NProto::TBlobCompressionInfo& compression,
    ui32 blobSize,
    std::span<const ui16> blockOffsets)
{
    auto layout = TBlobChunkLayout::Create(compression, blobSize, blockSize);
    if (HasError(layout.GetError())) {
        return layout.GetError();
    }
    if (!blockOffsets.empty() &&
        static_cast<ui64>(blockOffsets.back()) * blockSize >=
            layout.GetResult().GetUncompressedSize())
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Block offset " << blockOffsets.back()
                             << " is outside the uncompressed blob");
    }
    return TValidatedBlobRead{blockSize, layout.ExtractResult()};
}

TError DecompressBlob(
    ui32 blockSize,
    const TRope& compressedData,
    const NProto::TBlobCompressionInfo& compression,
    ui32 blobSize,
    std::span<const ui16> blockOffsets,
    std::span<const TBlockDataRef> outputBuffers,
    TVector<ui32>* blockChecksums)
{
    auto read =
        ValidateReadBlobInput(blockSize, compression, blobSize, blockOffsets);
    if (HasError(read.GetError())) {
        return read.GetError();
    }
    return DecompressBlob(
        read.GetResult(),
        compressedData,
        blockOffsets,
        outputBuffers,
        blockChecksums);
}

TError DecompressBlob(
    const TValidatedBlobRead& read,
    const TRope& compressedData,
    std::span<const ui16> blockOffsets,
    std::span<const TBlockDataRef> outputBuffers,
    TVector<ui32>* blockChecksums)
{
    if (auto error = ValidateDecompressionBuffers(
            read.BlockSize,
            compressedData,
            blockOffsets,
            outputBuffers);
        HasError(error))
    {
        return error;
    }
    const auto& layout = read.Layout;
    TRope::TConstIterator iter = compressedData.Begin();
    size_t remainingBytes = compressedData.GetSize();

    TString storedChunk;
    TString decoded;

    TChunkCursor cursor{layout};
    for (size_t i = 0; i < blockOffsets.size(); ++i) {
        const ui64 blockStart =
            static_cast<ui64>(blockOffsets[i]) * read.BlockSize;
        if (cursor.Advance(blockStart)) {
            const ui32 chunk = cursor.Chunk;
            const size_t storedSize = layout.GetStoredChunkLength(chunk);

            if (storedSize > remainingBytes) {
                return MakeError(
                    E_REJECTED,
                    TStringBuilder() << "Incomplete compressed chunk " << chunk
                                     << ": expected " << storedSize
                                     << " bytes, got " << remainingBytes);
            }

            const auto stored =
                ExtractContiguous(iter, storedSize, storedChunk);
            remainingBytes -= storedSize;

            decoded.ReserveAndResize(layout.GetChunkLength(chunk));
            auto error = DecompressChunk(
                layout,
                chunk,
                stored,
                TArrayRef<char>(decoded.begin(), decoded.size()));
            if (HasError(error)) {
                return MakeError(E_REJECTED, error.GetMessage());
            }
        }

        const char* block =
            decoded.data() + (blockStart - layout.GetChunkOffset(cursor.Chunk));

        if (blockChecksums) {
            blockChecksums->push_back(
                ComputeDefaultDigest({block, read.BlockSize}));
        }

        std::memcpy(
            const_cast<char*>(outputBuffers[i].Data()),
            block,
            read.BlockSize);
    }

    if (remainingBytes != 0) {
        return MakeError(E_REJECTED, "Unexpected compressed data after chunks");
    }
    return {};
}

TResultOrError<TReadPlan> PlanReadBlob(
    ui32 blockSize,
    const NKikimr::TLogoBlobID& blobId,
    const NProto::TBlobCompressionInfo& compression,
    std::span<const ui16> blockOffsets)
{
    if (blockOffsets.empty()) {
        return TReadPlan{};
    }

    auto read = ValidateReadBlobInput(
        blockSize,
        compression,
        blobId.BlobSize(),
        blockOffsets);
    if (HasError(read.GetError())) {
        return read.GetError();
    }
    return PlanReadBlob(read.GetResult(), blobId, blockOffsets);
}

TReadPlan PlanReadBlob(
    const TValidatedBlobRead& read,
    const NKikimr::TLogoBlobID& blobId,
    std::span<const ui16> blockOffsets)
{
    const auto& layout = read.Layout;
    using TQuery = NKikimr::TEvBlobStorage::TEvGet::TQuery;

    size_t queryCount = 0;
    {
        TChunkCursor cursor{layout};
        ui32 nextChunk = 0;
        for (const auto blockOffset: blockOffsets) {
            const ui64 blockStart =
                static_cast<ui64>(blockOffset) * read.BlockSize;
            if (cursor.Advance(blockStart)) {
                if (!queryCount || cursor.Chunk != nextChunk) {
                    ++queryCount;
                }
                nextChunk = cursor.Chunk + 1;
            }
        }
    }

    TVector<TQuery> queries;
    queries.reserve(queryCount);

    TVector<TBlockRange32> queryBlocksRanges;
    queryBlocksRanges.reserve(queryCount);

    TChunkCursor cursor{layout};
    ui32 nextChunk = 0;
    for (const auto blockOffset: blockOffsets) {
        const ui64 blockStart = static_cast<ui64>(blockOffset) * read.BlockSize;
        if (cursor.Advance(blockStart)) {
            const ui32 chunk = cursor.Chunk;
            const auto size =
                SafeIntegerCast<ui32>(layout.GetStoredChunkLength(chunk));
            if (!queries.empty() && chunk == nextChunk) {
                queries.back().Size += size;
            } else {
                queries.emplace_back().Set(
                    blobId,
                    SafeIntegerCast<ui32>(layout.GetStoredChunkOffset(chunk)),
                    size);
                queryBlocksRanges.push_back(
                    TBlockRange32::MakeOneBlock(blockOffset));
            }
            nextChunk = chunk + 1;
        }
        queryBlocksRanges.back().End = blockOffset;
    }

    return TReadPlan{
        .Queries = std::move(queries),
        .QueryBlockRanges = std::move(queryBlocksRanges)};
}

TBlobCompressor::TBlobCompressor(
    NCloud::NProto::ECompressionCodec codec,
    std::optional<TMetricsConfig> metricsConfig)
    : BlobCodec(GetCompressionCodec(codec))
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

    const ui32 blockSize = MetricsConfig->BlockSize;
    Y_ABORT_UNLESS(blockSize && blobContent.size() % blockSize == 0);
    ui64 zeroBlocks = 0;
    for (size_t offset = 0; offset < blobContent.size(); offset += blockSize) {
        zeroBlocks +=
            NCloud::IsAllZeroes(blobContent.data() + offset, blockSize);
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
            counters->Cumulative.ZeroBytesWritten.Increment(
                zeroBlocks * blockSize);
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
