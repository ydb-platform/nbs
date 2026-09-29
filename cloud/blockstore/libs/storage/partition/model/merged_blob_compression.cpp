#include "merged_blob_compression.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>

#include <contrib/libs/lz4/lz4.h>

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

NProto::TError InvalidFormat(TStringBuf reason)
{
    return MakeError(E_IO, TString("Invalid compressed Merged blob: ") + reason);
}

}   // namespace

NProto::TError ValidateMergedBlobCompression(
    const NProto::TBlobCompression& c,
    ui32 physicalBytes,
    ui32 blockSize)
{
    if (c.GetVersion() != MergedBlobCompressionVersion ||
        c.GetCodec() != MergedBlobCompressionLz4 ||
        c.GetChunkSize() != MergedBlobCompressionChunkSize)
    {
        return InvalidFormat("unsupported version, codec or chunk size");
    }
    if (!blockSize || !c.GetLogicalSize() ||
        c.GetLogicalSize() > MaxMergedBlobLogicalBytes ||
        c.GetLogicalSize() % blockSize ||
        c.GetBlockSize() != blockSize)
    {
        return InvalidFormat("invalid logical size or block size");
    }

    const ui32 count =
        (c.GetLogicalSize() - 1) / MergedBlobCompressionChunkSize + 1;
    if (c.ChunkSizesSize() != count || c.ChunkChecksumsSize() != count) {
        return InvalidFormat("invalid chunk table size");
    }

    ui64 total = 0;
    for (ui32 i = 0; i < count; ++i) {
        const ui32 logicalSize = Min(
            MergedBlobCompressionChunkSize,
            c.GetLogicalSize() - i * MergedBlobCompressionChunkSize);
        const ui32 size = c.GetChunkSizes(i);
        if (!size || size > ui32(LZ4_compressBound(logicalSize))) {
            return InvalidFormat("invalid compressed chunk size");
        }
        total += size;
    }
    if (total != physicalBytes || physicalBytes >= c.GetLogicalSize()) {
        return InvalidFormat("physical size mismatch");
    }
    return {};
}

NProto::TError CompressMergedBlob(
    TStringBuf raw,
    ui32 blockSize,
    ui32 minSavingsPercentage,
    const NProto::TBlobMeta& blobMeta,
    TCompressedMergedBlob& result)
{
    result = {};
    if (!blockSize || raw.empty() || raw.size() > MaxMergedBlobLogicalBytes ||
        raw.size() % blockSize || minSavingsPercentage > 100 ||
        !blobMeta.HasMergedBlocks() || blobMeta.HasCompression())
    {
        return MakeError(E_ARGUMENT, "Invalid Merged blob compression input");
    }

    TCompressedMergedBlob candidate;
    auto& c = candidate.Compression;
    c.SetVersion(MergedBlobCompressionVersion);
    c.SetCodec(MergedBlobCompressionLz4);
    c.SetLogicalSize(raw.size());
    c.SetChunkSize(MergedBlobCompressionChunkSize);
    c.SetBlockSize(blockSize);

    auto buffer = TString::Uninitialized(
        LZ4_compressBound(MergedBlobCompressionChunkSize));
    for (size_t offset = 0; offset < raw.size();
         offset += MergedBlobCompressionChunkSize)
    {
        const ui32 size = Min<size_t>(
            MergedBlobCompressionChunkSize,
            raw.size() - offset);
        const int encodedSize = LZ4_compress_default(
            raw.data() + offset,
            buffer.begin(),
            size,
            buffer.size());
        if (encodedSize <= 0) {
            return MakeError(E_FAIL, "LZ4 failed to compress Merged blob");
        }
        c.AddChunkSizes(encodedSize);
        c.AddChunkChecksums(
            ComputeDefaultDigest({buffer.data(), size_t(encodedSize)}));
        candidate.Payload.append(buffer.data(), encodedSize);
    }

    // Count both durable copies, including the enclosing protobuf field's tag
    // and length. UnconfirmedBlobs is temporary and is deliberately excluded.
    auto encodedMeta = blobMeta;
    *encodedMeta.MutableCompression() = c;
    candidate.MetadataBytes = c.ByteSizeLong() +
        encodedMeta.ByteSizeLong() - blobMeta.ByteSizeLong();

    const ui64 stored = candidate.Payload.size() + candidate.MetadataBytes;
    if (stored * 100 > ui64(raw.size()) * (100 - minSavingsPercentage) ||
        candidate.Payload.size() >= raw.size())
    {
        return {};
    }

    result = std::move(candidate);
    return {};
}

NProto::TError PlanCompressedBlobRead(
    const NProto::TBlobCompression& c,
    ui32 physicalBytes,
    ui32 blockSize,
    const TVector<ui16>& blockOffsets,
    TVector<TCompressedBlobChunk>& chunks)
{
    chunks.clear();
    auto error = ValidateMergedBlobCompression(c, physicalBytes, blockSize);
    if (HasError(error)) {
        return error;
    }

    TVector<bool> needed(c.ChunkSizesSize(), false);
    for (ui32 block: blockOffsets) {
        const ui64 begin = ui64(block) * blockSize;
        const ui64 end = begin + blockSize;
        if (end > c.GetLogicalSize()) {
            return InvalidFormat("logical read outside blob");
        }
        for (ui32 i = begin / MergedBlobCompressionChunkSize;
             i <= (end - 1) / MergedBlobCompressionChunkSize;
             ++i)
        {
            needed[i] = true;
        }
    }

    ui32 offset = 0;
    for (ui32 i = 0; i < needed.size(); ++i) {
        const ui32 size = c.GetChunkSizes(i);
        if (needed[i]) {
            chunks.push_back({i, offset, size});
        }
        offset += size;
    }
    return {};
}

NProto::TError DecodeMergedBlobChunk(
    const NProto::TBlobCompression& c,
    ui32 chunkIndex,
    TStringBuf payload,
    TString& decoded)
{
    decoded.clear();
    // Also protect direct callers. Validation must not depend on a prior call
    // to PlanCompressedBlobRead.
    ui64 physicalBytes = 0;
    for (ui32 size: c.GetChunkSizes()) {
        physicalBytes += size;
    }
    if (physicalBytes > Max<ui32>()) {
        return InvalidFormat("physical size overflow");
    }
    auto error = ValidateMergedBlobCompression(
        c, physicalBytes, c.GetBlockSize());
    if (HasError(error)) {
        return error;
    }
    if (chunkIndex >= ui32(c.ChunkSizesSize()) ||
        payload.size() != c.GetChunkSizes(chunkIndex))
    {
        return InvalidFormat("truncated chunk or invalid index");
    }

    if (ComputeDefaultDigest({payload.data(), payload.size()}) !=
        c.GetChunkChecksums(chunkIndex))
    {
        return InvalidFormat("corrupted encoded chunk");
    }

    const ui32 size = Min(
        MergedBlobCompressionChunkSize,
        c.GetLogicalSize() - chunkIndex * MergedBlobCompressionChunkSize);
    auto result = TString::Uninitialized(size);
    const int decodedSize = LZ4_decompress_safe(
        payload.data(), result.begin(), payload.size(), size);
    if (decodedSize != int(size))
    {
        return InvalidFormat("corrupted chunk");
    }
    decoded = std::move(result);
    return {};
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
