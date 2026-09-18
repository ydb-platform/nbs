#include "compressed_bitmap.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace {

ui64 GetCompressedBitmapChunkCount(const ui64 bitCount)
{
    return bitCount ? ((bitCount - 1) / TCompressedBitmap::CHUNK_SIZE) + 1 : 0;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NCloud::TCompressedBitmap LoadCompressedBitmap(
    const NProtoPrivate::TCompressedBitmapData& proto,
    const ui64 minBitCount)
{
    const ui64 minBitsForBitmap = 1;
    const ui64 bitCount =
        Max<ui64>(proto.GetBitCount(), minBitCount, minBitsForBitmap);

    NCloud::TCompressedBitmap bitmap(bitCount);

    const auto serializedChunkCount =
        GetCompressedBitmapChunkCount(proto.GetBitCount());

    for (const auto& chunk: proto.GetChunks()) {
        if (chunk.GetChunkIdx() >= serializedChunkCount || !chunk.GetData()) {
            continue;
        }

        bitmap.Update({chunk.GetChunkIdx(), chunk.GetData()});
    }

    return bitmap;
}

void SaveCompressedBitmap(
    const NCloud::TCompressedBitmap& bitmap,
    const ui64 bitCount,
    NProtoPrivate::TCompressedBitmapData& proto)
{
    proto.Clear();
    proto.SetBitCount(bitCount);

    if (!bitCount) {
        return;
    }

    auto serializer =
        bitmap.RangeSerializer(0, Min(bitCount, bitmap.Capacity()));

    NCloud::TCompressedBitmap::TSerializedChunk chunk;
    while (serializer.Next(&chunk)) {
        if (NCloud::TCompressedBitmap::IsZeroChunk(chunk)) {
            continue;
        }

        auto* out = proto.AddChunks();
        out->SetChunkIdx(chunk.ChunkIdx);
        out->SetData(chunk.Data.data(), chunk.Data.size());
    }
}

NProto::TError ValidateShardCreationState(
    const NProtoPrivate::TFileSystemShardCreationState& state,
    const ui32 maxShardCount)
{
    const auto& bitmap = state.GetCreatedShardBitmap();

    if (bitmap.GetBitCount() > maxShardCount) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Compressed bitmap bit count exceeds limit: "
                             << bitmap.GetBitCount() << " > " << maxShardCount);
    }

    const auto chunkCount = GetCompressedBitmapChunkCount(bitmap.GetBitCount());

    if (static_cast<ui64>(bitmap.ChunksSize()) > chunkCount) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Compressed bitmap chunk count exceeds limit: "
                             << bitmap.ChunksSize() << " > " << chunkCount);
    }

    ui32 chunkIndex = 0;
    for (const auto& chunk: bitmap.GetChunks()) {
        if (chunk.GetChunkIdx() >= chunkCount) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Compressed bitmap chunk index is out of range: "
                    << chunk.GetChunkIdx() << " >= " << chunkCount);
        }

        if (!chunk.GetData()) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Compressed bitmap chunk data is empty: " << chunkIndex);
        }

        ++chunkIndex;
    }

    return {};
}

}   // namespace NCloud::NFileStore::NStorage
