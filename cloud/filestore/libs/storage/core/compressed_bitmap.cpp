#include "compressed_bitmap.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <util/generic/algorithm.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NCloud::TCompressedBitmap LoadCompressedBitmap(
    const NProtoPrivate::TCompressedBitmapData& proto,
    const ui64 minBitCount)
{
    const ui64 minBitsForBitmap = 1;
    const ui64 bitCount =
        Max<ui64>(proto.GetBitCount(), minBitCount, minBitsForBitmap);

    NCloud::TCompressedBitmap bitmap(bitCount);

    for (const auto& chunk: proto.GetChunks()) {
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

    auto serializer = bitmap.RangeSerializer(0, bitCount);

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

}   // namespace NCloud::NFileStore::NStorage
