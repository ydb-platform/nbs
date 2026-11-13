#pragma once

#include "public.h"

#include "compressed_bitmap.h"
#include "error.h"

#include <memory>

namespace NCloud::NProto {
class TCompressedBitmapData;
}   // namespace NCloud::NProto

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// Use for logging or measuring serialized bitmap payloads without dumping them.
// Returns the sum of chunk data sizes in bytes, excluding protobuf metadata.
// Does not validate the bitmap or decode its chunks.
ui64 GetCompressedBitmapChunkDataSize(
    const NProto::TCompressedBitmapData& bitmap);

// Restore a bitmap from its serialized chunks after ValidateCompressedBitmapData.
// Allocate for at least max(proto.BitCount, minBitCount, 1) bits; omitted chunks
// remain zero. Returns E_ARGUMENT if minBitCount exceeds maxBitCount or validation
// fails. maxBitCount limits the logical bit count, not allocation granularity.
TResultOrError<std::unique_ptr<TCompressedBitmap>> LoadCompressedBitmap(
    const NProto::TCompressedBitmapData& proto,
    ui64 minBitCount,
    ui64 maxBitCount);

// Replace proto with a sparse serialization of bitmap: record bitCount and emit
// nonzero chunks from [0, min(bitCount, bitmap.Capacity())). A zero bitCount clears
// all chunks. bitCount is the logical size; it is not derived from bitmap capacity.
void SaveCompressedBitmap(
    const TCompressedBitmap& bitmap,
    ui64 bitCount,
    NProto::TCompressedBitmapData& proto);

// Combine serialized state with an existing bitmap by OR-ing each chunk into it.
// Preserves existing set bits; omitted chunks leave the bitmap unchanged.
// Does not validate proto or resize bitmap, and ignores proto.BitCount.
// The caller must ensure chunk payloads are valid and indices fit within capacity.
void MergeCompressedBitmap(
    TCompressedBitmap& bitmap,
    const NProto::TCompressedBitmapData& proto);

// Check that BitCount does not exceed maxBitCount, the number of chunks and their
// indices fit within BitCount, and every chunk has a nonempty payload.
// Returns E_ARGUMENT on failure, or success otherwise. Does not decode payloads
// or check for duplicate chunk indices; this is not full payload validation.
NProto::TError ValidateCompressedBitmapData(
    const NProto::TCompressedBitmapData& bitmap,
    ui64 maxBitCount);

}   // namespace NCloud
