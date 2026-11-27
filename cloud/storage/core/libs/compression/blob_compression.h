#pragma once

#include "codec.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/compression.pb.h>

#include <util/generic/array_ref.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

bool IsWorthStoringCompressed(
    ui64 compressedSize,
    ui64 size,
    ui32 minSavedPercent);

TVector<ui32> SplitIntoChunks(TConstArrayRef<ui32> runSizes, ui32 chunkSize);

struct TCompressedBlob
{
    TString Content;
    NProto::TBlobCompressionInfo Info;
};

// Compresses data chunk by chunk. chunkSize is a multiple of blockSize.
// chunkSizes are the uncompressed chunk sizes: each one is a nonzero multiple
// of blockSize and at most chunkSize, their sum is data.size().
// A chunk which is not worth storing compressed is stored raw.
// Invalid arguments return E_ARGUMENT, codec failures return E_FAIL.
TResultOrError<TCompressedBlob> CompressChunks(
    NProto::ECompressionCodec codec,
    ui32 blockSize,
    ui32 chunkSize,
    ui32 minSavedPercent,
    TStringBuf data,
    TConstArrayRef<ui32> chunkSizes);

////////////////////////////////////////////////////////////////////////////////

// Offsets of the chunks of a compressed blob.
class TBlobChunkLayout
{
private:
    // Codec of the chunks which are not raw.
    const NBlockCodecs::ICodec* Codec = nullptr;
    // Offsets[i] is the offset of chunk i in the uncompressed blob,
    // Offsets.back() is the uncompressed blob size.
    TVector<ui64> Offsets;
    // StoredOffsets[i] is the offset of chunk i in the stored blob,
    // StoredOffsets.back() is the stored blob size.
    TVector<ui64> StoredOffsets;
    // RawChunks[i] is set if chunk i is stored uncompressed.
    TVector<bool> RawChunks;

public:
    static TResultOrError<TBlobChunkLayout> Create(
        const NProto::TBlobCompressionInfo& info,
        ui64 blobSize,
        ui32 blockSize);

    const NBlockCodecs::ICodec* GetCodec() const;
    ui32 GetChunkCount() const;
    ui64 GetUncompressedSize() const;

    // offset < GetUncompressedSize()
    ui32 GetChunkIndex(ui64 offset) const;

    ui64 GetChunkOffset(ui32 chunk) const;
    ui64 GetChunkLength(ui32 chunk) const;
    ui64 GetSpanLength(ui32 firstChunk, ui32 lastChunk) const;
    ui64 GetStoredChunkOffset(ui32 chunk) const;
    ui64 GetStoredChunkLength(ui32 chunk) const;
    ui64 GetStoredSpanLength(ui32 firstChunk, ui32 lastChunk) const;
    bool IsRawChunk(ui32 chunk) const;
};

////////////////////////////////////////////////////////////////////////////////

// out.size() is layout.GetChunkLength(chunk).
NProto::TError DecompressChunk(
    const TBlobChunkLayout& layout,
    ui32 chunk,
    TStringBuf stored,
    TArrayRef<char> out);

// out.size() is layout.GetSpanLength(firstChunk, lastChunk).
NProto::TError DecompressChunks(
    const TBlobChunkLayout& layout,
    TStringBuf stored,
    ui32 firstChunk,
    ui32 lastChunk,
    TArrayRef<char> out);

}   // namespace NCloud
