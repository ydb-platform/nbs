#pragma once

#include <util/generic/string.h>

#include <array>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Blob is split into compressed or non-compressed (fixed size) chunks.
class TCompressedBlobInfo
{
private:
    static constexpr ui16 NonCompressedChunkSize = 40*1024; // 40 KiB
    static constexpr ui32 ChunkCount = 103; // enough for 4 MiB blob

    bool Empty = true;
    // Contains (compressed) sizes of chunks.
    // If some element is zero - then it represents fixed size non-compressed
    // chunk.
    std::array<ui16, ChunkCount> CompressedChunkSizes;

public:
    explicit operator bool() const
    {
        return !Empty;
    }

    [[nodiscard]] TStringBuf SerializeAsString() const;
    void DeserializeFromString(TStringBuf s);
};

// Returns empty info if compression has not occurred.
TCompressedBlobInfo TryCompressBlob(const TString& input, TString* output);

}   // namespace NCloud::NBlockStore::NStorage
