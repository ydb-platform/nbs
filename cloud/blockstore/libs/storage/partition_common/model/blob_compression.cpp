#include "blob_compression.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TStringBuf TCompressedBlobInfo::SerializeAsString() const
{
    constexpr auto valueSize = sizeof(decltype(CompressedChunkSizes)::value_type);
    static_assert(valueSize == 2);

    Y_ABORT_UNLESS(!Empty);
    Y_ABORT_UNLESS(CompressedChunkSizes.size() == ChunkCount);

    // TODO: endianness
    return {
        reinterpret_cast<const char*>(CompressedChunkSizes.data()),
        valueSize * ChunkCount
    };
}

void TCompressedBlobInfo::DeserializeFromString(TStringBuf s)
{
    constexpr auto valueSize = sizeof(decltype(CompressedChunkSizes)::value_type);
    static_assert(valueSize == 2);

    Y_ABORT_UNLESS(s);
    Y_ABORT_UNLESS(s.size() == valueSize * ChunkCount);

    // TODO: endianness
    memcpy(reinterpret_cast<char*>(CompressedChunkSizes.data()), s.data(), s.length());
    Empty = false;
}

TCompressedBlobInfo TryCompressBlob(const TString& input, TString* output)
{
    Y_UNUSED(input);
    Y_UNUSED(output);

    return {};
}

}   // namespace NCloud::NBlockStore::NStorage
