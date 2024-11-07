#include "blob_compression.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TBlobCompressionInfo TryCompressBlob(
    ui32 chunkSize,
    const NBlockCodecs::ICodec* codec,
    TString* content)
{
    Y_UNUSED(chunkSize);
    Y_UNUSED(codec);
    Y_UNUSED(content);
    return {};
}

}   // namespace NCloud::NFileStore::NStorage
