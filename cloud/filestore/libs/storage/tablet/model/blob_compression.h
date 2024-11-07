#pragma once

#include "public.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NBlockCodecs {

////////////////////////////////////////////////////////////////////////////////

struct ICodec;

}   // namespace NBlockCodecs

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TBlobCompressionInfo
{
public:
    explicit operator bool() const
    {
        return false;
    }

    TString Encode() const
    {
        return "";
    }
};

////////////////////////////////////////////////////////////////////////////////

TBlobCompressionInfo TryCompressBlob(
    ui32 chunkSize,
    const NBlockCodecs::ICodec* codec,
    TString* content);

}   // namespace NCloud::NFileStore::NStorage
