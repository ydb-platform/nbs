#pragma once

#include "public.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

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

TBlobCompressionInfo TryCompressBlob(TString* content);

}   // namespace NCloud::NFileStore::NStorage
