#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/file_ring_buffer.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache
{
private:
    TFileRingBuffer RequestsToProcess;

public:
    TWriteBackCache(const TString& filePath, ui32 size);

    // TODO: implement me
};

////////////////////////////////////////////////////////////////////////////////

TWriteBackCachePtr CreateWriteBackCache(const TString& filePath, ui32 size);

}   // namespace NCloud::NFileStore::NFuse
