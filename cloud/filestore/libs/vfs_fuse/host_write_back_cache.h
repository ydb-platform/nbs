#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/file_ring_buffer.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class THostWriteBackCache
{
private:
    TFileRingBuffer RequestsToProcess;

public:
    THostWriteBackCache(const TString& filePath, ui32 size);

    // TODO: implement me
};

////////////////////////////////////////////////////////////////////////////////

THostWriteBackCachePtr CreateHostWriteBackCache(
    const TString& filePath,
    ui32 size);

}   // namespace NCloud::NFileStore::NFuse
