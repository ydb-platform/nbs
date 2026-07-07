#pragma once

#include <util/system/types.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TFlushBatchLimits
{
    // The maximum size of a single consolidated WriteData request
    ui32 MaxWriteRequestSize = 0;

    // The maximum number of consolidated WriteData requests
    ui32 MaxWriteRequestsCount = 0;

    // The maximum total size of all consolidated WriteData requests
    ui32 MaxSumWriteRequestsSize = 0;
};

} // namespace NCloud::NFileStore::NFuse::NWriteBackCache
