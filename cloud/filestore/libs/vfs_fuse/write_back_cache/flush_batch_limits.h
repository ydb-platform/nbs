#pragma once

#include <util/system/types.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TFlushBatchLimits
{
    // The maximum size of a single consolidated WriteData request
    const ui32 MaxWriteRequestSize;

    // The maximum number of consolidated WriteData requests
    const ui32 MaxWriteRequestsCount;

    // The maximum total size of all consolidated WriteData requests
    const ui32 MaxSumWriteRequestsSize;
};

} // namespace NCloud::NFileStore::NFuse::NWriteBackCache
