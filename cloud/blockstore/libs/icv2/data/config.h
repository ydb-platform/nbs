#pragma once

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct TDataConfig
{
    // Limits serialized request size and read/write data size separately.
    ui64 MaxRequestBytes = 4_MB;
    // Exceeding either inflight limit rejects the request with E_REJECTED.
    ui32 MaxInflightRequests = 1024;
    // Buffer budget, separate from the request channel limits.
    ui64 MaxInflightBytes = 64_MB;

    bool IsValid() const;
};

}   // namespace NCloud::NBlockStore::NIcV2
