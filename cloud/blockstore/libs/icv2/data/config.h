#pragma once

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct TDataConfig
{
    // Exceeding either inflight limit rejects the request with E_REJECTED.
    ui32 MaxInflightRequests = 1024;
    // Outgoing write payloads or incoming serialized requests and read buffers.
    ui64 MaxInflightBytes = 64_MB;

    bool IsValid() const;
};

}   // namespace NCloud::NBlockStore::NIcV2
