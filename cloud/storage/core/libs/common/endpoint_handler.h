#pragma once

#include "public.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// Keep references back to the endpoint or its owner weak.
// Callbacks may be late; do not throw or wait for shutdown.
struct IClientEndpointHandler
{
    virtual ~IClientEndpointHandler();

    // Ready for I/O.
    virtual void HandleConnected() = 0;
    virtual void HandleDisconnected() = 0;
    // A connection attempt failed. May repeat before the first connection.
    virtual void HandleUnavailable() = 0;
};

}   // namespace NCloud
