#pragma once

#include "connection.h"

#include <contrib/ydb/library/actors/interconnect/interconnect_uring_engine.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct ITransport
{
    virtual ~ITransport() = default;

    // Prepares routing without registering any I/O. The transport assigns an
    // internal connection identity independent of the event actor IDs.
    // xdcSocket is optional; when supplied it belongs to the same logical
    // connection and is shut down together with socket. XDC I/O errors close
    // the pair; an idle XDC disconnect is detected on subsequent payload I/O.
    // maxOutputQueueBytes must be positive; Send rejects admission above it.
    // Returns null on invalid arguments or after RequestStop().
    virtual IConnectionPtr CreateConnection(
        TIntrusivePtr<NInterconnect::TStreamSocket> socket,
        TIntrusivePtr<NInterconnect::TStreamSocket> xdcSocket,
        std::shared_ptr<IConnectionHandler> handler,
        bool sendPings,
        ui64 maxOutputQueueBytes) = 0;

    // Safe from callbacks. Rejects new work and closes every connection.
    virtual void RequestStop() = 0;

    // External thread only. Concurrent callers wait for the same completion.
    // Joins workers and waits for user callbacks, then releases the engine.
    // The transport owner must call Stop before releasing its last reference;
    // destruction must also take place outside transport callbacks.
    virtual void Stop() = 0;
};

using ITransportPtr = std::shared_ptr<ITransport>;

// Takes exclusive control of a fresh engine: installs its client and owns Stop.
// Callers must not register connections or call Stop directly on this engine.
ITransportPtr CreateTransport(NActors::TUringEnginePtr engine);

}   // namespace NCloud::NBlockStore::NIcV2
