#pragma once

#include "connection.h"

#include <util/generic/ptr.h>
#include <util/generic/size_literals.h>

namespace NActors {

class IUringEngine;
class IEventDestructor;
using TUringEnginePtr = TIntrusivePtr<IUringEngine>;

}   // namespace NActors

namespace NInterconnect {

class TStreamSocket;

}   // namespace NInterconnect

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultMaxOutputQueueBytes = 512_MB;

struct ITransport
{
    virtual ~ITransport() = default;

    // Requires Activate() before use.
    // Optional xdcSocket is closed together with socket; an error in either
    // closes both. Idle XDC disconnects may remain undetected until a data
    // transfer is attempted. Returns null if socket or handler is null, or
    // shutdown has started.
    virtual IConnectionPtr CreateConnection(
        TIntrusivePtr<NInterconnect::TStreamSocket> socket,
        TIntrusivePtr<NInterconnect::TStreamSocket> xdcSocket,
        std::shared_ptr<IConnectionHandler> handler,
        bool sendPings) = 0;

    // Safe from callbacks. Rejects new work without waiting for callbacks to
    // finish.
    virtual void RequestStop() = 0;

    // Waits for all callbacks. Call outside callbacks and before releasing
    // the transport's last reference.
    virtual void Stop() = 0;
};

using ITransportPtr = std::shared_ptr<ITransport>;

// Requires exclusive control of an unused engine.
// maxOutputQueueBytes limits the combined output queues of all connections,
// including data not yet released after a connection closes.
// Zero maxOutputQueueBytes selects DefaultMaxOutputQueueBytes.
// Optional eventDestructor receives outgoing payloads for deferred destruction.
// Stop() does not wait for deferred destruction to complete.
// Returns null if engine is null.
ITransportPtr CreateTransport(
    NActors::TUringEnginePtr engine,
    ui64 maxOutputQueueBytes,
    std::shared_ptr<NActors::IEventDestructor> eventDestructor);

}   // namespace NCloud::NBlockStore::NIcV2
