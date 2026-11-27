#pragma once

#include "connection.h"

#include <util/generic/ptr.h>

namespace NActors {

class IUringEngine;
class IEventDestructor;
using TUringEnginePtr = TIntrusivePtr<IUringEngine>;

}   // namespace NActors

namespace NInterconnect {

class TStreamSocket;

}   // namespace NInterconnect

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct ITransport
{
    virtual ~ITransport() = default;

    // xdcSocket is optional. Activate the returned connection before use.
    // An error in either socket closes both; idle XDC failures may remain
    // undetected until data is sent.
    // Returns null for a null socket or handler, or after shutdown begins.
    virtual IConnectionPtr CreateConnection(
        TIntrusivePtr<NInterconnect::TStreamSocket> socket,
        TIntrusivePtr<NInterconnect::TStreamSocket> xdcSocket,
        std::shared_ptr<IConnectionHandler> handler,
        bool sendPings) = 0;

    // Begins shutdown without waiting. Safe in callbacks.
    virtual void RequestStop() = 0;

    // Waits for callbacks, including WhenClosed() subscribers. Call outside
    // callbacks and keep the transport alive until it returns.
    virtual void Stop() = 0;
};

using ITransportPtr = std::shared_ptr<ITransport>;

// Pass an unused engine and do not use it elsewhere.
// maxOutputQueueBytes limits total outgoing data, including data retained
// after closure. Optional eventDestructor defers releasing outgoing payloads;
// Stop() does not wait for it.
// Returns null if engine is null or maxOutputQueueBytes is zero.
ITransportPtr CreateTransport(
    NActors::TUringEnginePtr engine,
    ui64 maxOutputQueueBytes,
    std::shared_ptr<NActors::IEventDestructor> eventDestructor);

}   // namespace NCloud::NIcV2
