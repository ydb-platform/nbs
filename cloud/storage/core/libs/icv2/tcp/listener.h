#pragma once

#include "socket.h"

#include <cloud/storage/core/libs/common/startable.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Calls are serialized. Do not throw, call listener.Stop(), or destroy it here.
struct IListenerHandler
{
    virtual ~IListenerHandler();

    // Take the accepted socket and use it to handle the connection.
    virtual void OnSocketAccepted(TSocketPtr socket) = 0;
    // Reports a terminal error; create a new listener to resume accepting.
    virtual void OnError(const NProto::TError& error) = 0;
};

using IListenerHandlerPtr = std::shared_ptr<IListenerHandler>;

// Start once to receive connections. Stop releases the port and waits for
// handler calls, including after OnError. Stop can be repeated; restart cannot.
struct IListener: public IStartable
{
    // Includes the assigned port when port 0 was requested.
    virtual const NInterconnect::TAddress& GetAddress() const = 0;

    // Safe in handler methods; call Stop elsewhere to await shutdown.
    virtual void RequestStop() = 0;
};

using IListenerPtr = std::shared_ptr<IListener>;

// Binds immediately and keeps handler alive. Notifications begin after Start.
// Note: handler should keep a weak reference to the listener to avoid cycles.
TResultOrError<IListenerPtr> CreateListener(
    const NInterconnect::TAddress& address,
    IListenerHandlerPtr handler);

}   // namespace NCloud::NIcV2
