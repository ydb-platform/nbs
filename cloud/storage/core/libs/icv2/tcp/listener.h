#pragma once

#include "socket.h"

#include <cloud/storage/core/libs/common/startable.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Calls are serialized. Do not throw, call Stop() or destroy the listener here.
struct IListenerHandler
{
    virtual ~IListenerHandler();

    virtual void OnSocketAccepted(TSocketPtr socket) = 0;
    // Reports a terminal error; create a new listener to resume accepting.
    virtual void OnError(const NProto::TError& error) = 0;
};

using IListenerHandlerPtr = std::shared_ptr<IListenerHandler>;

// Start once to receive connections. Stop() releases the port and waits for
// callbacks, including after OnError(). Stop() is repeatable; restart is not.
struct IListener: public IStartable
{
    // Includes the assigned port when port 0 was requested.
    virtual const NInterconnect::TAddress& GetAddress() const = 0;

    // Safe in callbacks; call Stop() outside callbacks to finish shutdown.
    virtual void RequestStop() = 0;
};

using IListenerPtr = std::shared_ptr<IListener>;

// Binds immediately. Handler callbacks begin after Start().
TResultOrError<IListenerPtr> CreateListener(
    const NInterconnect::TAddress& address,
    IListenerHandlerPtr handler);

}   // namespace NCloud::NIcV2
