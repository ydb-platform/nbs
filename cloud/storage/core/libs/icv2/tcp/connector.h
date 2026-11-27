#pragma once

#include "socket.h"

#include <cloud/storage/core/libs/common/startable.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Start once before Connect(). Stop() waits for result callbacks.
// Callbacks must not throw, call Stop(), or destroy the connector.
struct IConnector: public IStartable
{
    // Timeout starts at this call. Calls before Start() or after shutdown are
    // rejected. Returned sockets remain open after Stop().
    virtual TSocketFuture Connect(
        const NInterconnect::TAddress& address,
        TDuration timeout) = 0;

    // Safe in callbacks; call Stop() outside callbacks to finish shutdown.
    virtual void RequestStop() = 0;
};

using IConnectorPtr = std::shared_ptr<IConnector>;

TResultOrError<IConnectorPtr> CreateConnector();

}   // namespace NCloud::NIcV2
