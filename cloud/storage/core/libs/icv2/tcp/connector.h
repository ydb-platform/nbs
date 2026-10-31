#pragma once

#include "socket.h"

#include <cloud/storage/core/libs/common/startable.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Connect asynchronously after Start(). Restart is not supported.
// Stop() waits for completions. Completion callbacks must not throw,
// call Stop(), or release the last reference to the connector.
struct IConnector: public IStartable
{
    // Timeout starts at this call. Stop() does not close returned sockets.
    // Calls before Start() or after shutdown are rejected.
    virtual TSocketFuture Connect(
        const NInterconnect::TAddress& address,
        TDuration timeout) = 0;

    // Safe in completion callbacks; call Stop() afterwards to await shutdown.
    virtual void RequestStop() = 0;
};

using IConnectorPtr = std::shared_ptr<IConnector>;

TResultOrError<IConnectorPtr> CreateConnector();

}   // namespace NCloud::NIcV2
