#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/generic/vector.h>

class TNetworkAddress;

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

// The listener runs in |executor|. The connections are spread over
// |connectionExecutors| in turn: a connection reads its requests, calls the
// backend and sends the responses in its own executor. If the list is empty,
// |executor| serves the connections too. The backend must be safe to call
// from all these threads.
IStartablePtr CreateServer(
    const TNetworkAddress& listenAddress,
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IServerBackendPtr backend,
    TVector<TExecutorPtr> connectionExecutors = {});

}   // namespace NCloud::NJournalled
