#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/generic/vector.h>

class TNetworkAddress;

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

// The listener and the backend live on |executor|; the connections are
// spread over |connectionExecutors| round robin, or served by |executor|
// when the list is empty.
IStartablePtr CreateServer(
    const TNetworkAddress& listenAddress,
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IServerBackendPtr backend,
    TVector<TExecutorPtr> connectionExecutors = {});

}   // namespace NCloud::NJournalled
