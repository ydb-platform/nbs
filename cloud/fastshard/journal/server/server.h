#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

class TNetworkAddress;

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

IStartablePtr CreateServer(
    const TNetworkAddress& listenAddress,
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IServerBackendPtr backend);

}   // namespace NCloud::NJournalled
