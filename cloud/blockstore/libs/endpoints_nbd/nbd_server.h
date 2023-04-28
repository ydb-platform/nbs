#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/nbd/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IEndpointListenerPtr CreateNbdEndpointListener(
    NBD::IServerPtr server,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats);

}   // namespace NCloud::NBlockStore::NServer
