#pragma once

#include "public.h"

#include <cloud/blockstore/config/server.pb.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/nbd/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IEndpointListenerPtr CreateNbdEndpointListener(
    NBD::IServerPtr server,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    NProto::TChecksumFlags checksumFlags,
    ui64 maxZeroBlocksSubRequestSize);

}   // namespace NCloud::NBlockStore::NServer
