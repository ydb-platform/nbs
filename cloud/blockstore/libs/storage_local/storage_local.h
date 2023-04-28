#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateLocalStorageProvider(
    TString localAgentId,
    IStorageProviderPtr upstream,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats);

}   // namespace NCloud::NBlockStore::NServer
