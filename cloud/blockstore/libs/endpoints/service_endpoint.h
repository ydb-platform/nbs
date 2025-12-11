#pragma once

#include "public.h"

#include <cloud/blockstore/config/client.pb.h>
#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/service/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateMultipleEndpointService(
    IBlockStorePtr service,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IEndpointManagerPtr endpointManager);

}   // namespace NCloud::NBlockStore::NServer
