#pragma once

#include "public.h"

#include <cloud/blockstore/config/client.pb.h>

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/incomplete_requests.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/keyring/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IEndpointService
    : public IBlockStore
    , public IIncompleteRequestProvider
{
    virtual NThreading::TFuture<void> RestoreEndpoints() = 0;
};

////////////////////////////////////////////////////////////////////////////////

IEndpointServicePtr CreateMultipleEndpointService(
    IBlockStorePtr service,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    IEndpointStoragePtr endpointStorage,
    IEndpointManagerPtr endpointManager,
    NProto::TClientConfig clientConfig);

}   // namespace NCloud::NBlockStore::NServer
