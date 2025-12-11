#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/incomplete_requests.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/service.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct IMetricClient
    : public IBlockStore
    , public IIncompleteRequestProvider
{
};

////////////////////////////////////////////////////////////////////////////////

IMetricClientPtr CreateMetricClient(
    IBlockStorePtr client,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats);

}   // namespace NCloud::NBlockStore::NClient
