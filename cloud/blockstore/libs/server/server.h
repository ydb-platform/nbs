#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/incomplete_requests.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/startable.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IServer
    : public IStartable
    , public IIncompleteRequestProvider
{
    virtual IClientStorageFactoryPtr GetClientStorageFactory() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TServerOptions
{
    TString CellId;
};

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    TServerAppConfigPtr config,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    IBlockStorePtr service,
    IBlockStorePtr udsService,
    TServerOptions options);

}   // namespace NCloud::NBlockStore::NServer
