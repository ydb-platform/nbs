#pragma once

#include "public.h"

#include <cloud/blockstore/config/server.pb.h>

#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/server/public.h>

#include <cloud/storage/core/libs/common/startable.h>

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/client/throttling.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/blockstore/libs/server/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

using TSuServiceMap = THashMap<TString, IBlockStorePtr>;

////////////////////////////////////////////////////////////////////////////////

struct ISuDiscoveryService
    : public IBlockStore
{
    virtual IBlockStorePtr GetSuProxyService(TString suId) = 0;
};


////////////////////////////////////////////////////////////////////////////////

struct IRemoteSuServiceFactory
{
    virtual ~IRemoteSuServiceFactory() = default;
    virtual IBlockStorePtr GetSuService(TString suId) = 0;
};

////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateSuService(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    const NProto::TShardHostInfo& config,
    std::optional<TString> clientId = {});

IBlockStorePtr CreateSuDataService(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    const NProto::TShardHostInfo& config,
    std::optional<TString> clientId = {});

ISuDiscoveryServicePtr CreateSuDiscoveryService(
    IBlockStorePtr service,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    const TServerAppConfigPtr& config);


IRemoteSuServiceFactoryPtr CreateRemoteSuServiceFactory(
    IBlockStorePtr service,
    TString suId,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    const TServerAppConfigPtr& config);

IStoragePtr CreateRemoteEndpoint(IBlockStorePtr endpoint);

}   // namespace NCloud::NBlockStore::NServer
