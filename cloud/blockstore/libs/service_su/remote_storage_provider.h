#pragma once

#include "public.h"

#include <cloud/blockstore/config/server.pb.h>
#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/server/public.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/blockstore/libs/rdma/iface/client.h>

#include <cloud/blockstore/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TRemoteShardEndpoint
{
    IBlockStorePtr Service;
    IBlockStorePtr StorageService;
};

using TRemoteEndpoints = THashMap<TString, TRemoteShardEndpoint>;

////////////////////////////////////////////////////////////////////////////////

struct IRemoteStorageProvider
    : public IStartable
{
    virtual ~IRemoteStorageProvider() = default;

    [[nodiscard]] virtual TRemoteShardEndpoint CreateStorage(
        const TString& shardId,
        NClient::TClientAppConfigPtr clientConfig) = 0;

    [[nodiscard]] virtual TRemoteEndpoints GetRemoteEndpoints(
        NClient::TClientAppConfigPtr clientConfig)  = 0;
};

////////////////////////////////////////////////////////////////////////////////

IRemoteStorageProviderPtr CreateRemoteStorageProvider(
    TServerAppConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    NClient::IClientPtr grpcClient,
    NRdma::IClientPtr rdmaClient);

}   // namespace NCloud::NBlockStore::NServer
