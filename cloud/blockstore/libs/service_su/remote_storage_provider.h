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

struct TRemoteShardEndpoints
{
    IBlockStorePtr Service;
    IBlockStorePtr Storage;

    TRemoteShardEndpoints() = default;

    TRemoteShardEndpoints(IBlockStorePtr service, IBlockStorePtr storage)
        : Service(std::move(service))
        , Storage(std::move(storage))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct IRemoteStorageProvider
{
    virtual ~IRemoteStorageProvider() = default;

    virtual TResultOrError<TRemoteShardEndpoints> CreateStorage(
        const TString& shardId,
        NClient::TClientAppConfigPtr clientConfig,
        std::optional<TString> clientId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

using TTransportFactory = std::function<IBlockStorePtr(const TString&, const NProto::TShardInfo, std::optional<TString> clientId)>;
using TServiceFactory = std::function<IBlockStorePtr(const TString&, const NProto::TShardInfo&, std::optional<TString> clientId)>;

////////////////////////////////////////////////////////////////////////////////

using TRemoteStorageFactories =
    TVector<std::pair<NProto::EShardDataTransport, TTransportFactory>>;

////////////////////////////////////////////////////////////////////////////////

IRemoteStorageProviderPtr CreateRemoteStorageProvider(
    TServerAppConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    NRdma::IClientPtr rdmaClient);

}   // namespace NCloud::NBlockStore::NServer
