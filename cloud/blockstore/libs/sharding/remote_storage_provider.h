#pragma once

#include "public.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/blockstore/libs/rdma/iface/client.h>

#include <cloud/blockstore/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

class TShardClient
{
private:
    const TString LogTag;
    const IBlockStorePtr Service;
    const IStoragePtr Storage;

    static TString BuildLogTag(
        const NClient::TClientAppConfigPtr& clientConfig,
        const TString& fqdn);

public:
    TShardClient(
            const NClient::TClientAppConfigPtr& clientConfig,
            const TString& fqdn,
            IBlockStorePtr service,
            IStoragePtr storage)
        : LogTag(BuildLogTag(clientConfig, fqdn))
        , Service(std::move(service))
        , Storage(std::move(storage))
    {}

    const TString& GetLogTag() const
    {
        return LogTag;
    }

    [[nodiscard]] IBlockStorePtr GetService() const
    {
        return Service;
    }

    [[nodiscard]] IStoragePtr GetStorage() const
    {
        return Storage;
    }
};

using TShardClients = THashMap<TString, TVector<TShardClient>>;

////////////////////////////////////////////////////////////////////////////////

struct IRemoteStorageProvider
    : public IStartable
{
    virtual ~IRemoteStorageProvider() = default;

    [[nodiscard]] virtual TShardClient GetShardClient(
        const TString& shardId,
        NClient::TClientAppConfigPtr clientConfig) = 0;

    [[nodiscard]] virtual TShardClients GetShardsClients(
        NClient::TClientAppConfigPtr clientConfig)  = 0;
};

////////////////////////////////////////////////////////////////////////////////

IRemoteStorageProviderPtr CreateRemoteStorageProvider(
    TShardingConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    NClient::IClientPtr grpcClient,
    NRdma::IClientPtr rdmaClient);

}   // namespace NCloud::NBlockStore::NSharding
