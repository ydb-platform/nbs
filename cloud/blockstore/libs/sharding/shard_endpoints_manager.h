#pragma once

#include "public.h"

#include "config.h"
#include "shard_client.h"
#include "sharding_common.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/rdma/impl/client.h>
#include <cloud/blockstore/libs/rdma/impl/verbs.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/sharding/config.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <util/generic/hash_set.h>
#include <util/random/random.h>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

struct IShardEndpointsManager
{
    using TShardClients = TVector<TShardClient>;

    [[nodiscard]] virtual TShardClient GetShardClient(
        NClient::TClientAppConfigPtr clientConfig) = 0;

    [[nodiscard]] virtual TShardClients GetShardClients(
        NClient::TClientAppConfigPtr clientConfig) = 0;

    virtual ~IShardEndpointsManager() = default;
};

using IShardEndpointsManagerPtr = std::shared_ptr<IShardEndpointsManager>;

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename TEndpoint>
struct TShardEndpointManagerBase
    : public IShardEndpointsManager
    , public std::enable_shared_from_this<TShardEndpointManagerBase<T, TEndpoint>>
{
    const TShardingArguments ShardArgs;
    const TShardConfig ShardConfig;

    TAdaptiveLock Lock;

    THashMap<TString, TEndpoint> Active;
    THashSet<TString> Activating;
    TVector<TString> Unused;

    TShardEndpointManagerBase(
            TShardingArguments args,
            TShardConfig shardConfig)
        : ShardArgs(std::move(args))
        , ShardConfig(std::move(shardConfig))
    {
        for (auto& host: ShardConfig.GetHosts()) {
            Unused.emplace_back(host);
        }
    }

    TShardClient PickHost(NClient::TClientAppConfigPtr clientConfig)
    {
        ResizeIfNeeded();

        with_lock (Lock) {
            if (auto host = ShardConfig.GetFixedHost(); !host.Empty()) {
                if (Active.count(host)) {
                    auto p = static_cast<T*>(this);
                    return p->CreateShardClient(
                        Active[host],
                        std::move(clientConfig));
                }
            }
            if (Active.empty()) {
                return {};
            }

            auto index = RandomNumber<ui32>(Active.size());

            auto p = static_cast<T*>(this);
            return p->CreateShardClient(
                std::next(Active.begin(), index)->second,
                std::move(clientConfig));
        }
    }

    TShardClients PickHosts(
        ui32 count,
        NClient::TClientAppConfigPtr clientConfig)
    {
        ResizeIfNeeded();

        with_lock (Lock) {
            TShardClients res;
            auto p = static_cast<T*>(this);
            auto it = Active.begin();
            while (count-- && it != Active.end()) {
                auto endpoint = p->CreateShardClient(
                    it->second, std::move(clientConfig));
                res.emplace_back(endpoint);
                ++it;
            }
            return res;
        }
    }

    void ResizeIfNeeded()
    {
        TVector<TString> tmp;
        with_lock(Lock) {
            if (Active.size() >= ShardConfig.GetMinShardConnections()) {
                return;
            }

            auto delta = ShardConfig.GetMinShardConnections() - Active.size();
            while (delta-- && !Unused.empty()) {
                auto host = Unused.back();
                Unused.pop_back();
                Activating.emplace(host);
                tmp.push_back(host);
            }
        }

        auto weakPtr = this->weak_from_this();
        for (const auto& host: tmp) {
            auto future = static_cast<T*>(this)->SetupHostEndpoint(host);
            future.Subscribe(
                [=] (const auto& future) {
                    if (auto pThis = weakPtr.lock(); pThis) {
                        with_lock(pThis->Lock) {
                            pThis->Active.emplace(
                                host,
                                future.GetValue());
                            pThis->Activating.erase(host);
                        }
                    }
            });
        }
    }

   TShardClient GetShardClient(
        NClient::TClientAppConfigPtr clientConfig) override
    {
        return PickHost(clientConfig);
    }

    TShardClients GetShardClients(
        NClient::TClientAppConfigPtr clientConfig) override
    {
        return PickHosts(ShardConfig.GetShardDescribeHostCnt(), clientConfig);
    }
};

////////////////////////////////////////////////////////////////////////////////

IShardEndpointsManagerPtr CreateGrpcEndpointManager(
    TShardingArguments args,
    TShardConfig ShardConfig);

IShardEndpointsManagerPtr CreateRdmaEndpointManager(
    TShardingArguments args,
    TShardConfig ShardConfig);

}   // namespace NCloud::NBlockStore::NSharding
