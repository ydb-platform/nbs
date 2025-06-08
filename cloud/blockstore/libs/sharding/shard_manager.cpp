#include "shard_manager.h"

#include "config.h"

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

TShardManager::TShardManager(
        TShardingArguments args,
        TShardConfig config)
    : Args(std::move(args))
    , Config(std::move(config))
{
    for (const auto& host: Config.GetHosts()) {
        Unused.emplace_back(host.first);
    }
}

TResultOrError<THostEndpoint> TShardManager::PickHost(
    const NClient::TClientAppConfigPtr& clientConfig)
{
    ResizeIfNeeded();

    with_lock (Lock) {
        if (Active.empty()) {
            return MakeError(
                E_REJECTED,
                TStringBuilder() <<
                    "No endpoints available in shard " <<
                    Config.GetShardId());
        }

        if (auto host = Config.GetFixedHost(); !host.Empty()) {
            if (Active.count(host)) {
                auto hostManager = Active[host];
                return hostManager->GetHostEndpoint(
                    clientConfig,
                    {},
                    true);
            }
        }

        auto index = RandomNumber<ui32>(Active.size());

        auto hostManagetIt = std::next(Active.begin(), index);
        return hostManagetIt->second->GetHostEndpoint(
            clientConfig,
            {},
            true);
    }
}

TShardEndpoints TShardManager::PickHosts(
        ui32 count,
        const NClient::TClientAppConfigPtr& clientConfig)
{
    ResizeIfNeeded();

    with_lock (Lock) {
        TShardEndpoints res;
        auto it = Active.begin();
        while (count && it != Active.end()) {
            auto result = it->second->GetHostEndpoint(
                clientConfig, NProto::GRPC, false);
            if (!HasError(result.GetError())) {
                auto endpoint = result.ExtractResult();
                res.emplace_back(endpoint);
                --count;
            }
            ++it;
        }
        return res;
    }
}

void TShardManager::ResizeIfNeeded()
{
    TVector<IHostEndpointsManagerPtr> tmp;
    with_lock(Lock) {
        if (Active.size() >= Config.GetMinShardConnections()) {
            return;
        }

        auto delta = Config.GetMinShardConnections() - Active.size();
        while (delta-- && !Unused.empty()) {
            auto host = Unused.back();
            Unused.pop_back();
            auto hostManager = CreateHostEndpointsManager(
                Config.GetHosts().find(host)->second,
                Args);
            Activating.emplace(host, hostManager);
            tmp.push_back(hostManager);
        }
    }

    auto weakPtr = this->weak_from_this();
    for (const auto& host: tmp) {
        auto future = host->Start();
        future.Subscribe(
            [=] (const auto& future) {
                Y_UNUSED(future);
                if (auto pThis = weakPtr.lock(); pThis) {
                    with_lock(pThis->Lock) {
                        auto fqdn = host->GetConfig().GetFqdn();
                        pThis->Active.emplace(
                            fqdn,
                            pThis->Activating.find(fqdn)->second);
                        pThis->Activating.erase(fqdn);
                    }
                }
        });
    }
}

}   // namespace NCloud::NBlockStore::NSharding
