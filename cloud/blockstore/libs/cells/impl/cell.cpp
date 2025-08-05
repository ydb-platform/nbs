#include "cell.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/rdma/impl/client.h>
#include <cloud/blockstore/libs/rdma/impl/verbs.h>
#include <cloud/blockstore/libs/server/config.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <util/generic/hash_set.h>
#include <util/random/random.h>
#include <util/random/shuffle.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

TCell::TCell(
        TBootstrap args,
        TCellConfig config)
    : Args(std::move(args))
    , Config(std::move(config))
{
    for (const auto& host: Config.GetHosts()) {
        Unused.emplace_back(host.first);
    }
    Shuffle(Unused.begin(), Unused.end());
}

TResultOrError<THostEndpoint> TCell::PickHost(
    const NClient::TClientAppConfigPtr& clientConfig)
{
    ResizeIfNeeded();

    with_lock (Lock) {
        if (Active.empty()) {
            return MakeError(
                E_REJECTED,
                TStringBuilder() <<
                    "No endpoints available in cell " <<
                    Config.GetCellId());
        }

        auto index = RandomNumber<ui32>(Active.size());

        auto hostManagetIt = std::next(Active.begin(), index);
        return hostManagetIt->second->GetHostEndpoint(
            clientConfig,
            {},
            false);
    }
}

TCellEndpoints TCell::PickHosts(
        ui32 count,
        const NClient::TClientAppConfigPtr& clientConfig)
{
    ResizeIfNeeded();

    with_lock (Lock) {
        TCellEndpoints res;
        auto it = Active.begin();
        while (count && it != Active.end()) {
            auto result = it->second->GetHostEndpoint(
                clientConfig, NProto::CELL_DATA_TRANSPORT_GRPC, false);
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

void TCell::ResizeIfNeeded()
{
    TVector<IHostPtr> tmp;
    with_lock(Lock) {
        if (Active.size() >= Config.GetMinCellConnections()) {
            return;
        }

        auto delta = Config.GetMinCellConnections() - Active.size();
        while (delta-- && !Unused.empty()) {
            auto host = Unused.back();
            Unused.pop_back();
            auto hostManager = CreateHost(
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

ICellPtr CreateCell(
    TBootstrap args,
    TCellConfig config)
{
    return std::make_shared<TCell>(std::move(args), std::move(config));
}

}   // namespace NCloud::NBlockStore::NCells
