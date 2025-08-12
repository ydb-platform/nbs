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

TCell::TCell(TBootstrap boorstrap, TCellConfig config)
    : Bootstrap(std::move(boorstrap))
    , Config(std::move(config))
{
    for (const auto& host: Config.GetHosts()) {
        UnusedHosts.emplace_back(host.first);
    }
    Shuffle(UnusedHosts.begin(), UnusedHosts.end());
}

TResultOrError<TCellHostEndpoint> TCell::PickHost(
    const NClient::TClientAppConfigPtr& clientConfig)
{
    AdjustActiveHostsToMinConnections();

    ICellHostPtr host;

    with_lock (Lock) {
        if (ActiveHosts.empty()) {
            return MakeError(
                E_REJECTED,
                TStringBuilder() <<
                    "No endpoints available in cell " <<
                    Config.GetCellId());
        }

        auto index = RandomNumber<ui32>(ActiveHosts.size());

        host = std::next(ActiveHosts.begin(), index)->second;
    }
    // empty optional value, TCellHost will choose transport based on config
    std::optional<NProto::ECellDataTransport> transport;
    return host->GetHostEndpoint(
        clientConfig,
        transport,
        false);

}

TCellHostEndpoints TCell::PickHosts(
    ui32 count,
    const NClient::TClientAppConfigPtr& clientConfig)
{
    AdjustActiveHostsToMinConnections();

    with_lock (Lock) {
        TCellHostEndpoints res;
        auto it = ActiveHosts.begin();
        while (count && it != ActiveHosts.end()) {
            auto result = it->second->GetHostEndpoint(
                clientConfig,
                NProto::CELL_DATA_TRANSPORT_GRPC,
                false);
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

void TCell::AdjustActiveHostsToMinConnections()
{
    TVector<ICellHostPtr> hostsToActivate;
    with_lock(Lock) {
        if (Config.GetMinCellConnections() <= ActiveHosts.size()) {
            return;
        }

        auto delta = Config.GetMinCellConnections() - ActiveHosts.size();
        while (delta-- && !UnusedHosts.empty()) {
            auto fqdn = UnusedHosts.back();
            UnusedHosts.pop_back();
            auto host = CreateHost(
                Config.GetHosts().find(fqdn)->second,
                Bootstrap);
            ActivatingHosts.emplace(fqdn, host);
            hostsToActivate.push_back(host);
        }
    }

    auto weakPtr = this->weak_from_this();
    for (const auto& host: hostsToActivate) {
        auto future = host->Start();
        future.Subscribe(
            [=] (const auto& ) {
                if (auto self = weakPtr.lock(); self) {
                    with_lock(self->Lock) {
                        auto fqdn = host->GetConfig().GetFqdn();
                        self->ActiveHosts.emplace(
                            fqdn,
                            self->ActivatingHosts.find(fqdn)->second);
                        self->ActivatingHosts.erase(fqdn);
                    }
                }
        });
    }
}

ICellPtr CreateCell(TBootstrap boorstrap, TCellConfig config)
{
    return std::make_shared<TCell>(std::move(boorstrap), std::move(config));
}

}   // namespace NCloud::NBlockStore::NCells
