#pragma once

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/cells/iface/cell.h>
#include <cloud/blockstore/libs/cells/iface/host.h>
#include <cloud/blockstore/libs/cells/iface/host_endpoint.h>
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

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

class TCell
    : public ICell
    , public std::enable_shared_from_this<TCell>
{
private:
    const TBootstrap Bootstrap;
    const TCellConfig Config;

    TAdaptiveLock Lock;

    THashMap<TString, ICellHostPtr> ActiveHosts;
    THashMap<TString, ICellHostPtr> ActivatingHosts;
    THashSet<ICellHostPtr> DeactivatingHosts;
    TVector<TString> UnusedHosts;

public:
    TCell(TBootstrap bootstrap, TCellConfig config);

    [[nodiscard]] TResultOrError<TCellHostEndpoint> GetCellClient(
        const NClient::TClientAppConfigPtr& clientConfig) override
    {
        return PickHost(clientConfig);
    }

    [[nodiscard]] TCellHostEndpoints GetCellClients(
        const NClient::TClientAppConfigPtr& clientConfig) override
    {
        return PickHosts(Config.GetDescribeVolumeHostCount(), clientConfig);
    }

    [[nodiscard]]THashMap<TString, ICellHostPtr>  GetActiveHosts() const
    {
        with_lock(Lock) {
            return ActiveHosts;
        }
    }

    [[nodiscard]]THashMap<TString, ICellHostPtr>  GetActivatingHosts() const
    {
        with_lock(Lock) {
            return ActivatingHosts;
        }
    }

    [[nodiscard]]THashSet<ICellHostPtr>  GetDeactivatingHosts() const
    {
        with_lock(Lock) {
            return DeactivatingHosts;
        }
    }

    void Start() override
    {
        AdjustActiveHostsToMinConnections();
    }

    void Stop() override
    {
    }

private:
    TResultOrError<TCellHostEndpoint> PickHost(
        const NClient::TClientAppConfigPtr& clientConfig);
    TCellHostEndpoints PickHosts(
        ui32 count,
        const NClient::TClientAppConfigPtr& clientConfig);

    void AdjustActiveHostsToMinConnections();
};

using TCellPtr = std::shared_ptr<TCell>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NCells
