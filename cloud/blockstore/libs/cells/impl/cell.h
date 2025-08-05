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
    const TBootstrap Args;
    const TCellConfig Config;

    TAdaptiveLock Lock;

    THashMap<TString, IHostPtr> Active;
    THashMap<TString, IHostPtr> Activating;
    THashSet<IHostPtr> Deactivating;
    TVector<TString> Unused;

public:
    TCell(
        TBootstrap args,
        TCellConfig config);

    [[nodiscard]] TResultOrError<THostEndpoint> GetCellClient(
        const NClient::TClientAppConfigPtr& clientConfig) override
    {
        return PickHost(clientConfig);
    }

    [[nodiscard]] TCellEndpoints GetCellClients(
        const NClient::TClientAppConfigPtr& clientConfig) override
    {
        return PickHosts(Config.GetDescribeVolumeHostCount(), clientConfig);
    }

    [[nodiscard]]THashMap<TString, IHostPtr>  GetActive() const
    {
        with_lock(Lock) {
            return Active;
        }
    }

    [[nodiscard]]THashMap<TString, IHostPtr>  GetActivating() const
    {
        with_lock(Lock) {
            return Activating;
        }
    }

    [[nodiscard]]THashSet<IHostPtr>  GetDeactivating() const
    {
        with_lock(Lock) {
            return Deactivating;
        }
    }

    void Start() override
    {
        ResizeIfNeeded();
    }

    void Stop() override
    {
    }

private:
    TResultOrError<THostEndpoint> PickHost(
        const NClient::TClientAppConfigPtr& clientConfig);
    TCellEndpoints PickHosts(
        ui32 count,
        const NClient::TClientAppConfigPtr& clientConfig);

    void ResizeIfNeeded();
};

using TCellPtr = std::shared_ptr<TCell>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NCells
