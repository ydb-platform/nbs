#pragma once

#include "bootstrap.h"
#include "endpoint_bootstrap.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/cells/iface/host_endpoint.h>
#include <cloud/blockstore/libs/cells/iface/public.h>
#include <cloud/blockstore/libs/client/public.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/system/spinlock.h>

#include <memory>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

// Keeps the control channels to the hosts of a single cell and tracks which
// of them are usable.
//
// Two populations live here. Configured hosts come from TCellConfig, are kept
// warm and serve as the guaranteed way back into the cell after a host dies.
// The rest are discovered at runtime from the tablet host reported in mount
// responses: they are created on demand and dropped once nobody holds them.
//
// Thread-safe.
class TCellHostPool
{
private:
    struct TChannel
    {
        ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture Endpoint;
        ui32 RefCount = 0;
        bool Configured = false;
        bool Alive = true;
    };

    const TCellConfigPtr Config;
    const TBootstrap Bootstrap;

    mutable TAdaptiveLock Lock;
    THashMap<TString, TChannel> Channels;

public:
    TCellHostPool(TCellConfigPtr config, TBootstrap bootstrap);

    void Start();

    [[nodiscard]] TCellHostEndpoints GetDescribeEndpoints(
        const NClient::TClientAppConfigPtr& clientConfig);

    [[nodiscard]] TResultOrError<TCellHostConfig> PickConfiguredHost() const;

    [[nodiscard]] TCellHostConfig MakeHostConfig(const TString& fqdn) const;

    void SetHostAlive(const TString& fqdn, bool alive);

    ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture
        AcquireControlChannel(const TString& fqdn);
    void ReleaseControlChannel(const TString& fqdn);

private:
    ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture
        EnsureChannelLocked(const TString& fqdn);
};

using TCellHostPoolPtr = std::shared_ptr<TCellHostPool>;

}   // namespace NCloud::NBlockStore::NCells
