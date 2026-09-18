#pragma once

#include "public.h"

#include "connection.h"
#include "inbound_activity.h"
#include "host_endpoint.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/rdma/iface/client.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

using TDescribeVolumeFuture =
    NThreading::TFuture<NProto::TDescribeVolumeResponse>;

struct ICellManager: public IStartable
{
    TCellsConfigPtr Config;

    explicit ICellManager(TCellsConfigPtr config)
        : Config(std::move(config))
    {}

    [[nodiscard]] virtual TCellConnectionFuture CreateConnection(
        const TString& cellId,
        const TString& fqdn,
        const NClient::TClientAppConfigPtr& clientConfig,
        ICellConnectionObserverPtr observer) = 0;

    [[nodiscard]] virtual TDescribeVolumeFuture DescribeVolume(
        TCallContextPtr callContext,
        const TString& diskId,
        const NProto::THeaders& headers,
        IBlockStorePtr service,
        const NProto::TClientConfig& clientConfig) = 0;

    // The table the receiving-side forward service records inter-cell
    // requests into, so the cells mon page can show them next to the
    // outbound view. Owned here; the forward service is handed it to write.
    [[nodiscard]] virtual std::shared_ptr<TCellInboundActivity>
        GetInboundActivity() = 0;
};

////////////////////////////////////////////////////////////////////////////////

ICellManagerPtr CreateCellManagerStub();

}   // namespace NCloud::NBlockStore::NCells
