#pragma once

#include "public.h"
#include "host_endpoint.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/blockstore/libs/rdma/iface/client.h>

#include <cloud/blockstore/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

struct ICellManager
    : public IStartable
{
    TCellsConfigPtr Config;

    explicit ICellManager(TCellsConfigPtr config)
        : Config(std::move(config))
    {}

    [[nodiscard]] virtual TResultOrError<TCellHostEndpoint> GetCellEndpoint(
        const TString& cellId,
        const NClient::TClientAppConfigPtr& clientConfig) = 0;

    [[nodiscard]] virtual NThreading::TFuture<NProto::TDescribeVolumeResponse>
    DescribeVolume(
        TCallContextPtr callContext,
        const TString& diskId,
        const NProto::THeaders& headers,
        IBlockStorePtr localService,
        const NProto::TClientConfig& clientConfig) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ICellManagerPtr CreateCellManagerStub();

}   // namespace NCloud::NBlockStore::NCells
