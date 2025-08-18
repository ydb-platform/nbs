#pragma once

#include "bootstrap.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/cells/iface/host_endpoint.h>
#include <cloud/blockstore/libs/cells/iface/public.h>
#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

struct ICellHost
{
    const TCellHostConfig Config;

    explicit ICellHost(TCellHostConfig config)
        : Config(std::move(config))
    {}

    const TCellHostConfig& GetConfig() const
    {
        return Config;
    }

    virtual NThreading::TFuture<TResultOrError<TCellHostConfig>> Start() = 0;
    virtual NThreading::TFuture<TResultOrError<TCellHostConfig>> Stop() = 0;

    [[nodiscard]] virtual TResultOrError<TCellHostEndpoint> GetHostEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig,
        std::optional<NProto::ECellDataTransport> transport,
        bool allowGrpcFallback) = 0;

    virtual ~ICellHost() = default;
};

using ICellHostPtr = std::shared_ptr<ICellHost>;

////////////////////////////////////////////////////////////////////////////////

ICellHostPtr CreateHost(TCellHostConfig config, TBootstrap bootstrap);

}   // namespace NCloud::NBlockStore::NCells
