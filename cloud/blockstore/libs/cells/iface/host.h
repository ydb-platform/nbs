#pragma once

#include "config.h"
#include "endpoint_bootstrap.h"
#include "host_endpoint.h"

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

    virtual NThreading::TFuture<void> Start() = 0;
    virtual NThreading::TFuture<void> Stop() = 0;

    [[nodiscard]] virtual TResultOrError<TCellHostEndpoint> GetHostEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig,
        std::optional<NProto::ECellDataTransport> transport,
        bool allowGrpcFallback) = 0;

    virtual ~ICellHost() = default;
};

using ICellHostPtr = std::shared_ptr<ICellHost>;

////////////////////////////////////////////////////////////////////////////////

ICellHostPtr CreateHost(TCellHostConfig config, TBootstrap args);

}   // namespace NCloud::NBlockStore::NCells
