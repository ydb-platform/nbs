#pragma once

#include "config.h"
#include "endpoints_setup.h"
#include "host_endpoint.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

struct IHostEndpointsManager
{
    const TShardHostConfig Config;

    explicit IHostEndpointsManager(TShardHostConfig config)
        : Config(std::move(config))
    {}

    const TShardHostConfig& GetConfig() const
    {
        return Config;
    }

    virtual NThreading::TFuture<void> Start() = 0;
    virtual NThreading::TFuture<void> Stop() = 0;

    [[nodiscard]] virtual TResultOrError<THostEndpoint> GetHostEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig,
        std::optional<NProto::EShardDataTransport> transport,
        bool allowGrpcFallback) = 0;

    virtual ~IHostEndpointsManager() = default;
};

using IHostEndpointsManagerPtr = std::shared_ptr<IHostEndpointsManager>;

////////////////////////////////////////////////////////////////////////////////

IHostEndpointsManagerPtr CreateHostEndpointsManager(
    TShardHostConfig config,
    TShardingArguments args);

}   // namespace NCloud::NBlockStore::NSharding
