#pragma once

#include "public.h"
#include "config.h"
#include "host_endpoint.h"
#include "sharding_common.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

class THostEndpointsManager
    : public std::enable_shared_from_this<THostEndpointsManager>
{
private:
    enum class EEndpointState
    {
        INACTIVE,
        INITIALIZING,
        ACTIVE,
        DEINITIALIZING
    };

    using TSetupEndpointFuture = NThreading::TFuture<void>;
    using TShutdownEndpointFuture = NThreading::TFuture<void>;

    const TShardHostConfig Config;
    const TShardingArguments Args;

    NClient::IMultiClientEndpointPtr GrpcHostEndpoint;
    IBlockStorePtr RdmaHostEndpoint;

    EEndpointState GrpcState = EEndpointState::INACTIVE;
    EEndpointState RdmaState = EEndpointState::INACTIVE;

    TAdaptiveLock StateLock;
public:
    explicit THostEndpointsManager(
            TShardHostConfig config,
            TShardingArguments args)
        : Config(std::move(config))
        , Args(std::move(args))
    {}

    NThreading::TFuture<void> Start();
    NThreading::TFuture<void> Stop();

    [[nodiscard]] TResultOrError<THostEndpoint> GetHostEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig,
        std::optional<NProto::EShardDataTransport> transport,
        bool allowGrpcFallback);

    [[nodiscard]] TShardHostConfig GetConfig() const
    {
        return Config;
    }
private:

    [[nodiscard]] THostEndpoint CreateGrpcEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig);

    [[nodiscard]] THostEndpoint CreateRdmaEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig);

    TSetupEndpointFuture SetupHostGrpcEndpoint();
    TSetupEndpointFuture SetupHostRdmaEndpoint();

    bool IsReady(NProto::EShardDataTransport transport);

    void SetEndpointState(EEndpointState& state, EEndpointState value);
};

using THostEndpointsManagerPtr = std::shared_ptr<THostEndpointsManager>;

}   // namespace NCloud::NBlockStore::NSharding
