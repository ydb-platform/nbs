#pragma once

#include "public.h"
#include "config.h"
<<<<<<< HEAD
#include "endpoints_setup.h"
=======
>>>>>>> update
#include "host_endpoint.h"
#include "sharding_common.h"

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

struct THostEndpointsManager
    : public IHostEndpointsManager
    , public std::enable_shared_from_this<THostEndpointsManager>
{
    enum class EState
    {
        INACTIVE,
        ACTIVATING,
        ACTIVE,
        DEACTIVATING
    };

    using TSetupHostFuture = NThreading::TFuture<void>;
    using TShutdownHostFuture = NThreading::TFuture<void>;

    const TShardingArguments Args;

    NClient::IMultiClientEndpointPtr GrpcHostEndpoint;
    IBlockStorePtr RdmaHostEndpoint;

    EState GrpcState = EState::INACTIVE;
    EState RdmaState = EState::INACTIVE;

    TAdaptiveLock StateLock;
    EState State;

    NThreading::TPromise<void> StartPromise = NThreading::NewPromise<void>();
    NThreading::TPromise<void> StopPromise = NThreading::NewPromise<void>();

    THostEndpointsManager(
            TShardHostConfig config,
            TShardingArguments args)
        : IHostEndpointsManager(config)
        , Args(std::move(args))
    {}

    NThreading::TFuture<void> Start() override;
    NThreading::TFuture<void> Stop() override;

    [[nodiscard]] TResultOrError<THostEndpoint> GetHostEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig,
        std::optional<NProto::EShardDataTransport> transport,
        bool allowGrpcFallback) override;

    [[nodiscard]] TShardHostConfig GetConfig() const
    {
        return Config;
    }

    bool IsReady(NProto::EShardDataTransport transport) const;

private:
    using TOptionalRdmaFuture =
        std::optional<IHostEndpointsSetupProvider::TSetupRdmaEndpointFuture>;

    TOptionalRdmaFuture SetupRdmaIfNeeded();

    [[nodiscard]] THostEndpoint CreateGrpcEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig);

    [[nodiscard]] THostEndpoint CreateRdmaEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig);
};

using THostEndpointsManagerPtr = std::shared_ptr<THostEndpointsManager>;

////////////////////////////////////////////////////////////////////////////////

IHostEndpointsManagerPtr CreateHostEndpointsManager(
    TShardHostConfig config,
    TShardingArguments args);

}   // namespace NCloud::NBlockStore::NSharding
