#pragma once

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/cells/iface/endpoint_bootstrap.h>
#include <cloud/blockstore/libs/cells/iface/host.h>
#include <cloud/blockstore/libs/cells/iface/host_endpoint.h>
#include <cloud/blockstore/libs/cells/iface/public.h>
#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

struct TCellHost
    : public ICellHost
    , public std::enable_shared_from_this<TCellHost>
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

    const TBootstrap Args;

    NClient::IMultiClientEndpointPtr GrpcHostEndpoint;
    IBlockStorePtr RdmaHostEndpoint;

    EState GrpcState = EState::INACTIVE;
    EState RdmaState = EState::INACTIVE;

    TAdaptiveLock StateLock;
    EState State = EState::INACTIVE;

    NThreading::TPromise<void> StartPromise = NThreading::NewPromise<void>();
    NThreading::TPromise<void> StopPromise = NThreading::NewPromise<void>();

    IHostEndpointsBootstrap::TRdmaEndpointBootstrapFuture RdmaFuture;

    TCellHost(
            TCellHostConfig config,
            TBootstrap args)
        : ICellHost(std::move(config))
        , Args(std::move(args))
    {}

    NThreading::TFuture<void> Start() override;
    NThreading::TFuture<void> Stop() override;

    [[nodiscard]] TResultOrError<TCellHostEndpoint> GetHostEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig,
        std::optional<NProto::ECellDataTransport> transport,
        bool allowGrpcFallback) override;

    [[nodiscard]] TCellHostConfig GetConfig() const
    {
        return Config;
    }

    bool IsReady(NProto::ECellDataTransport transport) const;

private:
    bool SetupRdmaIfNeeded();

    void HandleRdmaSetupResult(
        const TResultOrError<IBlockStorePtr>& result);

    [[nodiscard]] TCellHostEndpoint CreateGrpcEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig);

    [[nodiscard]] TCellHostEndpoint CreateRdmaEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig);
};

}   // namespace NCloud::NBlockStore::NCells
