#pragma once

#include <cloud/blockstore/libs/cells/iface/public.h>
#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/cells/iface/endpoints_setup.h>
#include <cloud/blockstore/libs/cells/iface/host_endpoint.h>
#include <cloud/blockstore/libs/cells/iface/cell_host.h>
#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

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

    const TArguments Args;

    NClient::IMultiClientEndpointPtr GrpcHostEndpoint;
    IBlockStorePtr RdmaHostEndpoint;

    EState GrpcState = EState::INACTIVE;
    EState RdmaState = EState::INACTIVE;

    TAdaptiveLock StateLock;
    EState State = EState::INACTIVE;

    NThreading::TPromise<void> StartPromise = NThreading::NewPromise<void>();
    NThreading::TPromise<void> StopPromise = NThreading::NewPromise<void>();

    IHostEndpointsSetupProvider::TSetupRdmaEndpointFuture RdmaFuture;

    THostEndpointsManager(
            TCellHostConfig config,
            TArguments args)
        : IHostEndpointsManager(std::move(config))
        , Args(std::move(args))
    {}

    NThreading::TFuture<void> Start() override;
    NThreading::TFuture<void> Stop() override;

    [[nodiscard]] TResultOrError<THostEndpoint> GetHostEndpoint(
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
        const IHostEndpointsSetupProvider::TRdmaResult& result);

    [[nodiscard]] THostEndpoint CreateGrpcEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig);

    [[nodiscard]] THostEndpoint CreateRdmaEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig);
};

}   // namespace NCloud::NBlockStore::NCells
