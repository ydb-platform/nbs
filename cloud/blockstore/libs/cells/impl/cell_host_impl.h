#pragma once

#include "bootstrap.h"
#include "cell_host.h"
#include "endpoint_bootstrap.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
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

    const TBootstrap Args;

    NClient::IMultiClientEndpointPtr GrpcHostEndpoint;
    IBlockStorePtr RdmaHostEndpoint;

    EState GrpcState = EState::INACTIVE;
    EState RdmaState = EState::INACTIVE;

    TAdaptiveLock StateLock;
    EState State = EState::INACTIVE;

    NThreading::TPromise<TResultOrError<TCellHostConfig>> StartPromise =
        NThreading::NewPromise<TResultOrError<TCellHostConfig>>();
    NThreading::TPromise<TResultOrError<TCellHostConfig>> StopPromise =
        NThreading::NewPromise<TResultOrError<TCellHostConfig>>();

    ICellHostEndpointBootstrap::TRdmaEndpointBootstrapFuture RdmaFuture;

    TCellHost(TCellHostConfig config, TBootstrap bootstrap)
        : ICellHost(std::move(config))
        , Args(std::move(bootstrap))
    {}

    NThreading::TFuture<TResultOrError<TCellHostConfig>> Start() override;
    NThreading::TFuture<TResultOrError<TCellHostConfig>> Stop() override;

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

    void HandleRdmaSetupResult(const TResultOrError<IBlockStorePtr>& result);

    [[nodiscard]] TCellHostEndpoint CreateGrpcEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig);

    [[nodiscard]] TCellHostEndpoint CreateRdmaEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig);
};

}   // namespace NCloud::NBlockStore::NCells
