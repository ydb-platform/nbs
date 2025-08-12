#include "host.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/rdma/impl/client.h>
#include <cloud/blockstore/libs/rdma/impl/verbs.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TCellHostEndpoint> TCellHost::GetHostEndpoint(
    const NClient::TClientAppConfigPtr& clientConfig,
    std::optional<NProto::ECellDataTransport> desiredTransport,
    bool allowGrpcFallback)
{
    auto transport =
        desiredTransport.has_value() ? *desiredTransport : Config.GetTransport();
    Y_ENSURE(transport != NProto::CELL_DATA_TRANSPORT_UNSET);

    with_lock(StateLock) {
        if (!IsReady(transport)) {
            if (transport != NProto::CELL_DATA_TRANSPORT_GRPC) {
                if (allowGrpcFallback && IsReady(NProto::CELL_DATA_TRANSPORT_GRPC)) {
                    transport = NProto::CELL_DATA_TRANSPORT_GRPC;
                } else {
                    transport = NProto::CELL_DATA_TRANSPORT_UNSET;
                }
            } else {
                transport = NProto::CELL_DATA_TRANSPORT_UNSET;
            }
        }

        if (transport != NProto::CELL_DATA_TRANSPORT_UNSET) {
            switch (transport) {
                case NProto::CELL_DATA_TRANSPORT_GRPC:
                    return CreateGrpcEndpoint(clientConfig);
                case NProto::CELL_DATA_TRANSPORT_RDMA:
                    return CreateRdmaEndpoint(clientConfig);
                default:
                    return MakeError(
                        E_INVALID_STATE,
                        TStringBuilder() <<
                            "Unsupported transport type " <<
                            static_cast<int>(transport));
            }
        }

        return MakeError(
            E_REJECTED,
            TStringBuilder() << "No transport available");
    }
}

bool TCellHost::IsReady(NProto::ECellDataTransport transport) const
{
    switch (transport) {
        case NProto::CELL_DATA_TRANSPORT_GRPC: {
            return GrpcState == EState::ACTIVE;
        }
        case NProto::CELL_DATA_TRANSPORT_RDMA: {
            return RdmaState == EState::ACTIVE;
        }
        default:
            return false;
    }
    return false;
}

TFuture<void> TCellHost::Start()
{
    auto target = Config.GetTransport();
    Y_ENSURE(
        target != NProto::CELL_DATA_TRANSPORT_UNSET &&
        target != NProto::CELL_DATA_TRANSPORT_NBD);

    auto weak = weak_from_this();
    ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture future;

    with_lock(StateLock) {
        if (State == EState::ACTIVATING || State == EState::ACTIVE) {
            return StartPromise.GetFuture();
        }
        if (State == EState::DEACTIVATING) {
            return StopPromise.GetFuture().Apply([=] (const auto& future) {
                Y_UNUSED(future);
                if (auto self = weak.lock(); self) {
                    return self->Start();
                }
                return MakeFuture();
            });
        }
        State = EState::ACTIVATING;
        future = Args.EndpointsSetup->SetupHostGrpcEndpoint(Args, Config);
    }

    future.Subscribe([=] (const auto& f) mutable {
        Y_UNUSED(f);
        if (auto self = weak.lock(); self) {
            bool needRdmaSetup = false;;
            with_lock(self->StateLock) {
                self->GrpcState = EState::ACTIVE;
                self->GrpcHostEndpoint = f.GetValue();
                needRdmaSetup = self->SetupRdmaIfNeeded();
            }
            if (needRdmaSetup) {
                self->RdmaFuture.Subscribe([=] (const auto& f) {
                    if (auto self = weak.lock(); self) {
                        self->HandleRdmaSetupResult(f.GetValue());
                    }
                });
            }
            self->StartPromise.SetValue();
        }
    });
    return StartPromise.GetFuture();
}

void TCellHost::HandleRdmaSetupResult(
    const TResultOrError<IBlockStorePtr>& result)
{
    if (HasError(result.GetError())) {
        RdmaState = EState::INACTIVE;
        with_lock(StateLock) {
            SetupRdmaIfNeeded();
        }

        auto weak = weak_from_this();
        RdmaFuture.Subscribe([weak=std::move(weak)] (const auto& f) {
            if (auto self = weak.lock(); self) {
                self->HandleRdmaSetupResult(f.GetValue());
            }
        });
    } else {
        with_lock(StateLock) {
            RdmaState = EState::ACTIVE;
            RdmaHostEndpoint = result.GetResult();
        }
    }
}

bool TCellHost::SetupRdmaIfNeeded()
{
    if (Config.GetTransport() != NProto::CELL_DATA_TRANSPORT_RDMA) {
        return false;
    }
    RdmaState = EState::ACTIVATING;
    RdmaFuture = Args.EndpointsSetup->SetupHostRdmaEndpoint(
        Args,
        Config,
        GrpcHostEndpoint);
    return true;
}

TFuture<void> TCellHost::Stop()
{
    return {};
}

TCellHostEndpoint TCellHost::CreateGrpcEndpoint(
    const NClient::TClientAppConfigPtr& clientConfig)
{
    auto& endp = GrpcHostEndpoint;
    auto service = endp->CreateClientEndpoint(
        clientConfig->GetClientId(),
        clientConfig->GetInstanceId());

    return {
        clientConfig,
        Config.GetFqdn(),
        service,
        service};
}

TCellHostEndpoint TCellHost::CreateRdmaEndpoint(
    const NClient::TClientAppConfigPtr& clientConfig)
{
    auto& endp = GrpcHostEndpoint;
    auto service = endp->CreateClientEndpoint(
        clientConfig->GetClientId(),
        clientConfig->GetInstanceId());

    return {
        clientConfig,
        Config.GetFqdn(),
        service,
        RdmaHostEndpoint};
}

////////////////////////////////////////////////////////////////////////////////

ICellHostPtr CreateHost(TCellHostConfig config, TBootstrap boorstrap)
{
    return std::make_shared<TCellHost>(std::move(config), std::move(boorstrap));
}

}   // namespace NCloud::NBlockStore::NCells
