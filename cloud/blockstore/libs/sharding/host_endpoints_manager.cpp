#include "host_endpoints_manager.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/rdma/impl/client.h>
#include <cloud/blockstore/libs/rdma/impl/verbs.h>

namespace NCloud::NBlockStore::NSharding {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TResultOrError<THostEndpoint> THostEndpointsManager::GetHostEndpoint(
    const NClient::TClientAppConfigPtr& clientConfig,
    std::optional<NProto::EShardDataTransport> desiredTransport,
    bool allowGrpcFallback)
{
    auto transport =
        desiredTransport.has_value() ? *desiredTransport : Config.GetTransport();
    Y_ENSURE(transport != NProto::UNSET);

    with_lock(StateLock) {
        if (!IsReady(transport)) {
            if (transport != NProto::GRPC) {
                if (allowGrpcFallback && IsReady(NProto::GRPC)) {
                    transport = NProto::GRPC;
                } else {
                    transport = NProto::UNSET;
                }
            } else {
                transport = NProto::UNSET;
            }
        }

        if (transport != NProto::UNSET) {
            switch (transport) {
                case NProto::GRPC:
                    return CreateGrpcEndpoint(clientConfig);
                case NProto::RDMA:
                    return CreateRdmaEndpoint(clientConfig);
                default:
                    return MakeError(
                        E_INVALID_STATE,
                        TStringBuilder() <<
                            "Unsupportted transport type " <<
                            static_cast<int>(transport));
            }
        }

        return MakeError(
            E_REJECTED,
            TStringBuilder() << "No transport available");
    }
}

bool THostEndpointsManager::IsReady(NProto::EShardDataTransport transport)
{
    switch (transport) {
        case NProto::GRPC: {
            return GrpcState == EEndpointState::ACTIVE;
        }
        case NProto::RDMA: {
            return RdmaState == EEndpointState::ACTIVE;
        }
        default:
            return false;
    }
    return false;
}

TFuture<void> THostEndpointsManager::Start()
{
    auto transport = Config.GetTransport();
    Y_ENSURE(transport != NProto::UNSET && transport != NProto::NBD);

    auto weak = weak_from_this();
    TSetupEndpointFuture future;
    EEndpointState& state = transport == NProto::GRPC ? GrpcState: RdmaState;

    with_lock(StateLock) {
        if (transport == NProto::GRPC) {
            GrpcState = EEndpointState::INITIALIZING;
            auto future = SetupHostGrpcEndpoint();
        } else {
            RdmaState = EEndpointState::INITIALIZING;
            auto future = SetupHostRdmaEndpoint();
        }
    }

    future.Subscribe([=, weak = std::move(weak)] (const auto& f) mutable {
        Y_UNUSED(f);
        if (auto self = weak.lock(); self) {
            with_lock(self->StateLock) {
                self->SetEndpointState(state,EEndpointState::ACTIVE);
            }
        }
    });

    return future;
}

void THostEndpointsManager::SetEndpointState(EEndpointState& state, EEndpointState value)
{
    with_lock(StateLock) {
        state = value;
    }
}

TFuture<void> THostEndpointsManager::Stop()
{
    return {};
}

THostEndpointsManager::TSetupEndpointFuture THostEndpointsManager::SetupHostGrpcEndpoint()
{
    auto endpoint = CreateMultiClientEndpoint(
        Args.GrpcClient,
        Config.GetFqdn(),
        Config.GetGrpcPort(),
        false);

    auto promise = NThreading::NewPromise<void>();
    promise.SetValue();
    return promise.GetFuture() ;
}

THostEndpointsManager::TSetupEndpointFuture THostEndpointsManager::SetupHostRdmaEndpoint()
{
    auto endpoint = CreateMultiClientEndpoint(
        Args.GrpcClient,
        Config.GetFqdn(),
        Config.GetGrpcPort(),
        false);

    NClient::TRdmaEndpointConfig rdmaEndpoint {
        .Address = Config.GetFqdn(),
        .Port = Config.GetRdmaPort(),
    };

    auto future = CreateRdmaEndpointClientAsync(
        Args.Logging,
        Args.RdmaClient,
        endpoint,
        rdmaEndpoint);

    auto promise = NewPromise<void>();
    future.Subscribe([=] (const auto& future) mutable {
        Y_UNUSED(future);
        promise.SetValue();
    });

    return promise.GetFuture();
}

THostEndpoint THostEndpointsManager::CreateGrpcEndpoint(
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

THostEndpoint THostEndpointsManager::CreateRdmaEndpoint(
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


}   // namespace NCloud::NBlockStore::NSharding
