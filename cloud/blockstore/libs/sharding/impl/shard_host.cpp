#include "shard_host.h"

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

bool THostEndpointsManager::IsReady(NProto::EShardDataTransport transport) const
{
    switch (transport) {
        case NProto::GRPC: {
            return GrpcState == EState::ACTIVE;
        }
        case NProto::RDMA: {
            return RdmaState == EState::ACTIVE;
        }
        default:
            return false;
    }
    return false;
}

TFuture<void> THostEndpointsManager::Start()
{
    auto target = Config.GetTransport();
    Y_ENSURE(target != NProto::UNSET && target != NProto::NBD);

    auto weak = weak_from_this();
    IHostEndpointsSetupProvider::TSetupGrpcEndpointFuture future;

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

void THostEndpointsManager::HandleRdmaSetupResult(
    const IHostEndpointsSetupProvider::TRdmaResult& result)
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

bool THostEndpointsManager::SetupRdmaIfNeeded()
{
    if (Config.GetTransport() != NProto::RDMA) {
        return false;
    }
    RdmaState = EState::ACTIVATING;
    RdmaFuture = Args.EndpointsSetup->SetupHostRdmaEndpoint(
        Args,
        Config,
        GrpcHostEndpoint);
    return true;
}

TFuture<void> THostEndpointsManager::Stop()
{
    return {};
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

////////////////////////////////////////////////////////////////////////////////

IHostEndpointsManagerPtr CreateHostEndpointsManager(
    TShardHostConfig config,
    TShardingArguments args)
{
    return std::make_shared<THostEndpointsManager>(
        std::move(config),
        std::move(args));
}

}   // namespace NCloud::NBlockStore::NSharding
