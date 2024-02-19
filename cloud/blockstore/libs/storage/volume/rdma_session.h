#pragma once

#include <cloud/blockstore/libs/rdma/iface/client.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TEndpoint
    : IClientEndpointHandler
    , std::enable_shared_from_this<TClientEndpointHandler>
{
    std::weak_ptr<TRdmaSession> Session;
    NRdma::IClientEndpointPtr Pointer;
    std::atomic<EEndpointStatus> Status = DISCONNECTED;

    TClientEndpointHandler(
        TString host
        ui32 port,
        std::shared_ptr<TRdmaSession> session)
    {
        Session = std::move(session);
        Pointer = Client->StartEndpoint(
            std::move(host),
            port,
            shared_from_this());
    }

    void UpdateStatus(NRdma::EEndpointStatus status)
    {
        if (auto session = Session.lock()) {
            session.UpdateStatus(status);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// Persistent multi-endpoint session. Sends status update to the volume if all
// endpoints are up or any one is down
class TRdmaSession
    : std::enable_shared_from_this<TRdmaSession>
{
private:
    const TActorContext& Context;
    const TActorId& Volume;
    const NRdma::IRdmaClientPtr Client;

    TMap<std::pair<TString, ui32>, std::shared_ptr<TEndpoint>> Endpoints;
    std::atomic<ui64> Connected = 0;

public:
    TRdmaSession(
            const TActorContext &context,
            const TActorId& volume,
            NRdma::IRdmaClientPtr client)
        : Context(context)
        , Volume(volume)
        , Client(std::move(client))
    {}

    IClientEndpointPtr GetEndpoint(
        TString host,
        ui32 port)
    {
        TGuard<TAtomicLock> guard(Lock);
        auto& endpoint = Endpoints[{host, port}];

        if (endpoint == nullptr) {
            endpoint = std::make_shared<TClientEndpointHandler>(
                std::move(host),
                port,
                shared_from_this());
        }
        if (endpoint->Status == EEndpointStatus::Connected) {
            return endpoint->Pointer;
        }
        return nullptr;
    }

    void SendUpdate(NRdma::EEndpointStatus status)
    {
        NCloud::Send(
            Session.Context,
            Session.Volume,
            std::make_unique<TEvVolume::TEvUpdateRdmaStatus>(status));
    };

    void UpdateStatus(NRdma::EEndpointStatus endpointStatus)
    {
        switch (status) {
            case CONNECTED:
                if (++Disconnected == 1) {
                    SendUpdate(DISCONNECTED);
                }
                break;

            case DISCONNECTED:
                for (auto d = Disconnected; d > 0; ) {
                    if (Disconnected.atomic_compare_exchange_weak(d, d - 1)) {
                        break;
                    }
                }
                if (d == 1) {
                    SendUpdate(CONNECTED);
                }
                break;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

using TRdmaSessionPtr = std::shared_ptr<TRdmaSession>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage