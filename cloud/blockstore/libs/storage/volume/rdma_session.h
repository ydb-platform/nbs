#pragma once

#include <cloud/blockstore/libs/rdma/iface/client.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TEndpointObserver
    : IEndpointObserver()
    , std::enable_shared_from_this<TEndpointObserver>
{
    std::weak_ptr<TRdmaSession> Session;
    NRdma::IClientEndpointPtr Endpoint;
    std::atomic<EEndpointStatus> Status = EEndpointStatus::Undefined;

    TEndpointObserver(std::shared_ptr<TRdmaSession> session)
    {
        Pointer = Client->StartEndpoint(host, port, shared_from_this());
        Session = session;
    }

    void UpdateStatus(NRdma::EEndpointStatus status)
    {
        auto old = Status.exchange(status);
        if (auto session = Session.lock()) {
            session.UpdateStatus(old, status);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// Persistent multi-endpoint session. If initial connection to any endpoint Once the connection to all endpoints is
// established, it will keep them up forever. If initial connection
class TRdmaSession
    : std::enable_shared_from_this<TRdmaSession>
{
    const TActorContext& Context;
    const TActorId& Volume;
    const NRdma::IRdmaClientPtr Client;
    const ui32 Port;

    TMap<TString, std::shared_ptr<TEndpointObserver>> Endpoints;

    TRdmaSession(
            const TActorContext &context,
            const TActorId& volume,
            NRdma::IRdmaClientPtr client,
            TVector<TString> endpoints,
            ui32 port)
        : Context(context)
        , Volume(volume)
        , Client(std::move(client))
        , Port(port)
    {
        for (auto& host: hosts) {
            if (auto& endpoint = Endpoints[host]; endpoint == nullptr) {
                endpoint = std::make_shared<TEndpointObserver>(Session);
            }
        }
    }

    ResultOrError<NRdma::IClientEndpointPtr> GetEndpoint(TString host) {
        if (auto it = Endpoints.find(host); it != Endpoints.end()) {
            if (it->second && it->second->Status == EEndpointStatus::Connected) {
                return it->second->Pointer;
            }
            return MakeError(E_RDMA_UNAVAILABLE);
        }
    }

    void UpdateStatus(NRdma::EEndpointStatus status)
    {
        NCloud::Send(
            Session.Context,
            Session.Volume,
            std::make_unique<TEvVolume::TEvUpdateRdmaStatus>(status));
    };

    void UpdateStatus(NRdma::EEndpointStatus prev, NRdma::EEndpointStatus next)
    {
        if (prev == UNDEFINED && next == CONNECTED) {
            if (++Connected == Endpoints.size()) {
                UpdateStatus(CONNECTED);
            }
        }
        if (prev == UNDEFINED && next == DISCONNECTED) {
            if (++Disconnected == 1) {
                UpdateStatus(DISCONNECTED);
            }
        }
        if (prev == CONNECTED && next == DISCONNECTED) {
            if (++Disconnected == 1) {
                UpdateStatus(DISCONNECTED);
            }
            --Connected;
        }
        if (prev == DISCONNECTED && next == CONNECTED) {
            if (++Connected == Endpoints.size()) {
                UpdateStatus(CONNECTED);
            }
            --Disconnected;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

using TRdmaSessionPtr = std::shared_ptr<TRdmaSession>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage