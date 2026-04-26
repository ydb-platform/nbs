#pragma once

#include <cloud/storage/core/libs/rdma/iface/client.h>

namespace NCloud::NBlockStore::NStorage {

namespace NRdma = NCloud::NStorage::NRdma;

////////////////////////////////////////////////////////////////////////////////

// TODO thread safety
struct TRdmaEndpointProxy
    : NRdma::IClientEndpointHandler
    , std::enable_shared_from_this<TRdmaEndpointProxy>
{
    const TActorContext& Context;

    THashMap<std::pair<TString, ui32>, NRdma::IClientEndpointPtr> Endpoints;

    TRdmaEndpointProxy(TVector<std::pair<TString, ui32>> endponts)
    {
        for (auto& [host, port]: endpoints) {
            Endpoints.emplace(
                {host, port},
                Client->StartEndpoint(host, port, std::shared_from_this()));
        }
    }

    NRdma::IClientEndpointPtr GetEndpoint(const TString& host, ui32 port)
    {
        if (auto it = Endpoints.find({host, port}); it != Endpoints.end()) {
            return it->second;
        }
        return nullptr;
    }

    void HandleConnected(TString host, ui32 port) override
    {
        if (++Connected == Endpoints.size()) {
            ActorSystem->Send(
                std::make_unique<IEventHandle>(
                    Volume,
                    TActorId(),
                    std::make_unique<TEvVolume::TEvRdmaConnected>()));
        }
    }

    void HandleUnableToConnect(TString host, ui32 port) override
    {
        Endpoints.extract({host, port}).Stop();
        Endpoints.emplace(
            {host, port},
            Client->StartEndpoint(host, port, std::shared_from_this()));
    }

    void HandleDisconnected(TString host, ui32 port) override
    {
        Endpoints.extract({host, port}).Stop();
        Endpoints.emplace(
            {host, port},
            Client->StartEndpoint(host, port, std::shared_from_this()));

        if (Connected-- == Endpoints.size()) {
            ActorSystem->Send(
                std::make_unique<IEventHandle>(
                    Volume,
                    TActorId(),
                    std::make_unique<TEvVolume::TEvRdmaDisconnected>()));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
