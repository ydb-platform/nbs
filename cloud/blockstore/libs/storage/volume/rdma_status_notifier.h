#pragma once

#include <cloud/blockstore/libs/storage/api/volume.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/rdma/iface/client.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TRdmaStatusNotifier
    : public NCloud::NStorage::NRdma::IClientEndpointHandler
{
private:
    const NActors::TActorContext& Context;
    const NActors::TActorId Parent;

public:
    TRdmaStatusNotifier(
        const NActors::TActorContext& context,
        const NActors::TActorId parent)
        : Context(context)
        , Parent(parent)
    {}

    void HandleConnected() override
    {
        NCloud::Send(
            Context,
            Parent,
            std::make_unique<TEvVolume::TEvRdmaConnected>());
    }

    void HandleUnavailable() override
    {
        NCloud::Send(
            Context,
            Parent,
            std::make_unique<TEvVolume::TEvRdmaUnavailable>());
    }

    void HandleDisconnected() override
    {
        HandleUnavailable();
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
