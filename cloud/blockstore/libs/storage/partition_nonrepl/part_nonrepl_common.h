#pragma once

#include "public.h"

#include "config.h"

#include <cloud/blockstore/libs/storage/api/volume.h>

#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <library/cpp/actors/core/actorsystem.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

inline void SendEvReacquireDisk(
    const NActors::TActorContext& ctx,
    const NActors::TActorId& recipient)
{
    NCloud::Send(
        ctx,
        recipient,
        std::make_unique<TEvVolume::TEvReacquireDisk>()
    );
}

inline void SendEvReacquireDisk(
    NActors::TActorSystem& system,
    const NActors::TActorId& recipient)
{
    TAutoPtr<NActors::IEventHandle> event(
        new NActors::IEventHandle(
            recipient,
            {},
            new TEvVolume::TEvReacquireDisk(),
            0,
            0));

    system.Send(event);
}

template <typename TSystem>
void ProcessError(
    TSystem& system,
    TNonreplicatedPartitionConfig& config,
    NProto::TError& error)
{
    if (error.GetCode() == E_BS_INVALID_SESSION) {
        SendEvReacquireDisk(system, config.GetParentActorId());

        error.SetCode(E_REJECTED);
    }

    if (error.GetCode() == E_IO) {
        error = config.MakeIOError(std::move(error.GetMessage()), true);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
