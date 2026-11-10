#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TEvVolumeProxy
{
    //
    // keep-alive for base-disk pipe in VolumeProxy
    //

    struct TKeepAliveRequest
    {
        TString DiskId;

        explicit TKeepAliveRequest(TString diskId)
            : DiskId(std::move(diskId))
        {}
    };

    struct TKeepAliveResponse
    {
    };

    //
    // The volume was destroyed, VolumeProxy must resolve the DiskId again.
    //

    struct TForgetVolume
    {
        TString DiskId;

        explicit TForgetVolume(TString diskId)
            : DiskId(std::move(diskId))
        {}
    };

    enum EEvents
    {
        EvBegin = TBlockStoreEvents::VOLUME_PROXY_START,

        EvKeepAliveRequest = EvBegin + 1,
        EvKeepAliveResponse = EvBegin + 2,
        EvForgetVolume = EvBegin + 3,

        EvEnd
    };

    static_assert(
        EvEnd < (int)TBlockStoreEvents::VOLUME_PROXY_END,
        "EvEnd expected to be < TBlockStoreEvents::VOLUME_PROXY_END");

    using TEvKeepAliveRequest =
        TRequestEvent<TKeepAliveRequest, EvKeepAliveRequest>;

    using TEvKeepAliveResponse =
        TResponseEvent<TKeepAliveResponse, EvKeepAliveResponse>;

    using TEvForgetVolume = TRequestEvent<TForgetVolume, EvForgetVolume>;
};

NActors::TActorId MakeVolumeProxyServiceId();

}   // namespace NCloud::NBlockStore::NStorage
