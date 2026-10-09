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
        TString ShardId;

        explicit TKeepAliveRequest(TString diskId, TString shardId = {})
            : DiskId(std::move(diskId))
            , ShardId(std::move(shardId))
        {}
    };

    struct TKeepAliveResponse
    {
    };

    enum EEvents
    {
        EvBegin = TBlockStoreEvents::VOLUME_PROXY_START,

        EvKeepAliveRequest = EvBegin + 1,
        EvKeepAliveResponse = EvBegin + 2,

        EvEnd
    };

    static_assert(
        EvEnd < (int)TBlockStoreEvents::VOLUME_PROXY_END,
        "EvEnd expected to be < TBlockStoreEvents::VOLUME_PROXY_END");

    using TEvKeepAliveRequest =
        TRequestEvent<TKeepAliveRequest, EvKeepAliveRequest>;

    using TEvKeepAliveResponse =
        TResponseEvent<TKeepAliveResponse, EvKeepAliveResponse>;
};

NActors::TActorId MakeVolumeProxyServiceId();

}   // namespace NCloud::NBlockStore::NStorage
