#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/protos/volume.pb.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_VOLUME_BALANCER_REQUESTS_PRIVATE(xxx, ...) \
    // BLOCKSTORE_VOLUME_BALANCER_REQUESTS_PRIVATE

////////////////////////////////////////////////////////////////////////////////

struct TEvVolumeBalancerPrivate
{
    //
    // RemoteVolumeStats
    //

    struct TRemoteVolumeStats
    {
        TVector<NProto::TVolumeBalancerDiskStats> Stats;
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStorePrivateEvents::VOLUME_BALANCER_START,

        BLOCKSTORE_VOLUME_BALANCER_REQUESTS_PRIVATE(
            BLOCKSTORE_DECLARE_EVENT_IDS)

        EvRemoteVolumeStats,

        EvEnd
    };

    static_assert(
        EvEnd < (int)TBlockStorePrivateEvents::VOLUME_BALANCER_END,
        "EvEnd expected to be < TBlockStorePrivateEvents::VOLUME_BALANCER_END");

    BLOCKSTORE_VOLUME_BALANCER_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENTS)

    using TEvRemoteVolumeStats =
        TRequestEvent<TRemoteVolumeStats, EvRemoteVolumeStats>;
};

}   // namespace NCloud::NBlockStore::NStorage
