#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/service/request.h>

#include <cloud/blockstore/private/api/protos/balancer.pb.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TEvVolumeBalancer
{
    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStoreEvents::VOLUME_BALANCER_START,

        EvConfigureVolumeBalancerRequest = EvBegin + 1,
        EvConfigureVolumeBalancerResponse = EvBegin + 2,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStoreEvents::VOLUME_BALANCER_END,
        "EvEnd expected to be < TBlockStoreEvents::VOLUME_BALANCER_END");

    using TEvConfigureVolumeBalancerRequest = TProtoRequestEvent<
        NPrivateProto::TConfigureVolumeBalancerRequest,
        EvConfigureVolumeBalancerRequest
    >;

    using TEvConfigureVolumeBalancerResponse = TProtoRequestEvent<
        NPrivateProto::TConfigureVolumeBalancerResponse,
        EvConfigureVolumeBalancerResponse
    >;

};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeVolumeBalancerServiceId();

}   // namespace NCloud::NBlockStore::NStorage
