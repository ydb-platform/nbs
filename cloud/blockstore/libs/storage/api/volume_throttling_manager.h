#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/public/api/protos/volume_throttling.pb.h>

#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/events.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

struct TEvVolumeThrottlingManager
{
    struct TUpdateVolumeThrottlingConfigRequest
    {
        NProto::TVolumeThrottlingConfig ThrottlingConfig;
    };

    struct TUpdateVolumeThrottlingConfigResponse
    {
        NProto::TError Error;
    };

    struct TVolumeThrottlingConfigNotification
    {
        NProto::TVolumeThrottlingConfig Config;
    };

    enum EEvents
    {
        EvBegin = EventSpaceBegin(TEvents::ES_USERSPACE),

        EvVolumeThrottlingConfigNotification,

        EvUpdateVolumeThrottlingConfigRequest,
        EvUpdateVolumeThrottlingConfigResponse,

        EvEnd
    };

    using TEvVolumeThrottlingConfigNotification = TRequestEvent<
        TVolumeThrottlingConfigNotification,
        EvVolumeThrottlingConfigNotification>;

    using TEvUpdateVolumeThrottlingConfigRequest = TRequestEvent<
        TUpdateVolumeThrottlingConfigRequest,
        EvUpdateVolumeThrottlingConfigRequest>;
    using TEvUpdateVolumeThrottlingConfigResponse = TRequestEvent<
        TUpdateVolumeThrottlingConfigResponse,
        EvUpdateVolumeThrottlingConfigResponse>;
};

NActors::TActorId MakeVolumeThrottlingManagerServiceId();

}   // namespace NCloud::NBlockStore::NStorage
