#pragma once

#include "public.h"

#include <cloud/blockstore/public/api/protos/throttling.pb.h>
#include <cloud/blockstore/libs/kikimr/events.h>

#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/events.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

struct TEvThrottlingManager
{
    using TThrottlingItems =
        google::protobuf::RepeatedPtrField<NProto::TThrottlingRule>;

    struct TUpdateConfigRequest
    {
        NProto::TThrottlingConfig ThrottlingConfig;
    };

    struct TUpdateConfigResponse
    {
        NProto::TError Error;
    };

    struct TNotifyVolume
    {
        NProto::TThrottlingConfig Config;
    };

    enum EEvents
    {
        EvBegin = EventSpaceBegin(TEvents::ES_USERSPACE),

        EvNotifyVolume,

        EvUpdateConfigRequest,
        EvUpdateConfigResponse,

        EvEnd
    };

    using TEvUpdateConfigRequest = TRequestEvent<TUpdateConfigRequest, EvUpdateConfigRequest>;
    using TEvUpdateConfigResponse = TRequestEvent<TUpdateConfigResponse, EvUpdateConfigResponse>;
    using TEvNotifyVolume = TRequestEvent<TNotifyVolume, EvNotifyVolume>;
};

NActors::TActorId MakeThrottlingManagerServiceId();

}   // namespace NCloud::NBlockStore::NStorage
