#pragma once

#include "public.h"

#include <cloud/storage/core/libs/kikimr/components.h>
#include <cloud/storage/core/libs/kikimr/events.h>

namespace NCloud::NStorage {

struct TEvUserStats
{
    struct TUserStatsProviderCreate
    {
        IUserMetricsSupplierPtr Provider;

        TUserStatsProviderCreate(IUserMetricsSupplierPtr provider)
            : Provider(provider)
        {}
    };

    enum EEvents
    {
        EvBegin = TStoragePrivateEvents::USER_STATS_START,

        EvUserStatsProviderCreate,

        EvEnd
    };

    static_assert(
        EvEnd < (int)TStoragePrivateEvents::USER_STATS_END,
        "EvEnd expected to be < TStoragePrivateEvents::USER_STATS_END");

    using TEvUserStatsProviderCreate =
        TRequestEvent<TUserStatsProviderCreate, EvUserStatsProviderCreate>;
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeStorageUserStatsId();

}   // namespace NCloud::NStorage
