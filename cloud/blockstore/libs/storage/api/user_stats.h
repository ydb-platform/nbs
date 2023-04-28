#pragma once

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

namespace NCloud::NBlockStore::NStorage {

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
        EvBegin = TBlockStoreEvents::USER_STATS_START,

        EvUserStatsProviderCreate,

        EvEnd
    };

    static_assert(
        EvEnd < (int)TBlockStoreEvents::USER_STATS_END,
        "EvEnd expected to be < TBlockStoreEvents::USER_STATS_END");

    using TEvUserStatsProviderCreate = TRequestEvent<
        TUserStatsProviderCreate,
        EvUserStatsProviderCreate
    >;
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeStorageUserStatsId();

}   // NCloud::NBlockStore::NStorage
