#pragma once


#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TEvStatsServicePrivate
{
    //
    // UploadDisksStats notification
    //

    struct TUploadDisksStats
    {
    };

    //
    // UploadDisksStatsCompleted notification
    //

    struct TUploadDisksStatsCompleted
    {
    };

    //
    // YdbRetryTimeout
    //

    struct TStatsUploadRetryTimeout
    {
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStorePrivateEvents::STATS_SERVICE_START,

        EvUploadDisksStats,
        EvUploadDisksStatsCompleted,
        EvStatsUploadRetryTimeout,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStorePrivateEvents::STATS_SERVICE_END,
        "EvEnd expected to be < TBlockStorePrivateEvents::SERVICE_END");

    using TEvUploadDisksStats = TRequestEvent<
        TUploadDisksStats,
        EvUploadDisksStats
    >;

    using TEvUploadDisksStatsCompleted = TResponseEvent<
        TUploadDisksStatsCompleted,
        EvUploadDisksStatsCompleted
    >;

    using TEvStatsUploadRetryTimeout = TRequestEvent<
        TStatsUploadRetryTimeout,
        EvStatsUploadRetryTimeout
    >;
};

}   // namespace NCloud::NBlockStore::NStorage
