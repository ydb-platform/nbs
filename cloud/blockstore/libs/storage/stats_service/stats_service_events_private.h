#pragma once

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_STATS_SERVICE_REQUESTS_PRIVATE(xxx, ...) \
    xxx(RegisterTrafficSource, __VA_ARGS__)                 \
// BLOCKSTORE_STATS_SERVICE_REQUESTS_PRIVATE

////////////////////////////////////////////////////////////////////////////////

// The delay with which the sources of background operations are periodically
// registered (migrations and fillings of shadow disks).
constexpr auto RegisterBackgroundTrafficDuration = TDuration::Seconds(1);

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
    // CleanupBackgroundSources
    //

    struct TCleanupBackgroundSources
    {
    };

    //
    // RegisterTrafficSource
    //

    struct TRegisterTrafficSourceRequest
    {
        TString SourceId;

        // The maximum bandwidth allowed for the source.
        ui32 BandwidthMiBs = 0;
    };

    struct TRegisterTrafficSourceResponse
    {
        // The maximum bandwidth for the source, limited taking into account all
        // sources of background operations and the limit on the entire server.
        ui32 LimitedBandwidthMiBs = 0;
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStorePrivateEvents::STATS_SERVICE_START,

        BLOCKSTORE_STATS_SERVICE_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENT_IDS)

        EvUploadDisksStats,
        EvUploadDisksStatsCompleted,
        EvStatsUploadRetryTimeout,
        EvCleanupBackgroundSources,

        EvEnd
    };

    static_assert(
        EvEnd < (int)TBlockStorePrivateEvents::STATS_SERVICE_END,
        "EvEnd expected to be < TBlockStorePrivateEvents::SERVICE_END");

    BLOCKSTORE_STATS_SERVICE_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENTS)

    using TEvUploadDisksStats =
        TRequestEvent<TUploadDisksStats, EvUploadDisksStats>;

    using TEvUploadDisksStatsCompleted =
        TResponseEvent<TUploadDisksStatsCompleted, EvUploadDisksStatsCompleted>;

    using TEvStatsUploadRetryTimeout =
        TRequestEvent<TStatsUploadRetryTimeout, EvStatsUploadRetryTimeout>;

    using TEvCleanupBackgroundSources =
        TRequestEvent<TCleanupBackgroundSources, EvCleanupBackgroundSources>;
};

}   // namespace NCloud::NBlockStore::NStorage
