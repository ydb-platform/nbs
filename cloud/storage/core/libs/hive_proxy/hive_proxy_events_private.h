#pragma once

#include "public.h"

#include <cloud/storage/core/libs/kikimr/components.h>
#include <cloud/storage/core/libs/kikimr/events.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TEvHiveProxyPrivate
{
    //
    // RequestFinished
    //

    struct TRequestFinished
    {
        const ui64 TabletId;

        explicit TRequestFinished(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

    //
    // ChangeTabletClient
    //

    struct TChangeTabletClient
    {
        const NActors::TActorId ClientId;

        explicit TChangeTabletClient(NActors::TActorId clientId)
            : ClientId(clientId)
        {}
    };

    //
    // SendTabletMetrics
    //

    struct TSendTabletMetrics
    {
        const ui64 Hive;

        explicit TSendTabletMetrics(ui64 hive)
            : Hive(hive)
        {}
    };

    //
    // ReadTabletBootInfoCache
    //

    struct TReadTabletBootInfoCacheRequest
    {
        ui64 TabletId;

        explicit TReadTabletBootInfoCacheRequest(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

    struct TReadTabletBootInfoCacheResponse
    {
        NKikimr::TTabletStorageInfoPtr StorageInfo;
        ui32 SuggestedGeneration;

        TReadTabletBootInfoCacheResponse() = default;

        TReadTabletBootInfoCacheResponse(
                NKikimr::TTabletStorageInfoPtr storageInfo,
                ui32 suggestedGeneration)
            : StorageInfo(std::move(storageInfo))
            , SuggestedGeneration(suggestedGeneration)
        {}
    };

    //
    // UpdateTabletBootInfoCache
    //

    struct TUpdateTabletBootInfoCacheRequest
    {
        NKikimr::TTabletStorageInfoPtr StorageInfo;
        ui32 SuggestedGeneration;

        TUpdateTabletBootInfoCacheRequest(
                NKikimr::TTabletStorageInfoPtr storageInfo,
                ui32 suggestedGeneration)
            : StorageInfo(std::move(storageInfo))
            , SuggestedGeneration(suggestedGeneration)
        {}
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TStoragePrivateEvents::HIVE_PROXY_START,

        EvRequestFinished,
        EvChangeTabletClient,
        EvSendTabletMetrics,

        EvReadTabletBootInfoCacheRequest,
        EvReadTabletBootInfoCacheResponse,
        EvUpdateTabletBootInfoCacheRequest,

        EvEnd
    };


    static_assert(EvEnd < (int)TStoragePrivateEvents::HIVE_PROXY_END,
        "EvEnd expected to be < TStoragePrivateEvents::HIVE_PROXY_END");

    using TEvRequestFinished = TRequestEvent<TRequestFinished, EvRequestFinished>;
    using TEvChangeTabletClient = TRequestEvent<TChangeTabletClient, EvChangeTabletClient>;
    using TEvSendTabletMetrics = TRequestEvent<TSendTabletMetrics, EvSendTabletMetrics>;

    using TEvReadTabletBootInfoCacheRequest = TRequestEvent<
        TReadTabletBootInfoCacheRequest, EvReadTabletBootInfoCacheRequest>;
    using TEvReadTabletBootInfoCacheResponse = TResponseEvent<
        TReadTabletBootInfoCacheResponse, EvReadTabletBootInfoCacheResponse>;
    using TEvUpdateTabletBootInfoCacheRequest = TRequestEvent<
        TUpdateTabletBootInfoCacheRequest, EvUpdateTabletBootInfoCacheRequest>;
};

}   // namespace NCloud::NStorage
