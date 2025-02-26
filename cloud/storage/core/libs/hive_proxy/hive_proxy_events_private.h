#pragma once

#include "public.h"

#include "tablet_boot_info.h"

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
    // ReadTabletBootInfoBackup
    //

    struct TReadTabletBootInfoBackupRequest
    {
        ui64 TabletId;

        explicit TReadTabletBootInfoBackupRequest(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

    struct TReadTabletBootInfoBackupResponse
    {
        NKikimr::TTabletStorageInfoPtr StorageInfo;
        ui32 SuggestedGeneration;

        TReadTabletBootInfoBackupResponse() = default;

        TReadTabletBootInfoBackupResponse(
                NKikimr::TTabletStorageInfoPtr storageInfo,
                ui32 suggestedGeneration)
            : StorageInfo(std::move(storageInfo))
            , SuggestedGeneration(suggestedGeneration)
        {}
    };

    //
    // UpdateTabletBootInfoBackup
    //

    struct TUpdateTabletBootInfoBackupRequest
    {
        NKikimr::TTabletStorageInfoPtr StorageInfo;
        ui32 SuggestedGeneration;

        TUpdateTabletBootInfoBackupRequest(
                NKikimr::TTabletStorageInfoPtr storageInfo,
                ui32 suggestedGeneration)
            : StorageInfo(std::move(storageInfo))
            , SuggestedGeneration(suggestedGeneration)
        {}
    };

    //
    // ListTabletBootInfoBackups
    //

    struct TListTabletBootInfoBackupsRequest
    {
    };

    struct TListTabletBootInfoBackupsResponse
    {
        TVector<TTabletBootInfo> TabletBootInfos;

        explicit TListTabletBootInfoBackupsResponse(
                TVector<TTabletBootInfo> tabletBootInfos)
            : TabletBootInfos(std::move(tabletBootInfos))
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

        EvReadTabletBootInfoBackupRequest,
        EvReadTabletBootInfoBackupResponse,
        EvUpdateTabletBootInfoBackupRequest,

        EvEnd
    };


    static_assert(EvEnd < (int)TStoragePrivateEvents::HIVE_PROXY_END,
        "EvEnd expected to be < TStoragePrivateEvents::HIVE_PROXY_END");

    using TEvRequestFinished = TRequestEvent<TRequestFinished, EvRequestFinished>;
    using TEvChangeTabletClient = TRequestEvent<TChangeTabletClient, EvChangeTabletClient>;
    using TEvSendTabletMetrics = TRequestEvent<TSendTabletMetrics, EvSendTabletMetrics>;

    using TEvReadTabletBootInfoBackupRequest = TRequestEvent<
        TReadTabletBootInfoBackupRequest, EvReadTabletBootInfoBackupRequest>;
    using TEvReadTabletBootInfoBackupResponse = TResponseEvent<
        TReadTabletBootInfoBackupResponse, EvReadTabletBootInfoBackupResponse>;
    using TEvUpdateTabletBootInfoBackupRequest = TRequestEvent<
        TUpdateTabletBootInfoBackupRequest, EvUpdateTabletBootInfoBackupRequest>;
};

}   // namespace NCloud::NStorage
