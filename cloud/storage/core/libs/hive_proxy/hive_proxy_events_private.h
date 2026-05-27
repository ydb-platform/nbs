#pragma once

#include "public.h"

#include "tablet_boot_info.h"

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/kikimr/components.h>
#include <cloud/storage/core/libs/kikimr/events.h>

#include <contrib/ydb/core/base/blobstorage.h>

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
    // BootExternalCompleted
    //
    // Internal event: TBootRequestActor -> THiveProxyActor.
    // Carries the response received from HIVE (or an error) for a single
    // in-flight boot request. The proxy sends this information to every
    // waiter registered with the tablet ID.

    struct TBootExternalCompleted
    {
        const ui64 Hive;
        const ui64 TabletId;
        // helps to drop a completion that raced with idle teardown / a newer
        // episode for the same tabletId.
        const ui32 InflightGeneration = 0;
        const NProto::TError Error;
        const NKikimr::TTabletStorageInfoPtr StorageInfo;
        const ui32 SuggestedGeneration = 0;
        const TEvHiveProxy::TBootExternalResponse::EBootMode BootMode =
            TEvHiveProxy::TBootExternalResponse::EBootMode::MASTER;
        const ui32 SlaveId = 0;

        TBootExternalCompleted(
            ui64 hive,
            ui64 tabletId,
            ui32 inflightGeneration,
            NProto::TError error)
            : Hive(hive)
            , TabletId(tabletId)
            , InflightGeneration(inflightGeneration)
            , Error(std::move(error))
        {}

        TBootExternalCompleted(
            ui64 hive,
            ui64 tabletId,
            ui32 inflightGeneration,
            NKikimr::TTabletStorageInfoPtr storageInfo,
            ui32 suggestedGeneration,
            TEvHiveProxy::TBootExternalResponse::EBootMode bootMode,
            ui32 slaveId)
            : Hive(hive)
            , TabletId(tabletId)
            , InflightGeneration(inflightGeneration)
            , StorageInfo(std::move(storageInfo))
            , SuggestedGeneration(suggestedGeneration)
            , BootMode(bootMode)
            , SlaveId(slaveId)
        {}
    };

    //
    // BootExternalTimeout
    //
    // Self-scheduled wakeup used by THiveProxyActor to expire waiters of an
    // in-flight boot request. Carries tabletId to look up the inflight entry.
    //

    struct TBootExternalTimeout
    {
        const ui64 Hive;
        const ui64 TabletId;

        TBootExternalTimeout(ui64 hive, ui64 tabletId)
            : Hive(hive)
            , TabletId(tabletId)
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

        EvBootExternalCompleted,
        EvBootExternalTimeout,

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

    using TEvBootExternalCompleted =
        TRequestEvent<TBootExternalCompleted, EvBootExternalCompleted>;
    using TEvBootExternalTimeout =
        TRequestEvent<TBootExternalTimeout, EvBootExternalTimeout>;
};

}   // namespace NCloud::NStorage
