#pragma once

#include "public.h"

#include <cloud/storage/core/libs/hive_proxy/tablet_boot_info.h>
#include <cloud/storage/core/libs/kikimr/components.h>
#include <cloud/storage/core/libs/kikimr/events.h>

#include <contrib/ydb/core/base/hive.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/vector.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define STORAGE_HIVE_PROXY_REQUESTS(xxx, ...)                                  \
    xxx(LockTablet,     __VA_ARGS__)                                           \
    xxx(UnlockTablet,   __VA_ARGS__)                                           \
    xxx(GetStorageInfo, __VA_ARGS__)                                           \
    xxx(BootExternal,   __VA_ARGS__)                                           \
    xxx(ReassignTablet, __VA_ARGS__)                                           \
    xxx(CreateTablet,   __VA_ARGS__)                                           \
    xxx(LookupTablet,   __VA_ARGS__)                                           \
    xxx(DrainNode,      __VA_ARGS__)                                           \
                                                                               \
    xxx(BackupTabletBootInfos,     __VA_ARGS__)                                \
    xxx(ListTabletBootInfoBackups, __VA_ARGS__)                                \
// STORAGE_HIVE_PROXY_REQUESTS

////////////////////////////////////////////////////////////////////////////////

struct TEvHiveProxy
{
    //
    // LockTablet
    //

    struct TLockTabletRequest
    {
        const ui64 TabletId;

        explicit TLockTabletRequest(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

    struct TLockTabletResponse
    {
    };

    //
    // UnlockTablet
    //

    struct TUnlockTabletRequest
    {
        const ui64 TabletId;

        explicit TUnlockTabletRequest(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

    struct TUnlockTabletResponse
    {
    };

    //
    // GetStorageInfo
    //

    struct TGetStorageInfoRequest
    {
        const ui64 TabletId;

        explicit TGetStorageInfoRequest(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

    struct TGetStorageInfoResponse
    {
        const NKikimr::TTabletStorageInfoPtr StorageInfo;

        TGetStorageInfoResponse() = default;

        explicit TGetStorageInfoResponse(
                NKikimr::TTabletStorageInfoPtr storageInfo)
            : StorageInfo(std::move(storageInfo))
        {}
    };

    //
    // BootExternal
    //

    struct TBootExternalRequest
    {
        const ui64 TabletId;
        const TDuration RequestTimeout;

        explicit TBootExternalRequest(ui64 tabletId)
            : TabletId(tabletId)
        {}

        TBootExternalRequest(ui64 tabletId, TDuration requestTimeout)
            : TabletId(tabletId)
            , RequestTimeout(requestTimeout)
        {}
    };

    struct TBootExternalResponse
    {
        enum class EBootMode
        {
            MASTER,
            SLAVE,
        };

        const NKikimr::TTabletStorageInfoPtr StorageInfo;
        const ui32 SuggestedGeneration = 0;
        const EBootMode BootMode = EBootMode::MASTER;
        const ui32 SlaveId = 0;

        TBootExternalResponse() = default;

        TBootExternalResponse(
                NKikimr::TTabletStorageInfoPtr storageInfo,
                ui32 suggestedGeneration,
                EBootMode bootMode,
                ui32 slaveId)
            : StorageInfo(std::move(storageInfo))
            , SuggestedGeneration(suggestedGeneration)
            , BootMode(bootMode)
            , SlaveId(slaveId)
        {}
    };

    //
    // ReassignTablet
    //

    struct TReassignTabletRequest
    {
        const ui64 TabletId;
        const TVector<ui32> Channels;

        TReassignTabletRequest(
                ui64 tabletId,
                TVector<ui32> channels)
            : TabletId(tabletId)
            , Channels(std::move(channels))
        {}
    };

    struct TReassignTabletResponse
    {
    };

    //
    // CreateTablet
    //

    struct TCreateTabletRequest
    {
        const ui64 HiveId;
        const NKikimrHive::TEvCreateTablet Request;

        TCreateTabletRequest(
                ui64 hiveId,
                NKikimrHive::TEvCreateTablet request)
            : HiveId(hiveId)
            , Request(std::move(request))
        {}
    };

    struct TCreateTabletResponse
    {
        const ui64 TabletId;

        explicit TCreateTabletResponse(ui64 tabletId = 0)
            : TabletId(tabletId)
        {}
    };

    //
    // LookupTablet
    //

    struct TLookupTabletRequest
    {
        const ui64 HiveId;
        const ui64 Owner;
        const ui64 OwnerIdx;

        TLookupTabletRequest(
                ui64 hiveId,
                ui64 owner,
                ui64 ownerIdx)
            : HiveId(hiveId)
            , Owner(owner)
            , OwnerIdx(ownerIdx)
        {}
    };

    struct TLookupTabletResponse
    {
        const ui64 TabletId;

        explicit TLookupTabletResponse(ui64 tabletId = 0)
            : TabletId(tabletId)
        {}
    };

    //
    // TabletLockLost notification
    //

    struct TTabletLockLost
    {
        const ui64 TabletId;

        explicit TTabletLockLost(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

    //
    // Drain current node
    //

    struct TDrainNodeRequest
    {
        const bool KeepDown;

        explicit TDrainNodeRequest(bool keepDown)
            : KeepDown(keepDown)
        {}
    };

    struct TDrainNodeResponse
    {
    };

    //
    // BackupTabletBootInfos
    //

    struct TBackupTabletBootInfosRequest
    {
    };

    struct TBackupTabletBootInfosResponse
    {
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
                TVector<TTabletBootInfo> tabletBootInfos = {})
            : TabletBootInfos(std::move(tabletBootInfos))
        {}
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TStorageEvents::HIVE_PROXY_START,

        EvLockTabletRequest = EvBegin + 1,
        EvLockTabletResponse = EvBegin + 2,

        EvUnlockTabletRequest = EvBegin + 3,
        EvUnlockTabletResponse = EvBegin + 4,

        EvGetStorageInfoRequest = EvBegin + 5,
        EvGetStorageInfoResponse = EvBegin + 6,

        EvBootExternalRequest = EvBegin + 7,
        EvBootExternalResponse = EvBegin + 8,

        EvReassignTabletRequest = EvBegin + 9,
        EvReassignTabletResponse = EvBegin + 10,

        EvTabletLockLost = EvBegin + 11,

        EvCreateTabletRequest = EvBegin + 12,
        EvCreateTabletResponse = EvBegin + 13,

        EvLookupTabletRequest = EvBegin + 14,
        EvLookupTabletResponse = EvBegin + 15,

        EvDrainNodeRequest = EvBegin + 16,
        EvDrainNodeResponse = EvBegin + 17,

        EvBackupTabletBootInfosRequest = EvBegin + 18,
        EvBackupTabletBootInfosResponse = EvBegin + 19,

        EvListTabletBootInfoBackupsRequest = EvBegin + 20,
        EvListTabletBootInfoBackupsResponse = EvBegin + 21,

        EvEnd
    };

    static_assert(EvEnd < (int)TStorageEvents::HIVE_PROXY_END,
        "EvEnd expected to be < TStorageEvents::HIVE_PROXY_END");

    STORAGE_HIVE_PROXY_REQUESTS(STORAGE_DECLARE_EVENTS)

    using TEvTabletLockLost = TResponseEvent<TTabletLockLost, EvTabletLockLost>;
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeHiveProxyServiceId();

}   // namespace NCloud::NStorage
