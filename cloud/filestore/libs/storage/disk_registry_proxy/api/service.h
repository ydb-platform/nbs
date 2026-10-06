#pragma once

#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/events.h>

#include <cloud/filestore/private/api/protos/fastshard.pb.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/protos/media.pb.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_DISK_REGISTRY_PROXY_REQUESTS(xxx, ...)                       \
    xxx(AllocateDevices,    __VA_ARGS__)                                       \
    xxx(DescribeDevices,    __VA_ARGS__)                                       \
    xxx(MarkForCleanup,     __VA_ARGS__)                                       \
    xxx(DeallocateDevices,  __VA_ARGS__)                                       \
    xxx(FinishRepair,       __VA_ARGS__)                                       \
    xxx(FinishMigration,    __VA_ARGS__)                                       \
    xxx(ReplaceDevice,      __VA_ARGS__)                                       \
// FILESTORE_DISK_REGISTRY_PROXY_REQUESTS

////////////////////////////////////////////////////////////////////////////////

struct TEvDiskRegistryProxy
{
    struct TDeviceMigration
    {
        TString SourceUUID;
        NProtoPrivate::TStorageDevice Target;
    };

    struct TDeviceLayout
    {
        // The main replica first.
        TVector<TVector<NProtoPrivate::TStorageDevice>> Replicas;
        TVector<TDeviceMigration> Migrations;
        TVector<TString> ReplacementDeviceUUIDs;
        // Filled by AllocateDevices only.
        TVector<TString> UnavailableDeviceUUIDs;
    };

    //
    // AllocateDevices
    //

    struct TAllocateDevicesRequest
    {
        const TString FileSystemId;
        const ui64 TabletId;
        const TString CloudId;
        const TString FolderId;
        const NCloud::NProto::EStorageMediaKind MediaKind;
        const ui32 ReplicaCount;
        const ui64 DeviceBlocksCount;
        const TString DevicePoolName;

        TAllocateDevicesRequest(
                TString fileSystemId,
                ui64 tabletId,
                TString cloudId,
                TString folderId,
                NCloud::NProto::EStorageMediaKind mediaKind,
                ui32 replicaCount,
                ui64 deviceBlocksCount,
                TString devicePoolName)
            : FileSystemId(std::move(fileSystemId))
            , TabletId(tabletId)
            , CloudId(std::move(cloudId))
            , FolderId(std::move(folderId))
            , MediaKind(mediaKind)
            , ReplicaCount(replicaCount)
            , DeviceBlocksCount(deviceBlocksCount)
            , DevicePoolName(std::move(devicePoolName))
        {}
    };

    struct TAllocateDevicesResponse
    {
        const TDeviceLayout Layout;

        TAllocateDevicesResponse() = default;

        TAllocateDevicesResponse(TDeviceLayout layout)
            : Layout(std::move(layout))
        {}
    };

    //
    // DescribeDevices
    //

    struct TDescribeDevicesRequest
    {
        const TString FileSystemId;

        TDescribeDevicesRequest(TString fileSystemId)
            : FileSystemId(std::move(fileSystemId))
        {}
    };

    struct TDescribeDevicesResponse
    {
        const TDeviceLayout Layout;

        TDescribeDevicesResponse() = default;

        TDescribeDevicesResponse(TDeviceLayout layout)
            : Layout(std::move(layout))
        {}
    };

    //
    // MarkForCleanup
    //

    struct TMarkForCleanupRequest
    {
        const TString FileSystemId;
        const ui64 TabletId;

        TMarkForCleanupRequest(TString fileSystemId, ui64 tabletId)
            : FileSystemId(std::move(fileSystemId))
            , TabletId(tabletId)
        {}
    };

    struct TMarkForCleanupResponse
    {
    };

    //
    // DeallocateDevices
    //

    struct TDeallocateDevicesRequest
    {
        const TString FileSystemId;
        const ui64 TabletId;

        TDeallocateDevicesRequest(TString fileSystemId, ui64 tabletId)
            : FileSystemId(std::move(fileSystemId))
            , TabletId(tabletId)
        {}
    };

    struct TDeallocateDevicesResponse
    {
    };

    //
    // FinishRepair
    //

    struct TFinishRepairRequest
    {
        const TString FileSystemId;
        const TString DeviceUUID;

        TFinishRepairRequest(TString fileSystemId, TString deviceUUID)
            : FileSystemId(std::move(fileSystemId))
            , DeviceUUID(std::move(deviceUUID))
        {}
    };

    struct TFinishRepairResponse
    {
    };

    //
    // FinishMigration
    //

    // A set ReplicaIndex selects the replica disk <fs>/<index>, an unset one
    // the disk <fs> itself.
    struct TFinishMigrationRequest
    {
        const TString FileSystemId;
        const std::optional<ui32> ReplicaIndex;
        const TString SourceUUID;
        const TString TargetUUID;

        TFinishMigrationRequest(
                TString fileSystemId,
                std::optional<ui32> replicaIndex,
                TString sourceUUID,
                TString targetUUID)
            : FileSystemId(std::move(fileSystemId))
            , ReplicaIndex(replicaIndex)
            , SourceUUID(std::move(sourceUUID))
            , TargetUUID(std::move(targetUUID))
        {}
    };

    struct TFinishMigrationResponse
    {
    };

    //
    // ReplaceDevice
    //

    struct TReplaceDeviceRequest
    {
        const TString FileSystemId;
        const std::optional<ui32> ReplicaIndex;
        const TString DeviceUUID;

        TReplaceDeviceRequest(
                TString fileSystemId,
                std::optional<ui32> replicaIndex,
                TString deviceUUID)
            : FileSystemId(std::move(fileSystemId))
            , ReplicaIndex(replicaIndex)
            , DeviceUUID(std::move(deviceUUID))
        {}
    };

    struct TReplaceDeviceResponse
    {
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TFileStoreEvents::DISK_REGISTRY_PROXY_START,

        EvAllocateDevicesRequest = EvBegin + 1,
        EvAllocateDevicesResponse = EvBegin + 2,

        EvDescribeDevicesRequest = EvBegin + 3,
        EvDescribeDevicesResponse = EvBegin + 4,

        EvMarkForCleanupRequest = EvBegin + 5,
        EvMarkForCleanupResponse = EvBegin + 6,

        EvDeallocateDevicesRequest = EvBegin + 7,
        EvDeallocateDevicesResponse = EvBegin + 8,

        EvFinishRepairRequest = EvBegin + 9,
        EvFinishRepairResponse = EvBegin + 10,

        EvFinishMigrationRequest = EvBegin + 11,
        EvFinishMigrationResponse = EvBegin + 12,

        EvReplaceDeviceRequest = EvBegin + 13,
        EvReplaceDeviceResponse = EvBegin + 14,

        EvEnd,

        // Wire-to-wire mirror of TEvVolume::TEvReallocateDisk, so that the
        // filestore tablet does not depend on blockstore code.
        EvLayoutChangedRequest = 272761156,
        EvLayoutChangedResponse = 272761157,
    };

    static_assert(EvEnd < (int)TFileStoreEvents::DISK_REGISTRY_PROXY_END,
        "EvEnd expected to be < TFileStoreEvents::DISK_REGISTRY_PROXY_END");

    FILESTORE_DISK_REGISTRY_PROXY_REQUESTS(FILESTORE_DECLARE_EVENTS)

    FILESTORE_DECLARE_PROTO_EVENTS(LayoutChanged, NProtoPrivate)
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeFileStoreDiskRegistryProxyId();

}   // namespace NCloud::NFileStore::NStorage
