#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/queue.h>

namespace NCloud::NBlockStore::NStorage {

class TDiskRegistryDatabase;

////////////////////////////////////////////////////////////////////////////////

struct TVolumeDescr
{
    TString DiskId;
    bool CreateFinished;
    ui32 BlockSize;
    ui64 BlockCount;
    TString PlacementGroupId;
};

struct TDiskNotification
{
    TString DiskId;
    ui64 SeqNo = 0;

    TDiskNotification() = default;

    TDiskNotification(TString diskId, ui64 seqNo)
        : DiskId(std::move(diskId))
        , SeqNo(seqNo)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TDiskStateUpdate
{
    NProto::TDiskState State;
    ui64 SeqNo = 0;

    TDiskStateUpdate() = default;

    TDiskStateUpdate(
            NProto::TDiskState state,
            ui64 seqNo)
        : State(std::move(state))
        , SeqNo(seqNo)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TBrokenDiskInfo
{
    TString DiskId;
    TInstant TsToDestroy;
};

struct TDirtyDevice
{
    TString Id;
    TString DiskId;
};

struct TDiskRegistryStateSnapshot
{
    NProto::TDiskRegistryConfig Config;
    TVector<TDirtyDevice> DirtyDevices;
    TVector<NProto::TAgentConfig> OldAgents;
    TVector<NProto::TAgentConfig> Agents;
    TVector<NProto::TDiskConfig> Disks;
    TVector<NProto::TPlacementGroupConfig> PlacementGroups;
    TVector<TBrokenDiskInfo> BrokenDisks;
    TVector<TString> DisksToNotify;
    TVector<TDiskStateUpdate> DiskStateChanges;
    ui64 LastDiskStateSeqNo = 0;
    bool WritableState = false;
    TVector<TString> DisksToCleanup;
    TVector<TString> ErrorNotifications;
    TVector<TString> OutdatedVolumeConfigs;
    TVector<TString> SuspendedDevices;

    void Clear()
    {
        Config.Clear();
        DirtyDevices.clear();
        OldAgents.clear();
        Agents.clear();
        Disks.clear();
        PlacementGroups.clear();
        BrokenDisks.clear();
        DisksToNotify.clear();
        DiskStateChanges.clear();
        LastDiskStateSeqNo = 0;
        WritableState = false;
        DisksToCleanup.clear();
        ErrorNotifications.clear();
        OutdatedVolumeConfigs.clear();
        SuspendedDevices.clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

using TVolumeConfig = NKikimrBlockStore::TVolumeConfig;

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DISK_REGISTRY_REQUESTS_PRIVATE(xxx, ...)                    \
    xxx(CleanupDisks,                __VA_ARGS__)                              \
    xxx(SecureErase,                 __VA_ARGS__)                              \
    xxx(CleanupDevices,              __VA_ARGS__)                              \
    xxx(StartAcquireDisk,            __VA_ARGS__)                              \
    xxx(FinishAcquireDisk,           __VA_ARGS__)                              \
    xxx(RemoveDiskSession,           __VA_ARGS__)                              \
    xxx(DestroyBrokenDisks,          __VA_ARGS__)                              \
    xxx(ListBrokenDisks,             __VA_ARGS__)                              \
    xxx(NotifyDisks,                 __VA_ARGS__)                              \
    xxx(ListDisksToNotify,           __VA_ARGS__)                              \
    xxx(InitiateDiskReallocation,    __VA_ARGS__)                              \
    xxx(ReplaceDiskDevice,           __VA_ARGS__)                              \
    xxx(UpdateCmsHostDeviceState,    __VA_ARGS__)                              \
    xxx(UpdateCmsHostState,          __VA_ARGS__)                              \
    xxx(GetDependentDisks,           __VA_ARGS__)                              \
    xxx(PublishDiskStates,           __VA_ARGS__)                              \
    xxx(StartMigration,              __VA_ARGS__)                              \
    xxx(NotifyUsers,                 __VA_ARGS__)                              \
    xxx(NotifyDiskError,             __VA_ARGS__)                              \
    xxx(UpdateVolumeConfig,          __VA_ARGS__)                              \
    xxx(FinishVolumeConfigUpdate,    __VA_ARGS__)                              \
    xxx(RestoreDiskRegistryPart,     __VA_ARGS__)                              \
// BLOCKSTORE_DISK_REGISTRY_REQUESTS_PRIVATE

////////////////////////////////////////////////////////////////////////////////

struct TEvDiskRegistryPrivate
{
    //
    // StartAcquireDisk
    //

    struct TStartAcquireDiskRequest
    {
        TString DiskId;
        TString ClientId;

        TStartAcquireDiskRequest(TString diskId, TString clientId)
            : DiskId(std::move(diskId))
            , ClientId(std::move(clientId))
        {}
    };

    struct TStartAcquireDiskResponse
    {
        TVector<NProto::TDeviceConfig> Devices;
        ui32 LogicalBlockSize = 0;

        TStartAcquireDiskResponse() = default;

        explicit TStartAcquireDiskResponse(
                TVector<NProto::TDeviceConfig> devices,
                ui32 logicalBlockSize)
            : Devices(std::move(devices))
            , LogicalBlockSize(logicalBlockSize)
        {}
    };

    //
    // FinishAcquireDisk
    //

    struct TFinishAcquireDiskRequest
    {
        TString DiskId;
        TString ClientId;

        TFinishAcquireDiskRequest(TString diskId, TString clientId)
            : DiskId(std::move(diskId))
            , ClientId(std::move(clientId))
        {}
    };

    struct TFinishAcquireDiskResponse
    {};

    //
    // RemoveDiskSession
    //

    struct TRemoveDiskSessionRequest
    {
        TString DiskId;
        TString ClientId;

        TRemoveDiskSessionRequest(
                TString diskId,
                TString clientId)
            : DiskId(std::move(diskId))
            , ClientId(std::move(clientId))
        {}
    };

    struct TRemoveDiskSessionResponse
    {};

    //
    // CleanupDisks
    //

    struct TCleanupDisksRequest
    {};

    struct TCleanupDisksResponse
    {
        // TODO: affected disks
    };

    //
    // SecureErase
    //

    struct TSecureEraseRequest
    {
        TVector<NProto::TDeviceConfig> DirtyDevices;
        TDuration RequestTimeout;

        explicit TSecureEraseRequest(
                TVector<NProto::TDeviceConfig> dirtyDevices,
                TDuration requestTimeout)
            : DirtyDevices(std::move(dirtyDevices))
            , RequestTimeout(requestTimeout)
        {}
    };

    struct TSecureEraseResponse
    {
        size_t CleanDevices = 0;

        TSecureEraseResponse() = default;

        explicit TSecureEraseResponse(size_t cleanDevices)
            : CleanDevices(cleanDevices)
        {}
    };

    //
    // CleanupDevices
    //

    struct TCleanupDevicesRequest
    {
        TVector<TString> Devices;

        explicit TCleanupDevicesRequest(TVector<TString> devices)
            : Devices(std::move(devices))
        {}
    };

    struct TCleanupDevicesResponse
    {};

    //
    // DestroyBrokenDisks
    //

    struct TDestroyBrokenDisksRequest
    {
    };

    struct TDestroyBrokenDisksResponse
    {
        TCallContextPtr CallContext;

        TDestroyBrokenDisksResponse() = default;

        TDestroyBrokenDisksResponse(
                TCallContextPtr callContext)
            : CallContext(std::move(callContext))
        {
        }
    };

    //
    // ListBrokenDisks
    //

    struct TListBrokenDisksRequest
    {
    };

    struct TListBrokenDisksResponse
    {
        TVector<TString> DiskIds;

        TListBrokenDisksResponse() = default;

        TListBrokenDisksResponse(TVector<TString> diskIds)
            : DiskIds(std::move(diskIds))
        {
        }
    };

    //
    // NotifyDisks
    //

    struct TNotifyDisksRequest
    {
    };

    struct TNotifyDisksResponse
    {
        TCallContextPtr CallContext;
        TVector<TDiskNotification> NotifiedDisks;

        TNotifyDisksResponse() = default;

        TNotifyDisksResponse(
                TCallContextPtr callContext)
            : CallContext(std::move(callContext))
        {
        }
    };

    //
    // PublishDiskStates
    //

    struct TPublishDiskStatesRequest
    {
    };

    struct TPublishDiskStatesResponse
    {
        TCallContextPtr CallContext;
        ui64 MaxSeqNo = 0;

        TPublishDiskStatesResponse() = default;

        TPublishDiskStatesResponse(
                TCallContextPtr callContext,
                ui64 maxSeqNo)
            : CallContext(std::move(callContext))
            , MaxSeqNo(maxSeqNo)
        {
        }
    };

    //
    // InitiateDiskReallocation
    //

    struct TInitiateDiskReallocationRequest
    {
        TString DiskId;

        explicit TInitiateDiskReallocationRequest(
                TString diskId)
            : DiskId(std::move(diskId))
        {}
    };

    struct TInitiateDiskReallocationResponse
    {
        TCallContextPtr CallContext;

        TInitiateDiskReallocationResponse() = default;

        TInitiateDiskReallocationResponse(
                TCallContextPtr callContext)
            : CallContext(std::move(callContext))
        {
        }
    };

    //
    // ListDisksToNotify
    //

    struct TListDisksToNotifyRequest
    {
    };

    struct TListDisksToNotifyResponse
    {
        TVector<TString> DiskIds;

        TListDisksToNotifyResponse() = default;

        TListDisksToNotifyResponse(TVector<TString> diskIds)
            : DiskIds(std::move(diskIds))
        {
        }
    };

    //
    // ReplaceDevice
    //

    struct TReplaceDiskDeviceRequest
    {
        TString DiskId;
        TString DeviceId;
        TInstant Timestamp;

        TReplaceDiskDeviceRequest(
                TString diskId,
                TString deviceId,
                TInstant timestamp)
            : DiskId(std::move(diskId))
            , DeviceId(std::move(deviceId))
            , Timestamp(timestamp)
        {}
    };

    struct TReplaceDiskDeviceResponse
    {
        TMaybe<TDiskStateUpdate> DiskStateUpdate;

        TReplaceDiskDeviceResponse() = default;

        explicit TReplaceDiskDeviceResponse(
                TMaybe<TDiskStateUpdate> diskStateUpdate)
            : DiskStateUpdate(std::move(diskStateUpdate))
        {}
    };

    //
    // AgentConnectionLost notification
    //

    struct TAgentConnectionLost
    {
        TString AgentId;
        ui64 SeqNo;

        TAgentConnectionLost(
                TString agentId,
                ui64 seqNo)
            : AgentId(std::move(agentId))
            , SeqNo(seqNo)
        {}
    };

    //
    // UpdateHostDeviceStateRequest
    //

    struct TUpdateCmsHostDeviceStateRequest
    {
        TString Host;
        TString Path;
        NProto::EDeviceState State;
        bool DryRun;

        TUpdateCmsHostDeviceStateRequest(
                TString host,
                TString path,
                NProto::EDeviceState state,
                bool dryRun)
            : Host(std::move(host))
            , Path(std::move(path))
            , State(state)
            , DryRun(dryRun)
        {}
    };

    //
    // UpdateCmsHostState
    //

    struct TUpdateCmsHostStateRequest
    {
        TString Host;
        NProto::EAgentState State;
        bool DryRun;

        TUpdateCmsHostStateRequest(
                TString host,
                NProto::EAgentState state,
                bool dryRun)
            : Host(std::move(host))
            , State(state)
            , DryRun(dryRun)
        {}
    };

    //
    // GetDependentDisks
    //

    struct TGetDependentDisksRequest
    {
        TString Host;
        TString Path;

        TGetDependentDisksRequest(
                TString host,
                TString path)
            : Host(std::move(host))
            , Path(std::move(path))
        {}
    };

    //
    // CmsActionResponses
    //

    struct TCmsActionResponse
    {
        TDuration Timeout;
        TVector<TString> DependentDiskIds;
    };

    using TUpdateCmsHostDeviceStateResponse = TCmsActionResponse;
    using TUpdateCmsHostStateResponse = TCmsActionResponse;
    using TGetDependentDisksResponse = TCmsActionResponse;

    //
    // StartMigration
    //

    struct TStartMigrationRequest
    {
    };

    struct TStartMigrationResponse
    {
    };

    //
    // NotifyUsers
    //

    struct TNotifyUsersRequest
    {
    };

    struct TNotifyUsersResponse
    {
        TCallContextPtr CallContext;
        TVector<TString> Succeeded;
        TVector<TString> Failures;

        TNotifyUsersResponse() = default;

        TNotifyUsersResponse(
                TCallContextPtr callContext)
            : CallContext(std::move(callContext))
        {
        }
    };

    //
    // NotifyDiskError
    //

    struct TNotifyDiskErrorRequest
    {
        TString DiskId;

        explicit TNotifyDiskErrorRequest(TString diskId)
            : DiskId(std::move(diskId))
        {}
    };

    struct TNotifyDiskErrorResponse
    {
        TCallContextPtr CallContext;

        TNotifyDiskErrorResponse() = default;

        TNotifyDiskErrorResponse(
                TCallContextPtr callContext)
            : CallContext(std::move(callContext))
        {}
    };

    //
    // UpdateVolumeConfig
    //

    struct TUpdateVolumeConfigRequest
    {
        TString DiskId;

        TUpdateVolumeConfigRequest(TString diskId)
            : DiskId(std::move(diskId))
        {
        }
    };

    struct TUpdateVolumeConfigResponse
    {
        TVolumeConfig Config;
        ui64 SeqNo;

        TUpdateVolumeConfigResponse() = default;

        TUpdateVolumeConfigResponse(TVolumeConfig config, ui64 seqNo)
            : Config(std::move(config))
            , SeqNo(seqNo)
        {
        }
    };

    //
    // FinishVolumeConfigUpdate
    //

    struct TFinishVolumeConfigUpdateRequest
    {
        TString DiskId;

        TFinishVolumeConfigUpdateRequest(TString diskId)
            : DiskId(std::move(diskId))
        {
        }
    };

    struct TFinishVolumeConfigUpdateResponse
    {
    };

    struct TRestoreDiskRegistryPartRequest
    {
        using TFunction = std::function<void(TDiskRegistryDatabase&)>;

        const TRequestInfoPtr RequestInfo;

        TQueue<TFunction> Operations;

        TRestoreDiskRegistryPartRequest() = default;
        TRestoreDiskRegistryPartRequest(
                TRequestInfoPtr requestInfo,
                TQueue<TFunction> operations)
            : RequestInfo(std::move(requestInfo))
            , Operations(std::move(operations))
        {}
    };

    struct TRestoreDiskRegistryPartResponse
        : public TRestoreDiskRegistryPartRequest
    {
        const TRequestInfoPtr PartRequestInfo;

        TRestoreDiskRegistryPartResponse() = default;
        TRestoreDiskRegistryPartResponse(
                TRequestInfoPtr baseRequestInfo,
                TRequestInfoPtr partRequestInfo,
                TQueue<TFunction> operations)
            : TRestoreDiskRegistryPartRequest(
                std::move(baseRequestInfo),
                std::move(operations))
            , PartRequestInfo(std::move(partRequestInfo))
        {}
    };

    struct TRestoreDiskRegistryValidationResponse
    {
        const TRequestInfoPtr RequestInfo;

        TDiskRegistryStateSnapshot LoadDBState;

        TRestoreDiskRegistryValidationResponse() = default;
        explicit TRestoreDiskRegistryValidationResponse(
                TRequestInfoPtr requestInfo,
                TDiskRegistryStateSnapshot loadDBState = {})
            : RequestInfo(std::move(requestInfo))
            , LoadDBState(std::move(loadDBState))
        {}
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStorePrivateEvents::DISK_REGISTRY_START,

        BLOCKSTORE_DISK_REGISTRY_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENT_IDS)

        EvAgentConnectionLost,

        EvOperationCompleted,

        EvRestoreDiskRegistryValidationResponse,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStorePrivateEvents::DISK_REGISTRY_END,
        "EvEnd expected to be < TBlockStorePrivateEvents::DISK_REGISTRY_END");

    BLOCKSTORE_DISK_REGISTRY_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENTS)

    using TEvAgentConnectionLost = TRequestEvent<
        TAgentConnectionLost,
        EvAgentConnectionLost
    >;

    using TEvOperationCompleted = TResponseEvent<TEmpty, EvOperationCompleted>;

    using TEvRestoreDiskRegistryValidationResponse = TResponseEvent<
        TRestoreDiskRegistryValidationResponse,
        EvRestoreDiskRegistryValidationResponse>;
};

}   // namespace NCloud::NBlockStore::NStorage
