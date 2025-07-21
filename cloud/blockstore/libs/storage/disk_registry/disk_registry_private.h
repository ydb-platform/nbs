#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <util/generic/queue.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

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

struct TDiskNotificationResult
{
    TDiskNotification DiskNotification;
    TVector<NProto::TLaggingDevice> OutdatedLaggingDevices;

    TDiskNotificationResult(
        TDiskNotification diskNotification,
        TVector<NProto::TLaggingDevice> outdatedLaggingDevices)
        : DiskNotification(std::move(diskNotification))
        , OutdatedLaggingDevices(std::move(outdatedLaggingDevices))
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

struct TUserNotificationKey
{
    TString EntityId;
    ui64 SeqNo = 0;

    TUserNotificationKey() = default;

    TUserNotificationKey(TString entityId, ui64 seqNo)
        : EntityId(std::move(entityId))
        , SeqNo(seqNo)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TAgentAcquireDevicesCachedRequest
{
    TString AgentId;
    NProto::TAcquireDevicesRequest Request;
    TInstant RequestTime;
};

struct TAgentReleaseDevicesCachedRequest
{
    TString AgentId;
    NProto::TReleaseDevicesRequest Request;
};

struct TCachedAcquireKey
{
    TString DiskId;
    TString ClientId;

    friend bool operator<(
        const TCachedAcquireKey& lhs,
        const TCachedAcquireKey& rhs)
    {
        auto tie = [](const TCachedAcquireKey& arg)
        {
            return std::tie(arg.DiskId, arg.ClientId);
        };
        return tie(lhs) < tie(rhs);
    }
};

using TCachedAcquireRequests =
    TMap<TCachedAcquireKey, TAgentAcquireDevicesCachedRequest>;

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

struct TAutomaticallyReplacedDeviceInfo
{
    TString DeviceId;
    TInstant ReplacementTs;
};

struct TDiskRegistryStateSnapshot
{
    NProto::TDiskRegistryConfig Config;
    TVector<TDirtyDevice> DirtyDevices;
    TVector<NProto::TAgentConfig> Agents;
    TVector<NProto::TDiskConfig> Disks;
    TVector<NProto::TPlacementGroupConfig> PlacementGroups;
    TVector<TBrokenDiskInfo> BrokenDisks;
    TVector<TString> DisksToReallocate;
    TVector<TDiskStateUpdate> DiskStateChanges;
    ui64 LastDiskStateSeqNo = 0;
    bool WritableState = false;
    TVector<TString> DisksToCleanup;
    TVector<TString> ErrorNotifications;
    TVector<NProto::TUserNotification> UserNotifications;
    TVector<TString> OutdatedVolumeConfigs;
    TVector<NProto::TSuspendedDevice> SuspendedDevices;
    TDeque<TAutomaticallyReplacedDeviceInfo> AutomaticallyReplacedDevices;
    THashMap<TString, NProto::TDiskRegistryAgentParams>
        DiskRegistryAgentListParams;
    TVector<NProto::TReplicaWithRecentlyReplacedDevices>
        ReplicasWithRecentlyReplacedDevices;

    void Clear()
    {
        Config.Clear();
        DirtyDevices.clear();
        Agents.clear();
        Disks.clear();
        PlacementGroups.clear();
        BrokenDisks.clear();
        DisksToReallocate.clear();
        DiskStateChanges.clear();
        LastDiskStateSeqNo = 0;
        WritableState = false;
        DisksToCleanup.clear();
        ErrorNotifications.clear();
        UserNotifications.clear();
        OutdatedVolumeConfigs.clear();
        SuspendedDevices.clear();
        AutomaticallyReplacedDevices.clear();
        DiskRegistryAgentListParams.clear();
        ReplicasWithRecentlyReplacedDevices.clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

using TVolumeConfig = NKikimrBlockStore::TVolumeConfig;

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DISK_REGISTRY_REQUESTS_PRIVATE(xxx, ...)                    \
    xxx(CleanupDisks,                               __VA_ARGS__)               \
    xxx(SecureErase,                                __VA_ARGS__)               \
    xxx(CleanupDevices,                             __VA_ARGS__)               \
    xxx(FinishAcquireDisk,                          __VA_ARGS__)               \
    xxx(RemoveDiskSession,                          __VA_ARGS__)               \
    xxx(DestroyBrokenDisks,                         __VA_ARGS__)               \
    xxx(ListBrokenDisks,                            __VA_ARGS__)               \
    xxx(NotifyDisks,                                __VA_ARGS__)               \
    xxx(ListDisksToNotify,                          __VA_ARGS__)               \
    xxx(InitiateDiskReallocation,                   __VA_ARGS__)               \
    xxx(ReplaceDiskDevice,                          __VA_ARGS__)               \
    xxx(UpdateCmsHostDeviceState,                   __VA_ARGS__)               \
    xxx(UpdateCmsHostState,                         __VA_ARGS__)               \
    xxx(PublishDiskStates,                          __VA_ARGS__)               \
    xxx(StartMigration,                             __VA_ARGS__)               \
    xxx(NotifyUsers,                                __VA_ARGS__)               \
    xxx(NotifyUserEvent,                            __VA_ARGS__)               \
    xxx(UpdateVolumeConfig,                         __VA_ARGS__)               \
    xxx(FinishVolumeConfigUpdate,                   __VA_ARGS__)               \
    xxx(RestoreDiskRegistryPart,                    __VA_ARGS__)               \
    xxx(SwitchAgentDisksToReadOnly,                 __VA_ARGS__)               \
    xxx(PurgeHostCms,                               __VA_ARGS__)               \
// BLOCKSTORE_DISK_REGISTRY_REQUESTS_PRIVATE

////////////////////////////////////////////////////////////////////////////////

struct TEvDiskRegistryPrivate
{
    //
    // FinishAcquireDisk
    //

    struct TFinishAcquireDiskRequest
    {
        TString DiskId;
        TString ClientId;
        TVector<TAgentAcquireDevicesCachedRequest> SentRequests;

        TFinishAcquireDiskRequest(
                TString diskId,
                TString clientId,
                TVector<TAgentAcquireDevicesCachedRequest> sentRequests)
            : DiskId(std::move(diskId))
            , ClientId(std::move(clientId))
            , SentRequests(std::move(sentRequests))
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
        TVector<TAgentReleaseDevicesCachedRequest> SentRequests;

        TRemoveDiskSessionRequest(
                TString diskId,
                TString clientId,
                TVector<TAgentReleaseDevicesCachedRequest> sentRequests)
            : DiskId(std::move(diskId))
            , ClientId(std::move(clientId))
            , SentRequests(std::move(sentRequests))
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
        TString PoolName;
        TVector<NProto::TDeviceConfig> DirtyDevices;
        TDuration RequestTimeout;

        TSecureEraseRequest(
                TString poolName,
                TVector<NProto::TDeviceConfig> dirtyDevices,
                TDuration requestTimeout)
            : PoolName(std::move(poolName))
            , DirtyDevices(std::move(dirtyDevices))
            , RequestTimeout(requestTimeout)
        {}
    };

    struct TSecureEraseResponse
    {
        TString PoolName;
        size_t CleanDevices = 0;

        TSecureEraseResponse() = default;

        TSecureEraseResponse(
                TString poolName,
                size_t cleanDevices)
            : PoolName(std::move(poolName))
            , CleanDevices(cleanDevices)
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
        TVector<TString> Disks;

        TDestroyBrokenDisksResponse() = default;

        TDestroyBrokenDisksResponse(
                TCallContextPtr callContext,
                TVector<TString> disks)
            : CallContext(std::move(callContext))
            , Disks(std::move(disks))
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
        TVector<TDiskNotificationResult> NotifiedDisks;

        TNotifyDisksResponse() = default;

        TNotifyDisksResponse(
                TCallContextPtr callContext,
                TVector<TDiskNotificationResult> notifiedDisks)
            : CallContext(std::move(callContext))
            , NotifiedDisks(std::move(notifiedDisks))
        {}
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
        TString DeviceReplacementId;
        TInstant Timestamp;

        TReplaceDiskDeviceRequest(
                TString diskId,
                TString deviceId,
                TString deviceReplacementId,
                TInstant timestamp)
            : DiskId(std::move(diskId))
            , DeviceId(std::move(deviceId))
            , DeviceReplacementId(std::move(deviceReplacementId))
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
        ui64 SeqNo = 0;

        TAgentConnectionLost() = default;

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
        bool ShouldResumeDevice;
        bool DryRun;

        TUpdateCmsHostDeviceStateRequest(
                TString host,
                TString path,
                NProto::EDeviceState state,
                bool shouldResumeDevice,
                bool dryRun)
            : Host(std::move(host))
            , Path(std::move(path))
            , State(state)
            , ShouldResumeDevice(shouldResumeDevice)
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
    // CmsActionResponses
    //

    struct TCmsActionResponse
    {
        TDuration Timeout;
        TVector<TString> DependentDiskIds;

        TCmsActionResponse() = default;
        TCmsActionResponse(TDuration timeout, TVector<TString> dependentDiskIds)
            : Timeout(timeout)
            , DependentDiskIds(std::move(dependentDiskIds))
        {}
    };

    //
    // PurgeHostCms
    //

    struct TPurgeHostCmsRequest
    {
        TString Host;
        bool DryRun;

        TPurgeHostCmsRequest(
                TString host,
                bool dryRun)
            : Host(std::move(host))
            , DryRun(dryRun)
        {}
    };

    using TUpdateCmsHostDeviceStateResponse = TCmsActionResponse;
    using TUpdateCmsHostStateResponse = TCmsActionResponse;
    using TPurgeHostCmsResponse = TCmsActionResponse;

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
        TVector<TUserNotificationKey> Succeeded;
        TVector<TUserNotificationKey> Failures;

        TNotifyUsersResponse() = default;

        TNotifyUsersResponse(
                TCallContextPtr callContext)
            : CallContext(std::move(callContext))
        {
        }
    };

    //
    // NotifyUserEvent
    //

    struct TNotifyUserEventRequest
    {
        NProto::TUserNotification Notification;

        explicit TNotifyUserEventRequest(NProto::TUserNotification notification)
            : Notification(std::move(notification))
        {}
    };

    struct TNotifyUserEventResponse
    {
        TCallContextPtr CallContext;

        TNotifyUserEventResponse() = default;

        TNotifyUserEventResponse(
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

    struct TDiskRegistryAgentListExpiredParamsCleanup
    {};

    //
    // Switch to ReadOnly all disks associated with the agent
    //

    struct TSwitchAgentDisksToReadOnlyRequest
    {
        TString AgentId;

        TSwitchAgentDisksToReadOnlyRequest(TString agentId)
            : AgentId(std::move(agentId))
        {}
    };

    struct TSwitchAgentDisksToReadOnlyResponse
    {
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

        EvDiskRegistryAgentListExpiredParamsCleanup,

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

    using TEvDiskRegistryAgentListExpiredParamsCleanup = TRequestEvent<
        TDiskRegistryAgentListExpiredParamsCleanup,
        EvDiskRegistryAgentListExpiredParamsCleanup>;
};

}   // namespace NCloud::NBlockStore::NStorage
