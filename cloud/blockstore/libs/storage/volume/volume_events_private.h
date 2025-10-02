#pragma once

#include "public.h"
#include "volume_state.h"

#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/protos/volume.pb.h>

#include <contrib/ydb/library/actors/core/scheduler_cookie.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_VOLUME_REQUESTS_PRIVATE(xxx, ...)                           \
    xxx(ResetMountSeqNumber,                __VA_ARGS__)                       \
    xxx(ReadHistory,                        __VA_ARGS__)                       \
    xxx(UpdateDevices,                      __VA_ARGS__)                       \
    xxx(UpdateCheckpointRequest,            __VA_ARGS__)                       \
    xxx(UpdateShadowDiskState,              __VA_ARGS__)                       \
    xxx(ReadMetaHistory,                    __VA_ARGS__)                       \
    xxx(DeviceTimedOut,                     __VA_ARGS__)                       \
    xxx(UpdateFollowerState,                __VA_ARGS__)                       \
    xxx(TakeVolumeRequestId,                __VA_ARGS__)                       \
// BLOCKSTORE_VOLUME_REQUESTS_PRIVATE

////////////////////////////////////////////////////////////////////////////////

struct TEvVolumePrivate
{
    //
    // ProcessUpdateVolumeConfig
    //

    struct TProcessUpdateVolumeConfig
    {
    };

    //
    // AllocateDiskIfNeeded
    //

    struct TAllocateDiskIfNeeded
    {
    };

    //
    // RetryStartPartition
    //

    struct TRetryStartPartition
    {
        const ui64 TabletId;
        NActors::TSchedulerCookieHolder Cookie;

        TRetryStartPartition(ui64 tabletId, NActors::ISchedulerCookie* cookie)
            : TabletId(tabletId)
            , Cookie(cookie)
        {}
    };

    //
    // ResetMountSeqNumber
    //

    struct TResetMountSeqNumberRequest
    {
        TString ClientId;

        TResetMountSeqNumberRequest(TString clientId)
            : ClientId(std::move(clientId))
        {}
    };

    struct TResetMountSeqNumberResponse
    {
    };

    //
    // AcquireDisk
    //

    struct TAcquireDiskIfNeeded
    {
    };

    //
    // ReadHistory
    //

    struct TReadHistoryRequest
    {
        TInstant StartTs;
        std::optional<TInstant> EndTs;
        size_t RecordCount;

        TReadHistoryRequest(
                TInstant timestamp,
                size_t recordCount)
            : StartTs(timestamp)
            , RecordCount(recordCount)
        {}

        TReadHistoryRequest(
                TInstant startTs,
                TInstant endTs,
                size_t recordCount)
            : StartTs(startTs)
            , EndTs(endTs)
            , RecordCount(recordCount)
        {}
    };

    struct TReadHistoryResponse
    {
        TVector<THistoryLogItem> History;
    };

    //
    // ReadMetaHistory
    //

    struct TReadMetaHistoryRequest
    {
    };

    struct TReadMetaHistoryResponse
    {
        TVector<TVolumeMetaHistoryItem> MetaHistory;
    };

    //
    // DeviceTimedOut
    //

    struct TDeviceTimedOutRequest
    {
        TString DeviceUUID;

        explicit TDeviceTimedOutRequest(TString deviceUUID)
            : DeviceUUID(std::move(deviceUUID))
        {}
    };

    struct TDeviceTimedOutResponse
    {
    };

    //
    // TakeVolumeRequestId
    //

    struct TTakeVolumeRequestIdRequest
    {
    };

    struct TTakeVolumeRequestIdResponse
    {
        ui64 VolumeRequestId = 0;
    };

    //
    // UpdateLaggingAgentMigrationState
    //

    struct TUpdateLaggingAgentMigrationState
    {
        TString AgentId;
        ui64 CleanBlockCount;
        ui64 DirtyBlockCount;

        TUpdateLaggingAgentMigrationState(
                TString agentId,
                ui64 cleanBlockCount,
                ui64 dirtyBlockCount)
            : AgentId(std::move(agentId))
            , CleanBlockCount(cleanBlockCount)
            , DirtyBlockCount(dirtyBlockCount)
        {}
    };

    //
    // LaggingAgentMigrationFinished
    //

    struct TLaggingAgentMigrationFinished
    {
        const TString AgentId;

        explicit TLaggingAgentMigrationFinished(TString agentId)
            : AgentId(std::move(agentId))
        {}
    };

    //
    // UpdateDevices
    //

    struct TUpdateDevicesRequest
    {
        TDevices Devices;
        TMigrations Migrations;
        TVector<TDevices> Replicas;
        TVector<TString> FreshDeviceIds;
        TVector<TString> RemovedLaggingDevices;
        TVector<TString> UnavailableDeviceIds;
        NProto::EVolumeIOMode IOMode;
        TInstant IOModeTs;
        bool MuteIOErrors;

        TUpdateDevicesRequest(
                TDevices devices,
                TMigrations migrations,
                TVector<TDevices> replicas,
                TVector<TString> freshDeviceIds,
                TVector<TString> removedLaggingDevices,
                TVector<TString> unavailableDeviceIds,
                NProto::EVolumeIOMode ioMode,
                TInstant ioModeTs,
                bool muteIOErrors)
            : Devices(std::move(devices))
            , Migrations(std::move(migrations))
            , Replicas(std::move(replicas))
            , FreshDeviceIds(std::move(freshDeviceIds))
            , RemovedLaggingDevices(std::move(removedLaggingDevices))
            , UnavailableDeviceIds(std::move(unavailableDeviceIds))
            , IOMode(ioMode)
            , IOModeTs(ioModeTs)
            , MuteIOErrors(muteIOErrors)
        {}
    };

    struct TUpdateDevicesResponse
    {
        TVector<NProto::TLaggingDevice> LaggingDevices;

        TUpdateDevicesResponse() = default;

        explicit TUpdateDevicesResponse(
            TVector<NProto::TLaggingDevice> laggingDevices)
            : LaggingDevices(std::move(laggingDevices))
        {}
    };

    //
    // PartStatsSaved
    //

    struct TPartStatsSaved
    {};

    //
    // UpdateCheckpointRequest
    //

    struct TUpdateCheckpointRequestRequest
    {
        TRequestInfoPtr RequestInfo;
        ui64 RequestId;
        bool Completed;
        std::optional<TString> Error;
        TString ShadowDiskId;

        TUpdateCheckpointRequestRequest(
                TRequestInfoPtr requestInfo,
                ui64 requestId,
                bool completed,
                std::optional<TString> error,
                TString shadowDiskId)
            : RequestInfo(std::move(requestInfo))
            , RequestId(requestId)
            , Completed(completed)
            , Error(std::move(error))
            , ShadowDiskId(std::move(shadowDiskId))
        {
        }
    };

    struct TUpdateCheckpointRequestResponse
    {
    };

    //
    // WriteOrZeroCompleted
    //

    struct TWriteOrZeroCompleted
    {
        const ui64 VolumeRequestId;
        const ui32 ResultCode;

        TWriteOrZeroCompleted(
                ui64 volumeRequestId,
                ui32 resultCode)
            : VolumeRequestId(volumeRequestId)
            , ResultCode(resultCode)
        {}
    };

    struct TMultipartitionWriteOrZeroCompleted: TWriteOrZeroCompleted
    {
        using TWriteOrZeroCompleted::TWriteOrZeroCompleted;
    };

    //
    // UpdateReadWriteClientInfo
    //

    struct TUpdateReadWriteClientInfo
    {
    };

    //
    // RemoveExpiredVolumeParams
    //

    struct TRemoveExpiredVolumeParams
    {
    };

    //
    // ReportLaggingDevicesToDR
    //

    struct TReportOutdatedLaggingDevicesToDR
    {
    };

    //
    // ShadowDiskAcquired
    //

    struct TShadowDiskAcquired
    {
        using TDevices =
            google::protobuf::RepeatedPtrField<NProto::TDeviceConfig>;

        NProto::TError Error;
        TString ClientId;
        TDevices Devices;

        explicit TShadowDiskAcquired(NProto::TError error)
            : Error(std::move(error))
        {}
    };

    //
    //  UpdateShadowDiskStateRequest
    //

    struct TUpdateShadowDiskStateRequest
    {
        enum class EReason
        {
            FillProgressUpdate,
            FillCompleted,
            FillError,
        };

        TString CheckpointId;
        EReason Reason = EReason::FillError;
        ui64 ProcessedBlockCount = 0;

        TUpdateShadowDiskStateRequest(
                TString checkpointId,
                EReason reason,
                ui64 processedBlockCount)
            : CheckpointId(std::move(checkpointId))
            , Reason(reason)
            , ProcessedBlockCount(processedBlockCount)
        {}
    };

    struct TUpdateShadowDiskStateResponse
    {
        EShadowDiskState NewState = EShadowDiskState::None;
        ui64 ProcessedBlockCount = 0;

        TUpdateShadowDiskStateResponse() = default;

        TUpdateShadowDiskStateResponse(
                EShadowDiskState newState,
                ui64 processedBlockCount)
            : NewState(newState)
            , ProcessedBlockCount(processedBlockCount)
        {}
    };

    struct TExternalDrainDone
    {
        TExternalDrainDone() = default;
    };

    //
    // DevicesAcquireFinished
    //

    struct TDevicesAcquireFinished
    {
    };

    //
    // DevicesReleaseFinished
    //

    struct TDevicesReleaseFinished
    {
    };

    //
    //  LinkOnFollowerCreated/LinkOnFollowerDestroyed
    //

    struct TLinkOnFollowerCreated
    {
        TLeaderFollowerLink Link;

        explicit TLinkOnFollowerCreated(TLeaderFollowerLink link)
            : Link(std::move(link))
        {}
    };

    struct TLinkOnFollowerDestroyed
    {
        TLeaderFollowerLink Link;

        explicit TLinkOnFollowerDestroyed(TLeaderFollowerLink link)
            : Link(std::move(link))
        {}
    };

    struct TLinkOnFollowerDataTransferred
    {
        TLeaderFollowerLink Link;

        explicit TLinkOnFollowerDataTransferred(TLeaderFollowerLink link)
            : Link(std::move(link))
        {}
    };

    //
    //  CreateLinkFinished
    //

    struct TCreateLinkFinished
    {
        TLeaderFollowerLink Link;

        explicit TCreateLinkFinished(TLeaderFollowerLink link)
            : Link(std::move(link))
        {}
    };

    //
    //  UpdateFollowerStateRequest
    //

    struct TUpdateFollowerStateRequest
    {
        TFollowerDiskInfo Follower;

        explicit TUpdateFollowerStateRequest(TFollowerDiskInfo follower)
            : Follower(std::move(follower))
        {}
    };

    struct TUpdateFollowerStateResponse
    {
        TFollowerDiskInfo Follower;

        TUpdateFollowerStateResponse() = default;

        explicit TUpdateFollowerStateResponse(TFollowerDiskInfo follower)
            : Follower(std::move(follower))
        {}
    };

    //
    //  DeviceOperationStarted
    //

    struct TDeviceOperationStarted
    {
        enum class ERequestType
        {
            Read,
            Write
        };

        TString DeviceUUID;
        ERequestType RequestType;
        TString AgentId;
        ui64 OperationId;

        TDeviceOperationStarted(
            TString deviceUUID,
            ERequestType requestType,
            TString agentId,
            ui64 operationId)
            : DeviceUUID(std::move(deviceUUID))
            , RequestType(requestType)
            , AgentId(std::move(agentId))
            , OperationId(operationId)
        {}
    };

    //
    //  DeviceOperationFinished
    //

    struct TDeviceOperationFinished
    {
        ui64 OperationId;

        explicit TDeviceOperationFinished(ui64 operationId)
            : OperationId(operationId)
        {}
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStorePrivateEvents::VOLUME_START,

        BLOCKSTORE_VOLUME_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENT_IDS)

        EvUpdateCounters,
        EvUpdateThrottlerState,
        EvProcessUpdateVolumeConfig,
        EvAllocateDiskIfNeeded,
        EvRetryStartPartition,
        EvAcquireDiskIfNeeded,
        EvPartStatsSaved,
        EvWriteOrZeroCompleted,
        EvUpdateReadWriteClientInfo,
        EvRemoveExpiredVolumeParams,
        EvShadowDiskAcquired,
        EvExternalDrainDone,
        EvDevicesAcquireFinished,
        EvDevicesReleaseFinished,
        EvReportOutdatedLaggingDevicesToDR,
        EvUpdateLaggingAgentMigrationState,
        EvLaggingAgentMigrationFinished,
        EvLinkOnFollowerCreated,
        EvLinkOnFollowerDestroyed,
        EvLinkOnFollowerDataTransferred,
        EvCreateLinkFinished,
        EvDeviceOperationStarted,
        EvDeviceOperationFinished,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStorePrivateEvents::VOLUME_END,
        "EvEnd expected to be < TBlockStorePrivateEvents::VOLUME_END");

    BLOCKSTORE_VOLUME_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENTS)

    using TEvUpdateCounters = TRequestEvent<TEmpty, EvUpdateCounters>;

    using TEvUpdateThrottlerState = TRequestEvent<TEmpty, EvUpdateThrottlerState>;

    using TEvProcessUpdateVolumeConfig = TRequestEvent<
        TProcessUpdateVolumeConfig,
        EvProcessUpdateVolumeConfig
    >;

    using TEvAllocateDiskIfNeeded = TRequestEvent<
        TAllocateDiskIfNeeded,
        EvAllocateDiskIfNeeded
    >;

    using TEvRetryStartPartition = TRequestEvent<
        TRetryStartPartition,
        EvRetryStartPartition
    >;

    using TEvAcquireDiskIfNeeded = TRequestEvent<
        TAcquireDiskIfNeeded,
        EvAcquireDiskIfNeeded
    >;

    using TEvPartStatsSaved = TRequestEvent<
        TPartStatsSaved,
        EvPartStatsSaved
    >;

    using TEvWriteOrZeroCompleted = TRequestEvent<
        TWriteOrZeroCompleted,
        EvWriteOrZeroCompleted
    >;

    using TEvUpdateReadWriteClientInfo = TRequestEvent<
        TUpdateReadWriteClientInfo,
        EvUpdateReadWriteClientInfo
    >;

    using TEvUpdateLaggingAgentMigrationState = TRequestEvent<
        TUpdateLaggingAgentMigrationState,
        EvUpdateLaggingAgentMigrationState
    >;

    using TEvLaggingAgentMigrationFinished = TRequestEvent<
        TLaggingAgentMigrationFinished,
        EvLaggingAgentMigrationFinished
    >;

    using TEvReportOutdatedLaggingDevicesToDR = TRequestEvent<
        TReportOutdatedLaggingDevicesToDR,
        EvReportOutdatedLaggingDevicesToDR
    >;

    using TEvRemoveExpiredVolumeParams = TRequestEvent<
        TRemoveExpiredVolumeParams,
        EvRemoveExpiredVolumeParams
    >;

    using TEvShadowDiskAcquired = TRequestEvent<
        TShadowDiskAcquired,
        EvShadowDiskAcquired
    >;

    using TEvExternalDrainDone = TRequestEvent<
        TExternalDrainDone,
        EvExternalDrainDone
    >;

    using TEvDevicesAcquireFinished =
        TResponseEvent<TDevicesAcquireFinished, EvDevicesAcquireFinished>;

    using TEvDevicesReleaseFinished =
        TResponseEvent<TDevicesReleaseFinished, EvDevicesReleaseFinished>;

    using TEvLinkOnFollowerCreated =
        TResponseEvent<TLinkOnFollowerCreated, EvLinkOnFollowerCreated>;

    using TEvLinkOnFollowerDestroyed =
        TResponseEvent<TLinkOnFollowerDestroyed, EvLinkOnFollowerDestroyed>;

    using TEvLinkOnFollowerDataTransferred = TResponseEvent<
        TLinkOnFollowerDataTransferred,
        EvLinkOnFollowerDataTransferred>;

    using TEvCreateLinkFinished =
        TResponseEvent<TCreateLinkFinished, EvCreateLinkFinished>;

    using TEvDeviceOperationStarted =
        TRequestEvent<TDeviceOperationStarted, EvDeviceOperationStarted>;

    using TEvDeviceOperationFinished =
        TRequestEvent<TDeviceOperationFinished, EvDeviceOperationFinished>;
};

}   // namespace NCloud::NBlockStore::NStorage
