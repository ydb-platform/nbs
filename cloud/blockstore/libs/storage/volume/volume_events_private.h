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
    xxx(DeviceTimeouted,                    __VA_ARGS__)                       \
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
    // DeviceTimeouted
    //

    struct TDeviceTimeoutedRequest
    {
        TString DeviceUUID;

        explicit TDeviceTimeoutedRequest(TString deviceUUID)
            : DeviceUUID(std::move(deviceUUID))
        {}
    };

    struct TDeviceTimeoutedResponse
    {
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
        NProto::EVolumeIOMode IOMode;
        TInstant IOModeTs;
        bool MuteIOErrors;

        TUpdateDevicesRequest(
                TDevices devices,
                TMigrations migrations,
                TVector<TDevices> replicas,
                TVector<TString> freshDeviceIds,
                TVector<TString> removedLaggingDevices,
                NProto::EVolumeIOMode ioMode,
                TInstant ioModeTs,
                bool muteIOErrors)
            : Devices(std::move(devices))
            , Migrations(std::move(migrations))
            , Replicas(std::move(replicas))
            , FreshDeviceIds(std::move(freshDeviceIds))
            , RemovedLaggingDevices(std::move(removedLaggingDevices))
            , IOMode(ioMode)
            , IOModeTs(ioModeTs)
            , MuteIOErrors(muteIOErrors)
        {}
    };

    struct TUpdateDevicesResponse
    {};

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

    struct TReportLaggingDevicesToDR
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
        EvReportLaggingDevicesToDR,
        EvUpdateLaggingAgentMigrationState,
        EvLaggingAgentMigrationFinished,

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

    using TEvReportLaggingDevicesToDR = TRequestEvent<
        TReportLaggingDevicesToDR,
        EvReportLaggingDevicesToDR
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
};

}   // namespace NCloud::NBlockStore::NStorage
