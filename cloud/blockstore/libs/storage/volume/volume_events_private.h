#pragma once

#include "public.h"
#include "volume_state.h"

#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/protos/volume.pb.h>

#include <contrib/ydb/library/actors/core/scheduler_cookie.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_VOLUME_REQUESTS_PRIVATE(xxx, ...)                           \
    xxx(ResetMountSeqNumber,                __VA_ARGS__)                       \
    xxx(ReadHistory,                        __VA_ARGS__)                       \
    xxx(UpdateDevices,                      __VA_ARGS__)                       \
    xxx(UpdateCheckpointRequest,            __VA_ARGS__)                       \
    xxx(UpdateShadowDiskState,              __VA_ARGS__)                       \
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
        TInstant Timestamp;
        size_t RecordCount;

        TReadHistoryRequest(
                TInstant timestamp,
                size_t recordCount)
            : Timestamp(timestamp)
            , RecordCount(recordCount)
        {}
    };

    struct TReadHistoryResponse
    {
        TVector<THistoryLogItem> History;
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
        NProto::EVolumeIOMode IOMode;
        TInstant IOModeTs;
        bool MuteIOErrors;

        TUpdateDevicesRequest(
                TDevices devices,
                TMigrations migrations,
                TVector<TDevices> replicas,
                TVector<TString> freshDeviceIds,
                NProto::EVolumeIOMode ioMode,
                TInstant ioModeTs,
                bool muteIOErrors)
            : Devices(std::move(devices))
            , Migrations(std::move(migrations))
            , Replicas(std::move(replicas))
            , FreshDeviceIds(std::move(freshDeviceIds))
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
    // MultipartitionWriteOrZeroCompleted & WriteOrZeroCompleted
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
        EvMultipartitionWriteOrZeroCompleted,
        EvWriteOrZeroCompleted,
        EvUpdateReadWriteClientInfo,
        EvRemoveExpiredVolumeParams,
        EvShadowDiskAcquired,

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

    using TEvMultipartitionWriteOrZeroCompleted = TRequestEvent<
        TMultipartitionWriteOrZeroCompleted,
        EvMultipartitionWriteOrZeroCompleted
    >;

    using TEvWriteOrZeroCompleted = TRequestEvent<
        TWriteOrZeroCompleted,
        EvWriteOrZeroCompleted
    >;

    using TEvUpdateReadWriteClientInfo = TRequestEvent<
        TUpdateReadWriteClientInfo,
        EvUpdateReadWriteClientInfo
    >;

    using TEvRemoveExpiredVolumeParams = TRequestEvent<
        TRemoveExpiredVolumeParams,
        EvRemoveExpiredVolumeParams
    >;

    using TEvShadowDiskAcquired = TRequestEvent<
        TShadowDiskAcquired,
        EvShadowDiskAcquired
    >;
};

}   // namespace NCloud::NBlockStore::NStorage
