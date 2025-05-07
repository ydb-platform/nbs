#pragma once

#include "public.h"

#include "config.h"
#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/requests_in_progress.h>
#include <cloud/blockstore/libs/storage/partition_common/drain_actor_companion.h>
#include <cloud/blockstore/libs/storage/partition_common/get_device_for_range_companion.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/device_stats.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/mon.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Adjusts the behavior when a response is not received from the server within
// the timeout.
struct TRequestTimeoutPolicy
{
    // How long to wait for a response from the server.
    TDuration Timeout;

    // The error that needs to be generated when a timeout occurs.
    EWellKnownResultCodes ErrorCode;

    // Message for the response.
    TString OverrideMessage;
};

////////////////////////////////////////////////////////////////////////////////

class TNonreplicatedPartitionActor final
    : public NActors::TActorBootstrapped<TNonreplicatedPartitionActor>
{
private:
    using EDeviceStatus = TDeviceStat::EDeviceStatus;

    const TStorageConfigPtr Config;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const NActors::TActorId StatActorId;

    TVector<TDeviceStat> DeviceStats;

    struct TRequestData
    {
        TStackVec<int, 2> DeviceIndices;
    };
    TRequestsInProgress<
        EAllowedRequests::ReadWrite,
        NActors::TActorId,
        TRequestData>
        RequestsInProgress;
    TDrainActorCompanion DrainActorCompanion{
        RequestsInProgress,
        PartConfig->GetName()};
    TGetDeviceForRangeCompanion GetDeviceForRangeCompanion{
        TGetDeviceForRangeCompanion::EAllowedOperation::ReadWrite,
        Config,
        PartConfig,
        &DeviceStats};

    bool UpdateCountersScheduled = false;
    TPartitionDiskCountersPtr PartCounters;
    ui64 NetworkBytes = 0;
    TDuration CpuUsage;

    TRequestInfoPtr Poisoner;

public:
    TNonreplicatedPartitionActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        TNonreplicatedPartitionConfigPtr partConfig,
        NActors::TActorId statActorId);

    ~TNonreplicatedPartitionActor() override;

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    bool CheckReadWriteBlockRange(const TBlockRange64& range) const;
    void ScheduleCountersUpdate(const NActors::TActorContext& ctx);
    void SendStats(const NActors::TActorContext& ctx);
    bool IsInflightLimitReached() const;

    void UpdateStats(const NProto::TPartitionStats& update);

    void ReplyAndDie(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);
    STFUNC(StateZombie);

    TDuration GetMinRequestTimeout() const;
    TDuration GetMaxRequestTimeout() const;
    TDuration GetMaxTimedOutDeviceStateDuration() const;
    bool CalculateHasBrokenDeviceCounterValue(
        const NActors::TActorContext& ctx,
        bool silent) const;

    TRequestTimeoutPolicy MakeTimeoutPolicyForRequest(
        const TVector<TDeviceRequest>& deviceRequests,
        TInstant now,
        bool isBackground) const;

    template <typename TMethod>
    bool InitRequests(
        const typename TMethod::TRequest& msg,
        const NActors::TActorContext& ctx,
        const TRequestInfo& requestInfo,
        const TBlockRange64& blockRange,
        TVector<TDeviceRequest>* deviceRequests,
        TRequestTimeoutPolicy* timeoutPolicy,
        TRequestData* requestData);

    template <typename TRequest, typename TResponse>
    bool InitRequests(
        const char* methodName,
        const bool isWriteMethod,
        const TRequest& msg,
        const NActors::TActorContext& ctx,
        const TRequestInfo& requestInfo,
        const TBlockRange64& blockRange,
        TVector<TDeviceRequest>* deviceRequests,
        TRequestTimeoutPolicy* timeoutPolicy,
        TRequestData* requestData);

    void OnRequestCompleted(
        const TEvNonreplPartitionPrivate::TOperationCompleted& operation,
        TInstant now);
    void
    OnRequestSuccess(ui32 deviceIndex, TDuration executionTime, TInstant now);
    void
    OnRequestTimeout(ui32 deviceIndex, TDuration executionTime, TInstant now);

    void HandleUpdateCounters(
        const TEvNonreplPartitionPrivate::TEvUpdateCounters::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadBlocksCompleted(
        const TEvNonreplPartitionPrivate::TEvReadBlocksCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteBlocksCompleted(
        const TEvNonreplPartitionPrivate::TEvWriteBlocksCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleMultiAgentWriteBlocksCompleted(
        const TEvNonreplPartitionPrivate::TEvMultiAgentWriteBlocksCompleted::
            TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleZeroBlocksCompleted(
        const TEvNonreplPartitionPrivate::TEvZeroBlocksCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleChecksumBlocksCompleted(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDeviceTimedOutResponse(
        const TEvVolumePrivate::TEvDeviceTimedOutResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAgentIsUnavailable(
        const TEvNonreplPartitionPrivate::TEvAgentIsUnavailable::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAgentIsBackOnline(
        const TEvNonreplPartitionPrivate::TEvAgentIsBackOnline::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool HandleRequests(STFUNC_SIG);

    BLOCKSTORE_IMPLEMENT_REQUEST(ReadBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(ReadBlocksLocal, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocksLocal, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(ZeroBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(DescribeBlocks, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(ChecksumBlocks, TEvNonreplPartitionPrivate);
    BLOCKSTORE_IMPLEMENT_REQUEST(MultiAgentWrite, TEvNonreplPartitionPrivate);
    BLOCKSTORE_IMPLEMENT_REQUEST(Drain, NPartition::TEvPartition);
    BLOCKSTORE_IMPLEMENT_REQUEST(
        WaitForInFlightWrites,
        NPartition::TEvPartition);

    BLOCKSTORE_IMPLEMENT_REQUEST(CompactRange, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(GetCompactionStatus, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(RebuildMetadata, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(GetRebuildMetadataStatus, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(ScanDisk, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(GetScanDiskStatus, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(CheckRange, TEvVolume);
};

}   // namespace NCloud::NBlockStore::NStorage
