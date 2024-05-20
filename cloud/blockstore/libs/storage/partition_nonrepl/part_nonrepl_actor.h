#pragma once

#include "public.h"

#include "config.h"
#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/requests_in_progress.h>
#include <cloud/blockstore/libs/storage/partition_common/drain_actor_companion.h>
#include <cloud/blockstore/libs/storage/partition_common/get_changed_blocks_companion.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/mon.h>

#include <library/cpp/containers/ring_buffer/ring_buffer.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TNonreplicatedPartitionActor final
    : public NActors::TActorBootstrapped<TNonreplicatedPartitionActor>
{
private:
    const TStorageConfigPtr Config;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const NActors::TActorId StatActorId;

    struct TDeviceStat
    {
        TInstant LastTimeoutTs;
        TDuration TimedOutStateDuration;
        TDuration CurrentTimeout;
        TDuration ExpectedClientBackoff;

        TSimpleRingBuffer<TDuration> ResponseTimes{10};
    };
    TVector<TDeviceStat> DeviceStats;
    bool HasBrokenDevice = false;
    TInstant BrokenTransitionTs;

    struct TRequest
    {
        TInstant Ts;
        TStackVec<int, 2> DeviceIndices;
        TDuration Timeout;
    };

    TRequestsInProgress<NActors::TActorId, TRequest> RequestsInProgress{
        EAllowedRequests::ReadWrite};
    TDrainActorCompanion DrainActorCompanion{
        RequestsInProgress,
        PartConfig->GetName()};
    TGetChangedBlocksCompanion GetChangedBlocksCompanion;

    bool UpdateCountersScheduled = false;
    TPartitionDiskCountersPtr PartCounters;
    ui64 NetworkBytes = 0;
    TDuration CpuUsage;

    TRequestInfoPtr Poisoner;

public:
    TNonreplicatedPartitionActor(
        TStorageConfigPtr config,
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

    template <typename TMethod>
    bool InitRequests(
        const typename TMethod::TRequest& msg,
        const NActors::TActorContext& ctx,
        TRequestInfo& requestInfo,
        const TBlockRange64& blockRange,
        TVector<TDeviceRequest>* deviceRequests,
        TRequest* request);

    void OnResponse(ui32 deviceIndex, TDuration responseTime);

    bool IOErrorCooldownPassed(const TInstant now) const;

    void HandleUpdateCounters(
        const TEvNonreplPartitionPrivate::TEvUpdateCounters::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
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

    void HandleZeroBlocksCompleted(
        const TEvNonreplPartitionPrivate::TEvZeroBlocksCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleChecksumBlocksCompleted(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool HandleRequests(STFUNC_SIG);

    BLOCKSTORE_IMPLEMENT_REQUEST(ReadBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(ReadBlocksLocal, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocksLocal, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(ZeroBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(DescribeBlocks, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(ChecksumBlocks, TEvNonreplPartitionPrivate);
    BLOCKSTORE_IMPLEMENT_REQUEST(Drain, NPartition::TEvPartition);

    BLOCKSTORE_IMPLEMENT_REQUEST(CompactRange, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(GetCompactionStatus, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(RebuildMetadata, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(GetRebuildMetadataStatus, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(ScanDisk, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(GetScanDiskStatus, TEvVolume);
};

}   // namespace NCloud::NBlockStore::NStorage
