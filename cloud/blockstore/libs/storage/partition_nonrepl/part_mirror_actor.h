#pragma once

#include "public.h"

#include "checksum_range.h"
#include "config.h"
#include "part_mirror_state.h"
#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/requests_in_progress.h>
#include <cloud/blockstore/libs/storage/partition_common/drain_actor_companion.h>
#include <cloud/blockstore/libs/storage/partition_common/get_device_for_range_companion.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/mon.h>

#include <util/generic/deque.h>
#include <util/generic/hash_set.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TDuration CalculateScrubbingInterval(
    ui64 blockCount,
    ui32 blockSize,
    ui64 bandwidthPerTiB,
    ui64 maxBandwidth,
    ui64 minBandwidth);

////////////////////////////////////////////////////////////////////////////////

class TMirrorPartitionActor final
    : public NActors::TActorBootstrapped<TMirrorPartitionActor>
{
private:
    const TStorageConfigPtr Config;
    const IProfileLogPtr ProfileLog;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    NRdma::IClientPtr RdmaClient;
    const TString DiskId;
    const NActors::TActorId StatActorId;
    const NActors::TActorId ResyncActorId;

    TMirrorPartitionState State;

    TDeque<TPartitionDiskCountersPtr> ReplicaCounters;
    bool UpdateCountersScheduled = false;
    ui64 NetworkBytes = 0;
    TDuration CpuUsage;

    THashSet<ui64> DirtyReadRequestIds;
    TRequestsInProgress<ui64, TBlockRange64> RequestsInProgress{
        EAllowedRequests::ReadWrite};
    TDrainActorCompanion DrainActorCompanion{
        RequestsInProgress,
        DiskId};
    TGetDeviceForRangeCompanion GetDeviceForRangeCompanion{
        TGetDeviceForRangeCompanion::EAllowedOperation::Read};

    TRequestInfoPtr Poisoner;
    size_t AliveReplicas = 0;

    NProto::TError Status;

    bool ScrubbingScheduled = false;
    ui64 ScrubbingRangeId = 0;
    TChecksumRangeActorCompanion ChecksumRangeActorCompanion;
    bool WriteIntersectsWithScrubbing = false;
    ui64 ScrubbingThroughput = 0;
    TInstant ScrubbingRangeStarted;
    bool ScrubbingRangeRescheduled  = false;
    bool ResyncRangeStarted = false;
    ui32 ChecksumMismatches = 0;

public:
    TMirrorPartitionActor(
        TStorageConfigPtr config,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        TString rwClientId,
        TNonreplicatedPartitionConfigPtr partConfig,
        TMigrations migrations,
        TVector<TDevices> replicas,
        NRdma::IClientPtr rdmaClient,
        NActors::TActorId statActorId,
        NActors::TActorId resyncActorId);

    ~TMirrorPartitionActor();

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void KillActors(const NActors::TActorContext& ctx);
    void SetupPartitions(const NActors::TActorContext& ctx);
    void ScheduleCountersUpdate(const NActors::TActorContext& ctx);
    void ScheduleScrubbingNextRange(const NActors::TActorContext& ctx);
    void SendStats(const NActors::TActorContext& ctx);
    void CompareChecksums(const NActors::TActorContext& ctx);
    void ReplyAndDie(const NActors::TActorContext& ctx);
    TBlockRange64 GetScrubbingRange() const;
    void StartScrubbingRange(
        const NActors::TActorContext& ctx,
        ui64 scrubbingRangeId);
    void StartResyncRange(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);
    STFUNC(StateZombie);

    void HandleWriteOrZeroCompleted(
        const TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleMirroredReadCompleted(
        const TEvNonreplPartitionPrivate::TEvMirroredReadCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRWClientIdChanged(
        const TEvVolume::TEvRWClientIdChanged::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePartCounters(
        const TEvVolume::TEvDiskRegistryBasedPartitionCounters::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateCounters(
        const TEvNonreplPartitionPrivate::TEvUpdateCounters::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleScrubbingNextRange(
        const TEvNonreplPartitionPrivate::TEvScrubbingNextRange::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleChecksumResponse(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleChecksumUndelivery(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRangeResynced(
        const TEvNonreplPartitionPrivate::TEvRangeResynced::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetDeviceForRange(
        const TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonTaken(
        const NActors::TEvents::TEvPoisonTaken::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    void MirrorRequest(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    void ReadBlocks(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    BLOCKSTORE_IMPLEMENT_REQUEST(ReadBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(ReadBlocksLocal, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocksLocal, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(ZeroBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(Drain, NPartition::TEvPartition);

    BLOCKSTORE_IMPLEMENT_REQUEST(DescribeBlocks, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(CompactRange, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(GetCompactionStatus, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(RebuildMetadata, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(GetRebuildMetadataStatus, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(ScanDisk, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(GetScanDiskStatus, TEvVolume);
};

}   // namespace NCloud::NBlockStore::NStorage
