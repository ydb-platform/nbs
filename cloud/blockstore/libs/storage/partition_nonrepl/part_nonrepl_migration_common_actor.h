#pragma once

#include "public.h"

#include "config.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/requests_in_progress.h>
#include <cloud/blockstore/libs/storage/partition_common/drain_actor_companion.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/processing_blocks.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/mon.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class IMigrationOwner
{
public:
    virtual ~IMigrationOwner() = default;

    virtual void FinishMigration(
        const NActors::TActorContext& ctx,
        bool isRetry) = 0;

    [[nodiscard]] virtual TDuration CalculateMigrationTimeout() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TNonreplicatedPartitionMigrationCommonActor
    : public NActors::TActorBootstrapped<
          TNonreplicatedPartitionMigrationCommonActor>
{
private:
    IMigrationOwner* const MigrationOwner = nullptr;
    const TStorageConfigPtr Config;
    const IProfileLogPtr ProfileLog;
    const TString DiskId;
    const ui64 BlockSize;
    TString RWClientId;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const NActors::TActorId ParentActorId;
    const NActors::TActorId StatActorId;

    TProcessingBlocks ProcessingBlocks;
    bool MigrationInProgress = false;

    TRequestsInProgress<ui64, TBlockRange64> WriteAndZeroRequestsInProgress{
        EAllowedRequests::WriteOnly};
    TDrainActorCompanion DrainActorCompanion{
        WriteAndZeroRequestsInProgress,
        DiskId};

    TInstant LastRangeMigrationStartTs;

    TPartitionDiskCountersPtr SrcCounters;
    TPartitionDiskCountersPtr DstCounters;

    bool UpdateCountersScheduled = false;

    TRequestInfoPtr Poisoner;

    NActors::TActorId SrcActorId;
    NActors::TActorId DstActorId;

    ui64 NetworkBytes = 0;
    TDuration CpuUsage;

public:
    TNonreplicatedPartitionMigrationCommonActor(
        IMigrationOwner* migrationOwner,
        TStorageConfigPtr config,
        TString diskId,
        ui64 blockCount,
        ui64 blockSize,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        ui64 initialMigrationIndex,
        TString rwClientId,
        NActors::TActorId parentActorId,
        NActors::TActorId statActorId);

    ~TNonreplicatedPartitionMigrationCommonActor() override;

    virtual void Bootstrap(const NActors::TActorContext& ctx);

    // Called from the inheritor to get started.
    void StartWork(
        const NActors::TActorContext& ctx,
        NActors::TActorId srcActorId,
        NActors::TActorId dstActorId);

    // Called from the inheritor to mark ranges that do not need to be processed.
    void MarkMigratedBlocks(TBlockRange64 range);

    // Called from the inheritor to get to get the next processing range.
    TBlockRange64 GetNextProcessingRange() const;

    TProcessingBlocks& GetProcessingBlocksForTesting();

private:
    void KillActors(const NActors::TActorContext& ctx);
    void ScheduleCountersUpdate(const NActors::TActorContext& ctx);
    void SendStats(const NActors::TActorContext& ctx);

    void ScheduleMigrateNextRange(const NActors::TActorContext& ctx);
    void MigrateNextRange(const NActors::TActorContext& ctx);
    void ContinueMigrationIfNeeded(const NActors::TActorContext& ctx);

    void ReplyAndDie(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);
    STFUNC(StateZombie);

    void HandleRangeMigrated(
        const TEvNonreplPartitionPrivate::TEvRangeMigrated::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleMigrateNextRange(
        const TEvNonreplPartitionPrivate::TEvMigrateNextRange::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteOrZeroCompleted(
        const TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleFinishMigrationResponse(
        const TEvDiskRegistry::TEvFinishMigrationResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleMigrationStateUpdated(
        const TEvVolume::TEvMigrationStateUpdated::TPtr& ev,
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
