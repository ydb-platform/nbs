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
#include <cloud/storage/core/libs/actors/poison_pill_helper.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/mon.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// The successor class must provide an implementation of this interface so that
// it can notify the progress and completion of the migration.
class IMigrationOwner
{
public:
    virtual ~IMigrationOwner() = default;

    // Delegates the processing of unknown messages to the owner.
    virtual void OnMessage(TAutoPtr<NActors::IEventHandle>& ev) = 0;

    // Calculates the time during which a 4MB block should migrate.
    [[nodiscard]] virtual TDuration CalculateMigrationTimeout() = 0;

    // Notifies that a sufficiently large block of data has been migrated. The
    // size is determined by the settings.
    virtual void OnMigrationProgress(
        const NActors::TActorContext& ctx,
        ui64 migrationIndex) = 0;

    // Notifies that the data migration was completed successfully.
    virtual void OnMigrationFinished(const NActors::TActorContext& ctx) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// To migrate data, it is necessary to inherit from this class. To get started,
// you need to call the StartWork() method and pass the source and destination
// actors to it.

// About error handling. If migration errors occur, they cannot be fixed, on the
// VolumeActor/PartitionActor side since DiskRegistry manages the allocation of
// devices. Therefore, in this case, the MigrationFailed critical error is only
// fired here. When DiskRegistry detects that the device or agent is broken, it
// selects a new migration target and starts it again by sending a new
// configuration to VolumeActor, which will lead to the migration actor being
// recreated with a new device config.
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
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    TString RWClientId;

    NActors::TActorId SrcActorId;
    NActors::TActorId DstActorId;

    TProcessingBlocks ProcessingBlocks;
    bool MigrationInProgress = false;

    TRequestsInProgress<ui64, TBlockRange64> WriteAndZeroRequestsInProgress{
        EAllowedRequests::WriteOnly};
    TDrainActorCompanion DrainActorCompanion{
        WriteAndZeroRequestsInProgress,
        DiskId};

    TInstant LastRangeMigrationStartTs = {};

    // Statistics
    const NActors::TActorId StatActorId;
    bool UpdateCountersScheduled = false;
    TPartitionDiskCountersPtr SrcCounters;
    TPartitionDiskCountersPtr DstCounters;

    // PoisonPill
    TPoisonPillHelper PoisonPillHelper;

    // Usage statistics
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

    // Called from the inheritor to get the next processing range.
    TBlockRange64 GetNextProcessingRange() const;

private:
    void ScheduleCountersUpdate(const NActors::TActorContext& ctx);
    void SendStats(const NActors::TActorContext& ctx);

    void ScheduleMigrateNextRange(const NActors::TActorContext& ctx);
    void MigrateNextRange(const NActors::TActorContext& ctx);
    void ContinueMigrationIfNeeded(const NActors::TActorContext& ctx);

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
