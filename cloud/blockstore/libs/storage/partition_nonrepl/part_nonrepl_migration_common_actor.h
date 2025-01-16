#pragma once

#include "public.h"

#include "config.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/requests_in_progress.h>
#include <cloud/blockstore/libs/storage/partition_common/drain_actor_companion.h>
#include <cloud/blockstore/libs/storage/partition_common/get_device_for_range_companion.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/migration_timeout_calculator.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/changed_ranges_map.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/disjoint_range_set.h>
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

    // Bootstrap for migration owner.
    virtual void OnBootstrap(const NActors::TActorContext& ctx) = 0;

    // Delegates the processing of messages to the owner first.
    // If true is returned, then the message has been processed.
    virtual bool OnMessage(
        const NActors::TActorContext& ctx,
        TAutoPtr<NActors::IEventHandle>& ev) = 0;

    // Notifies that a sufficiently large block of data has been migrated. The
    // size is determined by the settings.
    virtual void OnMigrationProgress(
        const NActors::TActorContext& ctx,
        ui64 migrationIndex) = 0;

    // Notifies that the data migration was completed successfully.
    virtual void OnMigrationFinished(const NActors::TActorContext& ctx) = 0;

    // Notifies that an non-retriable error occurred during the migration.
    // And the migration was stopped.
    virtual void OnMigrationError(const NActors::TActorContext& ctx) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// To migrate data, it is necessary to inherit from this class. To get started,
// you need to call the InitWork() method and pass the source and destination
// actors to it. Then when you ready to run migration call StartWork()

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
    , IPoisonPillHelperOwner
{
private:
    using TBase = NActors::TActorBootstrapped<
        TNonreplicatedPartitionMigrationCommonActor>;
    using EExecutionSide =
        TEvNonreplPartitionPrivate::TEvRangeMigrated::EExecutionSide;

    IMigrationOwner* const MigrationOwner = nullptr;
    const TStorageConfigPtr Config;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const IProfileLogPtr ProfileLog;
    const TString DiskId;
    const ui64 BlockSize;
    const ui64 BlockCount;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const ui32 MaxIoDepth;
    TString RWClientId;

    NActors::TActorId SrcActorId;
    NActors::TActorId DstActorId;
    std::unique_ptr<TMigrationTimeoutCalculator> TimeoutCalculator;

    TProcessingBlocks ProcessingBlocks;
    bool MigrationEnabled = false;
    bool RangeMigrationScheduled = false;
    TInstant LastRangeMigrationStartTs;

    TDisjointRangeSet MigrationsInProgress;
    TDisjointRangeSet DeferredMigrations;

    TChangedRangesMap ChangedRangesMap;

    // Current migration progress is persistently stored inside a volume tablet.
    // Once we migrated a range that exceeds currently stored one by configured
    // interval, we remember this fact and wait for all ranges with addresses
    // less than this offset to migrate. After that, we notify the volume by
    // calling MigrationOwner->OnMigrationProgress().
    bool MigrationThresholdAchieved = false;

    TRequestsInProgress<ui64, TBlockRange64> WriteAndZeroRequestsInProgress{
        EAllowedRequests::WriteOnly};
    TDrainActorCompanion DrainActorCompanion{
        WriteAndZeroRequestsInProgress,
        DiskId};
    TGetDeviceForRangeCompanion GetDeviceForRangeCompanion{
        TGetDeviceForRangeCompanion::EAllowedOperation::ReadWrite};

    // Statistics
    const NActors::TActorId StatActorId;
    bool UpdateCountersScheduled = false;
    TPartitionDiskCountersPtr SrcCounters;
    TPartitionDiskCountersPtr DstCounters;
    TPartitionDiskCountersPtr MigrationCounters = CreatePartitionDiskCounters(
        EPublishingPolicy::DiskRegistryBased,
        DiagnosticsConfig->GetHistogramCounterOptions());

    // Usage statistics
    ui64 NetworkBytes = 0;
    TDuration CpuUsage;

protected:
    // Derived class that wishes to handle wakeup messages should make its own
    // enum which starts with `WR_REASON_COUNT` value.
    enum EWakeupReason
    {
        WR_REGISTER_TRAFFIC_SOURCE = 1,

        WR_REASON_COUNT
    };

    // PoisonPill
    TPoisonPillHelper PoisonPillHelper;

public:
    TNonreplicatedPartitionMigrationCommonActor(
        IMigrationOwner* migrationOwner,
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        TString diskId,
        ui64 blockCount,
        ui64 blockSize,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        ui64 initialMigrationIndex,
        TString rwClientId,
        NActors::TActorId statActorId,
        ui32 maxIoDepth);

    ~TNonreplicatedPartitionMigrationCommonActor() override;

    virtual void Bootstrap(const NActors::TActorContext& ctx);

    // Called from the inheritor to initialize migration.
    void InitWork(
        const NActors::TActorContext& ctx,
        NActors::TActorId srcActorId,
        NActors::TActorId dstActorId,
        std::unique_ptr<TMigrationTimeoutCalculator> timeoutCalculator);

    // Called from the inheritor to start migration.
    void StartWork(const NActors::TActorContext& ctx);

    // Called from the inheritor to mark ranges that do not need to be
    // processed.
    void MarkMigratedBlocks(TBlockRange64 range);

    // Called from the inheritor to get the number of blocks that need to be
    // processed.
    ui64 GetBlockCountNeedToBeProcessed() const;

    // IPoisonPillHelperOwner implementation
    void Die(const NActors::TActorContext& ctx) override
    {
        TBase::Die(ctx);
    }

protected:
    [[nodiscard]] TString GetChangedBlocks(TBlockRange64 range) const;
    const TStorageConfigPtr& GetConfig() const;
    const TDiagnosticsConfigPtr& GetDiagnosticsConfig() const;

private:
    bool IsMigrationAllowed() const;
    bool IsMigrationFinished() const;
    bool IsIoDepthLimitReached() const;
    bool OverlapsWithInflightWriteAndZero(TBlockRange64 range) const;

    std::optional<TBlockRange64> GetNextMigrationRange() const;
    std::optional<TBlockRange64>
    TakeNextMigrationRange(const NActors::TActorContext& ctx);

    void ScheduleCountersUpdate(const NActors::TActorContext& ctx);
    void SendStats(const NActors::TActorContext& ctx);

    void ScheduleRangeMigration(const NActors::TActorContext& ctx);
    void StartRangeMigration(const NActors::TActorContext& ctx);
    void MigrateRange(const NActors::TActorContext& ctx, TBlockRange64 range);

    void NotifyMigrationProgressIfNeeded(
        const NActors::TActorContext& ctx,
        TBlockRange64 migratedRange);
    void NotifyMigrationFinishedIfNeeded(const NActors::TActorContext& ctx);

    // Calculates the time during which a 4MB block should migrate.
    TDuration CalculateMigrationTimeout(TBlockRange64 range) const;
    void DoRegisterTrafficSource(const NActors::TActorContext& ctx);

    void OnMigrationNonRetriableError(const NActors::TActorContext& ctx);

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

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
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
    BLOCKSTORE_IMPLEMENT_REQUEST(ChecksumBlocks, TEvNonreplPartitionPrivate);
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
