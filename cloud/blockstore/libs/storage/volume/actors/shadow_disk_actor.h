#pragma once

#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/migration_timeout_calculator.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_migration_common_actor.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>
#include <cloud/storage/core/libs/common/backoff_delay_provider.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/interconnect/types.h>

namespace NCloud::NBlockStore::NStorage {

//
class TShadowDiskActor final
    : public TNonreplicatedPartitionMigrationCommonActor
    , public IMigrationOwner
{
private:
    enum class EActorState
    {
        WaitAcquireForPrepareStart,
        WaitAcquireForPrepareContinue,
        WaitAcquireForRead,
        Preparing,
        CheckpointReady,
        Error,
    };

    enum class EWakeupReason
    {
        AcquireTimeout,
        PeriodicalReAcquire
    };

    enum class EAcquireReason
    {
        FirstAcquire,
        PeriodicalReAcquire,
        ForcedReAcquire,
    };

    const TStorageConfigPtr Config;
    const NRdma::IClientPtr RdmaClient;
    const TNonreplicatedPartitionConfigPtr SrcConfig;
    const TString CheckpointId;
    const TString SourceDiskId;
    const TString ShadowDiskId;
    const ui64 MountSeqNumber = 0;
    const ui32 Generation = 0;
    const TActorId VolumeActorId;
    const TActorId SrcActorId;
    const TString ShadowDiskClientId = "shadow-disk-client-id";

    TActorId DstActorId;
    ui64 ProcessedBlockCount = 0;

    EActorState State = EActorState::Error;
    TMigrationTimeoutCalculator TimeoutCalculator;

    // The time when we started acquiring the shadow disk.
    TInstant AcquireStartedAt = {};

    // Delay provider when retrying describe and acquire requests to disk
    // registry. This provider is used when writing to the source disk is
    // blocked.
    TBackoffDelayProvider BlockingDelays;
    // Delay provider when retrying describe and acquire requests to disk
    // registry. This provider is used when writing to the source disk is
    // not blocked.
    TBackoffDelayProvider NonBlockingDelays;

    // The list of devices received via the describe request.
    // This is necessary to check that all disk devices have been acquired.
    TDevices ShadowDiskDevices;
    TDevices AcquiredShadowDiskDevices;

    // We re-acquire shadow disk every ClientRemountPeriod interval.
    bool PeriodicalReAcquireShadowDiskScheduled = false;

    bool ForcedReAcquireInProgress = false;

public:
    TShadowDiskActor(
        TStorageConfigPtr config,
        NRdma::IClientPtr rdmaClient,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        TString rwClientId,
        ui64 mountSeqNumber,
        ui32 generation,
        TNonreplicatedPartitionConfigPtr srcConfig,
        TActorId volumeActorId,
        TActorId srcActorId,
        const TActiveCheckpointInfo& checkpointInfo);

    ~TShadowDiskActor() override;

    // IMigrationOwner implementation
    void OnBootstrap(const NActors::TActorContext& ctx) override;
    bool OnMessage(
        const NActors::TActorContext& ctx,
        TAutoPtr<NActors::IEventHandle>& ev) override;
    TDuration CalculateMigrationTimeout() override;
    void OnMigrationProgress(
        const NActors::TActorContext& ctx,
        ui64 migrationIndex) override;
    void OnMigrationFinished(const NActors::TActorContext& ctx) override;
    void OnMigrationError(const NActors::TActorContext& ctx) override;

private:
    void DescribeShadowDisk(const NActors::TActorContext& ctx);
    void AcquireShadowDisk(
        const NActors::TActorContext& ctx,
        EAcquireReason acquireReason);

    std::unique_ptr<TEvDiskRegistry::TEvDescribeDiskRequest>
    MakeDescribeDiskRequest() const;
    std::unique_ptr<TEvDiskRegistry::TEvAcquireDiskRequest>
    MakeAcquireDiskRequest() const;

    void HandleDescribeDiskResponse(
        const TEvDiskRegistry::TEvDescribeDiskResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleAcquireDiskResponse(
        const TEvDiskRegistry::TEvAcquireDiskResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDiskRegistryError(
        const NActors::TActorContext& ctx,
        const NProto::TError& error,
        std::unique_ptr<IEventHandle> retryEvent,
        const TString& actionName);

    void MaybeCreateShadowDiskPartitionActor(const NActors::TActorContext& ctx);
    void OnShadowDiskAcquiringCompleted(
        const NActors::TActorContext& ctx,
        EAcquireReason acquireReason);
    void SetErrorState(const NActors::TActorContext& ctx);
    void OnMaybeAcquireTimeout(const NActors::TActorContext& ctx);

    void SchedulePeriodicalReAcquire(const NActors::TActorContext& ctx);
    void OnPeriodicalReAcquire(const NActors::TActorContext& ctx);

    // If you haven't started migrating to the shadow disk yet, we can send
    // records directly to the source disk.
    bool CanJustForwardWritesToSrcDisk() const;

    // If the shadow disk is only partially migrated, and it is not ready to
    // write (because it is not captured), we reject writes to the source disk.
    bool IsWritesToSrcDiskForbidden() const;

    // If the shadow disk is not captured, or has lost capture, then user writes
    // to the source disk will not be considered completed, the client will
    // repeat recording attempts. Since we don't want to slow down the client's
    // recordings for a long time, we need to keep track of the time during
    // which the recordings did not occur in order to stop attempts to fill a
    // broken shadow disk.
    bool IsWritesToSrcDiskPossible() const;

    bool WaitingForAcquire() const;
    bool Acquired() const;
    bool ReadOnlyMount() const;

    template <typename TMethod>
    void ForwardRequestToSrcPartition(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    void ForwardRequestToShadowPartition(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    void HandleReadBlocks(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    bool HandleWriteZeroBlocks(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateShadowDiskStateResponse(
        const TEvVolumePrivate::TEvUpdateShadowDiskStateResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRdmaUnavailable(
        const TEvVolume::TEvRdmaUnavailable::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReacquireDisk(
        const TEvVolume::TEvReacquireDisk::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
