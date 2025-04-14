#pragma once

#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_migration_common_actor.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>
#include <cloud/storage/core/libs/common/backoff_delay_provider.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

// The actor for shadow disk. Can migrate data from the source disk into the
// shadow disk and serves read requests from the checkpoint.
class TShadowDiskActor final
    : public TNonreplicatedPartitionMigrationCommonActor
    , public IMigrationOwner
{
public:
    enum class EAcquireReason
    {
        FirstAcquire,
        PeriodicalReAcquire,
        ForcedReAcquire,
    };

private:

    // We always start from the WaitAcquireFor* or Error states.
    //
    // WaitAcquireForPrepareStart -> Preparing -> CheckpointReady
    //             |                     |
    //             v                     v
    //           Error                  Error
    //
    // WaitAcquireForPrepareContinue -> Preparing -> CheckpointReady
    //             |                       |
    //             v                       v
    //           Error                    Error
    //
    // WaitAcquireForRead -> CheckpointReady
    //             |
    //             v
    //           Error

    enum class EActorState
    {
        // We are waiting for the acquire of the shadow disk devices. The shadow
        // disk has not filled up yet and we are starting from the beginning of
        // the disk.
        // We do not block writes to the source disk.
        WaitAcquireForPrepareStart,

        // Waiting for the acquire of the shadow disk devices. The
        // shadow disk has already been partially filled, and we are not
        // starting from the beginning of the disk.
        // We block writes to the source disk.
        WaitAcquireForPrepareContinue,

        // Waiting for the acquire of the shadow disk devices. The disk is
        // already completely filled and we will only read checkpoint data from it.
        WaitAcquireForRead,

        // The devices of the shadow disk have been successfully acquired and we
        // are currently filling it.
        Preparing,

        // The devices of the shadow disk have been successfully acquired and we
        // are ready to read the checkpoint data.
        CheckpointReady,

        // Something went wrong and we stopped filling the shadow disk.
        // At the same time, we do not interfere with the operation of the
        // source disk.
        Error,
    };

    const NRdma::IClientPtr RdmaClient;
    const TNonreplicatedPartitionConfigPtr SrcConfig;
    const TString CheckpointId;
    const TString ShadowDiskId;
    const ui64 MountSeqNumber = 0;
    const ui32 Generation = 0;
    const NActors::TActorId VolumeActorId;
    const NActors::TActorId SrcActorId;

    TString SourceDiskClientId;
    // We update CurrentShadowDiskClientId when we send request to
    // TAcquireShadowDiskActor with desired clientId.
    TString CurrentShadowDiskClientId;
    TNonreplicatedPartitionConfigPtr DstConfig;
    NActors::TActorId DstActorId;
    ui64 ProcessedBlockCount = 0;

    EActorState State = EActorState::Error;

    NActors::TActorId AcquireActorId;
    // The list of devices received on first acquire.
    TDevices ShadowDiskDevices;

    bool ForcedReAcquireInProgress = false;

public:
    TShadowDiskActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticConfig,
        NRdma::IClientPtr rdmaClient,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        TString sourceDiskClientId,
        ui64 mountSeqNumber,
        ui32 generation,
        TNonreplicatedPartitionConfigPtr srcConfig,
        NActors::TActorId volumeActorId,
        NActors::TActorId srcActorId,
        const TActiveCheckpointInfo& checkpointInfo);

    ~TShadowDiskActor() override;

    // IMigrationOwner implementation
    void OnBootstrap(const NActors::TActorContext& ctx) override;
    bool OnMessage(
        const NActors::TActorContext& ctx,
        TAutoPtr<NActors::IEventHandle>& ev) override;
    void OnMigrationProgress(
        const NActors::TActorContext& ctx,
        ui64 migrationIndex) override;
    void OnMigrationFinished(const NActors::TActorContext& ctx) override;
    void OnMigrationError(const NActors::TActorContext& ctx) override;

private:
    enum EShadowDiskWakeupReason
    {
        SDWR_REACQUIRE =
            TNonreplicatedPartitionMigrationCommonActor::WR_REASON_COUNT,
    };

    void AcquireShadowDisk(
        const NActors::TActorContext& ctx,
        EAcquireReason acquireReason);
    void HandleShadowDiskAcquired(
        const TEvVolumePrivate::TEvShadowDiskAcquired::TPtr& ev,
        const NActors::TActorContext& ctx);

    void CreateShadowDiskConfig();
    void CreateShadowDiskPartitionActor(
        const NActors::TActorContext& ctx,
        const TDevices& acquiredShadowDiskDevices);
    void SetErrorState(const NActors::TActorContext& ctx);
    void SchedulePeriodicalReAcquire(const NActors::TActorContext& ctx);

    // If we haven't started migrating to the shadow disk yet, we can send
    // write and zero requests directly to the source disk.
    bool CanJustForwardWritesToSrcDisk() const;

    // If the shadow disk is only partially filled, and it is not ready to
    // write (because it is not acquired), we reject writes to the source disk.
    bool AreWritesToSrcDiskForbidden() const;

    // If the shadow disk is not acquired, or has lost acquiring, then user
    // writes to the source disk will not be considered completed, the client
    // will repeat write attempts. Since we don't want to slow down the
    // client's writing for a long time, we need to keep track of the time
    // during which the writing did not occur in order to stop attempts to
    // fill a broken shadow disk.
    bool AreWritesToSrcDiskImpossible() const;

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
    [[nodiscard]] bool HandleWriteZeroBlocks(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateShadowDiskStateResponse(
        const TEvVolumePrivate::TEvUpdateShadowDiskStateResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRdmaUnavailable(
        const TEvVolume::TEvRdmaUnavailable::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReacquireDisk(
        const TEvVolume::TEvReacquireDisk::TPtr& ev,
        const NActors::TActorContext& ctx);

    [[nodiscard]] bool HandleRWClientIdChanged(
        const TEvVolume::TEvRWClientIdChanged::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetChangedBlocks(
        const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
