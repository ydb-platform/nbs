#include "volume_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/api/bootstrapper.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/bootstrapper/bootstrapper.h>
#include <cloud/blockstore/libs/storage/partition/part.h>
#include <cloud/blockstore/libs/storage/partition2/part2.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_mirror.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_mirror_resync.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_migration.h>
#include <cloud/blockstore/libs/storage/volume/actors/shadow_disk_actor.h>
#include <cloud/storage/core/libs/common/media.h>

#include <util/string/builder.h>

#include <contrib/ydb/core/base/tablet.h>
#include <contrib/ydb/core/tablet/tablet_setup.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NBlockStore::NStorage::NPartition;

using namespace NCloud::NStorage;

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::SendBootExternalRequest(
    const TActorContext& ctx,
    TPartitionInfo& partition)
{
    if (partition.Bootstrapper || partition.RequestingBootExternal) {
        return false;
    }

    LOG_INFO(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Requesting external boot for tablet",
        partition.TabletId);

    NCloud::Send<TEvHiveProxy::TEvBootExternalRequest>(
        ctx,
        MakeHiveProxyServiceId(),
        partition.TabletId,
        partition.TabletId,
        partition.ExternalBootTimeout);

    partition.RetryCookie.Detach();
    partition.RequestingBootExternal = true;
    return true;
}

void TVolumeActor::ScheduleRetryStartPartition(
    const TActorContext& ctx,
    TPartitionInfo& partition)
{
    const auto now = ctx.Now();
    const auto deadline = partition.RetryPolicy.GetCurrentDeadline();

    partition.RetryPolicy.Update(now);

    if (now >= deadline) {
        // Don't schedule anything, retry immediately
        SendBootExternalRequest(ctx, partition);
        return;
    }

    const auto timeout = deadline - now;

    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Waiting before retrying start of partition %lu (timeout: %s)",
        TabletID(),
        partition.TabletId,
        ToString(timeout).data());

    partition.RetryCookie.Reset(ISchedulerCookie::Make3Way());
    ctx.Schedule(
        timeout,
        new TEvVolumePrivate::TEvRetryStartPartition(
            partition.TabletId,
            partition.RetryCookie.Get()),
        partition.RetryCookie.Get());
}

void TVolumeActor::OnStarted(const TActorContext& ctx)
{
    if (!StartCompletionTimestamp) {
        StartCompletionTimestamp = ctx.Now();

        LOG_INFO(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] Volume %s started. MountSeqNumber: %lu, generation: %lu, "
            "time: %lu",
            TabletID(),
            State->GetDiskId().Quote().c_str(),
            State->GetMountSeqNumber(),
            Executor()->Generation(),
            GetStartTime().MicroSeconds());
    }

    while (!PendingRequests.empty()) {
        ctx.Send(PendingRequests.front().Event.release());
        PendingRequests.pop_front();
    }

    ProcessCheckpointRequests(ctx);
}

void TVolumeActor::StartPartitionsIfNeeded(const TActorContext& ctx)
{
    if (State) {
        switch (PartitionsStartedReason) {
            case EPartitionsStartedReason::NOT_STARTED: {
                StartPartitionsForUse(ctx);
                return;
            }
            case EPartitionsStartedReason::STARTED_FOR_GC: {
                PartitionsStartedReason = EPartitionsStartedReason::STARTED_FOR_USE;
                return;
            }
            case EPartitionsStartedReason::STARTED_FOR_USE: {
                return;
            }
        }
    }
}

void TVolumeActor::SetupDiskRegistryBasedPartitions(const TActorContext& ctx)
{
    if (State->GetMeta().GetDevices().empty()) {
        return;
    }

    const auto& volumeConfig = GetNewestConfig();
    const auto mediaKind = State->GetConfig().GetStorageMediaKind();
    const auto& volumeParams = State->GetVolumeParams();

    State->SetBlockCountToMigrate(std::nullopt);

    auto maxTimedOutDeviceStateDuration =
        volumeParams.GetMaxTimedOutDeviceStateDurationOverride(ctx.Now());
    const auto maxTimedOutDeviceStateDurationOverridden = !!maxTimedOutDeviceStateDuration;

    if (!maxTimedOutDeviceStateDuration) {
        maxTimedOutDeviceStateDuration = State->GetMaxTimedOutDeviceStateDuration();
    }

    if (!maxTimedOutDeviceStateDuration) {
        if (IsReliableDiskRegistryMediaKind(mediaKind)) {
            // reliable disks should not return io errors upon agent timeout
            // see NBS-3688
            maxTimedOutDeviceStateDuration = TDuration::Max();
        } else {
            maxTimedOutDeviceStateDuration =
                Config->GetMaxTimedOutDeviceStateDurationFeatureValue(
                    volumeConfig.GetCloudId(),
                    volumeConfig.GetFolderId(),
                    volumeConfig.GetDiskId());
        }
    }

    // see NBS-3859#63ea719d86fa93257f92c6dd
    const bool useSimpleMigrationBandwidthLimiter =
        IsReliableDiskRegistryMediaKind(mediaKind);

    auto nonreplicatedConfig = std::make_shared<TNonreplicatedPartitionConfig>(
        State->GetMeta().GetDevices(),
        State->GetMeta().GetIOMode(),
        State->GetDiskId(),
        State->GetMeta().GetConfig().GetBlockSize(),
        TNonreplicatedPartitionConfig::TVolumeInfo{
            State->GetCreationTs(),
            mediaKind},
        SelfId(),
        State->GetMeta().GetMuteIOErrors(),
        State->GetFilteredFreshDevices(),
        maxTimedOutDeviceStateDuration,
        maxTimedOutDeviceStateDurationOverridden,
        useSimpleMigrationBandwidthLimiter);

    TActorId nonreplicatedActorId;

    const auto& migrations = State->GetMeta().GetMigrations();
    const auto& metaReplicas = State->GetMeta().GetReplicas();

    if (metaReplicas.empty()) {
        if (migrations.empty()) {
            // simple nonreplicated disk
            nonreplicatedActorId = NCloud::Register(
                ctx,
                CreateNonreplicatedPartition(
                    Config,
                    DiagnosticsConfig,
                    nonreplicatedConfig,
                    SelfId(),
                    GetRdmaClient()));
        } else {
            // nonreplicated disk in migration state
            nonreplicatedActorId = NCloud::Register(
                ctx,
                CreateNonreplicatedPartitionMigration(
                    Config,
                    DiagnosticsConfig,
                    ProfileLog,
                    BlockDigestGenerator,
                    State->GetMeta().GetMigrationIndex(),
                    State->GetReadWriteAccessClientId(),
                    nonreplicatedConfig,
                    migrations,
                    GetRdmaClient(),
                    SelfId()));
        }
    } else {
        // mirrored disk
        TVector<TDevices> replicas;
        for (const auto& metaReplica: metaReplicas) {
            replicas.push_back(metaReplica.GetDevices());
        }

        // XXX naming (nonreplicated)
        if (State->IsMirrorResyncNeeded()) {
            // mirrored disk in resync state
            nonreplicatedActorId = NCloud::Register(
                ctx,
                CreateMirrorPartitionResync(
                    Config,
                    DiagnosticsConfig,
                    ProfileLog,
                    BlockDigestGenerator,
                    State->GetReadWriteAccessClientId(),
                    nonreplicatedConfig,
                    migrations,
                    std::move(replicas),
                    GetRdmaClient(),
                    SelfId(),
                    State->GetMeta().GetResyncIndex()));
        } else {
            // mirrored disk (may be in migration state)
            nonreplicatedActorId = NCloud::Register(
                ctx,
                CreateMirrorPartition(
                    Config,
                    DiagnosticsConfig,
                    ProfileLog,
                    BlockDigestGenerator,
                    State->GetReadWriteAccessClientId(),
                    nonreplicatedConfig,
                    migrations,
                    std::move(replicas),
                    GetRdmaClient(),
                    SelfId()));
        }
    }

    State->SetDiskRegistryBasedPartitionActor(
        WrapNonreplActorIfNeeded(ctx, nonreplicatedActorId, nonreplicatedConfig),
        nonreplicatedConfig);
}

NActors::TActorId TVolumeActor::WrapNonreplActorIfNeeded(
    const TActorContext& ctx,
    NActors::TActorId nonreplicatedActorId,
    std::shared_ptr<TNonreplicatedPartitionConfig> srcConfig)
{
    for (const auto& [checkpointId, checkpointInfo]:
         State->GetCheckpointStore().GetActiveCheckpoints())
    {
        if (checkpointInfo.HasShadowActor ||
            !State->GetCheckpointStore().NeedShadowActor(checkpointId))
        {
            continue;
        }

        nonreplicatedActorId = NCloud::Register<TShadowDiskActor>(
            ctx,
            Config,
            DiagnosticsConfig,
            GetRdmaClient(),
            ProfileLog,
            BlockDigestGenerator,
            State->GetReadWriteAccessClientId(),
            State->GetMountSeqNumber(),
            Executor()->Generation(),
            srcConfig,
            SelfId(),
            nonreplicatedActorId,
            checkpointInfo);

        State->GetCheckpointStore().ShadowActorCreated(checkpointId);
        DoRegisterVolume(ctx, checkpointInfo.ShadowDiskId);
    }
    return nonreplicatedActorId;
}

void TVolumeActor::RestartDiskRegistryBasedPartition(
    const TActorContext& ctx,
    TDiskRegistryBasedPartitionStoppedCallback onPartitionStopped)
{
    if (!State->IsDiskRegistryMediaKind()) {
        if (onPartitionStopped) {
            std::invoke(onPartitionStopped, ctx);
        }
        return;
    }

    StopPartitions(ctx, std::move(onPartitionStopped));
    StartPartitionsForUse(ctx);
    ResetServicePipes(ctx);
}

void TVolumeActor::StartPartitionsImpl(const TActorContext& ctx)
{
    StartInitializationTimestamp = ctx.Now();

    Y_ABORT_UNLESS(State);
    State->SetReadWriteError({});

    // Request storage info for partitions
    for (auto& partition: State->GetPartitions()) {
        partition.ExternalBootTimeout = Config->GetMinExternalBootRequestTimeout();
        SendBootExternalRequest(ctx, partition);
    }

    if (State->IsDiskRegistryMediaKind()) {
        SetupDiskRegistryBasedPartitions(ctx);

        if (State->Ready()) {
            OnStarted(ctx);
        }
    }
}

void TVolumeActor::StartPartitionsForUse(const TActorContext& ctx)
{
    StartPartitionsImpl(ctx);
    PartitionsStartedReason = EPartitionsStartedReason::STARTED_FOR_USE;
}

void TVolumeActor::StartPartitionsForGc(const TActorContext& ctx)
{
    StartPartitionsImpl(ctx);
    PartitionsStartedReason = EPartitionsStartedReason::STARTED_FOR_GC;
}

void TVolumeActor::StopPartitions(
    const TActorContext& ctx,
    TDiskRegistryBasedPartitionStoppedCallback onPartitionStopped)
{
    if (!State) {
        if (onPartitionStopped) {
            std::invoke(onPartitionStopped, ctx);
        }
        return;
    }

    ui64 requestId = 0;
    if (onPartitionStopped) {
        requestId = VolumeRequestIdGenerator->GetValue();
        OnPartitionStopped[requestId] = std::move(onPartitionStopped);
    }

    for (const auto& [checkpointId, _]:
         State->GetCheckpointStore().GetActiveCheckpoints())
    {
        State->GetCheckpointStore().ShadowActorDestroyed(checkpointId);
    }

    for (auto& part : State->GetPartitions()) {
        // Reset previous boot attempts
        part.RetryCookie.Detach();
        part.RequestingBootExternal = false;
        part.SuggestedGeneration = 0;
        part.StorageInfo = {};
        part.ExternalBootTimeout = Config->GetMinExternalBootRequestTimeout();

        // Stop any currently booting or running tablet
        if (part.Bootstrapper) {
            NCloud::Send<TEvBootstrapper::TEvStop>(
                ctx,
                part.Bootstrapper);
            // Should clear bootstrapper before partitions start
            part.Bootstrapper = {};
        }
    }

    if (auto actorId = State->GetDiskRegistryBasedPartitionActor()) {
        LOG_INFO(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] Send poison pill to partition %s",
            TabletID(),
            actorId.ToString().c_str());

        NCloud::Send<TEvents::TEvPoisonPill>(ctx, actorId, requestId);
        State->SetDiskRegistryBasedPartitionActor({}, nullptr);
    }
}

void TVolumeActor::HandleRdmaUnavailable(
    const TEvVolume::TEvRdmaUnavailable::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_WARN(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Rdma unavailable, restarting without rdma",
        TabletID());

    StopPartitions(ctx, {});
    State->SetRdmaUnavailable();
    StartPartitionsForUse(ctx);
}

void TVolumeActor::HandleRetryStartPartition(
    const TEvVolumePrivate::TEvRetryStartPartition::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& msg = ev->Get();

    if (msg->Cookie.DetachEvent()) {
        auto* part = State->GetPartition(msg->TabletId);

        Y_ABORT_UNLESS(part, "Scheduled retry for missing partition %lu", msg->TabletId);

        part->RetryCookie.Detach();

        SendBootExternalRequest(ctx, *part);
    }
}

void TVolumeActor::HandleBootExternalResponse(
    const TEvHiveProxy::TEvBootExternalResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const ui64 partTabletId = ev->Cookie;

    auto* part = State->GetPartition(partTabletId);

    if (!part || !part->RequestingBootExternal || part->Bootstrapper) {
        LOG_ERROR(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] Received unexpected external boot info for part %lu",
            TabletID(),
            partTabletId);
        return;
    }

    part->RequestingBootExternal = false;

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] BootExternalRequest for part %lu failed: %s",
            TabletID(),
            partTabletId,
            FormatError(error).data());

        part->ExternalBootTimeout = Min(
            part->ExternalBootTimeout + Config->GetExternalBootRequestTimeoutIncrement(),
            Config->GetMaxExternalBootRequestTimeout());

        ++FailedBoots;

        part->SetFailed(TStringBuilder()
            << "BootExternalRequest failed: " << FormatError(error));
        ScheduleRetryStartPartition(ctx, *part);
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Received external boot info for part %lu",
        TabletID(),
        partTabletId);

    if (msg->StorageInfo->TabletType != TTabletTypes::BlockStorePartition &&
        msg->StorageInfo->TabletType != TTabletTypes::BlockStorePartition2) {
        // Partitions use specific tablet factory
        LOG_ERROR_S(ctx, TBlockStoreComponents::VOLUME,
            "[" << TabletID() << "] Unexpected part " << partTabletId
            << " with type " << msg->StorageInfo->TabletType);
        part->SetFailed(
            TStringBuilder()
                << "Unexpected tablet type: "
                << msg->StorageInfo->TabletType);
        // N.B.: this is a fatal error, don't retry
        return;
    }

    Y_ABORT_UNLESS(msg->StorageInfo->TabletID == partTabletId,
        "Tablet IDs mismatch: %lu vs %lu",
        msg->StorageInfo->TabletID,
        partTabletId);

    if (msg->SuggestedGeneration > part->SuggestedGeneration) {
        part->SuggestedGeneration = msg->SuggestedGeneration;
    }
    part->StorageInfo = msg->StorageInfo;

    const auto* appData = AppData(ctx);

    auto config = Config;
    auto partitionConfig = part->PartitionConfig;
    auto diagnosticsConfig = DiagnosticsConfig;
    auto profileLog = ProfileLog;
    auto blockDigestGenerator = BlockDigestGenerator;
    auto storageAccessMode = State->GetStorageAccessMode();
    auto siblingCount = State->GetPartitions().size();
    auto selfId = SelfId();

    auto factory = [=](
                       const TActorId& owner,
                       TTabletStorageInfo* storage) mutable
    {
        Y_ABORT_UNLESS(
            storage->TabletType == TTabletTypes::BlockStorePartition ||
            storage->TabletType == TTabletTypes::BlockStorePartition2);

        if (storage->TabletType == TTabletTypes::BlockStorePartition) {
            return NPartition::CreatePartitionTablet(
                       owner,
                       storage,
                       std::move(config),
                       std::move(diagnosticsConfig),
                       std::move(profileLog),
                       std::move(blockDigestGenerator),
                       std::move(partitionConfig),
                       storageAccessMode,
                       siblingCount,
                       selfId)
                .release();
        } else {
            return NPartition2::CreatePartitionTablet(
                       owner,
                       storage,
                       std::move(config),
                       std::move(diagnosticsConfig),
                       std::move(profileLog),
                       std::move(blockDigestGenerator),
                       std::move(partitionConfig),
                       storageAccessMode,
                       siblingCount,
                       selfId)
                .release();
        }
    };

    auto setupInfo = MakeIntrusive<TTabletSetupInfo>(
        factory,
        TMailboxType::ReadAsFilled,
        appData->UserPoolId,
        TMailboxType::ReadAsFilled,
        appData->SystemPoolId);

    TBootstrapperConfig bootConfig;
    bootConfig.SuggestedGeneration = part->SuggestedGeneration;
    bootConfig.BootAttemptsThreshold = 1;

    auto bootstrapper = NCloud::RegisterLocal(ctx, CreateBootstrapper(
        bootConfig,
        SelfId(),
        part->StorageInfo,
        std::move(setupInfo)));

    part->Init(bootstrapper);

    NCloud::Send<TEvBootstrapper::TEvStart>(ctx, bootstrapper);

    LOG_INFO(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Starting partition %lu with bootstrapper %s",
        TabletID(),
        partTabletId,
        ToString(bootstrapper).c_str());
}

void TVolumeActor::HandleTabletStatus(
    const TEvBootstrapper::TEvStatus::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto* partition = State->GetPartition(msg->TabletId);
    Y_ABORT_UNLESS(partition, "Missing partition state for %lu", msg->TabletId);

    if (partition->Bootstrapper != ev->Sender) {
        LOG_INFO_S(ctx, TBlockStoreComponents::VOLUME,
            "[" << TabletID() << "]" <<
            " Ignored status message " << static_cast<ui32>(msg->Status) <<
            " from outdated bootstrapper " << ToString(ev->Sender) <<
            " for partition " << msg->TabletId);
        // CompleteUpdateConfig calls StopPartitions, and then it
        // calls StartPartitions with a completely new state.
        // Ignore any signals from outdated bootstrappers.
        return;
    }

    bool shouldRestart = false;
    bool suggestOutdated = false;

    switch (msg->Status) {
        case TEvBootstrapper::STARTED:
            partition->SetStarted(msg->TabletUser);
            NCloud::Send<TEvPartition::TEvWaitReadyRequest>(
                ctx,
                msg->TabletUser,
                msg->TabletId);
            break;
        case TEvBootstrapper::STOPPED:
            partition->RetryPolicy.Reset(ctx.Now());
            partition->Bootstrapper = {};
            partition->SetStopped();
            break;
        case TEvBootstrapper::RACE:
            shouldRestart = true;
            partition->RetryPolicy.Reset(ctx.Now());
            break;
        case TEvBootstrapper::SUGGEST_OUTDATED:
            shouldRestart = true;
            suggestOutdated = true;
            // Retry immediately when hive generation is out of sync
            partition->RetryPolicy.Reset(ctx.Now());
            break;
        case TEvBootstrapper::FAILED:
            if (partition->State == TPartitionInfo::EState::READY &&
                    ctx.Now() > partition->RetryPolicy.GetCurrentDeadline())
            {
                partition->RetryPolicy.Reset(ctx.Now());
            }
            shouldRestart = true;
            break;
    }

    if (shouldRestart) {
        partition->Bootstrapper = {};
        partition->SetFailed(msg->Message);
        if (partition->StorageInfo) {
            partition->StorageInfo = {};
            if (suggestOutdated) {
                partition->SuggestedGeneration += 1;
            } else {
                partition->SuggestedGeneration = 0;
            }
            ScheduleRetryStartPartition(ctx, *partition);
        }
    }

    auto state = State->UpdatePartitionsState();

    if (state == TPartitionInfo::READY) {
        // All partitions ready, it's time to reply to requests
        OnStarted(ctx);
    }
}

void TVolumeActor::HandleWaitReadyResponse(
    const TEvPartition::TEvWaitReadyResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 tabletId = ev->Cookie;

    auto* partition = State->GetPartition(tabletId);

    // Drop unexpected responses in case of restart races
    if (partition &&
        partition->State == TPartitionInfo::STARTED &&
        partition->Owner == ev->Sender)
    {
        partition->SetReady();

        auto state = State->UpdatePartitionsState();

        if (state == TPartitionInfo::READY) {
            // All partitions ready, it's time to reply to requests
            OnStarted(ctx);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
