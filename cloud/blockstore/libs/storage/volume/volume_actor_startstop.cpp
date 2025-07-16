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

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/media.h>

#include <contrib/ydb/core/base/tablet.h>
#include <contrib/ydb/core/tablet/tablet_setup.h>

#include <util/string/builder.h>

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

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Requesting external boot for partition tablet: [%s]",
        LogTitle.GetWithTime().c_str(),
        TLogTitle::GetPartitionPrefix(
            partition.TabletId,
            partition.PartitionIndex,
            State->GetMeta().GetPartitions().size())
            .c_str());

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

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Waiting before retrying start of partition %lu (timeout: %s)",
        LogTitle.GetWithTime().c_str(),
        partition.TabletId,
        FormatDuration(timeout).c_str());

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

        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Volume started. MountSeqNumber: %lu, load time: %s, start "
            "time: %s",
            LogTitle.GetWithTime().c_str(),
            State->GetMountSeqNumber(),
            FormatDuration(GetLoadTime()).c_str(),
            FormatDuration(GetStartTime()).c_str());
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

    TNonreplicatedPartitionConfig::TNonreplicatedPartitionConfigInitParams
        params{
            State->GetMeta().GetDevices(),
            TNonreplicatedPartitionConfig::TVolumeInfo{
                .CreationTs = State->GetCreationTs(),
                .MediaKind = mediaKind,
                .EncryptionMode = static_cast<NProto::EEncryptionMode>(
                    volumeConfig.GetEncryptionDesc().GetMode())},
            State->GetDiskId(),
            State->GetMeta().GetConfig().GetBlockSize(),
            SelfId(),
            State->GetMeta().GetIOMode(),
            State->GetMeta().GetMuteIOErrors(),
            State->GetFilteredFreshDevices(),
            State->GetLaggingDeviceIds(),
            LaggingDevicesAreAllowed(),
            maxTimedOutDeviceStateDuration,
            maxTimedOutDeviceStateDurationOverridden,
            useSimpleMigrationBandwidthLimiter,
        };
    auto nonreplicatedConfig =
        std::make_shared<TNonreplicatedPartitionConfig>(std::move(params));

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
            auto resyncPolicy = State->IsForceMirrorResync()
                                    ? Config->GetForceResyncPolicy()
                                    : Config->GetAutoResyncPolicy();
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
                    State->GetMeta().GetResyncIndex(),
                    resyncPolicy,
                    State->GetMeta().GetAlertResyncChecksumMismatch()));
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
    ReportOutdatedLaggingDevicesToDR(ctx);
}

TActorsStack TVolumeActor::WrapNonreplActorIfNeeded(
    const TActorContext& ctx,
    NActors::TActorId nonreplicatedActorId,
    std::shared_ptr<TNonreplicatedPartitionConfig> srcConfig)
{
    TActorsStack result;
    result.Push(nonreplicatedActorId);

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

        result.Push(nonreplicatedActorId);
        State->GetCheckpointStore().ShadowActorCreated(checkpointId);
        DoRegisterVolume(ctx, checkpointInfo.ShadowDiskId);
    }
    return result;
}

void TVolumeActor::RestartPartition(
    const TActorContext& ctx,
    TPoisonCallback onPartitionStopped)
{
    StopPartitions(ctx, std::move(onPartitionStopped));

    switch (PartitionsStartedReason) {
        case EPartitionsStartedReason::STARTED_FOR_GC: {
            StartPartitionsForGc(ctx);
            break;
        }
        case EPartitionsStartedReason::STARTED_FOR_USE: {
            StartPartitionsForUse(ctx);
            break;
        }
        case EPartitionsStartedReason::NOT_STARTED: {
            break;
        }
    };

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

void TVolumeActor::HandleGracefulShutdown(
    const TEvVolume::TEvGracefulShutdownRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!State->IsDiskRegistryMediaKind()) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s GracefulShutdown request was sent to non-DR based disk",
            LogTitle.GetWithTime().c_str());

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvVolume::TEvGracefulShutdownResponse>(
                MakeError(E_NOT_IMPLEMENTED, "request is not supported")));
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Stop Partition before volume destruction",
        LogTitle.GetWithTime().c_str());

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext);
    TPoisonCallback onPartitionStopped = [requestInfo = std::move(requestInfo)](
                                             const TActorContext& ctx,
                                             NProto::TError error)
    {
        Y_UNUSED(error);

        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvVolume::TEvGracefulShutdownResponse>());
    };

    StopPartitions(ctx, std::move(onPartitionStopped));

    TerminateTransactions(ctx);
    KillActors(ctx);
    CancelRequests(ctx);
}

void TVolumeActor::StopPartitions(
    const TActorContext& ctx,
    TPoisonCallback onPartitionStopped)
{
    if (!State) {
        if (onPartitionStopped) {
            std::invoke(std::move(onPartitionStopped), ctx, MakeError(S_ALREADY));
        }
        return;
    }

    for (const auto& [checkpointId, _]:
         State->GetCheckpointStore().GetActiveCheckpoints())
    {
        State->GetCheckpointStore().ShadowActorDestroyed(checkpointId);
    }

    if (State->IsDiskRegistryMediaKind()) {
        StopDiskRegistryBasedPartition(ctx, std::move(onPartitionStopped));
        return;
    }

    // onPartitionStopped should used for DiskRegistry based volumes.
    Y_DEBUG_ABORT_UNLESS(!onPartitionStopped);

    for (auto& part: State->GetPartitions()) {
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
}

void TVolumeActor::StopDiskRegistryBasedPartition(
    const NActors::TActorContext& ctx,
    TPoisonCallback onPartitionStopped)
{
    const ui64 requestId = ++PartitionRestartCounter;
    const auto actorId = State->GetDiskRegistryBasedPartitionActor();

    WaitForPartitionDestroy.push_back(TPartitionDestroyCallback{
        .VolumeRequestId = requestId,
        .PartitionActorId = actorId,
        .PoisonCallback = std::move(onPartitionStopped)});

    if (actorId) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Send poison pill to partition %s",
            LogTitle.GetWithTime().c_str(),
            actorId.ToString().c_str());

        NCloud::Send<TEvents::TEvPoisonPill>(ctx, actorId, requestId);
        State->SetDiskRegistryBasedPartitionActor({}, nullptr);
    } else {
        OnDiskRegistryBasedPartitionStopped(
            ctx,
            actorId,
            requestId,
            MakeError(S_ALREADY));
    }
}

void TVolumeActor::OnDiskRegistryBasedPartitionStopped(
    const NActors::TActorContext& ctx,
    NActors::TActorId sender,
    ui64 volumeRequestId,
    NProto::TError error)
{
    for (auto& callback: WaitForPartitionDestroy) {
        if (callback.VolumeRequestId == volumeRequestId || volumeRequestId == 0)
        {
            callback.Destroyed = true;
            Y_DEBUG_ABORT_UNLESS(
                volumeRequestId == 0 || callback.PartitionActorId == sender);
        }
    }

    while (WaitForPartitionDestroy) {
        auto& callback = WaitForPartitionDestroy.front();
        if (!callback.Destroyed) {
            break;
        }
        if (callback.PoisonCallback) {
            std::invoke(std::move(callback.PoisonCallback), ctx, error);
        }
        WaitForPartitionDestroy.pop_front();
    }

    TVector<TString> stillWait;
    for (const auto& callback: WaitForPartitionDestroy) {
        stillWait.push_back(
            TStringBuilder()
            << "{ requestId="
            << TCompositeId::FromRaw(callback.VolumeRequestId).Print()
            << ", actorId=" << callback.PartitionActorId << " }");
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Partition stoped. Still wait [%s]",
        LogTitle.GetWithTime().c_str(),
        JoinSeq(", ", stillWait).c_str());
}

void TVolumeActor::HandleRdmaUnavailable(
    const TEvVolume::TEvRdmaUnavailable::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_WARN(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Rdma unavailable, restarting without rdma",
        LogTitle.GetWithTime().c_str());

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
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Received unexpected external boot info for part %lu",
            LogTitle.GetWithTime().c_str(),
            partTabletId);
        return;
    }

    part->RequestingBootExternal = false;

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s BootExternalRequest for part %lu failed: %s",
            LogTitle.GetWithTime().c_str(),
            partTabletId,
            FormatError(error).c_str());

        part->ExternalBootTimeout = Min(
            part->ExternalBootTimeout + Config->GetExternalBootRequestTimeoutIncrement(),
            Config->GetMaxExternalBootRequestTimeout());

        ++FailedBoots;

        part->SetFailed(TStringBuilder()
            << "BootExternalRequest failed: " << FormatError(error));
        ScheduleRetryStartPartition(ctx, *part);
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Received external boot info for part %lu",
        LogTitle.GetWithTime().c_str(),
        partTabletId);

    if (msg->StorageInfo->TabletType != TTabletTypes::BlockStorePartition &&
        msg->StorageInfo->TabletType != TTabletTypes::BlockStorePartition2) {
        // Partitions use specific tablet factory
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Unexpected part %lu with type %s",
            LogTitle.GetWithTime().c_str(),
            partTabletId,
            ToString(msg->StorageInfo->TabletType).c_str());
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

    {
        auto request = std::make_unique<TEvStatsService::TEvPartitionBootExternalCompleted>(
            State->GetDiskId(),
            partTabletId,
            msg->StorageInfo->Channels);
        NCloud::Send(ctx, MakeStorageStatsServiceId(), std::move(request));
    }

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
    auto partitionIndex = part->PartitionIndex;
    auto siblingCount = State->GetPartitions().size();
    auto selfId = SelfId();
    auto volumeTabletId = TabletID();

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
                       partitionIndex,
                       siblingCount,
                       selfId,
                       volumeTabletId)
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
                       selfId,
                       volumeTabletId)
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

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Starting partition [%s g:%u] with bootstrapper %s",
        LogTitle.GetWithTime().c_str(),
        TLogTitle::GetPartitionPrefix(partTabletId, partitionIndex, siblingCount).c_str(),
        bootConfig.SuggestedGeneration,
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
        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Ignored status message %lu from outdated bootstrapper %s for "
            "partition %lu",
            LogTitle.GetWithTime().c_str(),
            static_cast<ui32>(msg->Status),
            ToString(ev->Sender).c_str(),
            msg->TabletId);
        // CompleteUpdateConfig calls StopPartitions, and then it
        // calls StartPartitions with a completely new state.
        // Ignore any signals from outdated bootstrappers.
        return;
    }

    bool shouldRestart = false;
    bool suggestOutdated = false;

    switch (msg->Status) {
        case TEvBootstrapper::STARTED: {
            TActorsStack actors;
            actors.Push(msg->TabletUser);
            partition->SetStarted(std::move(actors));
            NCloud::Send<TEvPartition::TEvWaitReadyRequest>(
                ctx,
                msg->TabletUser,
                msg->TabletId);
            break;
        }
        case TEvBootstrapper::STOPPED: {
            partition->RetryPolicy.Reset(ctx.Now());
            partition->Bootstrapper = {};
            partition->SetStopped();
            break;
        }
        case TEvBootstrapper::RACE: {
            shouldRestart = true;
            partition->RetryPolicy.Reset(ctx.Now());
            break;
        }
        case TEvBootstrapper::SUGGEST_OUTDATED: {
            shouldRestart = true;
            suggestOutdated = true;
            // Retry immediately when hive generation is out of sync
            partition->RetryPolicy.Reset(ctx.Now());
            break;
        }
        case TEvBootstrapper::FAILED: {
            if (partition->State == TPartitionInfo::EState::READY &&
                ctx.Now() > partition->RetryPolicy.GetCurrentDeadline())
            {
                partition->RetryPolicy.Reset(ctx.Now());
            }
            shouldRestart = true;
            break;
        }
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
        partition->IsKnownActorId(ev->Sender))
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
