#include "volume_actor.h"

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/common/media.h>

#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_CACHED_COUNTERS(xxx, ...)                                   \
    xxx(MixedBytesCount,                                           __VA_ARGS__)\
    xxx(MergedBytesCount,                                          __VA_ARGS__)\
    xxx(FreshBytesCount,                                           __VA_ARGS__)\
    xxx(UsedBytesCount,                                            __VA_ARGS__)\
    xxx(LogicalUsedBytesCount,                                     __VA_ARGS__)\
    xxx(BytesCount,                                                __VA_ARGS__)\
    xxx(CheckpointBytes,                                           __VA_ARGS__)\
    xxx(CompactionScore,                                           __VA_ARGS__)\
    xxx(CompactionGarbageScore,                                    __VA_ARGS__)\
    xxx(CleanupQueueBytes,                                         __VA_ARGS__)\
    xxx(GarbageQueueBytes,                                         __VA_ARGS__)\
    xxx(ChannelHistorySize,                                        __VA_ARGS__)\
    xxx(UnconfirmedBlobCount,                                      __VA_ARGS__)\
    xxx(ConfirmedBlobCount,                                        __VA_ARGS__)\
// BLOCKSTORE_CACHED_COUNTERS

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::CopyCachedStatsToPartCounters(
    const NProto::TCachedPartStats& src,
    TPartitionStatInfo& dst)
{
#define POPULATE_COUNTERS(name, ...)                                           \
    dst.CachedCounters.Simple.name.Set(src.Get##name());                       \
// POPULATE_COUNTERS

    BLOCKSTORE_CACHED_COUNTERS(POPULATE_COUNTERS)
#undef POPULATE_COUNTERS

    dst.CachedCountersProto = src;
}

void TVolumeActor::CopyPartCountersToCachedStats(
    const TPartitionDiskCounters& src,
    NProto::TCachedPartStats& dst)
{
#define CACHE_COUNTERS(name, ...)                                             \
    dst.Set##name(src.Simple.name.Value);                                     \
// CACHE_COUNTERS

    BLOCKSTORE_CACHED_COUNTERS(CACHE_COUNTERS)
#undef CACHE_COUNTERS
}

void TVolumeActor::UpdateCachedStats(
    const TPartitionDiskCounters& src,
    TPartitionDiskCounters& dst)
{
#define UPDATE_COUNTERS(name, ...)                                            \
    dst.Simple.name.Value = src.Simple.name.Value;                            \
// UPDATE_COUNTERS

    BLOCKSTORE_CACHED_COUNTERS(UPDATE_COUNTERS)
#undef UPDATE_COUNTERS
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::UpdateActorStats(const TActorContext& ctx)
{
    if (Counters) {
        auto& actorQueue = Counters->Percentile()
            [TVolumeCounters::PERCENTILE_COUNTER_Actor_ActorQueue];
        auto& mailboxQueue = Counters->Percentile()
            [TVolumeCounters::PERCENTILE_COUNTER_Actor_MailboxQueue];

        auto actorQueues = ctx.CountMailboxEvents(1001);
        IncrementFor(actorQueue, actorQueues.first);
        IncrementFor(mailboxQueue, actorQueues.second);
    }
}

void TVolumeActor::HandlePartStatsSaved(
    const TEvVolumePrivate::TEvPartStatsSaved::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
}

void TVolumeActor::HandleDiskRegistryBasedPartCounters(
    const TEvVolume::TEvDiskRegistryBasedPartitionCounters::TPtr& ev,
    const TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(State->IsDiskRegistryMediaKind());

    auto* msg = ev->Get();

    if (auto* resourceMetrics = GetResourceMetrics(); resourceMetrics) {
        bool changed = false;
        if (msg->CpuUsage) {
            resourceMetrics->CPU.Increment(
                msg->CpuUsage.MicroSeconds(),
                ctx.Now());
            changed = true;
        }
        if (msg->NetworkBytes) {
            resourceMetrics->Network.Increment(msg->NetworkBytes, ctx.Now());
            changed = true;
        }

        if (changed) {
            resourceMetrics->TryUpdate(ctx);
        }
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext
    );

    auto* statInfo = State->GetPartitionStatByDiskId(msg->DiskId);

    if (!statInfo) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Counters from partition %s (%s) do not belong to disk %s",
            ToString(ev->Sender).c_str(),
            msg->DiskId.Quote().c_str(),
            State->GetDiskId().Quote().c_str());
        return;
    }

    if (!statInfo->LastCounters) {
        statInfo->LastCounters = CreatePartitionDiskCounters(
            State->CountersPolicy(),
            DiagnosticsConfig->GetHistogramCounterOptions());
    }

    statInfo->LastCounters->Add(*msg->DiskCounters);

    UpdateCachedStats(*msg->DiskCounters, statInfo->CachedCounters);
    CopyPartCountersToCachedStats(
        *msg->DiskCounters,
        statInfo->CachedCountersProto);

    TVolumeDatabase::TPartStats partStats;
    partStats.Stats = statInfo->CachedCountersProto;

    ExecuteTx<TSavePartStats>(
        ctx,
        std::move(requestInfo),
        std::move(partStats));
}

void TVolumeActor::HandlePartCounters(
    const TEvStatsService::TEvVolumePartCounters::TPtr& ev,
    const TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(!State->IsDiskRegistryMediaKind());

    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext
    );

    auto tabletId = State->FindPartitionTabletId(ev->Sender);
    auto* statInfo =
        tabletId ? State->GetPartitionStatInfoByTabletId(*tabletId) : nullptr;
    if (!statInfo) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Partition %s for disk %s counters not found",
            ToString(ev->Sender).c_str(),
            State->GetDiskId().Quote().c_str());
        return;
    }

    if (!statInfo->LastCounters) {
        statInfo->LastCounters = CreatePartitionDiskCounters(
            State->CountersPolicy(),
            DiagnosticsConfig->GetHistogramCounterOptions());
        statInfo->LastMetrics = std::move(msg->BlobLoadMetrics);
    }

    statInfo->LastSystemCpu += msg->VolumeSystemCpu;
    statInfo->LastUserCpu += msg->VolumeUserCpu;

    statInfo->LastCounters->Add(*msg->DiskCounters);

    UpdateCachedStats(*msg->DiskCounters, statInfo->CachedCounters);
    CopyPartCountersToCachedStats(
        *msg->DiskCounters,
        statInfo->CachedCountersProto);

    TVolumeDatabase::TPartStats partStats;

    partStats.TabletId = statInfo->TabletId;
    partStats.Stats = statInfo->CachedCountersProto;

    ExecuteTx<TSavePartStats>(
        ctx,
        std::move(requestInfo),
        std::move(partStats));
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareSavePartStats(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TSavePartStats& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteSavePartStats(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TSavePartStats& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);
    if (State->IsDiskRegistryMediaKind()) {
        db.WriteNonReplPartStats(args.PartStats.TabletId, args.PartStats.Stats);
    } else {
        Y_DEBUG_ABORT_UNLESS(args.PartStats.TabletId);
        db.WritePartStats(args.PartStats.TabletId, args.PartStats.Stats);
    }
}

void TVolumeActor::CompleteSavePartStats(
    const TActorContext& ctx,
    TTxVolume::TSavePartStats& args)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Part %lu stats saved",
        TabletID(),
        args.PartStats.TabletId);

    NCloud::Send(
        ctx,
        SelfId(),
        std::make_unique<TEvVolumePrivate::TEvPartStatsSaved>()
    );
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::SendPartStatsToService(const TActorContext& ctx)
{
    DoSendPartStatsToService(ctx, State->GetConfig().GetDiskId());

    for (const auto& [checkpointId, checkpointInfo]:
         State->GetCheckpointStore().GetActiveCheckpoints())
    {
        if (checkpointInfo.ShadowDiskId) {
            DoSendPartStatsToService(ctx, checkpointInfo.ShadowDiskId);
        }
    }
}

void TVolumeActor::DoSendPartStatsToService(
    const NActors::TActorContext& ctx,
    const TString& diskId)
{
    auto stats = CreatePartitionDiskCounters(
        State->CountersPolicy(),
        DiagnosticsConfig->GetHistogramCounterOptions());
    ui64 systemCpu = 0;
    ui64 userCpu = 0;
    // XXX - we need to "manually" calculate total channel history
    // size here because this counter is different from all other
    // partition counters. In volume it should be aggregated as
    // simple counter, but in service it should be aggregated as max counter.
    // Fix this if there are several such counters.
    ui64 channelsHistorySize = 0;

    NBlobMetrics::TBlobLoadMetrics offsetPartitionMetrics;

    bool partStatFound = false;
    for (auto& info: State->GetPartitionStatInfos())
    {
        if (info.DiskId != diskId) {
            continue;
        }
        if (!info.LastCounters) {
            stats->AggregateWith(info.CachedCounters);
            channelsHistorySize +=
                info.CachedCounters.Simple.ChannelHistorySize.Value;
        } else {
            stats->AggregateWith(*info.LastCounters);
            channelsHistorySize +=
                info.LastCounters->Simple.ChannelHistorySize.Value;
            systemCpu += info.LastSystemCpu;
            userCpu += info.LastUserCpu;
            offsetPartitionMetrics += info.LastMetrics;
        }

        info.LastCounters = nullptr;
        info.LastSystemCpu = 0;
        info.LastUserCpu = 0;
        partStatFound = true;
    }

    if (!partStatFound) {
        return;
    }
    stats->Simple.ChannelHistorySize.Set(channelsHistorySize);
    // having 2 metrics with the same meaning is pointless - will need to get
    // rid of one of them
    const auto vbytesCount = GetBlocksCount() * State->GetBlockSize();
    stats->Simple.BytesCount.Set(
        Max(stats->Simple.BytesCount.Value, vbytesCount));

    auto blobLoadMetrics = NBlobMetrics::MakeBlobLoadMetrics(
        State->GetMeta().GetVolumeConfig().GetVolumeExplicitChannelProfiles(),
        *Executor()->GetResourceMetrics());
    auto offsetLoadMetrics =
        NBlobMetrics::TakeDelta(PrevMetrics, blobLoadMetrics);
    offsetLoadMetrics += offsetPartitionMetrics;

    auto request = std::make_unique<TEvStatsService::TEvVolumePartCounters>(
        MakeIntrusive<TCallContext>(),
        diskId,
        std::move(stats),
        systemCpu,
        userCpu,
        State->GetCheckpointStore().GetActiveCheckpoints().size(),
        std::move(offsetLoadMetrics));

    PrevMetrics = std::move(blobLoadMetrics);

    NCloud::Send(ctx, MakeStorageStatsServiceId(), std::move(request));
}

void TVolumeActor::SendSelfStatsToService(const TActorContext& ctx)
{
    if (!VolumeSelfCounters) {
        return;
    }

    const auto& pp = State->GetConfig().GetPerformanceProfile();
    auto& simple = VolumeSelfCounters->Simple;
    simple.MaxReadBandwidth.Set(pp.GetMaxReadBandwidth());
    simple.MaxWriteBandwidth.Set(pp.GetMaxWriteBandwidth());
    simple.MaxReadIops.Set(pp.GetMaxReadIops());
    simple.MaxWriteIops.Set(pp.GetMaxWriteIops());

    {
        using EOperation =
            TEvPartitionCommonPrivate::TEvLongRunningOperation::EOperation;

        auto blobOperationTimeouts = LongRunningActors.ExtractLongRunningStat();

        simple.LongRunningReadBlob.Set(
            blobOperationTimeouts[EOperation::ReadBlob]);
        simple.LongRunningWriteBlob.Set(
            blobOperationTimeouts[EOperation::WriteBlob]);
    }

    const auto& tp = State->GetThrottlingPolicy();
    simple.RealMaxWriteBandwidth.Set(
        tp.GetWriteCostMultiplier()
        ? pp.GetMaxWriteBandwidth() / tp.GetWriteCostMultiplier() : 0);
    simple.PostponedQueueWeight.Set(tp.CalculatePostponedWeight());

    const auto& bp = tp.GetCurrentBackpressure();
    simple.BPFreshIndexScore.Set(100 * bp.FreshIndexScore);
    simple.BPCompactionScore.Set(100 * bp.CompactionScore);
    simple.BPDiskSpaceScore.Set(100 * bp.DiskSpaceScore);
    simple.BPCleanupScore.Set(100 * bp.CleanupScore);

    simple.VBytesCount.Set(GetBlocksCount() * State->GetBlockSize());
    simple.PartitionCount.Set(State->GetPartitions().size());

    if (auto blockCountToMigrate = State->GetBlockCountToMigrate()) {
        simple.MigrationStarted.Set(true);
        ui64 migratedBlockCount = GetBlocksCount() - *blockCountToMigrate;
        simple.MigrationProgress.Set(
            100 * migratedBlockCount / GetBlocksCount());
    } else {
        simple.MigrationStarted.Set(false);
        simple.MigrationProgress.Set(0);
    }

    simple.ResyncStarted.Set(State->IsMirrorResyncNeeded());
    simple.ResyncProgress.Set(
        100 * State->GetMeta().GetResyncIndex() / GetBlocksCount()
    );

    simple.HasLaggingDevices.Set(State->HasLaggingAgents());
    simple.LaggingDevicesCount.Set(State->GetLaggingDevices().size());
    {
        const auto& laggingInfos = State->GetLaggingAgentsMigrationInfo();
        ui64 cleanBlockCount = 0;
        ui64 dirtyBlockCount = 0;
        for (const auto& [_, laggingInfo]: laggingInfos) {
            cleanBlockCount += laggingInfo.CleanBlocks;
            dirtyBlockCount += laggingInfo.DirtyBlocks;
        }
        simple.LaggingMigrationProgress.Set(
            100 * cleanBlockCount /
            Max<ui64>(cleanBlockCount + dirtyBlockCount, 1));
    }

    simple.LastVolumeLoadTime.Set(GetLoadTime().MicroSeconds());
    simple.LastVolumeStartTime.Set(GetStartTime().MicroSeconds());
    simple.HasStorageConfigPatch.Set(HasStorageConfigPatch);
    simple.UseFastPath.Set(
        State->GetUseFastPath() &&
        State->GetMeta().GetMigrations().size() == 0);
    simple.HasPerformanceProfileModifications.Set(
        HasPerformanceProfileModifications);

    SendVolumeSelfCounters(ctx);
    VolumeSelfCounters = CreateVolumeSelfCounters(
        State->CountersPolicy(),
        DiagnosticsConfig->GetHistogramCounterOptions());
}

void TVolumeActor::HandleGetVolumeLoadInfo(
    const TEvVolume::TEvGetVolumeLoadInfoRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvVolume::TEvGetVolumeLoadInfoResponse>();

    auto& stats = *response->Record.MutableStats();
    stats.SetDiskId(State->GetDiskId());
    stats.SetCloudId(State->GetMeta().GetVolumeConfig().GetCloudId());

    for (auto& info: State->GetPartitionStatInfos()) {
        if (info.LastCounters) {
            stats.SetSystemCpu(stats.GetSystemCpu() + info.LastSystemCpu);
            stats.SetUserCpu(stats.GetUserCpu() + info.LastUserCpu);
            // TODO: report real number of threads
            stats.SetNumSystemThreads(0);
            stats.SetNumUserThreads(0);
        }
    }

    stats.SetHost(FQDNHostName());

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TVolumeActor::HandleLongRunningBlobOperation(
    const TEvPartitionCommonPrivate::TEvLongRunningOperation::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    using TEvLongRunningOperation =
        TEvPartitionCommonPrivate::TEvLongRunningOperation;

    const auto& msg = *ev->Get();

    if (msg.Reason == TEvLongRunningOperation::EReason::LongRunningDetected) {
        if (msg.FirstNotify) {
            LongRunningActors.Insert(ev->Sender);
            LongRunningActors.MarkLongRunning(ev->Sender, msg.Operation);
        }

        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME,
            "[%lu] For volume %s detected %s (actor %s, group %u) %s running "
            "for %s",
            TabletID(),
            State->GetDiskId().Quote().c_str(),
            ToString(msg.Operation).c_str(),
            ev->Sender.ToString().c_str(),
            msg.GroupId,
            msg.FirstNotify ? "long" : "still",
            msg.Duration.ToString().c_str());
    } else {
        LongRunningActors.Erase(ev->Sender);

        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME,
            "[%lu] For volume %s %s %s (actor %s, group %u) detected after %s",
            TabletID(),
            State->GetDiskId().Quote().c_str(),
            ToString(msg.Reason).c_str(),
            ToString(msg.Operation).c_str(),
            ev->Sender.ToString().c_str(),
            msg.GroupId,
            msg.Duration.ToString().c_str());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
