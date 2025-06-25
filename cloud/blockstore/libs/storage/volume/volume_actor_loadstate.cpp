#include "volume_actor.h"

#include "volume_database.h"

#include <cloud/blockstore/libs/storage/core/config.h>

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/media.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareLoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TLoadState& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);
    auto now = ctx.Now();

    std::initializer_list<bool> results = {
        db.ReadMeta(args.Meta),
        db.ReadMetaHistory(args.MetaHistory),
        db.ReadVolumeParams(args.VolumeParams),
        db.ReadStartPartitionsNeeded(args.StartPartitionsNeeded),
        db.ReadClients(args.Clients),
        db.ReadHistory(
            THistoryLogKey(now),
            args.OldestLogEntry,
            Config->GetVolumeHistoryCacheSize(),
            args.MountHistory),
        db.ReadPartStats(args.PartStats),
        db.ReadNonReplPartStats(args.PartStats),
        db.CollectCheckpointsToDelete(
            Config->GetDeletedCheckpointHistoryLifetime(),
            now,
            args.DeletedCheckpoints),
        db.ReadCheckpointRequests(
            args.DeletedCheckpoints,
            args.CheckpointRequests,
            args.OutdatedCheckpointRequestIds),
        db.ReadThrottlerState(args.ThrottlerStateInfo),
        db.ReadStorageConfig(args.StorageConfig),
        db.ReadFollowers(args.FollowerDisks),
    };

    if (args.Meta) {
        LogTitle.SetDiskId(args.Meta->GetConfig().GetDiskId());
    }

    bool ready = std::accumulate(
        results.begin(),
        results.end(),
        true,
        std::logical_and<>()
    );

    if (ready && args.Meta) {
        args.UsedBlocks.ConstructInPlace(ComputeBlockCount(*args.Meta));
        ready &= db.ReadUsedBlocks(*args.UsedBlocks);
    }

    return ready;
}

void TVolumeActor::ExecuteLoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TLoadState& args)
{
    TVolumeDatabase db(tx.DB);

    // Consider all clients loaded from persistent state disconnected
    // with disconnect time being the time of state loading. Set disconnect
    // timestamp only if it was not set before.
    // If these clients are still alive, they'd re-establish pipe connections
    // with the volume shortly and send AddClientRequest.
    auto now = ctx.Now();
    bool anyChanged = false;
    for (auto& client : args.Clients) {
        if (!client.second.GetVolumeClientInfo().GetDisconnectTimestamp()) {
            client.second.SetDisconnectTimestamp(now);
            anyChanged = true;
        }
    }

    if (anyChanged) {
        db.WriteClients(args.Clients);
    }

    for (const auto& o: args.OutdatedCheckpointRequestIds) {
        db.DeleteCheckpointEntry(o);
    }
}

void TVolumeActor::CompleteLoadState(
    const TActorContext& ctx,
    TTxVolume::TLoadState& args)
{
    if (args.StorageConfig.Defined()) {
        Config =
            TStorageConfig::Merge(GlobalStorageConfig, *args.StorageConfig);
        HasStorageConfigPatch = Config != GlobalStorageConfig;
    }

    if (args.Meta.Defined()) {
        TThrottlerConfig throttlerConfig(
            Config->GetMaxThrottlerDelay(),
            Config->GetMaxWriteCostMultiplier(),
            Config->GetDefaultPostponedRequestWeight(),
            args.ThrottlerStateInfo.Defined()
                ? TDuration::MilliSeconds(args.ThrottlerStateInfo->Budget)
                : CalculateBoostTime(
                    args.Meta->GetConfig().GetPerformanceProfile()),
            Config->GetDiskSpaceScoreThrottlingEnabled());

        bool startPartitionsNeeded = args.StartPartitionsNeeded.GetOrElse(false);

        TCachedVolumeMountHistory volumeHistory{
            Config->GetVolumeHistoryCacheSize(),
            std::move(args.MountHistory)};

        State = std::make_unique<TVolumeState>(
            Config,
            DiagnosticsConfig,
            std::move(*args.Meta),
            std::move(args.MetaHistory),
            std::move(args.VolumeParams),
            throttlerConfig,
            std::move(args.Clients),
            std::move(volumeHistory),
            std::move(args.CheckpointRequests),
            std::move(args.FollowerDisks),
            startPartitionsNeeded);

        HasPerformanceProfileModifications =
            State->HasPerformanceProfileModifications(*Config);

        ResetThrottlingPolicy();

        for (const auto& partStats: args.PartStats) {
            // info doesn't have to be always present
            // see NBS-1668#603e955e319cc33b747904fb
            if (auto* info =
                    State->GetPartitionStatInfoByTabletId(partStats.TabletId))
            {
                CopyCachedStatsToPartCounters(partStats.Stats, *info);
            }
        }

        Y_ABORT_UNLESS(CurrentState == STATE_INIT);
        BecomeAux(ctx, STATE_WORK);

        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s State initialization finished",
            LogTitle.GetWithTime().c_str());

        RegisterCounters(ctx);
        RegisterVolume(ctx);

        if (State->IsDiskRegistryMediaKind() || PendingRequests.size()) {
            StartPartitionsForUse(ctx);
        } else if (State->GetShouldStartPartitionsForGc(ctx.Now())
            && !Config->GetDisableStartPartitionsForGc())
        {
            StartPartitionsForGc(ctx);
        }
    }

    if (args.UsedBlocks) {
        State->AccessUsedBlocks() = std::move(*args.UsedBlocks);
    }

    StateLoadFinished = true;
    StateLoadTimestamp = ctx.Now();
    NextVolumeConfigVersion = GetCurrentConfigVersion();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s State data loaded, time: %s",
        LogTitle.GetWithTime().c_str(),
        FormatDuration(GetLoadTime()).c_str());

    ctx.Send(
        MakeDiskRegistryProxyServiceId(),
        new TEvDiskRegistryProxy::TEvGetDrTabletInfoRequest());

    SignalTabletActive(ctx);
    ScheduleProcessUpdateVolumeConfig(ctx);
    ScheduleAllocateDiskIfNeeded(ctx);

    if (State) {
        ProcessNextPendingClientRequest(ctx);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
