#include "volume_actor.h"

#include "volume_database.h"

#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/storage/core/libs/common/verify.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

using NPartition::TEvPartition;

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareResetStartPartitionsNeeded(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TResetStartPartitionsNeeded& args)
{
    Y_UNUSED(ctx, tx, args);

    return true;
}

void TVolumeActor::ExecuteResetStartPartitionsNeeded(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TResetStartPartitionsNeeded& args)
{
    Y_UNUSED(ctx, args);

    STORAGE_VERIFY(State, TWellKnownEntityTypes::TABLET, TabletID());

    if (State->GetShouldStartPartitionsForGc(ctx.Now())) {
        if (PartitionsStartedReason == EPartitionsStartedReason::STARTED_FOR_GC) {
            if (!FindPtr(GCCompletedPartitions, args.PartitionTabletId)) {
                GCCompletedPartitions.push_back(args.PartitionTabletId);
            }
            if (GCCompletedPartitions.size() != State->GetPartitions().size()) {
                return;
            }
            if (SmallBlobsRemovalEnabled) {
                for (const auto& partition: State->GetPartitions()) {
                    if (!partition.StorageInfo) {
                        return;
                    }
                    if (partition.StorageInfo->TabletType ==
                            TTabletTypes::BlockStorePartition &&
                        !SmallBlobsRemovedPartitions.contains(
                            partition.TabletId))
                    {
                        return;
                    }
                }
                // Final batches have already been incorporated by the response
                // handler. Forward them before resetting partition state.
                SendPartStatsToService(ctx);
                SendSelfStatsToService(ctx);
            }
            LOG_INFO(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s Stopping partitions after gc finished",
                LogTitle.GetWithTime().c_str());

            StopPartitions(ctx, {});
            State->Reset();
            PartitionsStartedReason = EPartitionsStartedReason::NOT_STARTED;
        }
        TVolumeDatabase db(tx.DB);
        State->SetStartPartitionsNeeded(false);
        db.WriteStartPartitionsNeeded(false);
    }
}

void TVolumeActor::CompleteResetStartPartitionsNeeded(
    const TActorContext& ctx,
    TTxVolume::TResetStartPartitionsNeeded& args)
{
    Y_UNUSED(ctx, args);
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleGarbageCollectorCompleted(
    const NPartition::TEvPartition::TEvGarbageCollectorCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto partitionTabletId = ev->Get()->TabletId;
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Received GarbageCollectorCompleted report from partition %lu",
        LogTitle.GetWithTime().c_str(),
        partitionTabletId);

    if (State->GetShouldStartPartitionsForGc(ctx.Now())) {
        if (SmallBlobsRemovalEnabled) {
            bool knownPartition = false;
            for (const auto& partition: State->GetPartitions()) {
                knownPartition |= partition.TabletId == partitionTabletId;
            }
            if (!knownPartition) {
                return;
            }
            const auto* actorId =
                SmallBlobsRemovalPartitions.FindPtr(partitionTabletId);
            if (actorId && *actorId != ev->Sender) {
                return;
            }
            if (!FindPtr(GCCompletedPartitions, partitionTabletId)) {
                // A timed-out check may have arrived while startup GC was
                // still running. Refresh every final batch after this report
                // so successful work completed in the meantime is included.
                ++SmallBlobsRemovalGeneration;
                SmallBlobsRemovedPartitions.clear();
                ScheduleCheckSmallBlobsRemoved(ctx);
            }
        }
        auto requestInfo = CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            MakeIntrusive<TCallContext>());

        ExecuteTx(ctx, CreateTx<TResetStartPartitionsNeeded>(
            requestInfo, partitionTabletId));
    }
}

void TVolumeActor::ScheduleCheckSmallBlobsRemoved(const TActorContext& ctx)
{
    if (!SmallBlobsRemovalCheckScheduled && SmallBlobsRemovalEnabled &&
        PartitionsStartedReason == EPartitionsStartedReason::STARTED_FOR_GC)
    {
        SmallBlobsRemovalCheckScheduled = true;
        ctx.Schedule(Max(TDuration::MilliSeconds(1),
                         Config->GetCheckSmallBlobsRemovedCheckInterval()),
                     new TEvVolumePrivate::TEvCheckSmallBlobsRemoved());
    }
}

void TVolumeActor::DisableCheckSmallBlobsRemoved(const TActorContext& ctx)
{
    ++SmallBlobsRemovalGeneration;
    for (const auto& [tabletId, actorId]: SmallBlobsRemovalPartitions) {
        Y_UNUSED(tabletId);
        NCloud::Send<TEvPartition::TEvDisableCheckSmallBlobsRemoved>(ctx,
                                                                     actorId);
    }
    SmallBlobsRemovalEnabled = false;
    SmallBlobsRemovalPartitions.clear();
    SmallBlobsRemovedPartitions.clear();
}

void TVolumeActor::HandleCheckSmallBlobsRemoved(
    const TEvVolumePrivate::TEvCheckSmallBlobsRemoved::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    SmallBlobsRemovalCheckScheduled = false;
    if (!SmallBlobsRemovalEnabled ||
        PartitionsStartedReason != EPartitionsStartedReason::STARTED_FOR_GC)
    {
        return;
    }
    for (const auto& [tabletId, actorId]: SmallBlobsRemovalPartitions) {
        if (!SmallBlobsRemovedPartitions.contains(tabletId)) {
            NCloud::Send(
                ctx,
                actorId,
                std::make_unique<
                    TEvPartition::TEvCheckSmallBlobsRemovedRequest>(),
                SmallBlobsRemovalGeneration);
        }
    }
    ScheduleCheckSmallBlobsRemoved(ctx);
}

void TVolumeActor::HandleCheckSmallBlobsRemovedResponse(
    const TEvPartition::TEvCheckSmallBlobsRemovedResponse::TPtr& ev,
    const TActorContext& ctx)
{
    if (!State || State->IsDiskRegistryMediaKind()) {
        return;
    }
    const auto tabletId = State->FindPartitionTabletId(ev->Sender);
    if (!tabletId) {
        return;
    }
    auto* msg = ev->Get();
    if (HasError(msg->Error) || !msg->FinalCounters) {
        return;
    }

    // Extraction consumes a statistics batch. Preserve it even if a client
    // attached while the response was in flight; only the shutdown decision
    // depends on the mode and polling generation.
    auto& counters = *msg->FinalCounters;
    TPartCountersData data(
        ev->Sender, ev->Cookie, counters.VolumeSystemCpu,
        counters.VolumeUserCpu, MakeIntrusive<TCallContext>(),
        std::move(counters.DiskCounters), std::move(counters.TabletMetrics),
        std::move(counters.BlobLoadMetrics));
    if (auto stats = UpdatePartCounters(ctx, data)) {
        ExecuteTx<TSavePartStats>(
            ctx, nullptr,
            TVector<TVolumeDatabase::TPartStats>{std::move(*stats)});
    } else {
        return;
    }

    if (!SmallBlobsRemovalEnabled || !msg->Removed ||
        PartitionsStartedReason != EPartitionsStartedReason::STARTED_FOR_GC ||
        ev->Cookie != SmallBlobsRemovalGeneration)
    {
        return;
    }
    const auto* actorId = SmallBlobsRemovalPartitions.FindPtr(*tabletId);
    if (!actorId || *actorId != ev->Sender) {
        return;
    }
    SmallBlobsRemovedPartitions.insert(*tabletId);
    if (FindPtr(GCCompletedPartitions, *tabletId)) {
        ExecuteTx(
            ctx,
            CreateTx<TResetStartPartitionsNeeded>(
                CreateRequestInfo(ev->Sender, ev->Cookie,
                                  MakeIntrusive<TCallContext>()), *tabletId));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
