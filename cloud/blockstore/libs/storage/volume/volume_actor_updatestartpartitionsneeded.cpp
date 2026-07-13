#include "volume_actor.h"

#include "volume_database.h"

#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/verify.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

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
            if (FindPtr(GCCompletedPartitions, args.PartitionTabletId)) {
                // This partition has already sent GC report
                return;
            }
            if (GCCompletedPartitions.size() + 1 != State->GetPartitions().size()) {
                // Not all partitions completed GC
                GCCompletedPartitions.push_back(args.PartitionTabletId);
                return;
            }

            const auto gracePeriod = TDuration::MilliSeconds(
                Config->GetStopPartitionsAfterGcGracePeriod());

            if (gracePeriod != TDuration::Zero()) {
                LOG_INFO(
                    ctx,
                    TBlockStoreComponents::VOLUME,
                    "%s Scheduling partition stop after %s grace period",
                    LogTitle.GetWithTime().c_str(),
                    FormatDuration(gracePeriod).c_str());

                StopPartitionsAfterGcScheduled = true;
                ctx.Schedule(
                    gracePeriod,
                    new TEvVolumePrivate::TEvStopPartitionsAfterGc());
            } else {
                LOG_INFO(
                    ctx,
                    TBlockStoreComponents::VOLUME,
                    "%s Stopping partitions after gc finished",
                    LogTitle.GetWithTime().c_str());

                StopPartitions(ctx, {});
                State->Reset();
                PartitionsStartedReason = EPartitionsStartedReason::NOT_STARTED;
            }
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
        auto requestInfo = CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            MakeIntrusive<TCallContext>());

        ExecuteTx(ctx, CreateTx<TResetStartPartitionsNeeded>(
            requestInfo, partitionTabletId));
    }
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleStopPartitionsAfterGc(
    const TEvVolumePrivate::TEvStopPartitionsAfterGc::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    StopPartitionsAfterGcScheduled = false;

    if (PartitionsStartedReason != EPartitionsStartedReason::STARTED_FOR_GC) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Grace period expired but partitions are no longer in gc mode "
            "(reason: %d), skipping stop",
            LogTitle.GetWithTime().c_str(),
            static_cast<int>(PartitionsStartedReason));
        return;
    }

    if (State && State->HasActiveClients(ctx.Now())) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Grace period expired but a client has connected, skipping stop",
            LogTitle.GetWithTime().c_str());
        PartitionsStartedReason = EPartitionsStartedReason::STARTED_FOR_USE;
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Grace period expired, stopping partitions after gc",
        LogTitle.GetWithTime().c_str());

    StopPartitions(ctx, {});
    if (State) {
        State->Reset();
    }
    PartitionsStartedReason = EPartitionsStartedReason::NOT_STARTED;
}

}   // namespace NCloud::NBlockStore::NStorage
