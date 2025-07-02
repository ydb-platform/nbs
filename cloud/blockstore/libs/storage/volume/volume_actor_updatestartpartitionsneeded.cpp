#include "volume_actor.h"

#include "volume_database.h"

#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
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
    LOG_INFO(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Received GarbageCollectorCompleted report from partition %lu",
        TabletID(), partitionTabletId);

    if (State->GetShouldStartPartitionsForGc(ctx.Now())) {
        auto requestInfo = CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            MakeIntrusive<TCallContext>());

        ExecuteTx(ctx, CreateTx<TResetStartPartitionsNeeded>(
            requestInfo, partitionTabletId));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
