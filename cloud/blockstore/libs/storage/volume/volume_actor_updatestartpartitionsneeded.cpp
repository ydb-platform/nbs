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

bool TVolumeActor::PrepareTidyUpPartitions(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TTidyUpPartitions& args)
{
    Y_UNUSED(ctx, tx, args);

    return true;
}

void TVolumeActor::ExecuteTidyUpPartitions(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TTidyUpPartitions& args)
{
    Y_UNUSED(ctx);

    if (IsTidyUpPartitionModeEnabled() &&
        !State->IsDiskRegistryMediaKind() &&
        PartitionsStartedReason == EPartitionsStartedReason::NOT_STARTED)
    {
        TVolumeDatabase db(tx.DB);
        State->SetStartPartitionsNeeded(true);
        db.WriteStartPartitionsNeeded(true);
        args.StartPartitions = true;
    }
}

void TVolumeActor::CompleteTidyUpPartitions(
    const TActorContext& ctx,
    TTxVolume::TTidyUpPartitions& args)
{
    if (args.StartPartitions &&
        PartitionsStartedReason == EPartitionsStartedReason::NOT_STARTED)
    {
        StartPartitionsForGc(ctx);
    }
    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<TEvVolume::TEvTidyUpPartitionsResponse>());
}

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
            if (GCCompletedPartitions.size() != State->GetPartitions().size() ||
                (TidyUpPartitionMode &&
                 TidyUpCompletedPartitions.size() != State->GetPartitions().size()))
            {
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
            TidyUpPartitionMode = false;
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

void TVolumeActor::HandleTidyUpCompleted(
    const NPartition::TEvPartition::TEvTidyUpCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    if (!TidyUpPartitionMode ||
        !State->GetShouldStartPartitionsForGc(ctx.Now()))
    {
        return;
    }
    const auto tabletId = ev->Get()->TabletId;
    if (!State->GetPartition(tabletId)) {
        return;
    }
    if (!FindPtr(TidyUpCompletedPartitions, tabletId)) {
        TidyUpCompletedPartitions.push_back(tabletId);
    }
    if (TidyUpCompletedPartitions.size() == State->GetPartitions().size() &&
        GCCompletedPartitions.size() == State->GetPartitions().size())
    {
        auto requestInfo = CreateRequestInfo(
            ev->Sender, ev->Cookie, MakeIntrusive<TCallContext>());
        ExecuteTx(ctx, CreateTx<TResetStartPartitionsNeeded>(requestInfo, tabletId));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
