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

    if (IsTidyUpPartitionModeAllowed() &&
        !State->IsDiskRegistryMediaKind() &&
        PartitionsStartedReason == EPartitionsStartedReason::NOT_STARTED)
    {
        TVolumeDatabase db(tx.DB);
        State->SetStartPartitionsNeeded(true);
        db.WriteStartPartitionsNeeded(true);
        args.ShouldStartPartitions = true;
    }
}

void TVolumeActor::CompleteTidyUpPartitions(
    const TActorContext& ctx,
    TTxVolume::TTidyUpPartitions& args)
{
    if (args.ShouldStartPartitions &&
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
            if (GCCompletedPartitions.size() != State->GetPartitions().size() ||
                (TidyUpPartitionMode &&
                 TidyUpCompletedPartitions.size() != State->GetPartitions().size()))
            {
                // Wait for all GC reports and, in tidy-up mode, all tidy-up reports.
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

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());
    HandlePartitionMaintenanceCompleted(
        ctx, std::move(requestInfo), partitionTabletId, false);
}

void TVolumeActor::HandleTidyUpCompleted(
    const NPartition::TEvPartition::TEvTidyUpCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());
    HandlePartitionMaintenanceCompleted(
        ctx, std::move(requestInfo), ev->Get()->TabletId, true);
}

void TVolumeActor::HandlePartitionMaintenanceCompleted(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    ui64 tabletId,
    bool tidyUpCompleted)
{
    if (!State->GetShouldStartPartitionsForGc(ctx.Now()) ||
        (tidyUpCompleted && !TidyUpPartitionMode) ||
        !State->GetPartition(tabletId))
    {
        return;
    }

    if (PartitionsStartedReason == EPartitionsStartedReason::STARTED_FOR_GC) {
        auto& completedPartitions = tidyUpCompleted
            ? TidyUpCompletedPartitions
            : GCCompletedPartitions;
        if (!FindPtr(completedPartitions, tabletId)) {
            completedPartitions.push_back(tabletId);
        }
        if (GCCompletedPartitions.size() != State->GetPartitions().size() ||
            (TidyUpPartitionMode &&
             TidyUpCompletedPartitions.size() != State->GetPartitions().size()))
        {
            return;
        }
    }

    ExecuteTx<TResetStartPartitionsNeeded>(ctx, std::move(requestInfo));
}

}   // namespace NCloud::NBlockStore::NStorage
