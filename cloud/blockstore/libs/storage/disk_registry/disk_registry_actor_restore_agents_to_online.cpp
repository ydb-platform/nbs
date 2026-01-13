#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr::NTabletFlatExecutor;

void TDiskRegistryActor::HandleRestoreAgentsToOnlineReadOnly(
    const TEvDiskRegistryPrivate::TEvDiskRegistryRestoreAgentsToOnline::TPtr&
        ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY, "Handle RestoreDisksToOnline readonly");

    ScheduleRestoreDisksToOnlineIfNeeded(ctx);
}

void TDiskRegistryActor::HandleRestoreAgentsToOnline(
    const TEvDiskRegistryPrivate::TEvDiskRegistryRestoreAgentsToOnline::TPtr&
        ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "Restoring agents with status \"back from unavailable\" and last state change more than "
        "%d seconds ago",
        Config->GetRestoreAgentsToOnlineInterval().Seconds());

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    ExecuteTx<TRestoreDisksToOnline>(ctx, std::move(requestInfo));
}

bool TDiskRegistryActor::PrepareRestoreDisksToOnline(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRestoreDisksToOnline& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY, "Prepare RestoreDisksToOnline");

    return true;
}

void TDiskRegistryActor::ExecuteRestoreDisksToOnline(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRestoreDisksToOnline& args)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY, "Execute RestoreDisksToOnline");
    TDiskRegistryDatabase db(tx.DB);
    args.Error = State->RestoreAgentsFromWarning(
        db,
        ctx.Now(),
        Config->GetRestoreAgentsToOnlineInterval(),
        Config->GetRestoreAgentsCountPerTransaction(),
        args.AffectedAgents,
        args.RemainingAgents);
}

void TDiskRegistryActor::CompleteRestoreDisksToOnline(
    const TActorContext& ctx,
    TTxDiskRegistry::TRestoreDisksToOnline& args)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY, "Complete RestoreDisksToOnline");
    Y_UNUSED(args);

    TStringBuilder affectedAgents;

    for (const auto& agent : args.AffectedAgents) {
        affectedAgents << agent << ", ";
    }

    if(affectedAgents.size() > 0) {
        affectedAgents.erase(affectedAgents.size()-2, 2);
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "Restored agents to online state: %s",
        affectedAgents.c_str());
    ScheduleRestoreDisksToOnlineIfNeeded(ctx, args.RemainingAgents);
}

}   // namespace NCloud::NBlockStore::NStorage
