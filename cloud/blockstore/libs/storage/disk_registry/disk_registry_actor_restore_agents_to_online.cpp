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
        "%s seconds ago",
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

    return true;
}

void TDiskRegistryActor::ExecuteRestoreDisksToOnline(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRestoreDisksToOnline& args)
{
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
    Y_UNUSED(args);

    TStringBuilder affectedAgents;

    for (const auto& agent : args.AffectedAgents) {
        affectedAgents << agent << ", ";
    }

    affectedAgents.erase(affectedAgents.size()-2, 2);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "Restored agents to online state: %s",
        affectedAgents.c_str());
    ScheduleRestoreDisksToOnlineIfNeeded(ctx, args.RemainingAgents);
}

}   // namespace NCloud::NBlockStore::NStorage
