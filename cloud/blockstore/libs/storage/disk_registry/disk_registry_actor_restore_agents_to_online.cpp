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
        "RestoreAgentsToOnlineInterval ago");

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
        args.affectedAgents);
}

void TDiskRegistryActor::CompleteRestoreDisksToOnline(
    const TActorContext& ctx,
    TTxDiskRegistry::TRestoreDisksToOnline& args)
{
    Y_UNUSED(args);

    for (const auto& agent : args.affectedAgents) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "Restored agent to online state: %s",
            agent.c_str());
    }
    bool immediatly = args.affectedAgents.size() >= Config->GetRestoreAgentsCountPerTransaction();
    ScheduleRestoreDisksToOnlineIfNeeded(ctx, immediatly);
}

}   // namespace NCloud::NBlockStore::NStorage
