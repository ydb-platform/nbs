#include "disk_registry_actor.h"
#include "cloud/storage/core/libs/common/format.h"
#include "util/string/join.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr::NTabletFlatExecutor;


void TDiskRegistryActor::HandleRestoreAgentsToOnline(
    const TEvDiskRegistryPrivate::TEvDiskRegistryRestoreAgentsToOnline::TPtr&
        ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "Restoring agents with status \"back from unavailable\" and last state change more than "
        "%s ago",
        FormatDuration(Config->GetRestoreBackFromUnavailableAgentsDelay()).c_str());

    ExecuteTx<TRestoreDisksToOnline>(ctx);
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
        Config->GetRestoreBackFromUnavailableAgentsDelay(),
        Config->GetRestoreAgentsCountPerTransaction(),
        args.AffectedAgents,
        args.RemainingAgents);
}

void TDiskRegistryActor::CompleteRestoreDisksToOnline(
    const TActorContext& ctx,
    TTxDiskRegistry::TRestoreDisksToOnline& args)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "Restored agents to online state: %s",
        JoinSeq(", ", args.AffectedAgents).c_str());
    ScheduleRestoreDisksToOnlineIfNeeded(ctx, args.RemainingAgents);
}

}   // namespace NCloud::NBlockStore::NStorage
