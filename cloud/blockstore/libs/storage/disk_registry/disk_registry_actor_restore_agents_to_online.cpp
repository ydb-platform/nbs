#include "disk_registry_actor.h"

#include <cloud/storage/core/libs/common/format.h>

#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr::NTabletFlatExecutor;

void TDiskRegistryActor::ProcessRestoreAgentsToOnline(const TActorContext& ctx)
{
    const auto delay = RestoreAgentsToOnlineIterations *
                       Config->GetAgentBackFromUnavailableCheckInterval();

    if (!delay || ctx.Now() < StartTime() + delay) {
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "Restoring agents with status \"back from unavailable\" and last state "
        "change more than %s ago",
        FormatDuration(Config->GetAgentBackFromUnavailableToOnlineDelay())
            .c_str());

    ExecuteTx<TRestoreAgentsToOnline>(ctx);
}

bool TDiskRegistryActor::PrepareRestoreAgentsToOnline(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRestoreAgentsToOnline& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteRestoreAgentsToOnline(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRestoreAgentsToOnline& args)
{
    TDiskRegistryDatabase db(tx.DB);
    args.Error = State->RestoreBackFromUnavailableAgents(
        db,
        ctx.Now(),
        args.AffectedAgents,
        args.AgentsRemained);
}

void TDiskRegistryActor::CompleteRestoreAgentsToOnline(
    const TActorContext& ctx,
    TTxDiskRegistry::TRestoreAgentsToOnline& args)
{
    if (args.AffectedAgents) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "Restored agents to online state: %s",
            JoinSeq(", ", args.AffectedAgents).c_str());
    }

    if (HasError(args.Error)) {
        auto message = TStringBuilder()
                       << "Failed to restore agents to online state: "
                       << args.Error.GetMessage();
        ReportRestoreAgentsToOnlineFailed(message);
        RestoreAgentsToOnlineIterations++;
        return;
    }

    if (!args.AgentsRemained) {
        RestoreAgentsToOnlineIterations++;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
