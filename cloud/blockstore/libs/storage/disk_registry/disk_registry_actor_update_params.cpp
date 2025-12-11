#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleUpdateDiskRegistryAgentListParams(
    const TEvDiskRegistry::TEvUpdateDiskRegistryAgentListParamsRequest::TPtr&
        ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    NProto::TDiskRegistryAgentParams params;
    params.SetDeadlineMs(
        ctx.Now().MilliSeconds() + msg->Record.GetParams().GetTimeoutMs());
    params.SetNewNonReplicatedAgentMinTimeoutMs(
        msg->Record.GetParams().GetNewNonReplicatedAgentMinTimeoutMs());
    params.SetNewNonReplicatedAgentMaxTimeoutMs(
        msg->Record.GetParams().GetNewNonReplicatedAgentMaxTimeoutMs());

    ExecuteTx<TUpdateDiskRegistryAgentListParams>(
        ctx,
        std::move(requestInfo),
        TVector<TString>{
            msg->Record.GetParams().GetAgentIds().begin(),
            msg->Record.GetParams().GetAgentIds().end()},
        params);
}

void TDiskRegistryActor::
    HandleDiskRegistryAgentListExpiredParamsCleanupReadOnly(
        const TEvDiskRegistryPrivate::
            TEvDiskRegistryAgentListExpiredParamsCleanup::TPtr& ev,
        const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    ScheduleDiskRegistryAgentListExpiredParamsCleanup(ctx);
}

void TDiskRegistryActor::HandleDiskRegistryAgentListExpiredParamsCleanup(
    const TEvDiskRegistryPrivate::TEvDiskRegistryAgentListExpiredParamsCleanup::
        TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    ExecuteTx<TCleanupExpiredAgentListParams>(ctx, std::move(requestInfo));
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareUpdateDiskRegistryAgentListParams(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateDiskRegistryAgentListParams& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteUpdateDiskRegistryAgentListParams(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateDiskRegistryAgentListParams& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    if (args.AgentIds.empty()) {
        args.Error = MakeError(E_ARGUMENT, "agentIds list is empty");
        return;
    }

    for (const auto& agentId: args.AgentIds) {
        if (!State->FindAgent(agentId)) {
            args.Error =
                MakeError(E_NOT_FOUND, "agentId not found: " + agentId);
            return;
        }
    }

    TDiskRegistryDatabase db(tx.DB);
    for (const auto& agentId: args.AgentIds) {
        State->SetDiskRegistryAgentListParams(db, agentId, args.Params);
    }
}

void TDiskRegistryActor::CompleteUpdateDiskRegistryAgentListParams(
    const TActorContext& ctx,
    TTxDiskRegistry::TUpdateDiskRegistryAgentListParams& args)
{
    auto response = std::make_unique<
        TEvDiskRegistry::TEvUpdateDiskRegistryAgentListParamsResponse>(
        std::move(args.Error));
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

bool TDiskRegistryActor::PrepareCleanupExpiredAgentListParams(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCleanupExpiredAgentListParams& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteCleanupExpiredAgentListParams(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCleanupExpiredAgentListParams& args)
{
    Y_UNUSED(args);

    TDiskRegistryDatabase db(tx.DB);
    State->CleanupExpiredAgentListParams(db, ctx.Now());
}

void TDiskRegistryActor::CompleteCleanupExpiredAgentListParams(
    const TActorContext& ctx,
    TTxDiskRegistry::TCleanupExpiredAgentListParams& args)
{
    Y_UNUSED(args);

    ScheduleDiskRegistryAgentListExpiredParamsCleanup(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
