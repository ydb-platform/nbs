#include "disk_registry_actor.h"
#include "disk_registry_database.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>

#include <cloud/storage/core/libs/kikimr/helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleSwitchAgentDisksToReadOnly(
    const TEvDiskRegistryPrivate::TEvSwitchAgentDisksToReadOnlyRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    // ????
    const bool connected = AnyOf(
        AgentRegInfo,
        [agentId = msg->AgentId](const auto& info)
        {
            return info.second.AgentId == agentId && info.second.Connected;
            //        && info.second.TemporaryAgent.Defined() &&
            //        !info.second.TemporaryAgent.Get();
        });
    if (connected) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Ignoring SwitchAgentDisksToReadOnly request"
            " since AgentId=%s reconnected",
            TabletID(),
            msg->AgentId.c_str());
        return;
    }

    BLOCKSTORE_DISK_REGISTRY_COUNTER(SwitchAgentDisksToReadOnly);

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received SwitchAgentDisksToReadOnly request: AgentId=%s",
        TabletID(),
        msg->AgentId.c_str());

    ExecuteTx<TSwitchAgentDisksToReadOnly>(
        ctx,
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        std::move(msg->AgentId));
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareSwitchAgentDisksToReadOnly(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TSwitchAgentDisksToReadOnly& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteSwitchAgentDisksToReadOnly(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TSwitchAgentDisksToReadOnly& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);

    TVector<TDiskId> affectedDisks;
    args.Error = State->SwitchAgentDisksToReadOnly(
        db,
        args.AgentId,
        affectedDisks);
}

void TDiskRegistryActor::CompleteSwitchAgentDisksToReadOnly(
    const TActorContext& ctx,
    TTxDiskRegistry::TSwitchAgentDisksToReadOnly& args)
{
    if (HasError(args.Error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "UpdateAgentState error: %s",
            FormatError(args.Error).c_str());
    }

    ReallocateDisks(ctx);

    auto response = std::make_unique<
        TEvDiskRegistryPrivate::TEvSwitchAgentDisksToReadOnlyResponse>(
        args.Error);
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
