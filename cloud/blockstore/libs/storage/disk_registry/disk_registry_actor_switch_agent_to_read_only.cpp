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
    const TEvDiskRegistryPrivate::TEvSwitchAgentDisksToReadOnlyRequest::TPtr&
        ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (auto it = AgentRegInfo.find(msg->AgentId);
        it != AgentRegInfo.end() && it->second.Connected)
    {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s Ignoring SwitchAgentDisksToReadOnly request"
            " since AgentId=%s reconnected",
            LogTitle.GetWithTime().c_str(),
            msg->AgentId.c_str());
        return;
    }

    BLOCKSTORE_DISK_REGISTRY_COUNTER(SwitchAgentDisksToReadOnly);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received SwitchAgentDisksToReadOnly request: AgentId=%s %s",
        LogTitle.GetWithTime().c_str(),
        msg->AgentId.c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

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
    args.Error =
        State->SwitchAgentDisksToReadOnly(db, args.AgentId, affectedDisks);
}

void TDiskRegistryActor::CompleteSwitchAgentDisksToReadOnly(
    const TActorContext& ctx,
    TTxDiskRegistry::TSwitchAgentDisksToReadOnly& args)
{
    if (HasError(args.Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s UpdateAgentState error: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(args.Error).c_str());
    }

    ReallocateDisks(ctx);

    auto response = std::make_unique<
        TEvDiskRegistryPrivate::TEvSwitchAgentDisksToReadOnlyResponse>(
        args.Error);
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
