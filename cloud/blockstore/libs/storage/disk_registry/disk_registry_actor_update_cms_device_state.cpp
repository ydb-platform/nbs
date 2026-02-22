#include "disk_registry_actor.h"

#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleUpdateCmsHostDeviceState(
    const TEvDiskRegistryPrivate::TEvUpdateCmsHostDeviceStateRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(UpdateCmsHostDeviceState);

    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received UpdateCmsHostDeviceState request: host=%s, path=%s, "
        "State=%s %s",
        LogTitle.GetWithTime().c_str(),
        msg->Host.c_str(),
        msg->Path.c_str(),
        NProto::EDeviceState_Name(msg->State).c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    ExecuteTx<TUpdateCmsHostDeviceState>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Host),
        std::move(msg->Path),
        msg->State,
        msg->ShouldResumeDevice,
        msg->DryRun);
}
////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareUpdateCmsHostDeviceState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateCmsHostDeviceState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteUpdateCmsHostDeviceState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateCmsHostDeviceState& args)
{
    TDiskRegistryDatabase db(tx.DB);

    args.TxTs = ctx.Now();

    auto result = State->UpdateCmsDeviceState(
        db,
        args.Host,
        args.Path,
        args.State,
        args.TxTs,
        args.ShouldResumeDevice,
        args.DryRun);

    args.Error = std::move(result.Error);
    args.AffectedDisks = std::move(result.AffectedDisks);
    args.Timeout = result.Timeout;
    // Round up to seconds because TActionResult::Timeout is specified in
    // seconds
    if (args.Timeout) {
        args.Timeout = Max(args.Timeout, TDuration::Seconds(1));
    }
}

void TDiskRegistryActor::CompleteUpdateCmsHostDeviceState(
    const TActorContext& ctx,
    TTxDiskRegistry::TUpdateCmsHostDeviceState& args)
{
    auto* agentRegInfo = AgentRegInfo.FindPtr(args.Host);
    const bool agentAvailable = agentRegInfo && agentRegInfo->Connected;
    const bool needToDetachPath = Config->GetAttachDetachPathsEnabled() &&
                                  args.State == NProto::DEVICE_STATE_WARNING &&
                                  !HasError(args.Error) && agentAvailable;

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s UpdateCmsDeviceState result: %s %u",
        LogTitle.GetWithTime().c_str(),
        FormatError(args.Error).c_str(),
        args.Timeout.Seconds());

    ReallocateDisks(ctx);
    NotifyUsers(ctx);
    PublishDiskStates(ctx);

    SecureErase(ctx);
    StartMigration(ctx);
    ProcessPathsToAttach(ctx);

    if (!needToDetachPath) {
        using TResponse =
            TEvDiskRegistryPrivate::TEvUpdateCmsHostDeviceStateResponse;

        auto response = std::make_unique<TResponse>(
            std::move(args.Error),
            args.Timeout,
            std::move(args.AffectedDisks));

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    TryToDetachPaths(
        ctx,
        args.Host,
        {args.Path},
        args.RequestInfo,
        NProto::TAction::REMOVE_DEVICE);
}

}   // namespace NCloud::NBlockStore::NStorage
