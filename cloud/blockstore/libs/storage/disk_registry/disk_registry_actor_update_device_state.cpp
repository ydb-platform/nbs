#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleChangeDeviceState(
    const TEvDiskRegistry::TEvChangeDeviceStateRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(ChangeDeviceState);

    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext,
        std::move(ev->TraceId));

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received ChangeDeviceState request: UUID=%s, State=%u",
        TabletID(),
        msg->Record.GetDeviceUUID().c_str(),
        static_cast<ui32>(msg->Record.GetDeviceState()));

    BLOCKSTORE_TRACE_RECEIVED(ctx, &requestInfo->TraceId, this, msg);

    ExecuteTx<TUpdateDeviceState>(
        ctx,
        std::move(requestInfo),
        std::move(*msg->Record.MutableDeviceUUID()),
        msg->Record.GetDeviceState(),
        ctx.Now(),
        msg->Record.GetReason());
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareUpdateDeviceState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateDeviceState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteUpdateDeviceState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateDeviceState& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);

    args.Error = State->UpdateDeviceState(
        db,
        args.DeviceId,
        args.State,
        args.StateTs,
        args.Reason,
        args.AffectedDisk);
}

void TDiskRegistryActor::CompleteUpdateDeviceState(
    const TActorContext& ctx,
    TTxDiskRegistry::TUpdateDeviceState& args)
{
    if (HasError(args.Error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "UpdateDeviceState error: %s",
            FormatError(args.Error).c_str());
    }

    NotifyDisks(ctx);
    NotifyUsers(ctx);
    PublishDiskStates(ctx);
    SecureErase(ctx);
    StartMigration(ctx);

    auto response = std::make_unique<TEvDiskRegistry::TEvChangeDeviceStateResponse>(
        std::move(args.Error));
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
