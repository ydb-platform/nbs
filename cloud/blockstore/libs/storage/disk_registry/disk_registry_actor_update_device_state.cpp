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
        msg->CallContext);

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received ChangeDeviceState request: UUID=%s, State=%u",
        TabletID(),
        msg->Record.GetDeviceUUID().c_str(),
        static_cast<ui32>(msg->Record.GetDeviceState()));

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
            "UpdateDeviceState error: %s, affected disk: %s",
            FormatError(args.Error).c_str(),
            args.AffectedDisk.c_str());
    }

    if (args.State == NProto::DEVICE_STATE_ONLINE && !HasError(args.Error)) {
        SendEnableDevice(ctx, args.DeviceId);
    }

    ReallocateDisks(ctx);
    NotifyUsers(ctx);
    PublishDiskStates(ctx);
    SecureErase(ctx);
    StartMigration(ctx);

    TDiskId failedAllocationDisk;
    if (args.State == NProto::DEVICE_STATE_ERROR) {
        failedAllocationDisk = State->CheckPendingAllocation(args.DeviceId);
    }

    if (failedAllocationDisk) {
        ReplyToPendingAllocations(
            ctx,
            failedAllocationDisk,
            MakeError(
                E_REJECTED,
                "Allocation failed due to disk agent problems"));
    }

    auto response = std::make_unique<TEvDiskRegistry::TEvChangeDeviceStateResponse>(
        std::move(args.Error));
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
