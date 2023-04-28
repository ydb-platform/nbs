#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleMarkReplacementDevice(
    const TEvDiskRegistry::TEvMarkReplacementDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(MarkReplacementDevice);

    const auto& record = ev->Get()->Record;

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received MarkReplacementDevice %s, %s, %d",
        TabletID(),
        record.GetDiskId().c_str(),
        record.GetDeviceId().c_str(),
        record.GetIsReplacement());

    ExecuteTx<TMarkReplacementDevice>(
        ctx,
        CreateRequestInfo<TEvDiskRegistry::TMarkReplacementDeviceMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext,
            std::move(ev->TraceId)
        ),
        record.GetDiskId(),
        record.GetDeviceId(),
        record.GetIsReplacement());
}

bool TDiskRegistryActor::PrepareMarkReplacementDevice(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TMarkReplacementDevice& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteMarkReplacementDevice(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TMarkReplacementDevice& args)
{
    TDiskRegistryDatabase db(tx.DB);

    args.Error = State->MarkReplacementDevice(
        db,
        args.DiskId,
        args.DeviceId,
        args.IsReplacement);

    if (HasError(args.Error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "MarkReplacementDevice %s execution failed: %s",
            args.ToString().c_str(),
            FormatError(args.Error).c_str());
        return;
    }

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "MarkReplacementDevice %s execution succeeded",
        args.ToString().c_str());
}

void TDiskRegistryActor::CompleteMarkReplacementDevice(
    const TActorContext& ctx,
    TTxDiskRegistry::TMarkReplacementDevice& args)
{
    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "MarkReplacementDevice %s complete",
        args.ToString().c_str());

    NotifyDisks(ctx);

    auto response =
        std::make_unique<TEvDiskRegistry::TEvMarkReplacementDeviceResponse>(
            std::move(args.Error));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage

