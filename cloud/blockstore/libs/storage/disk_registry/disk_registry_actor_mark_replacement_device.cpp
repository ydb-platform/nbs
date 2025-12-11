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

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received MarkReplacementDevice request: %s %s",
        LogTitle.GetWithTime().c_str(),
        record.ShortDebugString().c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    ExecuteTx<TMarkReplacementDevice>(
        ctx,
        CreateRequestInfo<TEvDiskRegistry::TMarkReplacementDeviceMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext),
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
        ctx.Now(),
        db,
        args.DiskId,
        args.DeviceId,
        args.IsReplacement);

    if (HasError(args.Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s MarkReplacementDevice %s execution failed: %s",
            LogTitle.GetWithTime().c_str(),
            args.ToString().c_str(),
            FormatError(args.Error).c_str());
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s MarkReplacementDevice %s execution succeeded",
        LogTitle.GetWithTime().c_str(),
        args.ToString().c_str());
}

void TDiskRegistryActor::CompleteMarkReplacementDevice(
    const TActorContext& ctx,
    TTxDiskRegistry::TMarkReplacementDevice& args)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s MarkReplacementDevice %s complete",
        LogTitle.GetWithTime().c_str(),
        args.ToString().c_str());

    ReallocateDisks(ctx);

    auto response =
        std::make_unique<TEvDiskRegistry::TEvMarkReplacementDeviceResponse>(
            std::move(args.Error));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
