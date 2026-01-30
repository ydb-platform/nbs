#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/common/safe_debug_print.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleChangeDiskDevice(
    const TEvDiskRegistry::TEvChangeDiskDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(ChangeDiskDevice);

    const auto& record = ev->Get()->Record;

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received ChangeDiskDevice request: %s %s",
        LogTitle.GetWithTime().c_str(),
        SafeDebugPrint(record).c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    ExecuteTx<TChangeDiskDevice>(
        ctx,
        CreateRequestInfo<TEvDiskRegistry::TChangeDiskDeviceMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext
        ),
        record.GetDiskId(),
        record.GetSourceDeviceId(),
        record.GetTargetDeviceId());
}

bool TDiskRegistryActor::PrepareChangeDiskDevice(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TChangeDiskDevice& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteChangeDiskDevice(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TChangeDiskDevice& args)
{
    TDiskRegistryDatabase db(tx.DB);

    TDiskRegistryState::TAllocateDiskResult result;
    args.Error = State->ChangeDiskDevice(
        ctx.Now(),
        db,
        args.DiskId,
        args.SourceDeviceId,
        args.TargetDeviceId);
}

void TDiskRegistryActor::CompleteChangeDiskDevice(
    const TActorContext& ctx,
    TTxDiskRegistry::TChangeDiskDevice& args)
{
    if (HasError(args.Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s ChangeDiskDevice failed. %s %s -> %s Error: %s",
            LogTitle.GetWithTime().c_str(),
            args.DiskId.Quote().c_str(),
            args.SourceDeviceId.Quote().c_str(),
            args.TargetDeviceId.Quote().c_str(),
            FormatError(args.Error).c_str());
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s ChangeDiskDevice succeeded. %s %s -> %s",
            LogTitle.GetWithTime().c_str(),
            args.DiskId.Quote().c_str(),
            args.SourceDeviceId.Quote().c_str(),
            args.TargetDeviceId.Quote().c_str());
    }

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<TEvDiskRegistry::TEvChangeDiskDeviceResponse>(
            std::move(args.Error)));

     ReallocateDisks(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
