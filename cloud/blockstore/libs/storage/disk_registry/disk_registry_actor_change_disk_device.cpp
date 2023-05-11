#include "disk_registry_actor.h"

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

    LOG_INFO_S(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[" << TabletID() << "] Received ChangeDiskDevice"
        << " DiskId: " << record.GetDiskId().c_str()
        << " SourceDeviceId: " << record.GetSourceDeviceId().c_str()
        << " TargetDeviceId: " << record.GetTargetDeviceId().c_str());

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
        LOG_ERROR_S(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "ChangeDiskDevice failed. Error: "
            << FormatError(args.Error));
    } else {
        LOG_INFO_S(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "ChangeDiskDevice succeeded.");
    }

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<TEvDiskRegistry::TEvChangeDiskDeviceResponse>(
            std::move(args.Error)));

     NotifyDisks(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
