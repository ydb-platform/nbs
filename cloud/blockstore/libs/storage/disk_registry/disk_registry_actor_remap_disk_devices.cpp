#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/common/safe_debug_print.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleRemapDiskDevices(
    const TEvDiskRegistry::TEvRemapDiskDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(CreateDiskFromDevices);

    const auto& record = ev->Get()->Record;

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received RemapDiskDevices request: %s %s",
        LogTitle.GetWithTime().c_str(),
        SafeDebugPrint(record).c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    ExecuteTx<TRemapDiskDevices>(
        ctx,
        CreateRequestInfo<TEvDiskRegistry::TRemapDiskDevicesMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext
        ),
        record.GetDiskId(),
        TVector<TDeviceId> (
            record.GetDevices().begin(),
            record.GetDevices().end())
        );
}

bool TDiskRegistryActor::PrepareRemapDiskDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRemapDiskDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteRemapDiskDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRemapDiskDevices& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);

    TDiskRegistryState::TAllocateDiskResult result;
    args.Error = State->RemapDiskDevices(
        db,
        args.DiskId,
        args.NewDevices);
}

void TDiskRegistryActor::CompleteRemapDiskDevices(
    const TActorContext& ctx,
    TTxDiskRegistry::TRemapDiskDevices& args)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s RemapDiskDevices result: %s",
        LogTitle.GetWithTime().c_str(),
        FormatError(args.Error).c_str());

    auto response =
        std::make_unique<TEvDiskRegistry::TEvCreateDiskFromDevicesResponse>(
            std::move(args.Error));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
