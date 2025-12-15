#include "disk_registry_actor.h"


namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleCompareDiskRegistryState(
    const TEvDiskRegistry::TEvCompareDiskRegistryStateRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(CompareDiskRegistryState);

    const auto& record = ev->Get()->Record;

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received CompareDiskRegistryState request: %s %s",
        LogTitle.GetWithTime().c_str(),
        record.ShortDebugString().c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    ExecuteTx<TCompareDiskRegistryState>(
        ctx,
        CreateRequestInfo<TEvDiskRegistry::TCompareDiskRegistryStateMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext
        ));
}

bool TDiskRegistryActor::PrepareCompareDiskRegistryState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCompareDiskRegistryState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TDiskRegistryDatabase db(tx.DB);
    if(!LoadState(db, args.StateArgs)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s Failed to load state",
            LogTitle.GetWithTime().c_str());
        args.Result.MutableError()->set_code(E_FAIL);
        return false;
    }

    return true;
}

void TDiskRegistryActor::ExecuteCompareDiskRegistryState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCompareDiskRegistryState& args)
{
    Y_UNUSED(tx);

    auto dbState = std::make_unique<TDiskRegistryState>(
        Logging,
        Config,
        ComponentGroup,
        std::move(args.StateArgs.Config),
        std::move(args.StateArgs.Agents),
        std::move(args.StateArgs.Disks),
        std::move(args.StateArgs.PlacementGroups),
        std::move(args.StateArgs.BrokenDisks),
        std::move(args.StateArgs.DisksToReallocate),
        std::move(args.StateArgs.DiskStateChanges),
        args.StateArgs.LastDiskStateSeqNo,
        std::move(args.StateArgs.DirtyDevices),
        std::move(args.StateArgs.DisksToCleanup),
        std::move(args.StateArgs.ErrorNotifications),
        std::move(args.StateArgs.UserNotifications),
        std::move(args.StateArgs.OutdatedVolumeConfigs),
        std::move(args.StateArgs.SuspendedDevices),
        std::move(args.StateArgs.AutomaticallyReplacedDevices),
        std::move(args.StateArgs.DiskRegistryAgentListParams));

    auto differences = State->GetDifferingFields(*dbState);
    args.Result.MutableDiffers()->Add(differences.begin(), differences.end());
    args.Result.MutableError()->set_code(S_OK);
    
    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s CompareDiskRegistryState execution succeeded",
        LogTitle.GetWithTime().c_str());
}

void TDiskRegistryActor::CompleteCompareDiskRegistryState(
    const TActorContext& ctx,
    TTxDiskRegistry::TCompareDiskRegistryState& args)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s CompareDiskRegistryState result",
        LogTitle.GetWithTime().c_str());

    auto response =
        std::make_unique<TEvDiskRegistry::TEvCompareDiskRegistryStateResponse>(
            std::move(args.Result));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage

