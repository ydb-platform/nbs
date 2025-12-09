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

    LOG_DEBUG(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "Received CompareDiskRegistryState request");
    
    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ExecuteTx<TCompareDiskRegistryState>(
        ctx,
        std::move(requestInfo));
    auto response =
        std::make_unique<TEvDiskRegistry::TEvCompareDiskRegistryStateResponse>();
    // auto diffrences = State->GetDifferingFields(const TDiskRegistryState &rhs);
    // response->Record.MutableDiffers()->Add(diffrences.begin(), diffrences.end());
    
    NCloud::Reply(ctx, *ev, std::move(response));
}

bool TDiskRegistryActor::PrepareCompareDiskRegistryState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCompareDiskRegistryState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteCompareDiskRegistryState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCompareDiskRegistryState& args)
{
    Y_UNUSED(ctx);
    TDiskRegistryDatabase db(tx.DB);
    TDiskRegistryStateSnapshot stateArgs;
    LoadState(db, stateArgs);
    auto DBState = std::make_unique<TDiskRegistryState>(
        Logging,
        Config,
        ComponentGroup,
        std::move(stateArgs.Config),
        std::move(stateArgs.Agents),
        std::move(stateArgs.Disks),
        std::move(stateArgs.PlacementGroups),
        std::move(stateArgs.BrokenDisks),
        std::move(stateArgs.DisksToReallocate),
        std::move(stateArgs.DiskStateChanges),
        stateArgs.LastDiskStateSeqNo,
        std::move(stateArgs.DirtyDevices),
        std::move(stateArgs.DisksToCleanup),
        std::move(stateArgs.ErrorNotifications),
        std::move(stateArgs.UserNotifications),
        std::move(stateArgs.OutdatedVolumeConfigs),
        std::move(stateArgs.SuspendedDevices),
        std::move(stateArgs.AutomaticallyReplacedDevices),
        std::move(stateArgs.DiskRegistryAgentListParams));
    auto differences = State->GetDifferingFields(*DBState);
    args.Result.MutableDiffers()->Add(differences.begin(), differences.end());
    // args.Diffs.Mu = State->GetDifferingFields(*DBState);
}

void TDiskRegistryActor::CompleteCompareDiskRegistryState(
    const TActorContext& ctx,
    TTxDiskRegistry::TCompareDiskRegistryState& args)
{
    // LOG_LOG(
    //     ctx,
    //     HasError(args.Error) ? NLog::PRI_ERROR : NLog::PRI_INFO,
    //     TBlockStoreComponents::DISK_REGISTRY,
    //     "CompareDiskRegistryState result: DiskId=%s Error=%s",
    //     args.DiskId.c_str(),
    //     FormatError(args.Error).c_str());

    auto response =
        std::make_unique<TEvDiskRegistry::TEvCompareDiskRegistryStateResponse>(
            std::move(args.Result));
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}