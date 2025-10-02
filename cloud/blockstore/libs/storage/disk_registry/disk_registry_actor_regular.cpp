#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::ProcessAutomaticallyReplacedDevices(
    const TActorContext& ctx)
{
    const auto delay =
        Config->GetAutomaticallyReplacedDevicesFreezePeriod();
    if (!delay
            || AutomaticallyReplacedDevicesDeletionInProgress
            || State->GetAutomaticallyReplacedDevices().empty())
    {
        return;
    }

    const auto until = ctx.Now() - delay;
    AutomaticallyReplacedDevicesDeletionInProgress = true;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "Processing AutomaticallyReplacedDevices, count: "
        << State->GetAutomaticallyReplacedDevices().size());

    ExecuteTx<TProcessAutomaticallyReplacedDevices>(
        ctx,
        nullptr,
        until);
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareProcessAutomaticallyReplacedDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TProcessAutomaticallyReplacedDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteProcessAutomaticallyReplacedDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TProcessAutomaticallyReplacedDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TDiskRegistryDatabase db(tx.DB);
    args.ProcessedCount =
        State->DeleteAutomaticallyReplacedDevices(db, args.Until);
}

void TDiskRegistryActor::CompleteProcessAutomaticallyReplacedDevices(
    const TActorContext& ctx,
    TTxDiskRegistry::TProcessAutomaticallyReplacedDevices& args)
{
    AutomaticallyReplacedDevicesDeletionInProgress = false;
    if (args.ProcessedCount) {
        SecureErase(ctx);
        ProcessPathsToAttachDetach(ctx);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
