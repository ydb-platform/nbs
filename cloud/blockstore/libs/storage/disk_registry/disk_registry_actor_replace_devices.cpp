#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::ProcessRecentlyReplaceDevices(const TActorContext& ctx)
{
    ExecuteTx<TProcessRecentlyReplaceDevices>(ctx);
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareProcessRecentlyReplaceDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TProcessRecentlyReplaceDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteProcessRecentlyReplaceDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TProcessRecentlyReplaceDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TDiskRegistryDatabase db(tx.DB);
    State->ReplaceBrokenDevicesAfterRestart(ctx.Now(), db);
}

void TDiskRegistryActor::CompleteProcessRecentlyReplaceDevices(
    const TActorContext& ctx,
    TTxDiskRegistry::TProcessRecentlyReplaceDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);
}

}   // namespace NCloud::NBlockStore::NStorage
