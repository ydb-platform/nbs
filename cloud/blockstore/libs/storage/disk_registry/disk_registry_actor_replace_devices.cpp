#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::ReplaceBrokenDevicesAfterRestart(
    const TActorContext& ctx)
{
    ExecuteTx<TReplaceBrokenDevicesAfterRestart>(ctx);
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareReplaceBrokenDevicesAfterRestart(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TReplaceBrokenDevicesAfterRestart& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteReplaceBrokenDevicesAfterRestart(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TReplaceBrokenDevicesAfterRestart& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TDiskRegistryDatabase db(tx.DB);
    State->ReplaceBrokenDevicesAfterRestart(ctx.Now(), db);
}

void TDiskRegistryActor::CompleteReplaceBrokenDevicesAfterRestart(
    const TActorContext& ctx,
    TTxDiskRegistry::TReplaceBrokenDevicesAfterRestart& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);
}

}   // namespace NCloud::NBlockStore::NStorage
