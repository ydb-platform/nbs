#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareCleanupDevicesWithoutAgent(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCleanupDevicesWithoutAgent& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteCleanupDevicesWithoutAgent(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCleanupDevicesWithoutAgent& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TDiskRegistryDatabase db(tx.DB);
    State->CleanupDevices(db);
}

void TDiskRegistryActor::CompleteCleanupDevicesWithoutAgent(
    const TActorContext& ctx,
    TTxDiskRegistry::TCleanupDevicesWithoutAgent& args)
{
    Y_UNUSED(args);
    Y_UNUSED(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
