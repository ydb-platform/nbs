#include "disk_registry_actor.h"


namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareCleanupOrphanDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCleanupOrphanDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteCleanupOrphanDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCleanupOrphanDevices& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    if (!args.OrphanDevices) {
        State->RemoveOrphanDevices(db, args.OrphanDevices);
    }
}

void TDiskRegistryActor::CompleteCleanupOrphanDevices(
    const TActorContext& ctx,
    TTxDiskRegistry::TCleanupOrphanDevices& args)
{
    Y_UNUSED(args);
    Y_UNUSED(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
