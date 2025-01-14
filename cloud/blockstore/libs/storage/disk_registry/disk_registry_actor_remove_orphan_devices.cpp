#include "disk_registry_actor.h"


namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareRemoveOrphanDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRemoveOrphanDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteRemoveOrphanDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRemoveOrphanDevices& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    if (!args.OrphanDevices) {
        State->RemoveOrphanDevices(db, args.OrphanDevices);
    }
}

void TDiskRegistryActor::CompleteRemoveOrphanDevices(
    const TActorContext& ctx,
    TTxDiskRegistry::TRemoveOrphanDevices& args)
{
    Y_UNUSED(args);
    Y_UNUSED(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
