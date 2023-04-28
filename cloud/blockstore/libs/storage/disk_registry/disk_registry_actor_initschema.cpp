#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareInitSchema(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TInitSchema& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteInitSchema(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TInitSchema& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TDiskRegistryDatabase db(tx.DB);
    db.InitSchema();
}

void TDiskRegistryActor::CompleteInitSchema(
    const TActorContext& ctx,
    TTxDiskRegistry::TInitSchema& args)
{
    Y_UNUSED(args);

    ExecuteTx<TLoadState>(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
