#include "part2_actor.h"

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareInitSchema(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TInitSchema& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteInitSchema(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TInitSchema& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TPartitionDatabase db(tx.DB);
    db.InitSchema();
}

void TPartitionActor::CompleteInitSchema(
    const TActorContext& ctx,
    TTxPartition::TInitSchema& args)
{
    Y_UNUSED(args);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%lu] Schema initialized",
        TabletID());

    ExecuteTx<TLoadState>(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
