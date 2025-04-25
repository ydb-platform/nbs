#include "part_actor.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

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

    // TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    db.InitSchema();
}

void TPartitionActor::CompleteInitSchema(
    const TActorContext& ctx,
    TTxPartition::TInitSchema& args)
{
    Y_UNUSED(args);

    LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] Schema initialized",
        TabletID(),
        PartitionConfig.GetDiskId().c_str());

    ExecuteTx<TLoadState>(ctx, args.BlocksCount);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
