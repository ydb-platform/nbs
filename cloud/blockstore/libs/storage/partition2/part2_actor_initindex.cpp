#include "part2_actor.h"

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleInitIndex(
    const TEvPartitionPrivate::TEvInitIndexRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto tx = CreateTx<TInitIndex>(ev->Sender, std::move(msg->BlockRanges));
    ExecuteTx(ctx, std::move(tx));
}

bool TPartitionActor::PrepareInitIndex(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TInitIndex& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);
    bool ready = true;

    for (const auto& blockRange: args.BlockRanges) {
        ready &= State->InitIndex(db, blockRange);
    }

    return ready;
}

void TPartitionActor::ExecuteInitIndex(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TInitIndex& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteInitIndex(
    const TActorContext& ctx,
    TTxPartition::TInitIndex& args)
{
    NCloud::Send<TEvPartitionPrivate::TEvInitIndexResponse>(ctx, args.ActorId);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
