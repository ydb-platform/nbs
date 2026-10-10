#include "fresh_blocks_companion_client.h"

#include "part2_actor.h"

namespace NCloud::NBlockStore::NStorage::NPartition2 {

void TFreshBlocksCompanionClient::FreshBlobsLoaded(
    const NActors::TActorContext& ctx)
{
    PartitionActor.FreshBlobsLoaded(ctx);
}

void TFreshBlocksCompanionClient::Poison(const NActors::TActorContext& ctx)
{
    PartitionActor.Suicide(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
