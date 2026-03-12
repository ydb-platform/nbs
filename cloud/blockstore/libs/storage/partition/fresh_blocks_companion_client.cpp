#include "fresh_blocks_companion_client.h"

#include "part_actor.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

void TFreshBlocksCompanionClient::FreshBlobsLoaded(
    const NActors::TActorContext& ctx)
{
    PartitionActor.FreshBlobsLoaded(ctx);
}

void TFreshBlocksCompanionClient::Poison(const NActors::TActorContext& ctx)
{
    PartitionActor.Suicide(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
