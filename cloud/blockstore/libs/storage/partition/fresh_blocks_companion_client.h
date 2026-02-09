#pragma once

#include <cloud/blockstore/libs/storage/partition_common/fresh_blocks_companion.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

class TPartitionActor;

struct TFreshBlocksCompanionClient: public IFreshBlocksCompanionClient
{
    TPartitionActor& PartitionActor;

    explicit TFreshBlocksCompanionClient(TPartitionActor& partitionActor)
        : PartitionActor(partitionActor)
    {}

    void FreshBlobsLoaded(const NActors::TActorContext& ctx) override;

    void Poison(const NActors::TActorContext& ctx) override;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
