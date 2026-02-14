#pragma once

#include <cloud/blockstore/libs/storage/partition_common/fresh_blocks_companion.h>

namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter {

class TFreshBlocksWriterActor;

struct TFreshBlocksCompanionClient: public IFreshBlocksCompanionClient
{
    TFreshBlocksWriterActor& FreshBlocksWriterActor;

    explicit TFreshBlocksCompanionClient(
        TFreshBlocksWriterActor& freshBlocksWriterActor);

    void FreshBlobsLoaded(const NActors::TActorContext& ctx) override;

    void Poison(const NActors::TActorContext& ctx) override;
};

}   // namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter
