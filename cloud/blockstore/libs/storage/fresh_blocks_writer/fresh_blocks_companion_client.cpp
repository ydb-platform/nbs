#include "fresh_blocks_companion_client.h"

#include "fresh_blocks_writer_actor.h"

namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter {

TFreshBlocksCompanionClient::TFreshBlocksCompanionClient(
    TFreshBlocksWriterActor& freshBlocksWriterActor)
    : FreshBlocksWriterActor(freshBlocksWriterActor)
{}

void TFreshBlocksCompanionClient::FreshBlobsLoaded(
    const NActors::TActorContext& ctx)
{
    FreshBlocksWriterActor.FreshBlobsLoaded(ctx);
}

void TFreshBlocksCompanionClient::Poison(const NActors::TActorContext& ctx)
{
    FreshBlocksWriterActor.Suicide(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter
