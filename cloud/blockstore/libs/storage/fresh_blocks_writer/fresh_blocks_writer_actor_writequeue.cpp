#include "fresh_blocks_writer_actor.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter {

using namespace NActors;

using namespace NPartition;

////////////////////////////////////////////////////////////////////////////////

void TFreshBlocksWriterActor::EnqueueProcessWriteQueueIfNeeded(
    const TActorContext& ctx)
{
    if (FlushState->GetWriteBuffer().GetWeight()) {
        return;
    }

    NCloud::Send<TEvPartitionPrivate::TEvProcessWriteQueue>(ctx, SelfId());
}

void TFreshBlocksWriterActor::HandleProcessWriteQueue(
    const TEvPartitionPrivate::TEvProcessWriteQueue::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    const auto totalWeight = FlushState->AccessWriteBuffer().GetWeight();
    if (!totalWeight) {
        return;
    }

    auto guard = FlushState->AccessWriteBuffer().Flush();
    auto& requests = guard.Get();

    auto writeBlobThreshold = GetWriteBlobThreshold(*Config, PartitionConfig.GetStorageMediaKind());
    auto maxBlocksInBlob = writeBlobThreshold / PartitionConfig.GetBlockSize();

    STORAGE_VERIFY(
        !requests.empty(),
        TWellKnownEntityTypes::TABLET,
        PartitionTabletID);

    auto* batchStartIt = requests.begin();
    ui64 currentBatchWeight = requests.begin()->Weight;

    for (auto* it = requests.begin() + 1; it != requests.end(); ++it) {
        if (currentBatchWeight + it->Weight >= maxBlocksInBlob) {
            WriteFreshBlocks(ctx, MakeArrayRef(batchStartIt, it));

            batchStartIt = it;
            currentBatchWeight = it->Weight;
            continue;
        }

        currentBatchWeight += it->Weight;
    }

    WriteFreshBlocks(ctx, MakeArrayRef(batchStartIt, requests.end()));
}

void TFreshBlocksWriterActor::ClearWriteQueue(const TActorContext& ctx)
{
    if (FlushState) {
        auto guard = FlushState->AccessWriteBuffer().Flush();
        auto& requests = guard.Get();
        for (auto& request: requests) {
            request.Data.RequestInfo->CancelRequest(ctx);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter
