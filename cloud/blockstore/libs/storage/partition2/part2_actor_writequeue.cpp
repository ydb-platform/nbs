#include "part2_actor.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::EnqueueProcessWriteQueueIfNeeded(const TActorContext& ctx)
{
    if (State->GetWriteBuffer().GetWeight()) {
        return;
    }

    NCloud::Send<TEvPartitionPrivate::TEvProcessWriteQueue>(
        ctx,
        SelfId());
}

void TPartitionActor::HandleProcessWriteQueue(
    const TEvPartitionPrivate::TEvProcessWriteQueue::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    const auto totalWeight = State->AccessWriteBuffer().GetWeight();
    if (!totalWeight) {
        return;
    }

    auto guard = State->AccessWriteBuffer().Flush();
    auto& requests = guard.Get();

    auto writeBlobThreshold =
        GetWriteBlobThreshold(*Config, PartitionConfig.GetStorageMediaKind());
    auto maxBlocksInBlob = writeBlobThreshold / PartitionConfig.GetBlockSize();

    STORAGE_VERIFY(
        !requests.empty(),
        TWellKnownEntityTypes::TABLET,
        TabletID());

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

void TPartitionActor::ClearWriteQueue(const TActorContext& ctx)
{
    if (State) {
        auto guard = State->AccessWriteBuffer().Flush();
        auto& requests = guard.Get();
        for (auto& request: requests) {
            request.Data.RequestInfo->CancelRequest(ctx);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
