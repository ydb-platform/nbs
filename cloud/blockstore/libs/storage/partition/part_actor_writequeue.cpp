#include "part_actor.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

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

    const auto totalWeight = State->GetWriteBuffer().GetWeight();
    if (!totalWeight) {
        return;
    }

    auto guard = State->GetWriteBuffer().Flush();
    auto& requests = guard.Get();

    // building mixed blob requests
    const auto mediaKind = PartitionConfig.GetStorageMediaKind();
    const auto writeMixedBlobThreshold =
        GetWriteMixedBlobThreshold(*Config, mediaKind);
    auto writeBlobThreshold = GetWriteBlobThreshold(*Config, mediaKind);
    if (writeMixedBlobThreshold && writeMixedBlobThreshold < writeBlobThreshold)
    {
        writeBlobThreshold = writeMixedBlobThreshold;
    }

    auto g = GroupRequests(
        requests,
        totalWeight,
        writeBlobThreshold / State->GetBlockSize(),
        State->GetMaxBlocksInBlob(),
        Config->GetMaxBlobRangeSize() / State->GetBlockSize()
    );

    if (!WriteMixedBlocks(ctx, g.Groups)) {
        for (auto& request: requests) {
            request.Data.RequestInfo->CancelRequest(ctx);
        }
        RebootPartitionOnCommitIdOverflow(ctx, "Write queue");
        return;
    }

    WriteFreshBlocks(ctx, MakeArrayRef(g.FirstUngrouped, requests.end()));
}

void TPartitionActor::ClearWriteQueue(const TActorContext& ctx)
{
    if (State) {
        auto guard = State->GetWriteBuffer().Flush();
        auto& requests = guard.Get();
        for (auto& request: requests) {
            request.Data.RequestInfo->CancelRequest(ctx);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
