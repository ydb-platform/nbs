#include "part_nonrepl_rdma_actor.h"

#include <cloud/blockstore/libs/storage/api/stats_service.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionRdmaActor::UpdateStats(
    const NProto::TPartitionStats& update)
{
    auto blockSize = PartConfig->GetBlockSize();
    PartCounters->Cumulative.BytesWritten.Increment(
        update.GetUserWriteCounters().GetBlocksCount() * blockSize);

    PartCounters->Cumulative.BytesRead.Increment(
        update.GetUserReadCounters().GetBlocksCount() * blockSize);
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionRdmaActor::SendStats(const TActorContext& ctx)
{
    PartCounters->Simple.IORequestsInFlight.Set(
        RequestsInProgress.GetRequestCount());
    PartCounters->Simple.BytesCount.Set(
        PartConfig->GetBlockCount() * PartConfig->GetBlockSize()
    );

    auto request = std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
        MakeIntrusive<TCallContext>(),
        std::move(PartCounters));

    PartCounters = CreatePartitionDiskCounters(EPublishingPolicy::NonRepl);

    NCloud::Send(ctx, StatActorId, std::move(request));
}

}   // namespace NCloud::NBlockStore::NStorage
