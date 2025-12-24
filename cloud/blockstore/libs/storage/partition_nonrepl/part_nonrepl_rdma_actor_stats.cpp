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

TPartNonreplCountersData TNonreplicatedPartitionRdmaActor::ExtractPartCounters()
{
    PartCounters->Simple.IORequestsInFlight.Set(
        RequestsInProgress.GetRequestCount());
    PartCounters->Simple.BytesCount.Set(
        PartConfig->GetBlockCount() * PartConfig->GetBlockSize());

    auto partCounters = CreatePartitionDiskCounters(
        EPublishingPolicy::DiskRegistryBased,
        DiagnosticsConfig->GetHistogramCounterOptions());

    return {
        .DiskCounters = std::exchange(PartCounters, std::move(partCounters)),
        .NetworkBytes = std::exchange(NetworkBytes, {}),
        .CpuUsage = std::exchange(CpuUsage, {}),
    };
}

void TNonreplicatedPartitionRdmaActor::SendStats(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
            MakeIntrusive<TCallContext>(),
            PartConfig->GetName(),
            ExtractPartCounters());

    NCloud::Send(ctx, StatActorId, std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionRdmaActor::
    HandleGetDiskRegistryBasedPartCounters(
        const TEvNonreplPartitionPrivate::
            TEvGetDiskRegistryBasedPartCountersRequest::TPtr& ev,
        const TActorContext& ctx)
{
    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<TEvNonreplPartitionPrivate::
                             TEvGetDiskRegistryBasedPartCountersResponse>(
            SelfId(),
            PartConfig->GetName(),
            ExtractPartCounters()));
}

}   // namespace NCloud::NBlockStore::NStorage
