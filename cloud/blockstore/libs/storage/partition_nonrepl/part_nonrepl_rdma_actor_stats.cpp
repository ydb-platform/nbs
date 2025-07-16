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

TDiskRegistryBasedPartCounters TNonreplicatedPartitionRdmaActor::GetStats()
{
    PartCounters->Simple.IORequestsInFlight.Set(
        RequestsInProgress.GetRequestCount());
    PartCounters->Simple.BytesCount.Set(
        PartConfig->GetBlockCount() * PartConfig->GetBlockSize());

    TDiskRegistryBasedPartCounters counters(
        NetworkBytes,
        CpuUsage,
        std::move(PartCounters));

    NetworkBytes = 0;
    CpuUsage = {};

    PartCounters = CreatePartitionDiskCounters(
        EPublishingPolicy::DiskRegistryBased,
        DiagnosticsConfig->GetHistogramCounterOptions());

    return counters;
}

void TNonreplicatedPartitionRdmaActor::SendStats(const TActorContext& ctx)
{
    auto&& [networkBytes, cpuUsage, partCounters] = GetStats();

    auto request =
        std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
            MakeIntrusive<TCallContext>(),
            std::move(partCounters),
            PartConfig->GetName(),
            networkBytes,
            cpuUsage);

    NCloud::Send(ctx, StatActorId, std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionRdmaActor::
    HandleGetDiskRegistryBasedPartCountersRequest(
        const TEvNonreplPartitionPrivate::
            TEvGetDiskRegistryBasedPartCountersRequest::TPtr& ev,
        const TActorContext& ctx)
{
    auto&& [networkBytes, cpuUsage, partCounters] = GetStats();

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<TEvNonreplPartitionPrivate::
                             TEvGetDiskRegistryBasedPartCountersResponse>(
            MakeError(S_OK),
            std::move(partCounters),
            networkBytes,
            cpuUsage,
            SelfId(),
            PartConfig->GetName()));
}

}   // namespace NCloud::NBlockStore::NStorage
