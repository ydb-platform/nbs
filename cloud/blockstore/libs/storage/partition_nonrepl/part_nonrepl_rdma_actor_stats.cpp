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

    TPartNonreplCountersData counters(
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
    auto&& [networkBytes, cpuUsage, diskCounters] = ExtractPartCounters();

    auto request =
        std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
            MakeIntrusive<TCallContext>(),
            std::move(diskCounters),
            PartConfig->GetName(),
            networkBytes,
            cpuUsage);

    NCloud::Send(ctx, StatActorId, std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionRdmaActor::
    HandleGetDiskRegistryBasedPartCounters(
        const TEvNonreplPartitionPrivate::
            TEvGetDiskRegistryBasedPartCountersRequest::TPtr& ev,
        const TActorContext& ctx)
{
    auto&& [networkBytes, cpuUsage, diskCounters] = ExtractPartCounters();

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<TEvNonreplPartitionPrivate::
                             TEvGetDiskRegistryBasedPartCountersResponse>(
            MakeError(S_OK),
            std::move(diskCounters),
            networkBytes,
            cpuUsage,
            SelfId(),
            PartConfig->GetName(),
            ev->Get()->VolumeStatisticSeqNo));
}

}   // namespace NCloud::NBlockStore::NStorage
